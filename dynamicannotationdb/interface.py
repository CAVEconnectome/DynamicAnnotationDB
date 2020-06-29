from sqlalchemy import create_engine, inspect, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import ArgumentError, InvalidRequestError, OperationalError, IntegrityError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.pool import NullPool
from marshmallow import EXCLUDE
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.flatten import flatten_dict
from emannotationschemas import models as em_models
from dynamicannotationdb.key_utils import build_table_id
from dynamicannotationdb.models import Metadata as AnnoMetadata
from dynamicannotationdb.errors import TableNameNotFound, TableAlreadyExists
from typing import List
import logging
import datetime
import time


class DynamicAnnotationInterface:

    def __init__(self, aligned_volume: str, sql_uri: str):
        """ Annotation DB interface layer for creating and querying tables in SQL

        Parameters
        ----------
        sql_uri : str
            SQL URI to use to connect to a Database.
        create_metadata : bool, optional
            Creates additional columns on new tables for CRUD operations, by default True
        """
        sql_uri = self.create_or_select_database(aligned_volume, sql_uri)

        self.engine = create_engine(sql_uri,
                                    pool_recycle=3600,
                                    pool_size=20,
                                    max_overflow=50)

        self.base = em_models.Base
        self.mapped_base = None

        self.base.metadata.create_all(self.engine)

        self.base.metadata.reflect(bind=self.engine)

        self.session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

        self.insp = inspect(self.engine)

        self._cached_session = None
        self._cached_tables = {}

    def create_or_select_database(self, aligned_volume: str, sql_uri: str):
        """Create a new database with the name of the aligned volume. Checks if 
        database exists before creating.

        Parameters
        ----------
        aligned_volume : str
            name of aligned volume which the database name will inherent 
        sql_uri : str
            base path to the sql server

        Returns
        -------
        sql_url instance
        """
        sql_base_uri = sql_uri.rpartition("/")[0]

        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
        

        temp_engine = create_engine(sql_base_uri, poolclass=NullPool)

        with temp_engine.connect() as connection:
            connection.execute("commit")
            result = connection.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{sql_uri.database}'")
            if not result.fetchone():
                logging.info(f"Creating new database {sql_uri.database}")
                connection.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                           WHERE pid <> pg_backend_pid() AND datname = '{sql_uri.database}';")
                connection.execute(f"create database {sql_uri.database} template template_postgis")
        temp_engine.dispose()
        return sql_uri

    @property
    def cached_session(self)->Session:
        if self._cached_session is None:
            self._cached_session = self.session()
        return self._cached_session

    def commit_session(self):
        try:
            self.cached_session.commit()
        except Exception:
            self.cached_session.rollback()
            logging.exception(f"SQL Error")
        finally:
            self.cached_session.close()
        self._cached_session = None

    def create_annotation_table(self,
                                aligned_volume: str,
                                table_name: str,
                                schema_type:str,
                                metadata_dict: dict):
        """Create new annotation table unless already exists

        Parameters
        ----------
        aligned_volume : str
            name of aligned_volume to attach a new annotation table
        table_name : str
            name of table
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        metadata_dict : dict
             metadata to attach ::
             
        dict: {
            "description": "a string with a human readable explanation of \ 
                        what is in the table. Including who made it"
            "user_id": "user_id"
            "reference_table": "reference table name, if required by this schema"
            }
        """
        table_id = build_table_id(aligned_volume, table_name)

        if table_id in self.get_existing_tables():
            logging.warning(f"Table creation failed: {table_id} already exists")
            raise TableAlreadyExists

        model = em_models.make_annotation_model(table_id,
                                                schema_type,
                                                metadata_dict,
                                                with_crud_columns=True)

        self.base.metadata.create_all(bind=self.engine)

        creation_time = datetime.datetime.now()

        metadata_dict.update({
            'schema_type': schema_type,
            'table_name': table_id,
            'valid': True,
            'created': creation_time
        })
        logging.info(f"Metadata for table: {table_id} is {metadata_dict}")
        anno_metadata = AnnoMetadata(**metadata_dict)
        self.cached_session.add(anno_metadata)
        self.commit_session()
        logging.info(f"Table: {table_id} created using {model} model at {creation_time}")
        return {"Created Succesfully": True, "Table Name": table_id, "Description": metadata_dict['description']}

    def create_segmentation_table(self, aligned_volume: str,
                                        annotation_table_name: str,
                                        schema_type:str,
                                        pcg_table_name: str,
                                        version: int):
        """Create new segmentation table linked to an annotation table
         unless it already exists. 

        Parameters
        ----------
        aligned_volume : str
            name of aligned_volume to attach a new segmentation table
        annotation_table_name : str
            name of table annotation to link the segmentation table 
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        pcg_table_name : str
            name of pychunked graph segmentation to use
        version : int
            version of table

        Returns
        -------
        dict
            description of table that is created
        """
        table_id = build_table_id(aligned_volume, annotation_table_name)
        segmentation_table_name = f"{table_id}_{pcg_table_name}_v{version}"

        if table_id in self.get_existing_tables():

            if segmentation_table_name in self.get_existing_tables():
                logging.warning(f"Table creation failed: {segmentation_table_name} already exists")
                return {f"Table {table_id} exists "}
            model = em_models.make_segmentation_model(table_id,
                                                      schema_type,
                                                      pcg_table_name,
                                                      version)

            self.base.metadata.tables[model.__name__].create(bind=self.engine)
            self.commit_session()
            creation_time = datetime.datetime.now()

            logging.info(f"Table: {table_id} created using {model} model at {creation_time}")
            return {"Created Succesfully": True, "Table Name": model.__name__}
        else:
            raise TableNameNotFound

    def get_table_metadata(self, aligned_volume: str, table_name: str):
        table_id = build_table_id(aligned_volume, table_name)
        metadata = self.cached_session.query(AnnoMetadata).\
                        filter(AnnoMetadata.table_name==table_id).first()
        metadata.__dict__.pop('_sa_instance_state')
        return metadata.__dict__

    def get_table_schema(self, aligned_volume: str, table_name: str):
        table_metadata = self.get_table_metadata(aligned_volume, table_name)
        return table_metadata['schema_type']

    def get_table_sql_metadata(self, table_name):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def get_model_columns(self, table_id: str) -> list:
        """ Return list of column names and types of a given table

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"

        Returns
        -------
        list
            column names and types
        """
        try:
            db_columns = self.insp.get_columns(table_id)
        except TableNameNotFound as error:
            logging.error(f"Error: {error}. No table name exists with name {table_id}.")
        model_columns = []
        for column in db_columns:
            model_columns.append(tuple([column['name'],
                                        column['type']]))
        return model_columns

    def get_existing_tables(self):
        """ Collects table_ids keys of existing tables

        Returns
        -------
        list
            List of table_ids
        """
        metadata = self.cached_session.query(AnnoMetadata).all()
        return [m.table_name for m in metadata]

    def has_table(self, table_name: str) -> bool:
        """Check if a table exists

        Parameters
        ----------
        table_name : str
            name of the table

        Returns
        -------
        bool
            whether the table exists
        """
        return table_name in self.get_existing_tables()

    def cached_table(self, table_id: str) -> DeclarativeMeta:
        """ Returns cached table 'DeclarativeMeta' callable for querying.

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
        Returns
        -------
        DeclarativeMeta
            SQLAlchemy callable. See 'https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#sqlalchemy.ext.declarative.declarative_base.params.metaclass'
        """
        try:
            self._load_table(table_id)
            return self._cached_tables[table_id]
        except TableNameNotFound as error:
            logging.error(f"Cannot load table {error}")

    def get_table_row_count(self, table_id: str) -> int:
        model = self.cached_table(table_id)
        return self.cached_session.query(func.count(model.id)).scalar()

    def get_annotation_table_size(self, aligned_volume: str, table_name: str) -> int:
        """Get the number of annotations in a table

        Parameters
        ----------
        aligned_volume: str
            name of aligned_volume to use as database
        table_name : str
            name of table contained within the aligned_volume database

        Returns
        -------
        int
            number of annotations
        """
        table_id = build_table_id(aligned_volume, table_name)
        Model = self.cached_table(table_id)
        return self.cached_session.query(Model).count()

    def _drop_table(self, aligned_volume: str, table_name: str):
        table_id = build_table_id(aligned_volume, table_name)
        table = self.base.metadata.tables.get(table_id)
        if table:
            logging.info(f'Deleting {table_id} table')
            self.base.metadata.drop_all(self.engine, [table], checkfirst=True)
            if self._is_cached(table):
                del self._cached_tables[table]
            return True
        return False

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)
        return self.mapped_base.classes[table_name]

    def _get_flattened_schema_data(self, schema_type: str, data: dict) -> dict:
        schema_type = get_schema(schema_type)
        schema = schema_type(context={'postgis': True})
        data = schema.load(data, unknown=EXCLUDE)

        check_is_nested = (any(isinstance(i, dict) for i in data.values()))
        if check_is_nested:
            data = flatten_dict(data)

        flat_annotation_schema, flat_segmentation_schema = em_models.split_annotation_schema(schema_type)

        return self._map_values_to_schema(data, flat_annotation_schema), self._map_values_to_schema(data, flat_segmentation_schema)

    def _map_values_to_schema(self, data, schema):
        return {key: data[key] for key, value in schema._declared_fields.items() if key in data}

    def _is_cached(self, table_name: str) -> bool:
        """ Check if table is loaded into cached instance dict of tables

        Parameters
        ----------
        table_name : str
            Name of table to check if loaded

        Returns
        -------
        bool
            True if table is loaded else False.
        """

        return table_name in self._cached_tables

    def _load_table(self, table_name: str):
        """ Load existing table into cached lookup dict instance

        Parameters
        ----------
        table_name : str
            Table name to be loaded from existing database tables

        Returns
        -------
        bool
            Returns True if table exists and is loaded into cached table dict.
        """
        if self._is_cached(table_name):
            return True

        try:
            self._cached_tables[table_name] = self._get_model_from_table_name(table_name)
            return True
        except KeyError as key_error:
            if table_name in self.get_existing_tables():
                logging.error(f"Could not load table: {key_error}")
            return False
