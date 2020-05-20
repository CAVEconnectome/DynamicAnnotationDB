from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from marshmallow import INCLUDE
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.base import flatten_dict
from emannotationschemas import models as em_models
from datetime import datetime
import logging


class AnnotationDB:

    def __init__(self, sql_uri: str, 
                       create_metadata: bool = True):
        """ Annotation DB interface layer for creating and querying tables in SQL

        Parameters
        ----------
        sql_uri : str
            SQL URI to use to connect to a Database.
        create_metadata : bool, optional
            Creates additional columns on new tables for CRUD operations, by default True
        """
        if not sql_uri:
            return

        try:
            self.engine = create_engine(sql_uri,
                                        pool_recycle=3600,
                                        pool_size=20,
                                        max_overflow=50)
        except ArgumentError as error:
            logging.error(f"Invalid SQLALCHEMY Argument: {error}")

        self.base = em_models.Base
        self.mapped_base = None

        if create_metadata:
            self.base.metadata.create_all(self.engine)
        
        self.base.metadata.reflect(bind=self.engine)

        self.session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

        self._cached_session = None
        self.insp = inspect(self.engine)

        self._cached_tables = {}

    @property
    def cached_session(self):
        if self._cached_session is None:
            self._cached_session = self.session()
        return self._cached_session

    def commit_session(self):
        try:
            self._cached_session.commit()
        except:
            self._cached_session.rollback()
            raise
        finally:
            self._cached_session.close()
        self._cached_session = None

    def create_table(self, 
                     dataset_name:str, 
                     table_name: str, 
                     schema_type:str, 
                     metadata_dict: dict=None,
                     description: str=None,
                     user_id: str=None,
                     valid: bool=True):
        """Create new annotation table. Checks if already exists and if cell segment
        table exists. If cell segment table is missing for the dataset name it will be
        created.

        Parameters
        ----------
        dataset_name : str
            Name of dataset to prefix to table, if no cell segment table exists with 
            this name a new table will be generated.

        table_name : str

        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        metadata_dict : dict, optional

        description : str, optional
            Additional text to describe table, by default None

        user_id : str, optional
            by default None

        valid : bool, optional
            Flags if table should be considered valid for analysis, by default True
        """
        new_table_name = f"{dataset_name}_{table_name}"
        cell_segement_table = f"{dataset_name}_cellsegment"
        if new_table_name in self.get_existing_tables():
            logging.info(f"Table creation failed: {new_table_name} already exists")
            raise f"Table {new_table_name} Already Exists!"
        
        if cell_segement_table in self.get_existing_tables():
            model = em_models.make_annotation_model(dataset_name,
                                                    schema_type,
                                                    table_name,
                                                    metadata_dict,
                                                    with_crud_columns=True)
        else:
            model = em_models.make_dataset_models(dataset_name,
                                                  [(schema_type, table_name)],
                                                  metadata_dict,
                                                  with_crud_columns=True)

        self.base.metadata.create_all(bind=self.engine)

        logging.info(f"Table: {new_table_name} created using {model} model")

    def get_table(self, table_name):
        return self.cached_table(table_name)

    def get_table_metadata(self, table_name):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def get_dataset_tables(self, dataset_name: str):
        tables = self.base.metadata.tables.keys()
        if tables:
            return list(filter(lambda x: x.startswith(f"{dataset_name}_"), tables))
        else:
            return None        

    def get_model_columns(self, table_id: str) -> list:
        """ Return list of column names and types of a given table

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"

        Returns
        -------
        list
            column names and types
        """
        try:
            db_columns = self.insp.get_columns(table_id)
        except ValueError as error:
            logging.error(f"Error: {error}. No table name exists with name {table_id}.")
        model_columns = []
        for column in db_columns:
            model_columns.append(tuple([column['name'],
                                        column['type']]))
        return model_columns


    def get_existing_tables(self):
        """ Collects table_ids of existing tables

        Annotation tables start with `anno`

        :return: list
        """

        tables = self.base.metadata.tables.keys()
        if tables:
            return list(tables)
        else:
            return None


    def cached_table(self, table_id: str) -> DeclarativeMeta:
        """ Returns cached table 'DeclarativeMeta' callable for querying

        Parameters
        ----------
        table_name : str
            Table name formatted in the form: "{dataset_name}_{table_name}"

        Returns
        -------
        DeclarativeMeta
            SQLAlchemy callable. See 'https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#sqlalchemy.ext.declarative.declarative_base.params.metaclass'
        """
        try:
            self._load_table(table_id)
            return self._cached_tables[table_id]
        except Exception as error:
            logging.error(f"Cannot load table {error}")

    def get_table_row_count(self, table_id: str) -> int:
        Model = self.cached_table(table_id)
        return self.cached_session.query(func.count(Model.id)).scalar()

    def get_annotation_table_info(self, table_id: str) -> int:
        Model = self.cached_table(table_id)
        return self.cached_session.query(Model).count()

    def get_annotation(self, table_id: str, schema_name: str, anno_id: int, structure_data=False) -> dict:
        """ Get signle annotation from database by

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"

        schema_name : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        anno_id : int
            annotation id 

        Returns
        -------
        dict
            annotation data
        """
        Model = self.cached_table(table_id)

        annotations = self.cached_session.query(Model).filter(Model.id==anno_id).first()

        if structure_data:
            data = annotations.__dict__
            if data['_sa_instance_state']:
                data.pop('_sa_instance_state')

            FlatSchema = get_flat_schema(schema_name)
            schema = FlatSchema()
            return schema.load(data, unknown=INCLUDE)

        return annotations

    def insert_annotation(self, table_id: str, schema_name: str, annotations: dict, structure_data=True):
        """Insert single annotation by type and schema. Optionally will structure data
        by using flat schema.

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"

        schema_name : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        annotations : dict
            Dictionary of signle annotation data. Must be flat dict unless structure_data flag is 
            set to True.

        structure_data : bool, optional
            Flag to flatten nested data dict, by default True
        """
        Model = self.cached_table(table_id)
        if structure_data:
            annotations = self._get_flattened_schema_data(schema_name, annotations)
        annotations['created'] = datetime.datetime.now()

        data = Model(**annotations)

        self.cached_session.add(data)
        self.commit_session()

    def update_annotation(self, table_id: str, schema_name: str, anno_id: int, new_annotations: dict, structure_data = True):
        """Updates an annotation by inserting a new row. The original annotation will refer to the new row
        with a superceded_id. Does not update inplace.

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"

        schema_name : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        anno_id : int
            Primary key ID to select annotation for updating.

        new_annotations : [type], optional

        structure_data : bool, optional
            Flag to flatten nested data dict, by default True
        """
        Model = self.cached_table(table_id)
        
        if structure_data:
            new_annotations = self._get_flattened_schema_data(schema_name, new_annotations)
        
        new_annotations['created'] = datetime.datetime.now()

        new_data = Model(**new_annotations)

        self.cached_session.add(new_data)
        self.cached_session.flush()

        old_annotation = self.cached_session.query(Model).filter(Model.id==anno_id).one()
        old_annotation.superceded_id = new_data.id

        self.commit_session()

    def delete_annotation(self, table_id: str, anno_id: int):
        """Flags an annotation for deletion via deletion timestamp. Will not remove data from
        the database.

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"
        anno_id : int
            Primary key ID to select annotation for deletion.
        """
        Model = self.cached_table(table_id)

        old_annotation = self.cached_session.query(Model).filter(Model.id==anno_id).one()
        old_annotation.deleted = datetime.datetime.now()

        self.commit_session()

    def drop_table(self, table_name: str):
        table = self.base.metadata.tables.get(table_name)
        if table is not None:
            logging.info(f'Deleting {table_name} table')
            self.base.metadata.drop_all(self.engine, [table], checkfirst=True)
            if self._is_cached(table):
                del self.cached_table[table]
            return True
        return False

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        self.mapped_base = automap_base(self.base.metadata)
        self.mapped_base.prepare(self.engine, reflect=True)
        return self.mapped_base.classes[table_name]

    def _get_flattened_schema_data(self, schema_name: str, data: dict) -> dict:
        schema_type = get_schema(schema_name)
        schema = schema_type(context={'postgis': True})
        data = schema.load(data)

        check_is_nested = (any(isinstance(i, dict) for i in data.values()))
        if check_is_nested:
            data = flatten_dict(data)

        return data

    def _drop_all(self):
        self.base.metadata.reflect(bind=self.engine)
        self.base.metadata.drop_all(self.engine)

    def _reset_table(self, dataset_name, table_name, n_retries=20, delay_s=5):
        metadata = self.get_table_metadata(dataset_name, table_name)

        if self.drop_table(dataset_name=dataset_name,
                           table_name=table_name):
            for _ in range(n_retries):
                time.sleep(delay_s)
                try:
                    if self.create_table(**metadata):
                        return True
                except:
                    time.sleep(delay_s)
        return False

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

