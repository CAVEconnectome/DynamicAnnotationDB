import datetime
import logging
from typing import List

from emannotationschemas import get_schema
from emannotationschemas.schemas.base import ReferenceAnnotation
from emannotationschemas import models as em_models
from emannotationschemas.flatten import flatten_dict
from marshmallow import EXCLUDE
import marshmallow
from sqlalchemy import create_engine, func, inspect, DDL, event
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from dynamicannotationdb.errors import (
    TableAlreadyExists,
    TableNameNotFound,
    SelfReferenceTableError,
)
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import AnnoMetadata, SegmentationMetadata


class DynamicAnnotationInterface:
    def __init__(self, sql_uri: str):
        """Annotation DB interface layer for creating and querying tables in SQL

        Parameters
        ----------
        sql_uri : str
            SQL URI to use to connect to a Database.
        create_metadata : bool, optional
            Creates additional columns on new tables for CRUD operations, by default True
        """
        self.sql_uri = sql_uri
        self.engine = create_engine(
            sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50
        )
        self.base = em_models.Base
        self.base.metadata.bind = self.engine

        table_objects = [
            AnnoMetadata.__tablename__,
            SegmentationMetadata.__tablename__,
        ]
        for table in table_objects:
            if not self.engine.dialect.has_table(self.engine, table):
                self.base.metadata.tables[table].create(bind=self.engine)

        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)

        self.session = scoped_session(
            sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        )

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

        temp_engine = create_engine(
            sql_base_uri,
            poolclass=NullPool,
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
        )

        with temp_engine.connect() as connection:
            connection.execute("commit")
            database_exists = connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{sql_uri.database}'"
            )
            if not database_exists.fetchone():
                logging.info(f"Creating new database: {sql_uri.database}")

                connection.execute(
                    f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                           WHERE pid <> pg_backend_pid() AND datname = '{sql_uri.database}';"
                )

                # check if template exists, create if missing
                template_exist = connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'template_postgis'"
                )

                if not template_exist.fetchone():

                    # create postgis template db
                    connection.execute("CREATE DATABASE template_postgis")

                    # create postgis extension
                    template_uri = make_url(f"{sql_base_uri}/template_postgis")
                    tempate_engine = create_engine(
                        template_uri,
                        poolclass=NullPool,
                        isolation_level="AUTOCOMMIT",
                        pool_pre_ping=True,
                    )
                    with tempate_engine.connect() as template_connection:
                        template_connection.execute(
                            "CREATE EXTENSION IF NOT EXISTS postgis"
                        )
                    tempate_engine.dispose()

                # finally create new annotation database
                connection.execute(
                    f"CREATE DATABASE {sql_uri.database} TEMPLATE template_postgis"
                )

        temp_engine.dispose()
        return sql_uri

    @property
    def cached_session(self) -> Session:
        if self._cached_session is None:
            self._cached_session = self.session()
        return self._cached_session

    def commit_session(self):
        try:
            self.cached_session.commit()
        except Exception as e:
            self.cached_session.rollback()
            logging.exception(f"SQL Error: {e}")
            raise (e)
        finally:
            self.cached_session.close()
        self._cached_session = None

    def check_table_is_unique(self, table_name):
        existing_tables = self._get_existing_table_names()
        if table_name in existing_tables:
            raise TableAlreadyExists(
                f"Table creation failed: {table_name} already exists"
            )
        return existing_tables

    def create_annotation_table(
        self,
        table_name: str,
        schema_type: str,
        description: str,
        user_id: str,
        voxel_resolution_x: float,
        voxel_resolution_y: float,
        voxel_resolution_z: float,
        table_metadata: dict = None,
        flat_segmentation_source: str = None,
        with_crud_columns: bool = True,
    ):
        r"""Create new annotation table unless already exists

        Parameters
        ----------
        aligned_volume : str
            name of aligned_volume to attach a new annotation table
        table_name : str
            name of table
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        description: str
            a string with a human readable explanation of
            what is in the table. Including who made it
            and any information that helps interpret the fields
            of the annotations.

        user_id: str
            user id for this table

        voxel_resolution_x: float
            voxel_resolution of this annotation table's point in x (typically nm)

        voxel_resolution_y: float
            voxel_resolution of this annotation table's point in y (typically nm)

        voxel_resolution_z: float
            voxel_resolution of this annotation table's point in z (typically nm)

        table_metadata: dict

        flat_segmentation_source: str
            a path to a segmentation source associated with this table
             i.e. 'precomputed:\\gs:\\my_synapse_seg\example1'

        with_crud_columns: bool
            add additional columns to track CRUD operations on rows
        """
        existing_tables = self.check_table_is_unique(table_name)
        if table_metadata:
            reference_table, track_updates = self._parse_table_metadata_params(
                schema_type, table_name, table_metadata, existing_tables
            )
        else:
            reference_table = None
            track_updates = None

        model = em_models.make_annotation_model(
            table_name, schema_type, table_metadata, with_crud_columns
        )

        if reference_table and track_updates:
            description = self.create_reference_update_trigger(
                table_name, description, reference_table, model
            )

        self.base.metadata.tables[model.__name__].create(bind=self.engine)
        creation_time = datetime.datetime.now()

        metadata_dict = {
            "description": description,
            "user_id": user_id,
            "reference_table": reference_table,
            "schema_type": schema_type,
            "table_name": table_name,
            "valid": True,
            "created": creation_time,
            "flat_segmentation_source": flat_segmentation_source,
            "voxel_resolution_x": voxel_resolution_x,
            "voxel_resolution_y": voxel_resolution_y,
            "voxel_resolution_z": voxel_resolution_z,
        }

        logging.info(f"Metadata for table: {table_name} is {metadata_dict}")
        anno_metadata = AnnoMetadata(**metadata_dict)
        self.cached_session.add(anno_metadata)
        self.commit_session()
        logging.info(
            f"Table: {table_name} created using {model} model at {creation_time}"
        )
        return table_name

    def _parse_table_metadata_params(
        self,
        schema_type: str,
        table_name: str,
        table_metadata: dict,
        existing_tables: list,
    ):
        reference_table = None
        track_updates = None

        for param, value in table_metadata.items():
            if param == "reference_table":
                Schema = get_schema(schema_type)
                if not issubclass(Schema, ReferenceAnnotation):
                    raise TypeError(
                        "Reference table must be a ReferenceAnnotation schema type"
                    )
                if table_name is value:
                    raise SelfReferenceTableError(
                        f"{reference_table} must target a different table not {table_name}"
                    )
                if value not in existing_tables:
                    raise TableNameNotFound(
                        f"Reference table target: '{value}' does not exist"
                    )
                reference_table = value
            elif param == "track_target_id_updates":
                track_updates = value
        return reference_table, track_updates

    def create_segmentation_table(
        self, annotation_table_name: str, schema_type: str, pcg_table_name: str
    ):
        """Create new segmentation table linked to an annotation table
         unless it already exists.

        Parameters
        ----------
        annotation_table_name : str
            name of table annotation to link the segmentation table
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        pcg_table_name : str
            name of chunkedgraph segmentation to use

        Returns
        -------
        dict
            description of table that is created
        """

        segmentation_table_name = build_segmentation_table_name(
            annotation_table_name, pcg_table_name
        )

        if annotation_table_name not in self._get_existing_table_names():
            raise TableNameNotFound

        model = em_models.make_segmentation_model(
            annotation_table_name, schema_type, pcg_table_name
        )
        self.base.metadata.tables[model.__name__].create(bind=self.engine)
        creation_time = datetime.datetime.now()

        metadata_dict = {
            "annotation_table": annotation_table_name,
            "schema_type": schema_type,
            "table_name": segmentation_table_name,
            "valid": True,
            "created": creation_time,
            "pcg_table_name": pcg_table_name,
        }
        seg_metadata = SegmentationMetadata(**metadata_dict)
        self.cached_session.add(seg_metadata)
        self.commit_session()

        logging.info(
            f"Table: {segmentation_table_name} created using {model} model at {creation_time}"
        )
        return {"Created Successfully": True, "Table Name": model.__name__}

    def get_table_metadata(self, table_name: str):
        result = (
            self.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .one()
        )
        if not result:
            raise TableNameNotFound(
                f"Error: No table name exists with name {table_name}."
            )
        metadata = {
            k: v for (k, v) in result.__dict__.items() if k != "_sa_instance_state"
        }
        return metadata

    def get_segmentation_table_metadata(self, table_name: str, pcg_table_name: str):
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        result = (
            self.cached_session.query(SegmentationMetadata)
            .filter(SegmentationMetadata.table_name == seg_table_name)
            .one()
        )
        if not result:
            raise TableNameNotFound(
                f"Error: No table name exists with name {table_name}."
            )
        metadata = {
            k: v for (k, v) in result.__dict__.items() if k != "_sa_instance_state"
        }
        return metadata

    def create_reference_update_trigger(
        self, table_name, description, reference_table, model
    ):
        func_name = f"{table_name}_update_reference_id"
        func = DDL(
            f"""
                    CREATE or REPLACE function {func_name}()
                    returns TRIGGER
                    as $func$
                    begin
                        update {table_name} ref
                        set target_id = new.superceded_id
                        where ref.target_id = old.id;
                        return new;
                    end;
                    $func$ language plpgsql;
                    """
        )
        trigger = DDL(
            f"""CREATE TRIGGER update_{table_name}_target_id AFTER UPDATE ON {reference_table}
                    FOR EACH ROW EXECUTE PROCEDURE {func_name}();"""
        )

        event.listen(
            model.__table__,
            "after_create",
            func.execute_if(dialect="postgresql"),
        )

        event.listen(
            model.__table__,
            "after_create",
            trigger.execute_if(dialect="postgresql"),
        )
        description += f" [Note: This table '{model.__name__}' will update the 'target_id' foreign_key when updates are made to the '{reference_table}' table]"
        return description

    def _get_existing_table_by_name(self) -> List[str]:
        """Get the table names of table that exist

        Returns
        -------
        List[str]
            list of table names that exist
        """
        table_names = self._get_existing_table_names()
        return table_names

    def _get_existing_table_names_metadata(self) -> List[dict]:
        """Get all the metadata for all tables

        Returns
        -------
        List[dict]
            all table metadata that exist
        """
        return [
            self.get_table_metadata(table_name)
            for table_name in self._get_existing_table_names()
        ]

    def get_table_schema(self, table_name: str) -> str:
        table_metadata = self.get_table_metadata(table_name)
        return table_metadata["schema_type"]

    def get_table_sql_metadata(self, table_name: str):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def _get_model_columns(self, table_name: str) -> List[tuple]:
        """Return list of column names and types of a given table

        Parameters
        ----------
        table_name : str
            Table name in database

        Returns
        -------
        list
            column names and types
        """
        db_columns = self.insp.get_columns(table_name)
        if not db_columns:
            raise TableNameNotFound(
                f"Error: No table name exists with name {table_name}."
            )
        return [tuple([column["name"], column["type"]]) for column in db_columns]

    def _get_all_tables(self):
        return self.cached_session.query(AnnoMetadata).all()

    def _get_existing_table_names(self) -> List[str]:
        """Collects table_names keys of existing tables

        Returns
        -------
        list
            List of table_names
        """
        metadata = self.cached_session.query(AnnoMetadata).all()
        return [m.table_name for m in metadata]

    def get_valid_table_names(self) -> List[str]:
        metadata = self.cached_session.query(AnnoMetadata).all()
        return [m.table_name for m in metadata if m.valid == True]

    def get_existing_segmentation_table_names(self) -> List[str]:
        """Collects table_names keys of existing segmentation tables
        contained in an aligned volume database. Used for materialization.

        Returns
        -------
        list
            List of segmentation table_names
        """
        metadata = self.cached_session.query(SegmentationMetadata).all()
        return [m.table_name for m in metadata]

    def get_reference_annotation_table_names(self, target_table: str) -> List[str]:
        metadata = (
            self.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.reference_table == target_table)
            .all()
        )
        return [table for table in metadata]

    def has_table(self, table_name: str) -> bool:
        """Check if a table exists

        Parameters
        ----------
        table_name : str
            Table name in database

        Returns
        -------
        bool
            whether the table exists
        """
        return table_name in self._get_existing_table_names()

    def _cached_table(self, table_name: str) -> DeclarativeMeta:
        """Returns cached table 'DeclarativeMeta' callable for querying.

        Parameters
        ----------
        table_name : str
            Table name in database
        Returns
        -------
        DeclarativeMeta
            SQLAlchemy callable. See 'https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#sqlalchemy.ext.declarative.declarative_base.params.metaclass'
        """
        try:
            self._load_table(table_name)
            return self._cached_tables[table_name]
        except TableNameNotFound as error:
            logging.error(f"Cannot load table {error}")

    def _get_table_row_count(self, table_name: str, filter_valid: bool = False) -> int:
        model = self._cached_table(table_name)
        if filter_valid:
            row_count = (
                self.cached_session.query(func.count(model.id))
                .filter(model.valid == True)
                .scalar()
            )
        else:
            row_count = self.cached_session.query(func.count(model.id)).scalar()
        return row_count

    def get_max_id_value(self, table_name: str) -> int:
        model = self._cached_table(table_name)
        return self.cached_session.query(func.max(model.id)).scalar()

    def get_min_id_value(self, table_name: str) -> int:
        model = self._cached_table(table_name)
        return self.cached_session.query(func.min(model.id)).scalar()

    def get_annotation_table_size(self, table_name: str) -> int:
        """Get the number of annotations in a table

        Parameters
        ----------
        table_name : str
            name of table contained within the aligned_volume database

        Returns
        -------
        int
            number of annotations
        """
        Model = self._cached_table(table_name)
        return self.cached_session.query(Model).count()

    def _drop_table(self, table_name: str) -> bool:
        table = self.base.metadata.tables.get(table_name)
        if table:
            logging.info(f"Deleting {table_name} table")
            self.base.metadata.drop_all(self.engine, [table], checkfirst=True)
            if self._is_cached(table):
                del self._cached_tables[table]
            return True
        return False

    def get_annotation_model(self, table_name: str):
        return self._get_model_from_table_name(table_name)

    def get_segmentation_model(self, table_name: str, pcg_table_name: str):
        table_name = build_segmentation_table_name(table_name, pcg_table_name)
        return self._get_model_from_table_name(table_name)

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)
        return self.mapped_base.classes[table_name]

    def _get_flattened_schema_data(self, schema_type: str, data: dict) -> dict:
        schema_type = get_schema(schema_type)
        schema = schema_type(context={"postgis": True})
        data = schema.load(data, unknown=EXCLUDE)

        check_is_nested = any(isinstance(i, dict) for i in data.values())
        if check_is_nested:
            data = flatten_dict(data)

        (
            flat_annotation_schema,
            flat_segmentation_schema,
        ) = em_models.split_annotation_schema(schema_type)

        return (
            self._map_values_to_schema(data, flat_annotation_schema),
            self._map_values_to_schema(data, flat_segmentation_schema),
        )

    def _get_flattened_schema(self, schema_type: str):
        schema_type = get_schema(schema_type)

        (
            flat_annotation_schema,
            flat_segmentation_schema,
        ) = em_models.split_annotation_schema(schema_type)

        return flat_annotation_schema, flat_segmentation_schema

    def _map_values_to_schema(self, data: dict, schema: marshmallow.Schema):
        return {
            key: data[key]
            for key, value in schema._declared_fields.items()
            if key in data
        }

    def _is_cached(self, table_name: str) -> bool:
        """Check if table is loaded into cached instance dict of tables

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
        """Load existing table into cached lookup dict instance

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
            self._cached_tables[table_name] = self._get_model_from_table_name(
                table_name
            )
            return True
        except KeyError as key_error:
            if table_name in self._get_existing_table_names():
                logging.error(f"Could not load table: {key_error}")
            return False
