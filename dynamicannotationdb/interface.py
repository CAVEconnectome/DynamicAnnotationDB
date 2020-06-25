from sqlalchemy import create_engine, inspect, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError, InvalidRequestError, OperationalError, IntegrityError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.pool import NullPool
from marshmallow import INCLUDE, EXCLUDE
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.flatten import flatten_dict
from emannotationschemas import models as em_models
from dynamicannotationdb.key_utils import build_table_id
from dynamicannotationdb.models import Metadata as AnnoMetadata
from typing import List
import logging
import datetime
import time

class DynamicAnnotationInterface:

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
        
        self.sql_uri = make_url(sql_uri)
        base_uri = sql_uri.rpartition("/")[0]

        temp_engine = create_engine(base_uri, poolclass=NullPool)

        with temp_engine.connect() as connection:
            connection.execute("commit")
            result = connection.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.sql_uri.database}'")
            if not result.fetchone():
                print(f"Creating new database {self.sql_uri.database}")
                connection.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                           WHERE pid <> pg_backend_pid() AND datname = '{self.sql_uri.database}';")
                connection.execute(f"create database {self.sql_uri.database} template postgres")

        temp_engine.dispose()
        
        self.engine = create_engine(self.sql_uri,
                                    pool_recycle=3600,
                                    pool_size=20,
                                    max_overflow=50)

        self.base = em_models.Base
        self.mapped_base = None

        if create_metadata:
            self.base.metadata.create_all(self.engine)
        
        self.base.metadata.reflect(bind=self.engine)

        self.session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

        self.insp = inspect(self.engine)
        
        self._cached_session = None
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
                     aligned_volume: str,
                     table_name: str, 
                     schema_type:str, 
                     metadata_dict: dict):
        """Create new annotation table. Checks if already exists. 
        If it is missing for the given em_dataset name it will be
        created.

        Parameters
        ----------
        aligned_volume : str
            Name of dataset to prefix to table, if no cell segment table exists with 
            this name a new table will be generated.
        table_name : str
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        metadata_dict : dict, optional
        """
        table_id = build_table_id(aligned_volume, table_name) 
        
        if table_id in self.get_existing_tables():
            logging.warning(f"Table creation failed: {table_id} already exists")
            return {f"Table {table_id} exists "}
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
        
    def get_table_metadata(self, aligned_volume: str, table_name: str):
        table_id = build_table_id(aligned_volume, table_name) 

        metadata = self.cached_session.query(AnnoMetadata).\
                        filter(AnnoMetadata.table_name==table_id).first()
        metadata.__dict__.pop('_sa_instance_state')
        return metadata.__dict__

    def get_table_sql_metadata(self, table_name):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def get_aligned_volume_tables(self, aligned_volume: str):
        metadata = self.cached_session.query(AnnoMetadata).\
                        filter(AnnoMetadata.aligned_volume==aligned_volume).all()
        return [m.table_name for m in metadata]
         

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
        except ValueError as error:
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

    def cached_table(self, table_id: str) -> DeclarativeMeta:
        """ Returns cached table 'DeclarativeMeta' callable for querying.

        Parameters
        ----------
        table_name : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
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

    def get_annotation_table_size(self, aligned_volume: str, table_name: str) -> int:
        table_id = build_table_id(aligned_volume, table_name)
        Model = self.cached_table(table_id)
        return self.cached_session.query(Model).count()

    def get_annotations(self, aligned_volume: str,
                              table_name: str,
                              schema_type: str,
                              annotation_ids: List[int]) -> dict:
        """ Get list of annotations from database by id.

        Parameters
        ----------
        aligned_volume : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
        anno_id : int
            annotation id 

        Returns
        -------
        list
            list of annotation data dicts
        """
        table_id = build_table_id(aligned_volume, table_name)

        AnnotationModel = self.cached_table(table_id)
        
        annotations = self.cached_session.query(AnnotationModel).\
                            filter(AnnotationModel.id.in_([x for x in annotation_ids])).all()

        try:
            FlatSchema = get_flat_schema(schema_type)
            schema = FlatSchema(unknown=INCLUDE)
            data = []
            
            for anno in annotations: 
                anno_data = anno.__dict__
                anno_data['created'] = str(anno_data.get('created'))
                anno_data['deleted'] = str(anno_data.get('deleted'))
                anno_data.pop('_sa_instance_state', None)
                merged_data = {**anno_data}
                data.append(merged_data)

            return schema.load(data, many=True)
            
        except Exception as e:
            logging.warning(f"No entries found for {annotation_ids}")
            return
        
    def insert_annotations(self, aligned_volume: str,
                                 table_name:str,
                                 schema_type: str,
                                 annotations: List[dict]):
        """Insert annotations by type and schema. Limited to 10,000 annotations. If more consider
        using a bulk insert script.

        Parameters
        ----------
        aligned_volume : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
        table_name: str
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        annotations : dict
            Dictionary of single annotation data. Must be flat dict unless structure_data flag is 
            set to True.
        """
        if len(annotations) > 10_000:
            return f"WARNING: Inserting {len(annotations)} annotations is too large."


        table_id = build_table_id(aligned_volume, table_name)
        formatted_anno_data = []
        
        AnnotationModel = self.cached_table(table_id)
        
        for annotation in annotations:
            
            annotation_data, __ = self._get_flattened_schema_data(schema_type, annotation)
            if annotation.get('id'):
                annotation_data['id'] = annotation['id']
                
            annotation_data['created'] = datetime.datetime.now()
            annotation_data['valid'] = True
            formatted_anno_data.append(annotation_data)
            
        annos = [AnnotationModel(**annotation_data) for annotation_data in formatted_anno_data]

        try:
            self.cached_session.add_all(annos)
                            
        except InvalidRequestError as e:
            self.cached_session.rollback()
        finally:
            self.commit_session()

    def update_annotations(self, aligned_volume: str,
                                 table_name: str,
                                 schema_type: str,
                                 anno_id: int,
                                 new_annotations: dict):
        """Updates an annotation by inserting a new row. 

        Parameters
        ----------
        aligned_volume : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        anno_id : int
            Primary key ID to select annotation for updating.
        new_annotations : [type], optional
        """
        table_id = build_table_id(aligned_volume, table_name)

        AnnotationModel = self.cached_table(table_id)
        
        new_annotation, __ = self._get_flattened_schema_data(schema_type, new_annotations)
        
        new_annotation['created'] = datetime.datetime.now()
        new_annotation['valid'] = True

        new_data = AnnotationModel(**new_annotation)
      
        old_anno = self.cached_session.query(AnnotationModel).filter(AnnotationModel.id==anno_id).one()
        
        self.cached_session.add(new_data)
        self.cached_session.flush()
        
        old_anno.superceded_id = new_data.id
        old_anno.valid = False
                
        self.commit_session()

    def delete_annotations(self, aligned_volume: str, table_name: str, anno_ids: List[int]):
        """Flags an annotation for deletion via deletion timestamp. Will not remove data from
        the database.

        Parameters
        ----------
        aligned_volume : str
            Table name formatted in the form: "{dataset_name}_{table_name}"
        anno_id : int
            Primary key ID to select annotation for deletion.
        """
        table_id = build_table_id(aligned_volume, table_name)
        Model = self.cached_table(table_id)

        annotations = self.cached_session.query(Model).filter(Model.id.in_(anno_ids)).all()
        deleted_time = datetime.datetime.now()
        for annotation in annotations:
            old_annotation.deleted = deleted_time

        self.commit_session()

    def drop_table(self, table_name: str):
        anno_table = self.base.metadata.tables.get(table_name)
        seg_table = self.base.metadata.tables.get(f"{table_name}_segmentation")
        if all(v is not None for v in [anno_table, seg_table]):
            logging.info(f'Deleting {table_name} table')
            self.base.metadata.drop_all(self.engine, [anno_table, seg_table], checkfirst=True)
            if self._is_cached(anno_table):
                del self.cached_table[anno_table], self.cached_table[seg_table]
            return True
        return False

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)
        return self.mapped_base.classes[table_name]

    def _get_flattened_schema_data(self, schema_type: str, data: dict) -> dict:
        schema_type = get_schema(schema_type)
        logging.info(f"DATA: {data}                ")
        schema = schema_type(context={'postgis': True})
        data = schema.load(data, unknown=EXCLUDE)
       
        check_is_nested = (any(isinstance(i, dict) for i in data.values()))
        if check_is_nested:
            data = flatten_dict(data)

        flat_annotation_schema, flat_segmentation_schema = em_models.split_annotation_schema(schema_type)

        return self._map_values_to_schema(data, flat_annotation_schema), self._map_values_to_schema(data, flat_segmentation_schema)

    def _map_values_to_schema(self, data, schema):
        return {key: data[key] for key, value in schema._declared_fields.items() if key in data} 
          
    def _drop_all(self):
        self.base.metadata.reflect(bind=self.engine)
        self.base.metadata.drop_all(self.engine)

    def _reset_table(self, dataset_name, table_name, n_retries=20, delay_s=5):
        metadata = self.get_table_sql_metadata(dataset_name, table_name)

        if self.drop_table(table_name=table_name):
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
