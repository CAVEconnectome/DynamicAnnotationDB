from sqlalchemy import create_engine, inspect, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError, InvalidRequestError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from marshmallow import INCLUDE, EXCLUDE
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.base import flatten_dict
from emannotationschemas import models as em_models
import logging
import datetime
import time

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

        self.insp = inspect(self.engine)
        
        self._cached_session = None
        self._cached_tables = {}
        self._cached_schemas = {}
        
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
                     em_dataset_name:str, 
                     table_name: str, 
                     schema_type:str, 
                     metadata_dict: dict=None,
                     description: str=None,
                     user_id: str=None,
                     valid: bool=True):
        """Create new annotation table. Checks if already exists. 
        If it is missing for the given em_dataset name it will be
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
        new_table_name = f"{em_dataset_name}_{table_name}"
        
        if new_table_name in self.get_existing_tables():
            logging.warning(f"Table creation failed: {new_table_name} already exists")
            
        model = em_models.make_annotation_model(em_dataset_name,
                                                table_name,
                                                schema_type,
                                                metadata_dict,
                                                with_crud_columns=True)

        self.base.metadata.create_all(bind=self.engine)

        AnnoMetadata = em_models.Metadata
        creation_time = datetime.datetime.now()
        metadata_dict = {
            'schema_type': schema_type,
            'table_name': f"{em_dataset_name}_{table_name}",
            'valid': True,
            'created': creation_time,
            'description': description,
            'user_id': None,
        }
        anno_metadata = AnnoMetadata(**metadata_dict)
        
        self.cached_session.add(anno_metadata)
        self.commit_session()     
        
        logging.info(f"Table: {new_table_name} created using {model} model at {creation_time}")
        return model
        
    def get_table(self, table_name):
        return self.cached_table(table_name)

    def get_table_metadata(self, table_name: str):
        AnnoMetadata = em_models.Metadata
        metadata = []
        for data in self.cached_session.query(AnnoMetadata).filter(AnnoMetadata.table_name==table_name).all():
            metadata.append(data.__dict__)
        return metadata

    def get_table_sql_metadata(self, table_name):
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
        """ Collects table_ids keys of existing tables

        Returns
        -------
        list
            List of table_ids
        """

        tables = self.base.metadata.tables.keys()
        if tables:
            return list(tables)
        else:
            return None


    def cached_table(self, table_id: str) -> DeclarativeMeta:
        """ Returns cached table 'DeclarativeMeta' callable for querying.

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

    def get_annotation(self, table_id: str, schema_name: str, anno_id: int) -> dict:
        """ Get single annotation from database by id.

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
        AnnotationModel = self.cached_table(table_id)
        SegmentationModel = self.cached_table(f"{table_id}_segmentation")
        try:
            annotations, segmentation = self.cached_session.query(AnnotationModel, SegmentationModel).\
                                        filter(AnnotationModel.id==anno_id).one()

            if annotations:
                anno_data = annotations.__dict__
                seg_data = segmentation.__dict__
                anno_data.pop('_sa_instance_state', None)
                seg_data.pop('_sa_instance_state', None)

                data = {**anno_data, **seg_data}

                FlatSchema = get_flat_schema(schema_name)
                schema = FlatSchema(unknown=EXCLUDE)
                return schema.load(data)

        except Exception as e:
            logging.warning(f"No entry found for {anno_id}")
            return

    def insert_annotation(self, table_id: str, schema_name: str, annotations: dict, assign_id=False):
        """Insert single annotation by type and schema. 

        Parameters
        ----------
        table_id : str
            Table name formatted in the form: "{dataset_name}_{table_name}"
        schema_name : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas
        annotations : dict
            Dictionary of single annotation data. Must be flat dict unless structure_data flag is 
            set to True.
        """
        AnnotationModel = self.cached_table(table_id)
        SegmentationModel = self.cached_table(f"{table_id}_segmentation")

        annotation_data, segmentation_data = self._get_flattened_schema_data(schema_name, annotations)

        annotation_data['created'] = datetime.datetime.now()
        if assign_id:
            annotation_data['id'] = annotations['id']
        anno_data = AnnotationModel(**annotation_data)
        try:
            self.cached_session.add(anno_data)
            self.cached_session.flush()
       
            seg_data = SegmentationModel(**segmentation_data)
            seg_data.annotation_id = anno_data.id
        
            self.cached_session.add(seg_data)
        except InvalidRequestError as e:
            self.cached_session.rollback()
        finally:
            self.commit_session()

    def update_annotation(self, table_id: str, schema_name: str, anno_id: int, new_annotations: dict):
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
        """
        AnnotationModel = self.cached_table(table_id)
        SegmentationModel = self.cached_table(f"{table_id}_segmentation")
        
        new_annotation, segmentation = self._get_flattened_schema_data(schema_name, new_annotations)
        
        new_annotation['created'] = datetime.datetime.now()
        new_annotation['valid'] = True

        new_data = AnnotationModel(**new_annotation)
      
        old_anno, old_seg = self.cached_session.query(AnnotationModel, SegmentationModel).filter(AnnotationModel.id==anno_id).one()
        
        self.cached_session.add(new_data)
        self.cached_session.flush()
        
        old_anno.superceded_id = new_data.id
        old_anno.valid = False
        
        old_seg.annotation_id = new_data.id
        
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
        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)
        return self.mapped_base.classes[table_name]

    def _get_flattened_schema_data(self, schema_name: str, data: dict) -> dict:
        schema_type = get_schema(schema_name)
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
