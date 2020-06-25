from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.errors import TableNameNotFoundException
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.flatten import flatten_dict
from emannotationschemas import models as em_models
from dynamicannotationdb.key_utils import build_table_id
from marshmallow import INCLUDE, EXCLUDE
from sqlalchemy.exc import ArgumentError, InvalidRequestError, OperationalError, IntegrityError
from typing import List
import logging
import datetime
import json


class DynamicMaterializationClient:
    def __init__(self, aligned_volume: str, sql_base_uri: str):

        sql_base_uri = sql_base_uri.rpartition("/")[0]
        sql_uri = f"{sql_base_uri}/{aligned_volume}"

        self._client = DynamicAnnotationInterface(sql_uri)
        self.aligned_volume = aligned_volume

        self._table = None
        self._cached_schemas = {}

    @property
    def aligned_volume(self):
        return self.aligned_volume

    @property
    def session(self):
        return self._client.cached_session

    @property
    def table(self):
        return self._table

    def load_table(self, table_name: str):
        self._table = self._client.cached_table(table_name)
        return self._table

    def has_table(self, table_name: str) -> bool:
        return table_name in self._client.get_existing_tables()

    def get_existing_tables(self):
        return self._client.get_existing_tables()

    def get_aligned_volume_tables(self, aligned_volume: str):
        return self._client.get_dataset_tables(aligned_volume=aligned_volume)

    def get_table_metadata(self, aligned_volume: str, table_name: str) -> dict:
        return self._client.get_table_metadata(aligned_volume, table_name)

    def get_table_schema(self, table_name: str):
        table_metadata = self.get_table_metadata(
            self.aligned_volume, table_name)
        return table_metadata['schema']

    def get_existing_tables_metadata(self, aligned_volume: str, table_name: str) -> list:

        return [
            self.get_table_metadata(aligned_volume, table_name=table_name)
            for table_id in self._client.get_dataset_tables(table_name)
        ]

    def get_annotation_table_length(self, table_id: str) -> int:
        return self._client.get_annotation_table_size(table_id)

    def create_table(self, table_name: str,
                           schema_type: str,
                           metadata_dict: dict):

        return self._client.create_table(self.aligned_volume,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

    def drop_table(self, table_name: str) -> bool:
        return self._client.drop_table(table_name)


    def get_linked_annotations(self, aligned_volume: str,
                                     table_name: str,
                                     pcg_table_name: str,
                                     pcg_version: int,
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
        SegmentationModel = self.cached_table(f"{table_id}_{pcg_table_name}_v{pcg_version}")
        
        annotations = self.cached_session.query(AnnotationModel, SegmentationModel).\
                                          join(SegmentationModel, SegmentationModel.annotation_id==AnnotationModel.id).\
                                          filter(AnnotationModel.id.in_([x for x in annotation_ids])).all()

        try:
            FlatSchema = get_flat_schema(schema_type)
            schema = FlatSchema(unknown=INCLUDE)
            data = []
            
            for anno, seg in annotations: 
                anno_data = anno.__dict__
                seg_data = seg.__dict__
                anno_data['created'] = str(anno_data.get('created'))
                anno_data['deleted'] = str(anno_data.get('deleted'))
                anno_data.pop('_sa_instance_state', None)
                seg_data.pop('_sa_instance_state', None)
                merged_data = {**anno_data, **seg_data}
                data.append(merged_data)

            return schema.load(data, many=True)
            
        except Exception as e:
            logging.warning(f"No entries found for {annotation_ids}")
            return

    def insert_linked_annotations(self, aligned_volume: str,
                                        table_name:str,
                                        pcg_table_name: str,
                                        pcg_version: int,
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
        formatted_seg_data = []
        
        AnnotationModel = self.cached_table(table_id)
        SegmentationModel = self.cached_table(f"{table_id}_{pcg_table_name}_v{pcg_version}")
        
        for annotation in annotations:
            
            annotation_data, segmentation_data = self._get_flattened_schema_data(schema_type, annotation)
            if annotation.get('id'):
                annotation_data['id'] = annotation['id']
                
            annotation_data['created'] = datetime.datetime.now()
            
            formatted_anno_data.append(annotation_data)
            formatted_seg_data.append(segmentation_data)
            
        annos = [AnnotationModel(**annotation_data) for annotation_data in formatted_anno_data]

        try:
            self.cached_session.add_all(annos)
            
            self.cached_session.flush()

            segs = [SegmentationModel(**segmentation_data, annotation_id=anno.id) for segmentation_data, anno in zip(formatted_seg_data, annos)]
                    
            self.cached_session.add_all(segs)
        except InvalidRequestError as e:
            self.cached_session.rollback()
        finally:
            self.commit_session()

    def update_linked_annotations(self, aligned_volume: str,
                                       table_name: str,
                                       pcg_table_name: str,
                                       pcg_version: int,
                                       schema_type: str,
                                       anno_id: int,
                                       new_annotations: dict):
        """Updates an annotation by inserting a new row. The original annotation will refer to the new row
        with a superceded_id. Does not update inplace.

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
        SegmentationModel = self.cached_table(f"{table_id}_{pcg_table_name}_v{pcg_version}")
        
        new_annotation, segmentation = self._get_flattened_schema_data(schema_type, new_annotations)
        
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