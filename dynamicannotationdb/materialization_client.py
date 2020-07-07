from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.errors import AnnotationInsertLimitExceeded, UpdateAnnotationError
from emannotationschemas import get_schema, get_flat_schema
from emannotationschemas.flatten import flatten_dict
from emannotationschemas import models as em_models
from dynamicannotationdb.key_utils import build_table_id, get_table_name_from_table_id, build_segmentation_table_id
from marshmallow import INCLUDE, EXCLUDE
from sqlalchemy.exc import ArgumentError, InvalidRequestError, OperationalError, IntegrityError
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm.exc import NoResultFound
from typing import List
import logging
import datetime
import json


class DynamicMaterializationClient(DynamicAnnotationInterface):
    def __init__(self, aligned_volume, sql_base_uri):
        sql_uri = self.create_or_select_database(aligned_volume, sql_base_uri)
        super().__init__(sql_uri)
    
        self.aligned_volume = aligned_volume
        self._table = None
        self._cached_schemas = {}

    @property
    def table(self):
        return self._table

    def load_table(self, table_name: str):
        self._table = self._cached_table(table_name)
        return self._table

    def _get_existing_table_ids_by_name(self) -> List[str]:
        """Get the table names of table that exist

        Returns
        -------
        List[str]
            list of table names that exist
        """
        table_ids = self._get_existing_table_ids()
        table_names = [get_table_name_from_table_id(tid) for tid in table_ids]
        return table_names

    def _get_existing_table_ids_metadata(self) -> List[dict]:
        """Get all the metadata for all tables

        Returns
        -------
        List[dict]
            all table metadata that exist
        """
        return [
            self.get_table_metadata(self.aligned_volume, table_name)
            for table_name in self._get_existing_table_ids()
        ]

    def create_and_attach_seg_table(self, table_name: str,
                                          pcg_table_name: str,
                                          pcg_version: int = 0):
                
        schema_type = self.get_table_schema(self.aligned_volume, table_name)
        return self.create_segmentation_table(self.aligned_volume,
                                              table_name,
                                              schema_type,
                                              pcg_table_name,
                                              pcg_version)

    def drop_table(self, table_name: str) -> bool:
        return self._drop_table(self.aligned_volume, table_name)

    def get_linked_annotations(self, table_name: str,
                                     pcg_table_name: str,
                                     pcg_version: int,
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
        
        schema_type = self.get_table_schema(self.aligned_volume, table_name)
        
        table_id = build_table_id(self.aligned_volume, table_name)
        seg_table_id = build_segmentation_table_id(self.aligned_volume,table_name,pcg_table_name, pcg_version )

        AnnotationModel = self._cached_table(table_id)
        SegmentationModel = self._cached_table(seg_table_id)
        
        annotations = self.cached_session.query(AnnotationModel, SegmentationModel).\
                                          join(SegmentationModel, SegmentationModel.annotation_id==AnnotationModel.id).\
                                          filter(AnnotationModel.id.in_([x for x in annotation_ids])).all()

        
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
            
    def insert_linked_annotations(self, table_name:str,
                                        pcg_table_name: str,
                                        pcg_version: int,
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
        insertion_limit = 10_000

        if len(annotations) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(annotations), insertion_limit)
        
        schema_type = self.get_table_schema(self.aligned_volume, table_name)

        table_id = build_table_id(self.aligned_volume, table_name)
        seg_table_id = build_segmentation_table_id(self.aligned_volume,table_name,pcg_table_name, pcg_version )

        formatted_anno_data = []
        formatted_seg_data = []
        
        AnnotationModel = self._cached_table(table_id)
        SegmentationModel = self._cached_table(seg_table_id)
        
        for annotation in annotations:
            
            annotation_data, segmentation_data = self._get_flattened_schema_data(
                schema_type, annotation)
            if annotation.get('id'):
                annotation_data['id'] = annotation['id']
                
            annotation_data['created'] = datetime.datetime.now()
            
            formatted_anno_data.append(annotation_data)
            formatted_seg_data.append(segmentation_data)
            
        annos = [AnnotationModel(**annotation_data)
                 for annotation_data in formatted_anno_data]

        self.cached_session.add_all(annos)
        self.cached_session.flush()
        segs = [SegmentationModel(**segmentation_data, annotation_id=anno.id)
                for segmentation_data, anno in zip(formatted_seg_data, annos)]
        self.cached_session.add_all(segs)
        self.commit_session()
        return True

    def update_linked_annotations(self, table_name: str,
                                        pcg_table_name: str,
                                        pcg_version: int,
                                        annotation: dict):
        """Updates an annotation by inserting a new row. The original annotation
        will refer to the new row with a superceded_id. Does not update inplace.

        Parameters
        ----------
        aligned_volume : str
            Table name formatted in the form: "{aligned_volume}_{table_name}"
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        new_annotations : dict, annotation to update by ID
        """
        anno_id = annotation.get('id')
        if not anno_id:
            return "Annotation requires an 'id' to update targeted row"

        table_id = build_table_id(self.aligned_volume, table_name)
        seg_table_id = build_segmentation_table_id(self.aligned_volume,table_name,pcg_table_name, pcg_version )

        schema_type = self.get_table_schema(self.aligned_volume, table_name)

        AnnotationModel = self._cached_table(table_id)
        SegmentationModel = self._cached_table(seg_table_id)
        
        new_annotation, __ = self._get_flattened_schema_data(schema_type, annotation)
        
        new_annotation['created'] = datetime.datetime.now()
        new_annotation['valid'] = True

        new_data = AnnotationModel(**new_annotation)
        try:
            old_anno, old_seg = self.cached_session.query(AnnotationModel, SegmentationModel).filter(AnnotationModel.id==anno_id).one()
        
            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)
            
            self.cached_session.add(new_data)
            self.cached_session.flush()
            
            old_anno.superceded_id = new_data.id
            old_anno.valid = False
            
            old_seg.annotation_id = new_data.id
            
            self.commit_session()
            return f"id {anno_id} updated"
        except NoResultFound as e:
            return f"No result found for {anno_id}. Error: {e}"

    def delete_linked_annotation(self, table_name: str,
                                       pcg_table_name: str,
                                       pcg_version: int,
                                       annotation_ids: List[int]):
        """Delete annotations by ids

        Parameters
        ----------
        table_name : str
            name of table to delete from
        annotation_ids : List[int]
            list of ids to delete

        Returns
        -------

        Raises
        ------
        """
        table_id = build_table_id(self.aligned_volume, table_name)
        seg_table_id = build_segmentation_table_id(self.aligned_volume,table_name,pcg_table_name, pcg_version )
        AnnotationModel = self._cached_table(table_id)
        SegmentationModel = self._cached_table(seg_table_id)
        
        annotations = self.cached_session.query(AnnotationModel, SegmentationModel).\
                                          join(SegmentationModel, SegmentationModel.annotation_id==AnnotationModel.id).\
                                          filter(AnnotationModel.id.in_([x for x in annotation_ids])).all()

        if annotations:
            deleted_time = datetime.datetime.now()
            for annotation in annotations:
                annotation.deleted = deleted_time
                annotation.valid = False
            self.commit_session()
        else:
            return None
        return True