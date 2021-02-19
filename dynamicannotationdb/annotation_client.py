from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.errors import AnnotationInsertLimitExceeded, \
                                      NoAnnotationsFoundWithID, \
                                      UpdateAnnotationError
from dynamicannotationdb.models import AnnoMetadata
from dynamicannotationdb.key_utils import get_table_name_from_table_id, build_table_id
from emannotationschemas import get_flat_schema
from marshmallow import INCLUDE
from sqlalchemy.exc import ArgumentError, InvalidRequestError, OperationalError, IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from typing import List
import datetime
import logging
import json


class DynamicAnnotationClient(DynamicAnnotationInterface):
    
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
        """Load a table

        Parameters
        ----------
        table_name : str
            name of table

        Returns
        -------
        DeclarativeMeta
            the sqlalchemy table of that name
        """
        self._table = self._cached_table(table_name)
        return self._table

    def get_existing_table_names(self) -> List[str]:
        """Get all annotation tables that exist for a aligned_volume

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
            self.get_table_metadata(self.aligned_volume, table_id)
            for table_id in self._get_existing_table_ids()
        ]

    def create_table(self, table_name: str,
                     schema_type: str,
                     description: str,
                     user_id: str,
                     reference_table: str = None,
                     flat_segmentation_source: str = None):
        """Create a new annotation table

        Parameters
        ----------
        table_name : str
            name of new table
            
        schema_type : str
            type of schema for that table

        description: str
            a string with a human readable explanation of 
            what is in the table. Including who made it
            and any information that helps interpret the fields
            of the annotations.

        user_id: str
            user id for this table

        reference_table: str
            reference table name, if required by this schema

        flat_segmentation_source: str
            a path to a segmentation source associated with this table
             i.e. 'precomputed:\\gs:\\my_synapse_seg\example1'

        Returns
        -------
        str
           desciption of table at creation time
        """
        # TODO: check that schemas that are reference schemas
        # have a reference_table in their metadata
        return self.create_annotation_table(self.aligned_volume,
                                            table_name,
                                            schema_type,
                                            description,
                                            user_id,
                                            reference_table=reference_table,
                                            flat_segmentation_source=flat_segmentation_source)

    def delete_table(self, table_name: str) -> bool:
        """Marks a table for deletion, which will
        remove it from user visible calls
        and stop materialization from happening on this table
        only updates metadata to reflect deleted timestamp.

        Parameters
        ----------
        table_name : str
             name of table to mark for deletion

        Returns
        -------
        bool
            whether table was successfully deleted
        """
        table_id = build_table_id(self.aligned_volume, table_name)
        metadata = self.cached_session.query(AnnoMetadata). \
            filter(AnnoMetadata.table_id == table_id).first()
        metadata.deleted = datetime.datetime.now()
        self.cached_session.update(metadata)
        self.commit_session()
        return True

    def drop_table(self, table_name: str) -> bool:
        """Drop a table, actually removes it from the database
        along with segmentation tables associated with it

        Parameters
        ----------
        table_name : str
            name of table to drop

        Returns
        -------
        bool
            whether drop was successful
        """
        return self._drop_table(self.aligned_volume, table_name)

    def insert_annotations(self, table_name: str,
                          annotations: List[dict]):
        """Insert some annotations.

        Parameters
        ----------
        table_name : str
            name of target table to insert annotations
        annotations : list of dict
            a list of dicts with the annotations
                                that meet the schema

        Returns
        -------
        bool
            True is succesfully inserted annotations

        Raises
        ------
        AnnotationInsertLimitExceeded
            Exception raised when amount of annotations exceeds defined limit.
        """
        insertion_limit = 10_000

        if len(annotations) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(annotations), insertion_limit)

        schema_type = self.get_table_schema(self.aligned_volume, table_name)

        table_id = build_table_id(self.aligned_volume, table_name)

        AnnotationModel = self._cached_table(table_id)

        formatted_anno_data = []
        for annotation in annotations:

            annotation_data, __ = self._get_flattened_schema_data(schema_type, annotation)
            if annotation.get('id'):
                annotation_data['id'] = annotation['id']

            annotation_data['created'] = datetime.datetime.now()
            annotation_data['valid'] = True
            formatted_anno_data.append(annotation_data)

        annos = [AnnotationModel(**annotation_data) for annotation_data in formatted_anno_data]

        self.cached_session.add_all(annos)
        self.commit_session()
        return True

    def get_annotations(self, table_name: str,
                       annotation_ids: List[int]) -> List[dict]:
        """Get a set of annotations by ID

        Parameters
        ----------
        table_name : str
            name of table
        annotation_ids : List[int]
            list of annotation ids to get

        Returns
        -------
        List[dict]
            list of returned annotations
        """
        table_id = build_table_id(self.aligned_volume, table_name)

        AnnotationModel = self._cached_table(table_id)

        annotations = self.cached_session.query(AnnotationModel). \
            filter(AnnotationModel.id.in_([x for x in annotation_ids])).all()
        
        schema_type = self.get_table_schema(self.aligned_volume, table_name)
        anno_schema, __ = self._get_flattened_schema(schema_type)
        schema = anno_schema(unknown=INCLUDE)
        try:
            data = []

            for anno in annotations:
                anno_data = anno.__dict__
                anno_data['created'] = str(anno_data.get('created'))
                anno_data['deleted'] = str(anno_data.get('deleted'))
                anno_data.pop('_sa_instance_state', None)
                data.append(anno_data)

            return schema.load(data, many=True)

        except Exception as e:
            logging.exception(e)
            raise NoAnnotationsFoundWithID(annotation_ids)

    def update_annotation(self, table_name: str,
                          annotation: dict):
        """Update an annotation

        Parameters
        ----------
        table_name : str
            name of targeted table to update annotations
        annotation : dict
            new data for that annotation

        Returns
        -------
        [type]
            [description]

        Raises
        ------
        TODO: make this raise an exception rather than return strings
        """
        anno_id = annotation.get('id')
        if not anno_id:
            return "Annotation requires an 'id' to update targeted row"
        schema_type = self.get_table_schema(self.aligned_volume, table_name)

        table_id = build_table_id(self.aligned_volume, table_name)

        AnnotationModel = self._cached_table(table_id)

        new_annotation, __ = self._get_flattened_schema_data(schema_type, annotation)

        new_annotation['created'] = datetime.datetime.now()
        new_annotation['valid'] = True

        new_data = AnnotationModel(**new_annotation)
        try:
            old_anno = self.cached_session.query(AnnotationModel).filter(AnnotationModel.id == anno_id).one()

            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

            self.cached_session.add(new_data)
            self.cached_session.flush()

            old_anno.superceded_id = new_data.id

            old_anno.valid = False

            self.commit_session()

            return f"id {anno_id} updated"
        except NoResultFound as e:
            return f"No result found for {anno_id}. Error: {e}"

    def delete_annotation(self, table_name: str,
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
        Model = self._cached_table(table_id)

        annotations = self.cached_session.query(Model).filter(Model.id.in_(annotation_ids)).all()
        if annotations:
            deleted_time = datetime.datetime.now()
            for annotation in annotations:
                annotation.deleted = deleted_time
                annotation.valid = False
            self.commit_session()
        else:
            return None
        return True
