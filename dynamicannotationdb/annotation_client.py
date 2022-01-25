import datetime
import logging
from typing import List

from marshmallow import INCLUDE
from sqlalchemy.orm.exc import NoResultFound

from dynamicannotationdb.errors import (
    AnnotationInsertLimitExceeded,
    NoAnnotationsFoundWithID,
    UpdateAnnotationError,
)
from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.models import AnnoMetadata


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

    def create_table(
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
    ):
        r"""Create a new annotation table

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

        voxel_resolution_x: float
            voxel_resolution of this annotation table's point in x (typically nm)

        voxel_resolution_y: float
            voxel_resolution of this annotation table's point in y (typically nm)

        voxel_resolution_z: float
            voxel_resolution of this annotation table's point in z (typically nm)


        table_metadata: dict
            metadata for table

        flat_segmentation_source: str
            a path to a segmentation source associated with this table
             i.e. 'precomputed:\\gs:\\my_synapse_seg\example1'

        Returns
        -------
        str
           description of table at creation time
        """
        # TODO: check that schemas that are reference schemas
        # have a reference_table in their metadata
        # TODO create table_metadata schema
        return self.create_annotation_table(
            table_name,
            schema_type,
            description,
            user_id,
            voxel_resolution_x=voxel_resolution_x,
            voxel_resolution_y=voxel_resolution_y,
            voxel_resolution_z=voxel_resolution_z,
            table_metadata=table_metadata,
            flat_segmentation_source=flat_segmentation_source,
        )

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
        metadata = (
            self.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .first()
        )
        metadata.deleted = datetime.datetime.now()
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
        return self._drop_table(table_name)

    def insert_annotations(self, table_name: str, annotations: List[dict]):
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
            True is successfully inserted annotations

        Raises
        ------
        AnnotationInsertLimitExceeded
            Exception raised when amount of annotations exceeds defined limit.
        """
        insertion_limit = 10_000

        if len(annotations) > insertion_limit:
            raise AnnotationInsertLimitExceeded(insertion_limit, len(annotations))

        schema_type = self.get_table_schema(table_name)
        AnnotationModel = self._cached_table(table_name)

        formatted_anno_data = []
        for annotation in annotations:

            annotation_data, __ = self._get_flattened_schema_data(
                schema_type, annotation
            )
            if annotation.get("id"):
                annotation_data["id"] = annotation["id"]
            if hasattr(AnnotationModel, "created"):
                annotation_data["created"] = datetime.datetime.now()
            annotation_data["valid"] = True
            formatted_anno_data.append(annotation_data)

        annos = [
            AnnotationModel(**annotation_data)
            for annotation_data in formatted_anno_data
        ]

        self.cached_session.add_all(annos)
        self.cached_session.flush()
        anno_ids = [anno.id for anno in annos]
        self.commit_session()
        return anno_ids

    def get_annotations(self, table_name: str, annotation_ids: List[int]) -> List[dict]:
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
        AnnotationModel = self._cached_table(table_name)

        annotations = (
            self.cached_session.query(AnnotationModel)
            .filter(AnnotationModel.id.in_([x for x in annotation_ids]))
            .all()
        )

        schema_type = self.get_table_schema(table_name)
        anno_schema, __ = self._get_flattened_schema(schema_type)
        schema = anno_schema(unknown=INCLUDE)
        try:
            data = []

            for anno in annotations:
                anno_data = anno.__dict__
                anno_data["created"] = str(anno_data.get("created"))
                anno_data["deleted"] = str(anno_data.get("deleted"))
                anno_data = {
                    k: v for (k, v) in anno_data.items() if k != "_sa_instance_state"
                }
                data.append(anno_data)

            return schema.load(data, many=True)

        except Exception as e:
            logging.exception(e)
            raise NoAnnotationsFoundWithID(annotation_ids)

    def update_annotation(self, table_name: str, annotation: dict):
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
        anno_id = annotation.get("id")
        if not anno_id:
            return "Annotation requires an 'id' to update targeted row"
        schema_type = self.get_table_schema(table_name)

        AnnotationModel = self._cached_table(table_name)

        new_annotation, __ = self._get_flattened_schema_data(schema_type, annotation)

        new_annotation["created"] = datetime.datetime.now()
        new_annotation["valid"] = True

        new_data = AnnotationModel(**new_annotation)
        try:
            old_anno = (
                self.cached_session.query(AnnotationModel)
                .filter(AnnotationModel.id == anno_id)
                .one()
            )

            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

            self.cached_session.add(new_data)
            self.cached_session.flush()

            deleted_time = datetime.datetime.now()
            old_anno.deleted = deleted_time
            old_anno.superceded_id = new_data.id
            old_anno.valid = False
            update_map = {anno_id: new_data.id}
            self.commit_session()

            return update_map
        except NoResultFound as e:
            return f"No result found for {anno_id}. Error: {e}"

    def delete_annotation(self, table_name: str, annotation_ids: List[int]):
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
        AnnotationModel = self._cached_table(table_name)

        annotations = (
            self.cached_session.query(AnnotationModel)
            .filter(AnnotationModel.id.in_(annotation_ids))
            .all()
        )
        deleted_ids = []
        if annotations:
            deleted_time = datetime.datetime.now()
            for annotation in annotations:
                annotation.deleted = deleted_time
                annotation.valid = False
                deleted_ids.append(annotation.id)
            self.commit_session()
        else:
            return None
        return deleted_ids
