import datetime
import logging
from typing import List

from marshmallow import INCLUDE
from sqlalchemy import DDL, event

from .database import DynamicAnnotationDB
from .errors import (
    AnnotationInsertLimitExceeded,
    NoAnnotationsFoundWithID,
    UpdateAnnotationError,
    TableNameNotFound,
)
from .models import AnnoMetadata
from .schema import DynamicSchemaClient


class DynamicAnnotationClient:
    def __init__(self, sql_url: str) -> None:
        self.db = DynamicAnnotationDB(sql_url)
        self.schema = DynamicSchemaClient()

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
        self._table = self.db.cached_table(table_name)
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
        with_crud_columns: bool = True,
        read_permission: str = "PUBLIC",
        write_permission: str = "PRIVATE",
        notice_text: str = None,
    ):
        r"""Create new annotation table unless already exists

        Parameters
        ----------
        table_name : str
            name of table
        schema_type : str
            Type of schema to use, must be a valid type from EMAnnotationSchemas

        description: str
            a string with a human-readable explanation of
            what is in the table. Including whom made it
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
        existing_tables = self.db._check_table_is_unique(table_name)

        if table_metadata:
            reference_table, _ = self.schema._parse_schema_metadata_params(
                schema_type, table_name, table_metadata, existing_tables
            )
        else:
            reference_table = None

        AnnotationModel = self.schema.create_annotation_model(
            table_name,
            schema_type,
            table_metadata=table_metadata,
            with_crud_columns=with_crud_columns,
        )

        self.db.base.metadata.tables[AnnotationModel.__name__].create(
            bind=self.db.engine
        )
        creation_time = datetime.datetime.utcnow()

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
            "read_permission": read_permission,
            "write_permission": write_permission,
            "last_modified": creation_time,
            "notice_text": notice_text,
        }

        logging.info(f"Metadata for table: {table_name} is {metadata_dict}")
        anno_metadata = AnnoMetadata(**metadata_dict)
        self.db.cached_session.add(anno_metadata)
        self.db.commit_session()
        logging.info(
            f"Table: {table_name} created using {AnnotationModel} model at {creation_time}"
        )
        return table_name

    def update_table_metadata(
        self,
        table_name: str,
        description: str = None,
        user_id: str = None,
        flat_segmentation_source: str = None,
        read_permission: str = None,
        write_permission: str = None,
        notice_text: str = None,
    ):
        r"""Update metadata for an annotation table.

        Parameters
        ----------
        table_name : str
            Name of the annotation table
        description: str, optional
            a string with a human-readable explanation of
            what is in the table. Including whom made it
            and any information that helps interpret the fields
            of the annotations.
        user_id : str, optional
            user id for this table
        flat_segmentation_source : str, optional
            a path to a segmentation source associated with this table
            i.e. 'precomputed:\\gs:\\my_synapse_seg\example1', by default None
        read_permission : str, optional
            set read permissions, by default None
        write_permission : str, optional
            set write permissions, by default None
        notice_text : str, optional
            set notice_text, by default None, if empty string will delete

        Returns
        -------
        dict
            The updated metadata for the target table

        Raises
        ------
        TableNameNotFound
            If no table with 'table_name' found in the metadata table
        """
        metadata = (
            self.db.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .first()
        )
        if metadata is None:
            raise TableNameNotFound(
                f"no table named {table_name} in database {self.sql_url} "
            )

        update_dict = {
            "description": description,
            "user_id": user_id,
            "flat_segmentation_source": flat_segmentation_source,
            "read_permission": read_permission,
            "write_permission": write_permission,
        }
        update_dict = {k: v for k, v in update_dict.items() if v is not None}
        if notice_text is not None:
            if notice_text.lower() == "none":
                notice_text = ""
            if len(notice_text) == 0:
                update_dict["notice_text"] = None
            else:
                update_dict["notice_text"] = notice_text
        for column, value in update_dict.items():
            if hasattr(metadata, str(column)):
                setattr(metadata, column, value)
        self.db.commit_session()
        logging.info(f"Table: {table_name} metadata updated ")
        return self.db.get_table_metadata(table_name)

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
            self.db.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .first()
        )
        if metadata is None:
            raise TableNameNotFound(
                f"no table named {table_name} in database {self.sql_url} "
            )
        metadata.deleted = datetime.datetime.utcnow()
        self.db.commit_session()
        return True

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

        schema_type, AnnotationModel = self._load_model(table_name)

        formatted_anno_data = []
        for annotation in annotations:

            annotation_data, __ = self.schema.split_flattened_schema_data(
                schema_type, annotation
            )
            if annotation.get("id"):
                annotation_data["id"] = annotation["id"]
            if hasattr(AnnotationModel, "created"):
                annotation_data["created"] = datetime.datetime.utcnow()
            annotation_data["valid"] = True
            formatted_anno_data.append(annotation_data)

        annos = [
            AnnotationModel(**annotation_data)
            for annotation_data in formatted_anno_data
        ]

        self.db.cached_session.add_all(annos)
        self.db.cached_session.flush()
        anno_ids = [anno.id for anno in annos]

        (
            self.db.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .update({AnnoMetadata.last_modified: datetime.datetime.utcnow()})
        )

        self.db.commit_session()
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
        schema_type, AnnotationModel = self._load_model(table_name)

        annotations = (
            self.db.cached_session.query(AnnotationModel)
            .filter(AnnotationModel.id.in_(list(annotation_ids)))
            .all()
        )

        anno_schema, __ = self.schema.split_flattened_schema(schema_type)
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
            raise NoAnnotationsFoundWithID(annotation_ids) from e

    def update_annotation(self, table_name: str, annotation: dict) -> str:
        """Update an annotation

        Parameters
        ----------
        table_name : str
            name of targeted table to update annotations
        annotation : dict
            new data for that annotation, allows for partial updates but 
            requires an 'id' field to target the row

        Returns
        -------
        dict:
            dict mapping of old id : new id values

        Raises
        ------
        NoAnnotationsFoundWithID:
            Raises if no Ids to be updated are found in the table.
        """
        anno_id = annotation.get("id")
        if not anno_id:
            return "Annotation requires an 'id' to update targeted row"
        schema_type, AnnotationModel = self._load_model(table_name)

        try:
            old_anno = (
                self.db.cached_session.query(AnnotationModel)
                .filter(AnnotationModel.id == anno_id)
                .one()
            )
        except NoAnnotationsFoundWithID as e:
            raise f"No result found for {anno_id}. Error: {e}" from e

        if old_anno.superceded_id:
            raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

        # Merge old data with new changes
        old_data = {
            column.name: getattr(old_anno, column.name)
            for column in old_anno.__table__.columns
        }
        updated_data = {**old_data, **annotation}

        new_annotation, __ = self.schema.split_flattened_schema_data(
            schema_type, updated_data
        )

        if hasattr(AnnotationModel, "created"):
            new_annotation["created"] = datetime.datetime.utcnow()
        if hasattr(AnnotationModel, "valid"):
            new_annotation["valid"] = True

        new_data = AnnotationModel(**new_annotation)

        self.db.cached_session.add(new_data)
        self.db.cached_session.flush()

        deleted_time = datetime.datetime.utcnow()
        old_anno.deleted = deleted_time
        old_anno.superceded_id = new_data.id
        old_anno.valid = False
        update_map = {anno_id: new_data.id}

        (
            self.db.cached_session.query(AnnoMetadata)
            .filter(AnnoMetadata.table_name == table_name)
            .update({AnnoMetadata.last_modified: datetime.datetime.utcnow()})
        )
        self.db.commit_session()

        return update_map

    def delete_annotation(
        self, table_name: str, annotation_ids: List[int]
    ) -> List[int]:
        """Delete annotations by ids

        Parameters
        ----------
        table_name : str
            name of table to delete from
        annotation_ids : List[int]
            list of ids to delete

        Returns
        -------
        List[int]:
            List of ids that were marked as deleted and no longer valid.
        """
        schema_type, AnnotationModel = self._load_model(table_name)

        annotations = (
            self.db.cached_session.query(AnnotationModel)
            .filter(AnnotationModel.id.in_(annotation_ids))
            .all()
        )
        deleted_ids = []
        if annotations:
            deleted_time = datetime.datetime.utcnow()

            for annotation in annotations:
                # TODO: This should be deprecated, as all tables should have
                # CRUD columns now, but leaving this for backward safety.
                if not hasattr(AnnotationModel, "deleted"):
                    self.db.cached_session.delete(annotation)
                else:
                    annotation.deleted = deleted_time
                    annotation.valid = False
                deleted_ids.append(annotation.id)

            (
                self.db.cached_session.query(AnnoMetadata)
                .filter(AnnoMetadata.table_name == table_name)
                .update({AnnoMetadata.last_modified: datetime.datetime.utcnow()})
            )

            self.db.commit_session()

        else:
            return None
        return deleted_ids

    def _load_model(self, table_name):
        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        # load reference table into metadata if not already present
        ref_table = metadata.get("reference_table")
        if ref_table:
            reference_table_name = self.db.cached_table(ref_table)

        AnnotationModel = self.db.cached_table(table_name)
        return schema_type, AnnotationModel
