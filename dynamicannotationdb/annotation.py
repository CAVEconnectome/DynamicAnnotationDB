import datetime
import logging
from typing import List, Dict, Any

from marshmallow import INCLUDE
from sqlalchemy import select, update

from dynamicannotationdb.database import DynamicAnnotationDB
from dynamicannotationdb.errors import (
    AnnotationInsertLimitExceeded,
    NoAnnotationsFoundWithID,
    UpdateAnnotationError,
    TableNameNotFound,
)
from dynamicannotationdb.models import AnnoMetadata
from dynamicannotationdb.schema import DynamicSchemaClient


class DynamicAnnotationClient:
    def __init__(self, sql_url: str) -> None:
        self.db = DynamicAnnotationDB(sql_url)
        self.schema = DynamicSchemaClient()
        self._table = None

    @property
    def table(self):
        if self._table is None:
            raise ValueError("No table loaded. Use load_table() first.")
        return self._table

    def load_table(self, table_name: str):
        """Load a table"""
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

        reference_table = None
        if table_metadata:
            reference_table, _ = self.schema._parse_schema_metadata_params(
                schema_type, table_name, table_metadata, existing_tables
            )

        AnnotationModel = self.schema.create_annotation_model(
            table_name,
            schema_type,
            table_metadata=table_metadata,
            with_crud_columns=True,
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

        with self.db.session_scope() as session:
            session.add(anno_metadata)

        logging.info(
            f"Table: {table_name} created using {AnnotationModel} model at {creation_time}"
        )
        return table_name

    def update_table_metadata(self, table_name: str, **kwargs):
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
        with self.db.session_scope() as session:
            metadata = session.execute(
                select(AnnoMetadata).where(AnnoMetadata.table_name == table_name)
            ).scalar_one_or_none()

            if metadata is None:
                raise TableNameNotFound(f"no table named {table_name} in database")

            for key, value in kwargs.items():
                if hasattr(metadata, key):
                    setattr(metadata, key, value)

            if "notice_text" in kwargs and kwargs["notice_text"] == "":
                metadata.notice_text = None

            # Explicitly flush the session to ensure the changes are visible
            session.flush()

            # Refresh the metadata object to get the updated values
            session.refresh(metadata)

        self.db.get_table_metadata.cache_clear()
        logging.info(f"Table: {table_name} metadata updated")

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
        with self.db.session_scope() as session:
            metadata = session.execute(
                select(AnnoMetadata).where(AnnoMetadata.table_name == table_name)
            ).scalar_one_or_none()

            if metadata is None:
                raise TableNameNotFound(f"no table named {table_name} in database")

            metadata.deleted = datetime.datetime.utcnow()

        return True

    def insert_annotations(
        self, table_name: str, annotations: List[Dict[str, Any]]
    ) -> List[int]:
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
            annotation_data, _ = self.schema.split_flattened_schema_data(
                schema_type, annotation
            )
            if "id" in annotation:
                annotation_data["id"] = annotation["id"]
            if hasattr(AnnotationModel, "created"):
                annotation_data["created"] = datetime.datetime.utcnow()
            annotation_data["valid"] = True
            formatted_anno_data.append(annotation_data)

        with self.db.session_scope() as session:
            annos = [AnnotationModel(**data) for data in formatted_anno_data]
            session.add_all(annos)
            session.flush()
            anno_ids = [anno.id for anno in annos]

            session.execute(
                update(AnnoMetadata)
                .where(AnnoMetadata.table_name == table_name)
                .values(last_modified=datetime.datetime.utcnow())
            )

        return anno_ids

    def get_annotations(
        self, table_name: str, annotation_ids: List[int]
    ) -> List[Dict[str, Any]]:
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

        with self.db.session_scope() as session:
            annotations = (
                session.execute(
                    select(AnnotationModel).where(
                        AnnotationModel.id.in_(annotation_ids)
                    )
                )
                .scalars()
                .all()
            )

        anno_schema, _ = self.schema.split_flattened_schema(schema_type)
        schema = anno_schema(unknown=INCLUDE)

        try:
            data = []
            for anno in annotations:
                anno_data = {
                    k: str(v) if isinstance(v, datetime.datetime) else v
                    for k, v in anno.__dict__.items()
                    if not k.startswith("_")
                }
                data.append(anno_data)

            return schema.load(data, many=True)
        except Exception as e:
            logging.exception(e)
            raise NoAnnotationsFoundWithID(annotation_ids) from e

    def update_annotation(
        self, table_name: str, annotation: Dict[str, Any]
    ) -> Dict[int, int]:
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
            raise ValueError("Annotation requires an 'id' to update targeted row")

        schema_type, AnnotationModel = self._load_model(table_name)

        with self.db.session_scope() as session:
            old_anno = session.execute(
                select(AnnotationModel).where(AnnotationModel.id == anno_id)
            ).scalar_one_or_none()

            if old_anno is None:
                raise NoAnnotationsFoundWithID(f"No result found for {anno_id}")

            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

            old_data = {
                column.name: getattr(old_anno, column.name)
                for column in old_anno.__table__.columns
            }
            updated_data = {**old_data, **annotation}

            new_annotation, _ = self.schema.split_flattened_schema_data(
                schema_type, updated_data
            )

            if hasattr(AnnotationModel, "created"):
                new_annotation["created"] = datetime.datetime.utcnow()
            if hasattr(AnnotationModel, "valid"):
                new_annotation["valid"] = True

            new_data = AnnotationModel(**new_annotation)
            session.add(new_data)
            session.flush()

            deleted_time = datetime.datetime.utcnow()
            old_anno.deleted = deleted_time
            old_anno.superceded_id = new_data.id
            old_anno.valid = False

            session.execute(
                update(AnnoMetadata)
                .where(AnnoMetadata.table_name == table_name)
                .values(last_modified=datetime.datetime.utcnow())
            )

        return {anno_id: new_data.id}

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

        with self.db.session_scope() as session:
            annotations = (
                session.execute(
                    select(AnnotationModel).where(
                        AnnotationModel.id.in_(annotation_ids)
                    )
                )
                .scalars()
                .all()
            )

            if not annotations:
                return []

            deleted_time = datetime.datetime.utcnow()
            deleted_ids = []

            for annotation in annotations:
                if not hasattr(AnnotationModel, "deleted"):
                    session.delete(annotation)
                else:
                    annotation.deleted = deleted_time
                    annotation.valid = False
                deleted_ids.append(annotation.id)

            session.execute(
                update(AnnoMetadata)
                .where(AnnoMetadata.table_name == table_name)
                .values(last_modified=datetime.datetime.utcnow())
            )

        return deleted_ids

    def _load_model(self, table_name):
        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata.anno_metadata.schema_type

        if metadata.anno_metadata.reference_table:
            self.db.cached_table(metadata.anno_metadata.reference_table)

        AnnotationModel = self.db.cached_table(table_name)
        return schema_type, AnnotationModel
