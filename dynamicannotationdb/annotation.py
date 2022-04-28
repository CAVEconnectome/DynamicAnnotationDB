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
)
from .models import AnnoMetadata
from .schema import DynamicSchemaClient


class DynamicAnnotationClient(DynamicAnnotationDB, DynamicSchemaClient):
    def __init__(self, url: str = None, aligned_volume: str = None) -> None:
        super().__init__(url, aligned_volume)
        self._table = None

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
        with_crud_columns: bool = True,
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
        existing_tables = self._check_table_is_unique(table_name)

        if table_metadata:
            reference_table, track_updates = self._parse_schema_metadata_params(
                schema_type, table_name, table_metadata, existing_tables
            )
        else:
            reference_table = None
            track_updates = None

        AnnotationModel = self.create_annotation_model(
            table_name,
            schema_type,
            table_metadata=table_metadata,
            with_crud_columns=with_crud_columns,
        )
        if hasattr(AnnotationModel, "target_id") and reference_table:

            reference_table_name = self.get_table_sql_metadata(reference_table)
            logging.info(
                f"{table_name} is targeting reference table: {reference_table_name}"
            )
            if track_updates:
                self.create_reference_update_trigger(
                    table_name, reference_table, AnnotationModel
                )
                description += (
                    f" [Note: This table '{AnnotationModel.__name__}' will update the 'target_id' "
                    f"foreign_key when updates are made to the '{reference_table}' table] "
                )

        self.base.metadata.tables[AnnotationModel.__name__].create(bind=self.engine)
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
            f"Table: {table_name} created using {AnnotationModel} model at {creation_time}"
        )
        return table_name

    def create_reference_update_trigger(self, table_name, reference_table, model):
        func_name = f"{table_name}_update_reference_id"
        func = DDL(
            f"""
                    CREATE or REPLACE function {func_name}()
                    returns TRIGGER
                    as $func$
                    begin
                        if EXISTS (select 1 from information_schema.columns where table_name='{reference_table}' and column_name='superceded_id') THEN
                            update {table_name} ref
                            set target_id = new.superceded_id
                            where ref.target_id = old.id;
                            return new;
                        else
                            return NULL;
                        END if;
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
        return True

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

        metadata = self.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        AnnotationModel = self._cached_table(table_name)

        formatted_anno_data = []
        for annotation in annotations:

            annotation_data, __ = self._split_flattened_schema_data(
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
            .filter(AnnotationModel.id.in_(list(annotation_ids)))
            .all()
        )

        metadata = self.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        anno_schema, __ = self._split_flattened_schema(schema_type)
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
            new data for that annotation

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
        metadata = self.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        AnnotationModel = self._cached_table(table_name)
        new_annotation, __ = self._split_flattened_schema_data(schema_type, annotation)

        if hasattr(AnnotationModel, "created"):
            new_annotation["created"] = datetime.datetime.now()
        if hasattr(AnnotationModel, "valid"):
            new_annotation["valid"] = True

        new_data = AnnotationModel(**new_annotation)

        try:
            old_anno = (
                self.cached_session.query(AnnotationModel)
                .filter(AnnotationModel.id == anno_id)
                .one()
            )
        except NoAnnotationsFoundWithID as e:
            raise f"No result found for {anno_id}. Error: {e}" from e
        if hasattr(AnnotationModel, "target_id"):
            new_data_map = self.get_automap_items(new_data)
            for column_name, value in new_data_map.items():
                setattr(old_anno, column_name, value)
            old_anno.valid = True
            update_map = {anno_id: old_anno.id}
        else:
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
                if hasattr(AnnotationModel, "target_id"):
                    self.cached_session.delete(annotation)
                else:
                    annotation.deleted = deleted_time
                    annotation.valid = False
                deleted_ids.append(annotation.id)
            self.commit_session()
        else:
            return None
        return deleted_ids
