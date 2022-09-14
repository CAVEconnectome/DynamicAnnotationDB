import datetime
import logging
from typing import List

from marshmallow import INCLUDE

from .database import DynamicAnnotationDB
from .errors import (
    AnnotationInsertLimitExceeded,
    IdsAlreadyExists,
    UpdateAnnotationError,
)
from .key_utils import build_segmentation_table_name
from .models import SegmentationMetadata
from .schema import DynamicSchemaClient
from .errors import TableNameNotFound

class DynamicSegmentationClient:
    def __init__(self, sql_url: str) -> None:
        self.db = DynamicAnnotationDB(sql_url)
        self.schema = DynamicSchemaClient()

    def create_segmentation_table(
            self,
            table_name: str,
            schema_type: str,
            segmentation_source: str,
            table_metadata: dict = None,
            with_crud_columns: bool = False,
    ):
        """Create a segmentation table with the primary key as foreign key
        to the annotation table.

        Parameters
        ----------
        table_name : str
            Name of annotation table to link to.
        schema_type : str
            schema type
        segmentation_source : str
            name of segmentation data source, used to create table name.
        table_metadata : dict, optional
            metadata to extend table behavior, by default None
        with_crud_columns : bool, optional
            add additional columns to track CRUD operations on rows, by default False

        Returns
        -------
        str
            name of segmentation table.
        """
        segmentation_table_name = build_segmentation_table_name(
            table_name, segmentation_source
        )

        self.db._check_table_is_unique(segmentation_table_name)

        SegmentationModel = self.schema.create_segmentation_model(
            table_name,
            schema_type,
            segmentation_source,
            table_metadata,
            with_crud_columns,
        )

        if (
                not self.db.cached_session.query(SegmentationMetadata)
                        .filter(SegmentationMetadata.table_name == segmentation_table_name)
                        .scalar()
        ):
            SegmentationModel.__table__.create(bind=self.db._engine, checkfirst=True)
            creation_time = datetime.datetime.utcnow()
            metadata_dict = {
                "annotation_table": table_name,
                "schema_type": schema_type,
                "table_name": segmentation_table_name,
                "valid": True,
                "created": creation_time,
                "pcg_table_name": segmentation_source,
            }

            seg_metadata = SegmentationMetadata(**metadata_dict)
            try:
                self.db.cached_session.add(seg_metadata)
                self.db.commit_session()
            except Exception as e:
                logging.error(f"SQL ERROR: {e}")

        return segmentation_table_name

    def get_linked_tables(self, table_name: str, pcg_table_name: str) -> List:
        try:
            return (
                self.db.cached_session.query(SegmentationMetadata)
                    .filter(SegmentationMetadata.annotation_table == table_name)
                    .filter(SegmentationMetadata.pcg_table_name == pcg_table_name)
                    .all()
            )

        except Exception as e:
            raise AttributeError(
                f"No table found with name '{table_name}'. Error: {e}"
            ) from e

    def get_segmentation_table_metadata(self, table_name: str, pcg_table_name: str):
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        try:
            result = (
                self.db.cached_session.query(SegmentationMetadata)
                .filter(SegmentationMetadata.table_name == seg_table_name)
                .one()
            )
            return self.db.get_automap_items(result)
        except Exception as e:
            self.db.cached_session.rollback()
            return None
        

    def get_linked_annotations(
            self, table_name: str, pcg_table_name: str, annotation_ids: List[int]
    ) -> dict:
        """Get list of annotations from database by id.

        Parameters
        ----------
        table_name : str
            name of annotation table
        pcg_table_name: str
            name of chunked graph reference table
        annotation_ids : int
            annotation id

        Returns
        -------
        list
            list of annotation data dicts
        """

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        annotations = (
            self.db.cached_session.query(AnnotationModel, SegmentationModel)
                .join(SegmentationModel, SegmentationModel.id == AnnotationModel.id)
                .filter(AnnotationModel.id.in_(list(annotation_ids)))
                .all()
        )

        FlatSchema = self.schema.get_flattened_schema(schema_type)
        schema = FlatSchema(unknown=INCLUDE)

        data = []
        for anno, seg in annotations:
            anno_data = anno.__dict__
            seg_data = seg.__dict__
            anno_data = {
                k: v for (k, v) in anno_data.items() if k != "_sa_instance_state"
            }
            seg_data = {
                k: v for (k, v) in seg_data.items() if k != "_sa_instance_state"
            }
            anno_data["created"] = str(anno_data.get("created"))
            anno_data["deleted"] = str(anno_data.get("deleted"))

            merged_data = {**anno_data, **seg_data}
            data.append(merged_data)

        return schema.load(data, many=True)

    def insert_linked_segmentation(
            self, table_name: str, pcg_table_name: str, segmentation_data: List[dict]
    ):
        """Insert segmentation data by linking to annotation ids.
        Limited to 10,000 inserts. If more consider using a bulk insert script.

        Parameters
        ----------
        table_name : str
            name of annotation table
        pcg_table_name: str
            name of chunked graph reference table
        segmentation_data : List[dict]
            List of dictionaries of single segmentation data.
        """
        insertion_limit = 10_000

        if len(segmentation_data) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(segmentation_data), insertion_limit)

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)

        SegmentationModel = self.db.cached_table(seg_table_name)
        formatted_seg_data = []

        _, segmentation_schema = self.schema.split_flattened_schema(schema_type)

        for segmentation in segmentation_data:
            segmentation_data = self.schema.flattened_schema_data(segmentation)
            flat_data = self.schema._map_values_to_schema(
                segmentation_data, segmentation_schema
            )
            flat_data["id"] = segmentation["id"]

            formatted_seg_data.append(flat_data)

        segs = [
            SegmentationModel(**segmentation_data)
            for segmentation_data in formatted_seg_data
        ]

        ids = [data["id"] for data in formatted_seg_data]
        q = self.db.cached_session.query(SegmentationModel).filter(
            SegmentationModel.id.in_(list(ids))
        )

        ids_exist = self.db.cached_session.query(q.exists()).scalar()

        if ids_exist:
            raise IdsAlreadyExists(f"Annotation IDs {ids} already linked in database ")
        self.db.cached_session.add_all(segs)
        seg_ids = [seg.id for seg in segs]
        self.db.commit_session()
        return seg_ids

    def insert_linked_annotations(
            self, table_name: str, pcg_table_name: str, annotations: List[dict]
    ):
        """Insert annotations by type and schema. Limited to 10,000
        annotations. If more consider using a bulk insert script.

        Parameters
        ----------
        table_name : str
            name of annotation table
        pcg_table_name: str
            name of chunked graph reference table
        annotations : dict
            Dictionary of single annotation data.
        """
        insertion_limit = 10_000

        if len(annotations) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(annotations), insertion_limit)

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)

        formatted_anno_data = []
        formatted_seg_data = []

        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)
        logging.info(f"{AnnotationModel.__table__.columns}")
        logging.info(f"{SegmentationModel.__table__.columns}")

        for annotation in annotations:

            anno_data, seg_data = self.schema.split_flattened_schema_data(
                schema_type, annotation
            )
            if annotation.get("id"):
                anno_data["id"] = annotation["id"]
            if hasattr(AnnotationModel, "created"):
                anno_data["created"] = datetime.datetime.utcnow()
            anno_data["valid"] = True
            formatted_anno_data.append(anno_data)
            formatted_seg_data.append(seg_data)
        logging.info(f"DATA TO BE INSERTED: {formatted_anno_data} {formatted_seg_data}")
        try:
            annos = [
                AnnotationModel(**annotation_data)
                for annotation_data in formatted_anno_data
            ]
        except Exception as e:
            raise e
        self.db.cached_session.add_all(annos)
        self.db.cached_session.flush()
        segs = [
            SegmentationModel(**segmentation_data, id=anno.id)
            for segmentation_data, anno in zip(formatted_seg_data, annos)
        ]
        ids = [anno.id for anno in annos]
        self.db.cached_session.add_all(segs)
        self.db.commit_session()
        return ids

    def update_linked_annotations(
            self, table_name: str, pcg_table_name: str, annotation: dict
    ):
        """Updates an annotation by inserting a new row. The original annotation
        will refer to the new row with a superseded_id. Does not update inplace.

        Parameters
        ----------
        table_name : str
            name of annotation table
        pcg_table_name: str
            name of chunked graph reference table
        annotation : dict, annotation to update by ID
        """
        anno_id = annotation.get("id")
        if not anno_id:
            return "Annotation requires an 'id' to update targeted row"

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata["schema_type"]

        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)

        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        new_annotation, __ = self.schema.split_flattened_schema_data(
            schema_type, annotation
        )

        new_annotation["created"] = datetime.datetime.utcnow()
        new_annotation["valid"] = True

        new_data = AnnotationModel(**new_annotation)

        data = (
            self.db.cached_session.query(AnnotationModel, SegmentationModel)
                .filter(AnnotationModel.id == anno_id)
                .filter(SegmentationModel.id == anno_id)
                .all()
        )
        update_map = {}
        for old_anno, old_seg in data:
            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

            self.db.cached_session.add(new_data)
            self.db.cached_session.flush()

            deleted_time = datetime.datetime.utcnow()
            old_anno.deleted = deleted_time
            old_anno.superceded_id = new_data.id
            old_anno.valid = False
            update_map[anno_id] = new_data.id
        self.db.commit_session()

        return update_map

    def delete_linked_annotation(
            self, table_name: str, pcg_table_name: str, annotation_ids: List[int]
    ):
        """Mark annotations by for deletion by list of ids.

        Parameters
        ----------
        table_name : str
            name of annotation table
        pcg_table_name: str
            name of chunked graph reference table
        annotation_ids : List[int]
            list of ids to delete

        Returns
        -------

        Raises
        ------
        """
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        annotations = (
            self.db.cached_session.query(AnnotationModel)
                .join(SegmentationModel, SegmentationModel.id == AnnotationModel.id)
                .filter(AnnotationModel.id.in_(list(annotation_ids)))
                .all()
        )

        if not annotations:
            return None
        deleted_ids = [annotation.id for annotation in annotations]
        deleted_time = datetime.datetime.utcnow()
        for annotation in annotations:
            annotation.deleted = deleted_time
            annotation.valid = False
        self.db.commit_session()
        return deleted_ids
