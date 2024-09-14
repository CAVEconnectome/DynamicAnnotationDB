import datetime
import logging
from typing import Any, Dict, List, Optional

from marshmallow import INCLUDE
from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from dynamicannotationdb.database import DynamicAnnotationDB
from dynamicannotationdb.errors import (
    AnnotationInsertLimitExceeded,
    IdsAlreadyExists,
    UpdateAnnotationError,
)
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import SegmentationMetadata
from dynamicannotationdb.schema import DynamicSchemaClient


class DynamicSegmentationClient:
    def __init__(self, sql_url: str) -> None:
        self.db = DynamicAnnotationDB(sql_url)
        self.schema = DynamicSchemaClient()

    def create_segmentation_table(
        self,
        table_name: str,
        schema_type: str,
        segmentation_source: str,
        table_metadata: Optional[Dict[str, Any]] = None,
        with_crud_columns: bool = False,
    ) -> str:
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

        with self.db.session_scope() as session:
            if not session.execute(
                select(SegmentationMetadata).filter_by(
                    table_name=segmentation_table_name
                )
            ).scalar():
                SegmentationModel.__table__.create(
                    bind=self.db._engine, checkfirst=True
                )
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
                session.add(seg_metadata)

        return segmentation_table_name

    def get_linked_tables(
        self, table_name: str, pcg_table_name: str
    ) -> List[SegmentationMetadata]:
        with self.db.session_scope() as session:
            try:
                stmt = select(SegmentationMetadata).filter_by(
                    annotation_table=table_name, pcg_table_name=pcg_table_name
                )
                result = session.execute(stmt)
                return result.scalars().all()
            except Exception as e:
                raise AttributeError(
                    f"No table found with name '{table_name}'. Error: {e}"
                ) from e

    def get_segmentation_table_metadata(
        self, table_name: str, pcg_table_name: str
    ) -> Optional[Dict[str, Any]]:
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        with self.db.session_scope() as session:
            try:
                stmt = select(SegmentationMetadata).filter_by(table_name=seg_table_name)
                result = session.execute(stmt).scalar_one_or_none()
                return self.db.get_automap_items(result) if result else None
            except Exception as e:
                logging.error(f"Error fetching segmentation table metadata: {e}")
                return None

    def get_linked_annotations(
        self, table_name: str, pcg_table_name: str, annotation_ids: List[int]
    ) -> List[Dict[str, Any]]:
        try:
            metadata = self.db.get_table_metadata(table_name)
            schema_type = metadata.anno_metadata.schema_type
            seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
            AnnotationModel, SegmentationModel = self._get_models(
                table_name, seg_table_name
            )

            with self.db.session_scope() as session:
                # Perform a join query
                stmt = (
                    select(AnnotationModel, SegmentationModel)
                    .join(SegmentationModel, AnnotationModel.id == SegmentationModel.id)
                    .filter(AnnotationModel.id.in_(annotation_ids))
                )
                result = session.execute(stmt).all()

            data = []
            for anno, seg in result:
                merged_data = self._format_model_to_dict(anno)
                merged_data.update(self._format_model_to_dict(seg))
                data.append(merged_data)

            FlatSchema = self.schema.get_flattened_schema(schema_type)
            schema = FlatSchema(unknown=INCLUDE)

            return schema.load(data, many=True)

        except Exception as e:
            logging.error(f"Error retrieving linked annotations: {str(e)}")
            raise

    def _get_models(self, table_name: str, seg_table_name: str):
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)
        return AnnotationModel, SegmentationModel

    def _format_model_to_dict(self, model):
        return {
            k: self._format_value(v)
            for k, v in model.__dict__.items()
            if not k.startswith("_")
        }

    def _format_value(self, value):
        return str(value) if isinstance(value, datetime.datetime) else value

    def insert_linked_segmentation(
        self,
        table_name: str,
        pcg_table_name: str,
        segmentation_data: List[Dict[str, Any]],
    ) -> List[int]:
        insertion_limit = 10_000
        if len(segmentation_data) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(segmentation_data), insertion_limit)

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata.anno_metadata.schema_type
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        _, segmentation_schema = self.schema.split_flattened_schema(schema_type)

        formatted_seg_data = [
            {
                **self.schema._map_values_to_schema(
                    self.schema.flattened_schema_data(segmentation), segmentation_schema
                ),
                "id": segmentation["id"],
            }
            for segmentation in segmentation_data
        ]

        with self.db.session_scope() as session:
            try:
                ids = [data["id"] for data in formatted_seg_data]

                # Check for existing IDs efficiently
                existing_ids = set(
                    session.execute(
                        select(SegmentationModel.id).filter(
                            SegmentationModel.id.in_(ids)
                        )
                    )
                    .scalars()
                    .all()
                )

                if existing_ids:
                    raise IdsAlreadyExists(
                        f"Annotation IDs {existing_ids} already linked in database"
                    )

                # Bulk insert using PostgreSQL's INSERT ... ON CONFLICT
                insert_stmt = insert(SegmentationModel).values(formatted_seg_data)
                insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["id"])
                result = session.execute(insert_stmt.returning(SegmentationModel.id))

                seg_ids = result.scalars().all()

                return seg_ids

            except SQLAlchemyError as e:
                logging.error(f"Error inserting linked segmentations: {str(e)}")
                raise

    def insert_linked_annotations(
        self, table_name: str, pcg_table_name: str, annotations: List[Dict[str, Any]]
    ) -> List[int]:
        insertion_limit = 10_000
        if len(annotations) > insertion_limit:
            raise AnnotationInsertLimitExceeded(len(annotations), insertion_limit)

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata.anno_metadata.schema_type

        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        formatted_anno_data = []
        formatted_seg_data = []

        for annotation in annotations:
            anno_data, seg_data = self.schema.split_flattened_schema_data(
                schema_type, annotation
            )
            if "id" in annotation:
                anno_data["id"] = annotation["id"]
            if hasattr(AnnotationModel, "created"):
                anno_data["created"] = datetime.datetime.utcnow()
            anno_data["valid"] = True
            formatted_anno_data.append(anno_data)
            formatted_seg_data.append(seg_data)

        logging.info(f"DATA TO BE INSERTED: {formatted_anno_data} {formatted_seg_data}")

        with self.db.session_scope() as session:
            try:
                annos = [AnnotationModel(**data) for data in formatted_anno_data]
                session.add_all(annos)
                session.flush()
                segs = [
                    SegmentationModel(**seg_data, id=anno.id)
                    for seg_data, anno in zip(formatted_seg_data, annos)
                ]
                session.add_all(segs)
                session.flush()
                ids = [anno.id for anno in annos]
            except Exception as e:
                logging.error(f"Error inserting linked annotations: {e}")
                raise

        return ids

    def update_linked_annotations(
        self, table_name: str, pcg_table_name: str, annotation: Dict[str, Any]
    ) -> Dict[int, int]:
        anno_id = annotation.get("id")
        if not anno_id:
            raise ValueError("Annotation requires an 'id' to update targeted row")

        metadata = self.db.get_table_metadata(table_name)
        schema_type = metadata.anno_metadata.schema_type
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        new_annotation, _ = self.schema.split_flattened_schema_data(
            schema_type, annotation
        )
        new_annotation["created"] = datetime.datetime.utcnow()
        new_annotation["valid"] = True

        with self.db.session_scope() as session:
            stmt = select(AnnotationModel, SegmentationModel).filter(
                and_(AnnotationModel.id == anno_id, SegmentationModel.id == anno_id)
            )
            result = session.execute(stmt).first()

            if not result:
                raise ValueError(f"No annotation found with id {anno_id}")

            old_anno, old_seg = result

            if old_anno.superceded_id:
                raise UpdateAnnotationError(anno_id, old_anno.superceded_id)

            new_data = AnnotationModel(**new_annotation)
            session.add(new_data)
            session.flush()

            deleted_time = datetime.datetime.utcnow()
            old_anno.deleted = deleted_time
            old_anno.superceded_id = new_data.id
            old_anno.valid = False

        return {anno_id: new_data.id}

    def delete_linked_annotation(
        self, table_name: str, pcg_table_name: str, annotation_ids: List[int]
    ) -> Optional[List[int]]:
        seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
        AnnotationModel = self.db.cached_table(table_name)
        SegmentationModel = self.db.cached_table(seg_table_name)

        with self.db.session_scope() as session:
            stmt = (
                select(AnnotationModel)
                .join(SegmentationModel, SegmentationModel.id == AnnotationModel.id)
                .filter(AnnotationModel.id.in_(annotation_ids))
            )
            result = session.execute(stmt)
            annotations = result.scalars().all()

            if not annotations:
                return None

            deleted_time = datetime.datetime.utcnow()
            for annotation in annotations:
                annotation.deleted = deleted_time
                annotation.valid = False

            deleted_ids = [annotation.id for annotation in annotations]

        return deleted_ids
