from typing import Sequence, Tuple

from emannotationschemas import get_schema
from emannotationschemas import models as em_models
from emannotationschemas.flatten import create_flattened_schema, flatten_dict
from emannotationschemas.schemas.base import ReferenceAnnotation, SegmentationField
from marshmallow import EXCLUDE, Schema

from .errors import SelfReferenceTableError, TableNameNotFound


class DynamicSchemaClient:
    @staticmethod
    def get_schema(schema_type: str):
        return get_schema(schema_type)

    @staticmethod
    def get_flattened_schema(schema_type: str):
        Schema = get_schema(schema_type)
        return em_models.create_flattened_schema(Schema)

    @staticmethod
    def create_annotation_model(
        table_name: str,
        schema_type: str,
        table_metadata: dict = None,
        with_crud_columns: bool = True,
        reset_cache: bool = False,
    ):
        return em_models.make_model_from_schema(
            table_name=table_name,
            schema_type=schema_type,
            table_metadata=table_metadata,
            with_crud_columns=with_crud_columns,
            reset_cache=reset_cache,
        )

    @staticmethod
    def create_segmentation_model(
        table_name: str,
        schema_type: str,
        segmentation_source: str,
        table_metadata: dict = None,
        reset_cache: bool = False,
    ):
        return em_models.make_model_from_schema(
            table_name=table_name,
            schema_type=schema_type,
            segmentation_source=segmentation_source,
            table_metadata=table_metadata,
            reset_cache=reset_cache,
        )

    @staticmethod
    def create_reference_annotation_model(
        table_name: str,
        schema_type: str,
        target_table: str,
        segmentation_source: str = None,
        with_crud_columns: bool = True,
        reset_cache: bool = False,
    ):
        return em_models.make_model_from_schema(
            table_name=table_name,
            schema_type=schema_type,
            segmentation_source=segmentation_source,
            table_metadata={"reference_table": target_table},
            with_crud_columns=with_crud_columns,
            reset_cache=reset_cache,
        )

    @staticmethod
    def create_flat_model(
        table_name: str,
        schema_type: str,
        table_metadata: dict = None,
        with_crud_columns: bool = False,
        reset_cache: bool = False,
    ):
        return em_models.make_flat_model(
            table_name=table_name,
            schema_type=schema_type,
            table_metadata=table_metadata,
            with_crud_columns=with_crud_columns,
            reset_cache=reset_cache
        )

    @staticmethod
    def create_dataset_models(
        aligned_volume: str,
        schemas_and_tables: Sequence[tuple],
        segmentation_source: str = None,
        include_contacts: bool = False,
        metadata_dict: dict = None,
        with_crud_columns: bool = True,
        reset_cache: bool = False,
    ):

        return em_models.make_dataset_models(
            aligned_volume,
            schemas_and_tables,
            segmentation_source,
            include_contacts,
            metadata_dict,
            with_crud_columns,
        )

    @staticmethod
    def get_split_models(
        table_name: str,
        schema_type: str,
        segmentation_source: str,
        table_metadata: dict = None,
        anno_crud_columns: bool = True,
        seg_crud_columns: bool = False,
        reset_cache: bool = False,
    ):
        """Return the annotation and segmentation models from a
        supplied schema. If the schema type requires no segmentation fields
        return only the annotation model and None for the segmentation model.

        Parameters
        ----------
        table_name : str
            name of the table
        schema_type :
            schema type, must be a valid type (hint see :func:`emannotationschemas.get_types`)
        segmentation_source : str, optional
            pcg table to use for root id lookups will return the
            segmentation model if not None, by default None
        table_metadata : dict, optional
            optional metadata dict, by default None
        anno_crud_columns : bool, optional
            add additional created, deleted and superceded_id columns on
            the annotation table model, by default True
        seg_crud_columns : bool, optional
            add additional created, deleted and superceded_id columns on
            the segmentation table model, by default False
        """
        anno_model = em_models.make_model_from_schema(
            table_name=table_name,
            schema_type=schema_type,
            segmentation_source=None,
            table_metadata=table_metadata,
            with_crud_columns=anno_crud_columns,
            reset_cache=reset_cache,
        )
        if DynamicSchemaClient.is_segmentation_table_required(schema_type):
            seg_model = em_models.make_model_from_schema(
                table_name=table_name,
                schema_type=schema_type,
                segmentation_source=segmentation_source,
                table_metadata=table_metadata,
                with_crud_columns=seg_crud_columns,
                reset_cache=reset_cache,
            )
            return anno_model, seg_model
        return anno_model, None

    @staticmethod
    def flattened_schema_data(data):
        return flatten_dict(data)

    @staticmethod
    def is_segmentation_table_required(schema_type: str) -> bool:
        """Check if schema contains any 'Segmentation Fields' column
        types and returns boolean"""
        schema = get_schema(schema_type)
        flat_schema = create_flattened_schema(schema)
        segmentation_columns = {
            key: field
            for key, field in flat_schema._declared_fields.items()
            if isinstance(field, SegmentationField)
        }

        return bool(segmentation_columns)

    @staticmethod
    def split_flattened_schema(schema_type: str):
        schema_type = get_schema(schema_type)

        (
            flat_annotation_schema,
            flat_segmentation_schema,
        ) = em_models.split_annotation_schema(schema_type)

        return flat_annotation_schema, flat_segmentation_schema

    def split_flattened_schema_data(
        self, schema_type: str, data: dict
    ) -> Tuple[dict, dict]:
        schema_type = get_schema(schema_type)
        schema = schema_type(context={"postgis": True})
        data = schema.load(data, unknown=EXCLUDE)

        check_is_nested = any(isinstance(i, dict) for i in data.values())
        if check_is_nested:
            data = flatten_dict(data)

        (
            flat_annotation_schema,
            flat_segmentation_schema,
        ) = em_models.split_annotation_schema(schema_type)

        return (
            self._map_values_to_schema(data, flat_annotation_schema),
            self._map_values_to_schema(data, flat_segmentation_schema),
        )

    @staticmethod
    def _map_values_to_schema(data: dict, schema: Schema):
        return {
            key: data[key]
            for key, value in schema._declared_fields.items()
            if key in data
        }

    def _parse_schema_metadata_params(
        self,
        schema_type: str,
        table_name: str,
        table_metadata: dict,
        existing_tables: list,
    ):
        reference_table = None
        track_updates = None

        for param, value in table_metadata.items():
            if param == "reference_table":
                Schema = self.get_schema(schema_type)
                if not issubclass(Schema, ReferenceAnnotation):
                    raise TypeError(
                        "Reference table must be a ReferenceAnnotation schema type"
                    )
                if table_name == value:
                    raise SelfReferenceTableError(
                        f"{reference_table} must target a different table not {table_name}"
                    )
                if value not in existing_tables:
                    raise TableNameNotFound(value)
                reference_table = value
            elif param == "track_target_id_updates":
                track_updates = value
        return reference_table, track_updates
