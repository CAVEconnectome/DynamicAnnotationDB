import marshmallow
from emannotationschemas.errors import UnknownAnnotationTypeException
import pytest
from sqlalchemy.ext.declarative.api import DeclarativeMeta


def test_get_schema(dadb_interface):
    valid_schema = dadb_interface.schema.get_schema("synapse")
    assert isinstance(valid_schema, marshmallow.schema.SchemaMeta)
    with pytest.raises(UnknownAnnotationTypeException) as excinfo:
        non_valid_schema = dadb_interface.schema.get_schema("bad_schema")
        assert non_valid_schema is None


def test_get_flattened_schema(dadb_interface):
    valid_flat_schema = dadb_interface.schema.get_flattened_schema("synapse")

    assert isinstance(valid_flat_schema, marshmallow.schema.SchemaMeta)


def test_create_annotation_model(dadb_interface):
    new_model = dadb_interface.schema.create_annotation_model(
        "test_synapse_2", "synapse"
    )
    assert isinstance(new_model, DeclarativeMeta)


def test_create_segmentation_model(dadb_interface):
    valid_schema = dadb_interface.schema.create_segmentation_model(
        "test_synapse_2", "synapse", "test_annodb"
    )

    assert isinstance(valid_schema, DeclarativeMeta)


def test_create_reference_annotation_model(dadb_interface):
    valid_ref_schema = dadb_interface.schema.create_reference_annotation_model(
        "test_ref_table_2", "presynaptic_bouton_type", "test_synapse_2"
    )

    assert isinstance(valid_ref_schema, DeclarativeMeta)


def test_get_split_model(dadb_interface):
    anno_model, seg_model = dadb_interface.schema.get_split_models(
        "test_synapse_2", "synapse", "test_annodb"
    )

    assert isinstance(anno_model, DeclarativeMeta)
    assert isinstance(seg_model, DeclarativeMeta)


def test_get_split_model_with_no_seg_columns(dadb_interface):
    table_metadata = {"reference_table": "test_synapse_2"}
    anno_model, seg_model = dadb_interface.schema.get_split_models(
        table_name="test_simple_group",
        schema_type="reference_simple_group",
        segmentation_source="test_annodb",
        table_metadata=table_metadata,
    )

    assert isinstance(anno_model, DeclarativeMeta)
    assert seg_model == None


def test_create_flat_model(dadb_interface):
    valid_ref_schema = dadb_interface.schema.create_flat_model(
        "test_flat_table_1",
        "synapse",
        "test_annodb",
    )

    assert isinstance(valid_ref_schema, DeclarativeMeta)


def test_flattened_schema_data(dadb_interface):
    test_data = {
        "id": 1,
        "pre_pt": {"position": [222, 123, 1232]},
        "ctr_pt": {"position": [121, 123, 1232]},
        "post_pt": {"position": [555, 555, 5555]},
    }
    flattened_data = dadb_interface.schema.flattened_schema_data(test_data)
    flat_data = {
        "ctr_pt_position": [121, 123, 1232],
        "id": 1,
        "post_pt_position": [555, 555, 5555],
        "pre_pt_position": [222, 123, 1232],
    }
    assert flattened_data == flat_data


def test_split_flattened_schema(dadb_interface):
    anno_schema, seg_schema = dadb_interface.schema.split_flattened_schema("synapse")
    assert isinstance(anno_schema, marshmallow.schema.SchemaMeta)
    assert isinstance(seg_schema, marshmallow.schema.SchemaMeta)


def test_split_flattened_schema_data(dadb_interface):
    test_data = {
        "id": 1,
        "pre_pt": {"position": [222, 123, 1232]},
        "ctr_pt": {"position": [121, 123, 1232]},
        "post_pt": {"position": [555, 555, 5555]},
    }
    flat_anno_data, flat_seg_data = dadb_interface.schema.split_flattened_schema_data(
        "synapse", test_data
    )

    assert flat_anno_data, flat_seg_data is False


def test__parse_schema_metadata_params(dadb_interface):
    metadata = {"reference_table": "some_other_table", "track_target_id_updates": True}
    metadata_params = dadb_interface.schema._parse_schema_metadata_params(
        "presynaptic_bouton_type", "test_table_3", metadata, ["some_other_table"]
    )

    assert metadata_params == ("some_other_table", True)
