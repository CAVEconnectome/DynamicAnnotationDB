import logging
import pytest

from emannotationschemas import type_mapping
from emannotationschemas.schemas.base import ReferenceAnnotation


def test_create_table(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    schema_type = annotation_metadata["schema_type"]
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]
    table = dadb_interface.annotation.create_table(
        table_name,
        schema_type,
        description="some description",
        user_id="foo@bar.com",
        voxel_resolution_x=vx,
        voxel_resolution_y=vy,
        voxel_resolution_z=vz,
        table_metadata=None,
        flat_segmentation_source=None,
    )
    assert table_name == table


def test_create_all_schema_types(dadb_interface, annotation_metadata):
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]

    ref_metadata = {
        "reference_table": "anno_test",
        "track_target_id_updates": True,
    }

    for schema_name, schema_type in type_mapping.items():
        table_metadata = (
            ref_metadata if issubclass(schema_type, ReferenceAnnotation) else None
        )
        table = dadb_interface.annotation.create_table(
            f"test_{schema_name}",
            schema_name,
            description="some description",
            user_id="foo@bar.com",
            voxel_resolution_x=vx,
            voxel_resolution_y=vy,
            voxel_resolution_z=vz,
            table_metadata=table_metadata,
            flat_segmentation_source=None,
        )
        assert f"test_{schema_name}" == table


def test_create_reference_table(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"
    schema_type = "presynaptic_bouton_type"
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]

    table_metadata = {
        "reference_table": "anno_test",
        "track_target_id_updates": True,
    }
    table = dadb_interface.annotation.create_table(
        table_name,
        schema_type,
        description="some description",
        user_id="foo@bar.com",
        voxel_resolution_x=vx,
        voxel_resolution_y=vy,
        voxel_resolution_z=vz,
        table_metadata=table_metadata,
        flat_segmentation_source=None,
        with_crud_columns=True,
    )
    assert table_name == table

    table_info = dadb_interface.database.get_table_metadata(table)
    assert table_info["reference_table"] == "anno_test"


def test_create_nested_reference_table(dadb_interface, annotation_metadata):
    table_name = "reference_tag"
    schema_type = "reference_tag"
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]

    table_metadata = {
        "reference_table": "presynaptic_bouton_types",
        "track_target_id_updates": True,
    }
    table = dadb_interface.annotation.create_table(
        table_name,
        schema_type,
        description="tags on 'presynaptic_bouton_types' table",
        user_id="foo@bar.com",
        voxel_resolution_x=vx,
        voxel_resolution_y=vy,
        voxel_resolution_z=vz,
        table_metadata=table_metadata,
        flat_segmentation_source=None,
        with_crud_columns=True,
    )
    assert table_name == table

    table_info = dadb_interface.database.get_table_metadata(table)
    assert table_info["reference_table"] == "presynaptic_bouton_types"


def test_bad_schema_reference_table(dadb_interface, annotation_metadata):
    table_name = "bad_reference_table"
    schema_type = "synapse"
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]

    table_metadata = {
        "reference_table": "anno_test",
        "track_target_id_updates": True,
    }
    with pytest.raises(Exception) as e:
        table = dadb_interface.annotation.create_table(
            table_name,
            schema_type,
            description="some description",
            user_id="foo@bar.com",
            voxel_resolution_x=vx,
            voxel_resolution_y=vy,
            voxel_resolution_z=vz,
            table_metadata=table_metadata,
            flat_segmentation_source=None,
        )
    assert str(e.value) == "Reference table must be a ReferenceAnnotation schema type"


def test_insert_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    test_data = [
        {
            "pre_pt": {"position": [121, 123, 1232]},
            "ctr_pt": {"position": [121, 123, 1232]},
            "post_pt": {"position": [333, 555, 5555]},
            "size": 1,
        }
    ]
    inserted_id = dadb_interface.annotation.insert_annotations(table_name, test_data)
    assert inserted_id == [1]


def test_insert_reference_annotation(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"

    test_data = [
        {
            "bouton_type": "pancake",
            "target_id": 1,
        }
    ]
    inserted_id = dadb_interface.annotation.insert_annotations(table_name, test_data)

    assert inserted_id == [1]


def test_insert_nested_reference_tag_annotation(dadb_interface, annotation_metadata):
    table_name = "reference_tag"

    test_data = [
        {
            "tag": "here is a tag",
            "target_id": 1,
        }
    ]
    inserted_id = dadb_interface.annotation.insert_annotations(table_name, test_data)

    assert inserted_id == [1]


def test_insert_another_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    test_data = [
        {
            "pre_pt": {"position": [111, 222, 333]},
            "ctr_pt": {"position": [444, 555, 666]},
            "post_pt": {"position": [777, 888, 999]},
            "size": 1,
        }
    ]
    inserted_id = dadb_interface.annotation.insert_annotations(table_name, test_data)

    assert inserted_id == [2]


def test_get_valid_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    test_data = dadb_interface.annotation.get_annotations(table_name, [1])
    logging.info(test_data)

    assert test_data[0]["id"] == 1
    assert test_data[0]["valid"] is True


def test_get_reference_annotation(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"
    test_data = dadb_interface.annotation.get_annotations(table_name, [1])
    logging.info(test_data)

    assert test_data[0]["id"] == 1
    assert test_data[0]["target_id"] == 1


def test_update_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    updated_test_data = {
        "id": 1,
        "pre_pt": {"position": [222, 123, 1232]},
        "ctr_pt": {"position": [121, 123, 1232]},
        "post_pt": {"position": [555, 555, 5555]},
    }
    update_map = dadb_interface.annotation.update_annotation(
        table_name, updated_test_data
    )

    assert update_map == {1: 3}
    test_data = dadb_interface.annotation.get_annotations(table_name, [1])
    assert test_data[0]["superceded_id"] == 3


def test_get_not_valid_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    test_data = dadb_interface.annotation.get_annotations(table_name, [1])
    logging.info(test_data)

    assert test_data[0]["id"] == 1
    assert test_data[0]["valid"] is False


def test_get_reference_annotation_again(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"
    test_data = dadb_interface.annotation.get_annotations(table_name, [1])
    logging.info(test_data)

    assert test_data[0]["id"] == 1
    assert test_data[0]["target_id"] == 1


def test_update_reference_annotation(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"

    test_data = {
        "id": 1,
        "bouton_type": "basmati",
    }

    update_map = dadb_interface.annotation.update_annotation(table_name, test_data)

    assert update_map == {1: 2}
    # return values from newly updated row
    test_data = dadb_interface.annotation.get_annotations(table_name, [2])
    assert test_data[0]["bouton_type"] == "basmati"


def test_nested_update_reference_annotation(dadb_interface, annotation_metadata):
    table_name = "reference_tag"

    test_data = {
        "tag": "here is a updated tag",
        "id": 1,
    }

    update_map = dadb_interface.annotation.update_annotation(table_name, test_data)

    assert update_map == {1: 2}
    # return values from newly updated row
    test_data = dadb_interface.annotation.get_annotations(table_name, [2])
    assert test_data[0]["tag"] == "here is a updated tag"


def test_delete_reference_annotation(dadb_interface, annotation_metadata):
    table_name = "presynaptic_bouton_types"

    ids_to_delete = [2]
    is_deleted = dadb_interface.annotation.delete_annotation(table_name, ids_to_delete)

    assert is_deleted == ids_to_delete


def test_delete_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    ids_to_delete = [3]
    is_deleted = dadb_interface.annotation.delete_annotation(table_name, ids_to_delete)

    assert is_deleted == ids_to_delete


def test_update_table_metadata(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    updated_metadata = dadb_interface.annotation.update_table_metadata(
        table_name, description="New description"
    )

    assert updated_metadata["description"] == "New description"
