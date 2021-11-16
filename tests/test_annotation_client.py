import logging
import pytest


def test_create_table(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    schema_type = annotation_metadata["schema_type"]
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]
    table = annotation_client.create_table(
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


def test_create_reference_table(annotation_client, annotation_metadata):
    table_name = "presynaptic_bouton_types"
    schema_type = "presynaptic_bouton_type"
    vx = annotation_metadata["voxel_resolution_x"]
    vy = annotation_metadata["voxel_resolution_y"]
    vz = annotation_metadata["voxel_resolution_z"]

    table_metadata = {
        "reference_table": "anno_test",
        "track_target_id_updates": True,
    }
    table = annotation_client.create_table(
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
    assert table_name == table

    table_info = annotation_client.get_table_metadata(table)
    assert table_info["reference_table"] == "anno_test"


def test_bad_schema_reference_table(annotation_client, annotation_metadata):
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
        table = annotation_client.create_table(
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


def test_insert_annotation(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    test_data = [
        {
            "pre_pt": {"position": [121, 123, 1232]},
            "ctr_pt": {"position": [121, 123, 1232]},
            "post_pt": {"position": [333, 555, 5555]},
            "size": 1,
        }
    ]
    is_committed = annotation_client.insert_annotations(table_name, test_data)
    annotation_client.cached_session.close()

    assert is_committed == True


def test_insert_reference__annotation(annotation_client, annotation_metadata):
    table_name = "presynaptic_bouton_types"

    test_data = [
        {
            "bouton_type": "pancake",
            "target_id": 1,
        }
    ]
    is_committed = annotation_client.insert_annotations(table_name, test_data)
    annotation_client.cached_session.close()

    assert is_committed == True


def test_insert_another_annotation(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    test_data = [
        {
            "pre_pt": {"position": [111, 222, 333]},
            "ctr_pt": {"position": [444, 555, 666]},
            "post_pt": {"position": [777, 888, 999]},
            "size": 1,
        }
    ]
    is_committed = annotation_client.insert_annotations(table_name, test_data)
    annotation_client.cached_session.close()

    assert is_committed == True


def test_get_annotation(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    test_data = annotation_client.get_annotations(table_name, [1])
    logging.info(test_data)
    annotation_client.cached_session.close()

    assert test_data[0]["id"] == 1


def test_update_annotation(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    updated_test_data = {
        "id": 1,
        "pre_pt": {"position": [222, 123, 1232]},
        "ctr_pt": {"position": [121, 123, 1232]},
        "post_pt": {"position": [555, 555, 5555]},
    }
    is_updated = annotation_client.update_annotation(table_name, updated_test_data)
    annotation_client.cached_session.close()

    assert is_updated == "id 1 updated"
    test_data = annotation_client.get_annotations(table_name, [1])
    assert test_data[0]["superceded_id"] == 3


def test_get_reference__annotation(annotation_client, annotation_metadata):
    table_name = "presynaptic_bouton_types"
    test_data = annotation_client.get_annotations(table_name, [1])
    logging.info(test_data)
    annotation_client.cached_session.close()

    assert test_data[0]["id"] == 1
    assert test_data[0]["target_id"] == 3


def test_delete_annotation(annotation_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    ids_to_delete = [1]
    is_deleted = annotation_client.delete_annotation(table_name, ids_to_delete)
    annotation_client.cached_session.close()

    assert is_deleted == True
