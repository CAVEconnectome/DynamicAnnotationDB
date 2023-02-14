import logging

from emannotationschemas import type_mapping
from emannotationschemas.schemas.base import ReferenceAnnotation


def test_create_segmentation_table(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    table_added_status = dadb_interface.segmentation.create_segmentation_table(
        table_name, "synapse", pcg_table_name
    )
    assert table_added_status == f"{table_name}__{pcg_table_name}"


def test_create_all_schema_types(dadb_interface, annotation_metadata):
    pcg_table_name = annotation_metadata["pcg_table_name"]

    ref_metadata = {
        "reference_table": "anno_test",
        "track_target_id_updates": True,
    }

    for schema_name, schema_type in type_mapping.items():
        table_metadata = (
            ref_metadata if issubclass(schema_type, ReferenceAnnotation) else None
        )
        table = dadb_interface.segmentation.create_segmentation_table(
            f"test_{schema_name}",
            schema_name,
            pcg_table_name,
            table_metadata=table_metadata,
        )
        assert f"test_{schema_name}__{pcg_table_name}" == table


def test_insert_linked_annotations(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    segmentation_data = [
        {
            "id": 8,
            "pre_pt": {
                "position": [121, 123, 1232],
                "supervoxel_id": 2344444,
                "root_id": 4,
            },
            "ctr_pt": {"position": [121, 123, 1232]},
            "post_pt": {
                "position": [121, 123, 1232],
                "supervoxel_id": 3242424,
                "root_id": 5,
            },
            "size": 2,
        }
    ]

    inserted_ids = dadb_interface.segmentation.insert_linked_annotations(
        table_name, pcg_table_name, segmentation_data
    )

    assert inserted_ids == [8]


def test_get_linked_annotations(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    annotations = dadb_interface.segmentation.get_linked_annotations(
        table_name, pcg_table_name, [8]
    )

    logging.info(annotations)
    assert annotations[0]["pre_pt_supervoxel_id"] == 2344444
    assert annotations[0]["pre_pt_root_id"] == 4
    assert annotations[0]["post_pt_supervoxel_id"] == 3242424
    assert annotations[0]["post_pt_root_id"] == 5


def test_insert_linked_segmentation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]
    segmentation_data = [
        {
            "id": 2,
            "pre_pt": {
                "supervoxel_id": 2344444,
                "root_id": 4,
            },
            "post_pt": {
                "supervoxel_id": 3242424,
                "root_id": 5,
            },
            "size": 2,
        }
    ]
    inserted_segmentation_data = dadb_interface.segmentation.insert_linked_segmentation(
        table_name, pcg_table_name, segmentation_data
    )
    logging.info(inserted_segmentation_data)

    assert inserted_segmentation_data == [2]


def test_update_linked_annotations(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]
    update_anno_data = {
        "id": 2,
        "pre_pt": {
            "position": [222, 223, 1232],
        },
        "ctr_pt": {"position": [121, 123, 1232]},
        "post_pt": {
            "position": [121, 123, 1232],
        },
        "size": 2,
    }

    updated_annotations = dadb_interface.segmentation.update_linked_annotations(
        table_name, pcg_table_name, update_anno_data
    )
    logging.info(updated_annotations)

    assert updated_annotations == {2: 4}


def test_insert_another_linked_segmentation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]
    segmentation_data = [
        {
            "id": 4,
            "pre_pt": {
                "supervoxel_id": 2344444,
                "root_id": 4,
            },
            "post_pt": {
                "supervoxel_id": 3242424,
                "root_id": 5,
            },
            "size": 2,
        }
    ]
    inserted_segmentation_data = dadb_interface.segmentation.insert_linked_segmentation(
        table_name, pcg_table_name, segmentation_data
    )
    logging.info(inserted_segmentation_data)

    assert inserted_segmentation_data == [4]


def test_get_updated_linked_annotations(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    annotations = dadb_interface.segmentation.get_linked_annotations(
        table_name, pcg_table_name, [4]
    )
    assert annotations[0]["pre_pt_supervoxel_id"] == 2344444
    assert annotations[0]["pre_pt_root_id"] == 4
    assert annotations[0]["post_pt_supervoxel_id"] == 3242424
    assert annotations[0]["post_pt_root_id"] == 5


def test_delete_linked_annotation(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]
    anno_ids_to_delete = [4]
    deleted_annotations = dadb_interface.segmentation.delete_linked_annotation(
        table_name, pcg_table_name, anno_ids_to_delete
    )
    logging.info(deleted_annotations)

    assert deleted_annotations == [4]
