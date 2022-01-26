import logging

from sqlalchemy.ext.declarative import api


def test_load_table(materialization_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    loaded_table = materialization_client.load_table(table_name)
    if isinstance(loaded_table, api.DeclarativeMeta):
        assert loaded_table.__name__ == table_name


def test_create_and_attach_seg_table(materialization_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    table_added_status = materialization_client.create_and_attach_seg_table(
        table_name, pcg_table_name
    )
    assert table_added_status == {
        "Created Successfully": True,
        "Table Name": f"{table_name}__{pcg_table_name}",
    }


def test_insert_linked_annotations(materialization_client, annotation_metadata):
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

    is_inserted = materialization_client.insert_linked_annotations(
        table_name, pcg_table_name, segmentation_data
    )
    materialization_client.cached_session.close()
    assert is_inserted == [8]


def test_get_linked_annotations(materialization_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    annotations = materialization_client.get_linked_annotations(
        table_name, pcg_table_name, [8]
    )
    materialization_client.cached_session.close()

    logging.info(annotations)
    assert annotations[0]["pre_pt_supervoxel_id"] == 2344444
    assert annotations[0]["pre_pt_root_id"] == 4
    assert annotations[0]["post_pt_supervoxel_id"] == 3242424
    assert annotations[0]["post_pt_root_id"] == 5


def test_insert_linked_segmentation(materialization_client, annotation_metadata):
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
    inserted_segmentations = materialization_client.insert_linked_segmentation(
        table_name, pcg_table_name, segmentation_data
    )
    materialization_client.cached_session.close()
    logging.info(inserted_segmentations)

    assert inserted_segmentations == [2]


def test_update_linked_annotations(materialization_client, annotation_metadata):
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

    updated_annotations = materialization_client.update_linked_annotations(
        table_name, pcg_table_name, update_anno_data
    )
    materialization_client.cached_session.close()
    logging.info(updated_annotations)

    assert updated_annotations == {2: 4}


def test_insert_another_linked_segmentation(
    materialization_client, annotation_metadata
):
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
    inserted_segmentations = materialization_client.insert_linked_segmentation(
        table_name, pcg_table_name, segmentation_data
    )
    materialization_client.cached_session.close()
    logging.info(inserted_segmentations)

    assert inserted_segmentations == [4]


def test_get_updated_linked_annotations(materialization_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]

    annotations = materialization_client.get_linked_annotations(
        table_name, pcg_table_name, [4]
    )
    assert annotations[0]["pre_pt_supervoxel_id"] == 2344444
    assert annotations[0]["pre_pt_root_id"] == 4
    assert annotations[0]["post_pt_supervoxel_id"] == 3242424
    assert annotations[0]["post_pt_root_id"] == 5


def test_delete_linked_annotation(materialization_client, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    pcg_table_name = annotation_metadata["pcg_table_name"]
    anno_ids_to_delete = [4]
    deleted_annotations = materialization_client.delete_linked_annotation(
        table_name, pcg_table_name, anno_ids_to_delete
    )
    materialization_client.cached_session.close()
    logging.info(deleted_annotations)

    assert deleted_annotations == [4]
