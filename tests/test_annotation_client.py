from dynamicannotationdb.annotation_client import DynamicAnnotationClient
import pytest
from os import environ, getenv
import logging
from .conftest import test_logger
from sqlalchemy import Table

TABLE_NAME = 'anno_test'
SCHEMA_TYPE = 'synapse'


def test_create_table():
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    anno_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)

    table_id = anno_client.create_table(TABLE_NAME, SCHEMA_TYPE,
                                        description="anno client description",
                                        user_id="anno@test.org",
                                        reference_table=None,
                                        flat_segmentation_source=None)
    test_logger.info(anno_client)
    assert table_id == f"annov1__{ALIGNED_VOLUME}__{TABLE_NAME}"


# def test_delete_table():
#     assert False


# def test_drop_table():
#     assert False


def test_insert_annotation():
    test_data = [{
        'pre_pt': {'position': [121, 123, 1232], 'supervoxel_id':  2344444, 'root_id': 4},
        'ctr_pt': {'position': [121, 123, 1232]},
        'post_pt':{'position': [333, 555, 5555], 'supervoxel_id':  3242424, 'root_id': 5},
    }]
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    anno_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)
    is_commmited = anno_client.insert_annotations(TABLE_NAME, test_data)
    assert is_commmited == True


def test_get_annotation():
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    anno_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)
    test_data = anno_client.get_annotations(TABLE_NAME, [1])
    test_logger.info(test_data)
    assert test_data[0]['id'] == 1


def test_update_annotation():
    updated_test_data = {
        'id': 1,
        'pre_pt': {'position': [222, 123, 1232], 'supervoxel_id':  2344444, 'root_id': 4},
        'ctr_pt': {'position': [121, 123, 1232]},
        'post_pt': {'position': [555, 555, 5555], 'supervoxel_id':  3242424, 'root_id': 5},
    }
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    anno_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)
    is_updated = anno_client.update_annotation(TABLE_NAME, updated_test_data)
    assert is_updated == "id 1 updated"


def test_delete_annotation():
    ids_to_delete = [1]
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    anno_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)
    is_deleted = anno_client.delete_annotation(TABLE_NAME, ids_to_delete)
    assert is_deleted == True
