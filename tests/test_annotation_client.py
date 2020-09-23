from dynamicannotationdb.annotation_client import DynamicAnnotationClient
import pytest
from os import environ, getenv
import logging
from .conftest import test_logger, TABLE_NAME, SCHEMA_TYPE
from sqlalchemy import Table


def test_create_table(annotation_client):
    table_name = annotation_client.create_table(TABLE_NAME, SCHEMA_TYPE,
                                        description="anno client description",
                                        user_id="foo@bar.com",
                                        reference_table=None,
                                        flat_segmentation_source=None)
    print(table_name)                                        
    test_logger.info(annotation_client)
    assert table_name == f"{TABLE_NAME}"


# def test_delete_table():
#     assert False


# def test_drop_table():
#     assert False


def test_insert_annotation(annotation_client):
    test_data = [{
        'pre_pt': {'position': [121, 123, 1232], 'supervoxel_id':  2344444, 'root_id': 4},
        'ctr_pt': {'position': [121, 123, 1232]},
        'post_pt':{'position': [333, 555, 5555], 'supervoxel_id':  3242424, 'root_id': 5},
    }]
    is_commmited = annotation_client.insert_annotations(TABLE_NAME, test_data)
    assert is_commmited == True


def test_get_annotation(annotation_client):
    test_data = annotation_client.get_annotations(TABLE_NAME, [1])
    test_logger.info(test_data)
    assert test_data[0]['id'] == 1


def test_update_annotation(annotation_client):
    updated_test_data = {
        'id': 1,
        'pre_pt': {'position': [222, 123, 1232], 'supervoxel_id':  2344444, 'root_id': 4},
        'ctr_pt': {'position': [121, 123, 1232]},
        'post_pt': {'position': [555, 555, 5555], 'supervoxel_id':  3242424, 'root_id': 5},
    }
    is_updated = annotation_client.update_annotation(TABLE_NAME, updated_test_data)
    assert is_updated == "id 1 updated"


def test_delete_annotation(annotation_client):
    ids_to_delete = [1]
    is_deleted = annotation_client.delete_annotation(TABLE_NAME, ids_to_delete)
    assert is_deleted == True
