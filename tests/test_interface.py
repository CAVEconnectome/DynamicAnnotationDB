from dynamicannotationdb.interface import DynamicAnnotationInterface
import pytest
from os import environ, getenv
import logging
from .conftest import (
    test_logger,
    ALIGNED_VOLUME,
    DB_HOST,
    TABLE_NAME,
    SCHEMA_TYPE,
    SQL_URI,
)
from sqlalchemy import Table


def test_create_or_select_database(dynamic_annotation_interface):
    new_sql_uri = dynamic_annotation_interface.create_or_select_database(
        ALIGNED_VOLUME, SQL_URI
    )
    test_logger.info(new_sql_uri)
    assert (
        str(new_sql_uri)
        == f"postgres://postgres:postgres@{DB_HOST}:5432/{ALIGNED_VOLUME}"
    )


# def test_create_segmentation_table(dynamic_annotation_interface):
#     pcg_table_name = 'test_pcg_table'
#     seg_table = dynamic_annotation_interface.create_segmentation_table(TABLE_NAME,
#                                                     SCHEMA_TYPE,
#                                                     pcg_table_name)

#     assert seg_table['Table Name'] == "anno_test__test_pcg_table"


def test_get_table_metadata(dynamic_annotation_interface):
    metadata = dynamic_annotation_interface.get_table_metadata(TABLE_NAME)
    test_logger.info(metadata)
    assert metadata["schema_type"] == SCHEMA_TYPE
    assert metadata["table_name"] == "anno_test"
    assert metadata["user_id"] == "foo@bar.com"
    assert metadata["description"] == "some description"
    assert metadata["voxel_resolution_x"] == 4.0
    assert metadata["voxel_resolution_y"] == 4.0
    assert metadata["voxel_resolution_z"] == 40.0


def test_get_table_schema(dynamic_annotation_interface):
    schema_info = dynamic_annotation_interface.get_table_schema(TABLE_NAME)
    assert schema_info == SCHEMA_TYPE


def test_get_table_sql_metadata(dynamic_annotation_interface):
    sql_metadata = dynamic_annotation_interface.get_table_sql_metadata(TABLE_NAME)
    test_logger.info(sql_metadata)
    assert isinstance(sql_metadata, Table)


def test__get_model_columns(dynamic_annotation_interface):
    model_columns = dynamic_annotation_interface._get_model_columns(TABLE_NAME)
    test_logger.info(model_columns)
    assert isinstance(model_columns, list)


def test__get_existing_table_ids(dynamic_annotation_interface):
    table_names = dynamic_annotation_interface._get_existing_table_names()
    assert isinstance(table_names, list)


def test_has_table(dynamic_annotation_interface):
    has_table = dynamic_annotation_interface.has_table(TABLE_NAME)
    assert has_table == True


def test_get_annotation_table_size(dynamic_annotation_interface):
    table_size = dynamic_annotation_interface.get_annotation_table_size(TABLE_NAME)
    assert table_size == 3
