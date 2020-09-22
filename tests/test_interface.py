from dynamicannotationdb.interface import DynamicAnnotationInterface
import pytest
from os import environ, getenv
import logging
from .conftest import test_logger
from sqlalchemy import Table

TABLE_NAME = 'synapse_test'
SCHEMA_TYPE = 'synapse'


def test_create_or_select_database():
    SQL_URI = environ["SQL_URI"]
    ALIGNED_VOLUME = environ["ALIGNED_VOLUME"]
    DB_HOST = environ["DB_HOST"]
    interface = DynamicAnnotationInterface(SQL_URI)
    new_sql_uri = interface.create_or_select_database(ALIGNED_VOLUME, SQL_URI)
    test_logger.info(new_sql_uri)
    assert str(
        new_sql_uri) == f"postgres://postgres:postgres@{DB_HOST}:5432/{ALIGNED_VOLUME}"


def test_create_annotation_table():
    SQL_URI = environ["SQL_URI"]

    interface = DynamicAnnotationInterface(SQL_URI)
    table_name = interface.create_annotation_table(
        TABLE_NAME,
        SCHEMA_TYPE,
        description="some description",
        user_id="foo@bar.com",
        reference_table=None,
        flat_segmentation_source=None)

    assert table_name == f"{TABLE_NAME}"


def test_create_segmentation_table():
    SQL_URI = environ["SQL_URI"]
    pcg_table_name = 'test_pcg_table'
    interface = DynamicAnnotationInterface(SQL_URI)
    seg_table = interface.create_segmentation_table(TABLE_NAME,
                                                    SCHEMA_TYPE,
                                                    pcg_table_name)

    assert seg_table['Table Name'] == "synapse_test__test_pcg_table"


def test_get_table_metadata():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    metadata = interface.get_table_metadata(TABLE_NAME)
    test_logger.info(metadata)
    assert metadata['schema_type'] == SCHEMA_TYPE
    assert metadata['table_id'] == "synapse_test"
    assert metadata['user_id'] == "foo@bar.com"
    assert metadata['description'] == "some description"


def test_get_table_schema():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    schema_info = interface.get_table_schema(TABLE_NAME)
    assert schema_info == SCHEMA_TYPE


def test_get_table_sql_metadata():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)

    sql_metadata = interface.get_table_sql_metadata(TABLE_NAME)
    test_logger.info(sql_metadata)
    assert isinstance(sql_metadata, Table)


def test__get_model_columns():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    model_columns = interface._get_model_columns(TABLE_NAME)
    test_logger.info(model_columns)
    assert isinstance(model_columns, list)


def test__get_existing_table_ids():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    table_names = interface._get_existing_table_names()
    assert isinstance(table_names, list)


def test_has_table():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    has_table = interface.has_table(TABLE_NAME)
    assert has_table == True


def test_get_annotation_table_size():
    SQL_URI = environ["SQL_URI"]
    interface = DynamicAnnotationInterface(SQL_URI)
    table_size = interface.get_annotation_table_size(TABLE_NAME)
    assert table_size == 0
