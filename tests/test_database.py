import logging
from sqlalchemy import Table
import pytest
import datetime


def test_get_table_metadata(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    schema_type = annotation_metadata["schema_type"]
    metadata = dadb_interface.database.get_table_metadata(table_name)
    logging.info(metadata)
    assert metadata["schema_type"] == schema_type
    assert metadata["table_name"] == "anno_test"
    assert metadata["user_id"] == "foo@bar.com"
    assert metadata["description"] == "some description"
    assert metadata["voxel_resolution_x"] == 4.0

    # test for missing column
    with pytest.raises(AttributeError) as e:
        bad_return = dadb_interface.database.get_table_metadata(
            table_name, "missing_column"
        )
    assert (
        str(e.value) == "type object 'AnnoMetadata' has no attribute 'missing_column'"
    )


def test_get_table_sql_metadata(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    sql_metadata = dadb_interface.database.get_table_sql_metadata(table_name)
    logging.info(sql_metadata)
    assert isinstance(sql_metadata, Table)


def test_get_model_columns(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    model_columns = dadb_interface.database._get_model_columns(table_name)
    logging.info(model_columns)
    assert isinstance(model_columns, list)


def test__get_existing_table_ids(dadb_interface):
    table_names = dadb_interface.database._get_existing_table_names()
    assert isinstance(table_names, list)


def test_get_table_row_count(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    result = dadb_interface.database.get_table_row_count(table_name)
    logging.info(f"{table_name} row count: {result}")
    assert result == 3


def test_get_table_valid_row_count(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    result = dadb_interface.database.get_table_row_count(table_name, filter_valid=True)
    logging.info(f"{table_name} valid row count: {result}")
    assert result == 2


def test_get_table_valid_timestamp_row_count(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    ts = datetime.datetime.utcnow() - datetime.timedelta(days=5)
    result = dadb_interface.database.get_table_row_count(
        table_name, filter_valid=True, filter_timestamp=str(ts)
    )
    logging.info(f"{table_name} valid and timestamped row count: {result}")
    assert result == 0


def test_get_annotation_table_size(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    table_size = dadb_interface.database.get_annotation_table_size(table_name)
    assert table_size == 3


def test_load_table(dadb_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    is_loaded = dadb_interface.database._load_table(table_name)

    assert is_loaded is True

    table_name = "non_existing_table"

    is_loaded = dadb_interface.database._load_table(table_name)
    assert is_loaded is False
