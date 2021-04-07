import logging

from sqlalchemy import Table


def test_create_or_select_database(dynamic_annotation_interface,
                                   database_metadata,
                                   annotation_metadata):
    aligned_volume = annotation_metadata["aligned_volume"]
    sql_uri = database_metadata["sql_uri"]
    new_sql_uri = dynamic_annotation_interface.create_or_select_database(
        aligned_volume, sql_uri
    )
    logging.info(new_sql_uri)
    assert str(new_sql_uri) == database_metadata["sql_uri"]


def test_get_table_metadata(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    schema_type = annotation_metadata["schema_type"]
    metadata = dynamic_annotation_interface.get_table_metadata(table_name)
    logging.info(metadata)
    assert metadata["schema_type"] == schema_type
    assert metadata["table_name"] == "anno_test"
    assert metadata["user_id"] == "foo@bar.com"
    assert metadata["description"] == "some description"


def test_get_table_schema(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]
    schema_type = annotation_metadata["schema_type"]
    schema_info = dynamic_annotation_interface.get_table_schema(table_name)
    assert schema_info == schema_type


def test_get_table_sql_metadata(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    sql_metadata = dynamic_annotation_interface.get_table_sql_metadata(table_name)
    logging.info(sql_metadata)
    assert isinstance(sql_metadata, Table)


def test_get_model_columns(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    model_columns = dynamic_annotation_interface._get_model_columns(table_name)
    logging.info(model_columns)
    assert isinstance(model_columns, list)


def test_get_existing_table_ids(dynamic_annotation_interface):
    table_names = dynamic_annotation_interface._get_existing_table_names()
    assert isinstance(table_names, list)


def test_has_table(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    has_table = dynamic_annotation_interface.has_table(table_name)
    assert has_table == True


def test_get_annotation_table_size(dynamic_annotation_interface, annotation_metadata):
    table_name = annotation_metadata["table_name"]

    table_size = dynamic_annotation_interface.get_annotation_table_size(table_name)
    assert table_size == 3
