import logging


def test_create_or_select_database(
    dadb_interface, database_metadata, annotation_metadata
):
    aligned_volume = annotation_metadata["aligned_volume"]
    sql_uri = database_metadata["sql_uri"]
    new_sql_uri = dadb_interface.create_or_select_database(sql_uri, aligned_volume)
    logging.info(new_sql_uri)
    assert str(new_sql_uri) == database_metadata["sql_uri"]
