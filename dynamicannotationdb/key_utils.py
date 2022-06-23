def build_segmentation_table_name(
    annotation_table_name: str, segmentation_source: str
) -> str:
    """Creates a table name that combines annotation table and appends
    segmentation table name

    Parameters
    ----------
    annotation_table_name : str
        exiting annotation table name
    segmentation_source : str
        name of chunkedgraph table

    Returns
    -------
    str
        formatted name of table combining the annotation table id with
        chunkedgraph segmentation source name
    """
    return f"{annotation_table_name}__{segmentation_source}"


def get_table_name_from_table_id(table_id: str) -> str:
    """Extracts table name from table_id string
    Parameters
    ----------
    table_id : str

    Returns
    -------
    str
        table name in table id
    """
    return table_id.split("__")[-1]


def get_dataset_name_from_table_id(table_id: str) -> str:
    """Extracts the aligned volume name from table id string

    Parameters
    ----------
    table_id : str

    Returns
    -------
    str
        name of aligned volume in table id
    """
    return table_id.split("__")[1]
