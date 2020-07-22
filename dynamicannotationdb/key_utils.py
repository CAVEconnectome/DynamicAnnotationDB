def build_table_id(aligned_volume: str, table_name: str) -> str:
    """Combines aligned_volume name and specified table name to 
    create specific table id


    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    table_name : str
        name of table assigned to an aligned volume

    Returns
    -------
    str
        formatted table_id 
    """
    return f"annov1__{aligned_volume}__{table_name}"

def build_segmentation_table_id(aligned_volume: str, 
                                annotation_table_name: str, 
                                pcg_table_name: str) -> str:
    """Create a table id that combines annotation table and appends 
    segmentation table name and version

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    annotation_table_name : str
        exiting annotation table name
    pcg_table_name : str
        name of pychunkedgraph table

    Returns
    -------
    str
        formatted name of table combining the annotation table id with 
        pychunkedgraph table name and segmentation version
    """
    return f"annov1__{aligned_volume}__{annotation_table_name}__{pcg_table_name}"

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