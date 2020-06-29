def build_table_id(aligned_volume, table_name):
    """ Combines aligned_volume name and specified table name to create specific table id

    :param aligned_volume: str
    :param table_name: str
    :return: str
    """

    return "annov1__%s__%s" % (aligned_volume, table_name)


def get_table_name_from_table_id(table_id):
    """ Extracts table name from table_id

    :param table_id: str
    :return: str
    """
    return table_id.split("__")[-1]


def get_dataset_name_from_table_id(table_id):
    """ Extracts dataset name from table_id

    :param table_id: str
    :return: str
    """
    return table_id.split("__")[1]