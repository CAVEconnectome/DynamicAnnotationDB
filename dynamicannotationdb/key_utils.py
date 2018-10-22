from typing import Iterable, Dict
from google.cloud import bigtable

import numpy as np


def pad_node_id(node_id: np.uint64) -> str:
    """ Pad node id to 20 digits

    :param node_id: int
    :return: str
    """
    return "%.20d" % node_id


def serialize_uint64(node_id: np.uint64) -> bytes:
    """ Serializes an id to be ingested by a bigtable table row

    :param node_id: int
    :return: str
    """
    return serialize_key(pad_node_id(node_id))  # type: ignore


def serialize_uint64s_to_regex(node_ids: Iterable[np.uint64]) -> bytes:
    """ Serializes an id to be ingested by a bigtable table row

    :param node_id: int
    :return: str
    """
    node_id_str = "".join(["%s|" % pad_node_id(node_id)
                           for node_id in node_ids])[:-1]
    return serialize_key(node_id_str)  # type: ignore


def deserialize_uint64(node_id: bytes) -> np.uint64:
    """ De-serializes a node id from a BigTable row

    :param node_id: bytes
    :return: np.uint64
    """
    return np.uint64(node_id.decode())  # type: ignore


def serialize_key(key: str) -> bytes:
    """ Serializes a key to be ingested by a bigtable table row

    :param key: str
    :return: bytes
    """
    return key.encode("utf-8")


def deserialize_key(key: bytes) -> str:
    """ Deserializes a row key

    :param key: bytes
    :return: str
    """
    return key.decode()


def build_table_id(dataset_name, table_name):
    """ Combines dataset name and annotation to create specific table id

    :param dataset_name: str
    :param table_name: str
    :return: str
    """

    return "annov1__%s__%s" % (dataset_name, table_name)


def get_table_name_from_table_id(table_id):
    """ Extracts dataset name from table_id

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