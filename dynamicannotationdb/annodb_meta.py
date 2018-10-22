import collections
import numpy as np
import time
import datetime
import os

from google.cloud import bigtable

from dynamicannotationdb.annodbv2 import AnnotationDB


# global variables
HOME = os.path.expanduser("~")
N_DIGITS_UINT64 = len(str(np.iinfo(np.uint64).max))
LOCK_EXPIRED_TIME_DELTA = datetime.timedelta(minutes=3, seconds=00)

# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    HOME + "/.cloudvolume/secrets/google-secret.json"


def build_table_id(dataset_name, annotation_type):
    """ Combines dataset name and annotation to create specific table id

    :param dataset_name: str
    :param annotation_type: str
    :return: str
    """

    return "anno__%s__%s" % (dataset_name, annotation_type)


def get_annotation_type_from_table_id(table_id):
    """ Extracts annotation type from table_id

    :param table_id: str
    :return: str
    """
    return table_id.split("__")[-1]


class AnnotationMetaDB(object):
    """ Manages annotations from all types and datasets """

    def __init__(self, client=None, instance_id='pychunkedgraph',
                 project_id="neuromancer-seung-import", credentials=None):

        if client is not None:
            self._client = client
        else:
            self._client = bigtable.Client(project=project_id, admin=True,
                                           credentials=credentials)

        self._instance = self.client.instance(instance_id)

        self._loaded_tables = {}

    @property
    def client(self):
        return self._client

    @property
    def instance(self):
        return self._instance

    @property
    def instance_id(self):
        return self.instance.instance_id

    @property
    def project_id(self):
        return self.client.project

    def _is_loaded(self, table_id):
        """ Checks whether table_id is in _loaded_tables

        :param table_id: str
        :return: bool
        """
        return table_id in self._loaded_tables

    def _load_table(self, table_id):
        """ Loads existing table

        :param table_id: str
        :return: bool
            success
        """

        if self._is_loaded(table_id):
            return True

        try:
            self._loaded_tables[table_id] = AnnotationDB(table_id=table_id,
                                                         client=self.client,
                                                         instance=self.instance)
            return True
        except:
            if table_id in self.get_existing_tables():
                print("Could not load table")
                return False
            else:
                print("Table id does not exist")
                return False

    def get_serialized_info(self):
        """ Rerturns dictionary that can be used to load this AnnotationMetaDB

        :return: dict
        """
        amdb_info = {"instance_id": self.instance_id,
                     "project_id": self.project_id,
                     "credentials": self.client.credentials}

        return amdb_info

    def has_table(self, dataset_name, annotation_type):
        """Checks whether a table exists in the database

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
            whether table already exists
        """
        table_id = build_table_id(dataset_name, annotation_type)

        return table_id in self.get_existing_tables()

    def get_existing_tables(self):
        """ Collects table_ids of existing tables

        Annotation tables start with `anno`

        :return: list
        """
        tables = self.instance.list_tables()

        annotation_tables = []
        for table in tables:
            table_name = table.name.split("/")[-1]
            if table_name.startswith("anno"):
                annotation_tables.append(table_name)

        return annotation_tables

    def get_existing_annotation_types(self, dataset_name):
        """ Collects annotation_types of existing tables

        Annotation tables start with `anno`

        :return: list
        """
        annotation_tables = self.get_existing_tables()

        annotation_types = []
        for table_name in annotation_tables:
            if table_name.startswith("anno__%s" % dataset_name):
                annotation_types.append(
                    get_annotation_type_from_table_id(table_name))

        return annotation_types

    def create_table(self, dataset_name, annotation_type):
        """ Creates new table

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
            success
        """
        assert not "__" in dataset_name
        assert not "__" in annotation_type

        table_id = build_table_id(dataset_name, annotation_type)

        if table_id not in self.get_existing_tables():
            self._loaded_tables[table_id] = AnnotationDB(table_id=table_id,
                                                         client=self.client,
                                                         instance=self.instance,
                                                         is_new=True)
            return True
        else:
            return False

    def _delete_table(self, dataset_name, annotation_type):
        """ Deletes a table

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
            success
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if table_id in self.get_existing_tables():
            self._loaded_tables[table_id] = AnnotationDB(table_id=table_id,
                                                         client=self.client,
                                                         instance=self.instance,
                                                         is_new=True)
            self._loaded_tables[table_id].table.delete()
            del self._loaded_tables[table_id]
            return True
        else:
            return False

    def _reset_table(self, dataset_name, annotation_type, n_retries=20,
                     delay_s=5):
        """ Deletes and creates table

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
        """

        if self._delete_table(dataset_name=dataset_name,
                              annotation_type=annotation_type):
            for i_try in range(n_retries):
                time.sleep(delay_s)
                try:
                    if self.create_table(dataset_name=dataset_name,
                                         annotation_type=annotation_type):
                        return True
                except:
                    time.sleep(delay_s)
            return False
        else:
            return False

    def insert_annotations(self, dataset_name, annotation_type, annotations,
                           user_id):
        """ Inserts new annotations into the database and returns assigned ids

        :param dataset_name: str
        :param annotation_type: str
        :param annotations: list of tuples
             [(sv_ids, serialized data), ...]
        :param user_id: str
        :return: list of uint64
            assigned ids (in same order as `annotations`)
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].insert_annotations(annotations,
                                                                user_id)

    def delete_annotations(self, dataset_name, annotation_type, annotation_ids,
                           user_id):
        """ Deletes annotations from the database

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_ids: list of uint64s
        :param user_id: str
        :return: bool
            success
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return False

        return self._loaded_tables[table_id].delete_annotations(annotation_ids,
                                                                user_id)

    def update_annotations(self, dataset_name, annotation_type, annotations,
                           user_id):
        """ Updates existing annotations

        :param dataset_name: str
        :param annotation_type: str
        :param annotations: list of tuples
             [(annotation_id, sv_ids, serialized data), ...]
        :param user_id: str
        :return: list of bools
            success
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return False

        return self._loaded_tables[table_id].update_annotations(annotations,
                                                                user_id)

    def get_annotation_ids_from_sv(self, dataset_name, annotation_type, sv_id,
                                   time_stamp=None):
        """ Acquires all annotation ids associated with a supervoxel

        To also read the data of the acquired annotations use
        `get_annotations_from_sv`

        :param dataset_name: str
        :param annotation_type: str
        :param sv_id: uint64
        :param time_stamp: None or datetime
        :return: list
            annotation ids
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation_ids_from_sv(sv_id, time_stamp=time_stamp)

    def get_annotation_data(self, dataset_name, annotation_type, annotation_id,
                            time_stamp=None):
        """ Reads the data of a single annotation object

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation_data(annotation_id,
                                                                 time_stamp=time_stamp)

    def get_annotation(self, dataset_name, annotation_type, annotation_id,
                       time_stamp=None):
        """ Reads the data and sv_ids of a single annotation object

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob, list of np.uint64
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation(annotation_id,
                                                            time_stamp=time_stamp)

    def get_annotation_sv_ids(self, dataset_name, annotation_type,
                              annotation_id, time_stamp=None):
        """ Reads the sv ids belonging to an annotation

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: list of uint64s
        """

        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation_sv_ids(annotation_id, time_stamp=time_stamp)

    def get_annotations_from_sv_ids(self, dataset_name, annotation_type, sv_ids, time_stamp=None):
        """ Collects the data from all annotations asociated with a set of supervoxels

        This function wraps `get_annotation_from_sv`

        :param dataset_name: str
        :param annotation_type: str
        :param sv_ids: list[uint64]
        :param time_stamp: None or datetime
        :return: dict
            dictionary with keys of annotation ids and values of annotations blobs
        """
        annotations = {}
        for sv_id in sv_ids:
            annotations.update(self.get_annotations_from_sv(dataset_name, annotation_type, sv_id, time_stamp=time_stamp))
        return annotations

    def get_annotations_from_sv(self, dataset_name, annotation_type, sv_id,
                                time_stamp=None):
        """ Collects the data from all annotations associated with a supervoxel

        This function chains `get_annotation_ids_from_sv` and `get_annotation`

        :param dataset_name: str
        :param annotation_type: str
        :param sv_id: uint64
        :param time_stamp: None or datetime
        :return: dict
            dictionary with keys of annotation ids and values of annotations
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotations_from_sv(sv_id, time_stamp=time_stamp)

    def get_max_annotation_id(self, dataset_name, annotation_type):
        """ Returns an upper limit on the annotation id in the table

        There is no guarantee that the returned id itself exists. It is only
        guaranteed that no larger id exists.

        :param dataset_name: str
        :param annotation_type: str
        :return: np.uint64
        """
        table_id = build_table_id(dataset_name, annotation_type)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_max_annotation_id()
