import numpy as np
import time
import datetime
import os

from google.cloud import bigtable

from dynamicannotationdb.annodb import AnnotationDB


# global variables
from dynamicannotationdb.key_utils import build_table_id, \
    get_table_name_from_table_id, get_dataset_name_from_table_id

HOME = os.path.expanduser("~")
N_DIGITS_UINT64 = len(str(np.iinfo(np.uint64).max))
LOCK_EXPIRED_TIME_DELTA = datetime.timedelta(minutes=3, seconds=00)

# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    HOME + "/.cloudvolume/secrets/google-secret.json"


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
                                                         client=self.client)
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
                     "project_id": self.project_id}

        try:
            amdb_info["credentials"] = self.client.credentials
        except:
            amdb_info["credentials"] = self.client._credentials

        return amdb_info

    def has_table(self, dataset_name, table_name):
        """Checks whether a table exists in the database

        :param dataset_name: str
        :param table_name: str
        :return: bool
            whether table already exists
        """
        table_id = build_table_id(dataset_name, table_name)

        return table_id in self.get_existing_tables()

    def get_table_metadata(self, dataset_name, table_name):
        """ Returns Metadata of a table

        :param dataset: str
        :param table_name: str
        :return: dict
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].metadata

    def get_existing_tables(self, dataset_name=None):
        """ Collects table_ids of existing tables

        Annotation tables start with `anno`

        :return: list
        """
        tables = self.instance.list_tables()

        annotation_tables = []
        for table_path in tables:
            table_id = table_path.name.split("/")[-1]
            if table_id.startswith("annov1__"):
                table_dataset_name = get_dataset_name_from_table_id(table_id)

                if dataset_name is not None:
                    if table_dataset_name != dataset_name:
                        continue

                # table_name = get_table_name_from_table_id(table_id)
                annotation_tables.append(table_id)

        return annotation_tables

    def get_existing_tables_metadata(self, dataset_name=None):
        """ Collects annotation_types of existing tables

        Annotation tables start with `anno`

        :return: list
        """
        metadata_list = []
        for table_id in self.get_existing_tables(dataset_name=dataset_name):
            dataset_name = get_dataset_name_from_table_id(table_id)
            table_name = get_table_name_from_table_id(table_id)
            metadata_list.append(self.get_table_metadata(dataset_name=dataset_name,
                                                         table_name=table_name))

        return metadata_list

    def create_table(self, user_id, dataset_name, table_name, schema_name,
                     chunk_size=[512, 512, 64]):
        """ Creates new table

        :param dataset_name: str
        :param table_name: str
        :return: bool
            success
        """
        assert not "__" in dataset_name
        assert not "__" in table_name

        table_id = build_table_id(dataset_name, table_name)

        if table_id not in self.get_existing_tables():
            self._loaded_tables[table_id] = AnnotationDB(table_id=table_id,
                                                         client=self.client,
                                                         schema_name=schema_name,
                                                         chunk_size=chunk_size,
                                                         is_new=True)
            return True
        else:
            return False

    def _delete_table(self, dataset_name, table_name):
        """ Deletes a table

        :param dataset_name: str
        :param table_name: str
        :return: bool
            success
        """
        table_id = build_table_id(dataset_name, table_name)

        if table_id in self.get_existing_tables():
            self._loaded_tables[table_id] = AnnotationDB(table_id=table_id,
                                                         client=self.client,
                                                         is_new=True)
            self._loaded_tables[table_id].table.delete()
            del self._loaded_tables[table_id]
            return True
        else:
            return False

    def _reset_table(self, user_id, dataset_name, table_name, schema_name,
                     n_retries=20, delay_s=5):
        """ Deletes and creates table

        :param dataset_name: str
        :param table_name: str
        :return: bool
        """

        if self._delete_table(dataset_name=dataset_name,
                              table_name=table_name):
            for i_try in range(n_retries):
                time.sleep(delay_s)
                try:
                    if self.create_table(user_id=user_id,
                                         dataset_name=dataset_name,
                                         table_name=table_name,
                                         schema_name=schema_name):
                        return True
                except:
                    time.sleep(delay_s)
            return False
        else:
            return False

    def insert_annotations(self, user_id, dataset_name, table_name,
                           annotations):
        """ Inserts new annotations into the database and returns assigned ids

        :param dataset_name: str
        :param table_name: str
        :param annotations: list of tuples
             [(sv_ids, serialized data), ...]
        :param user_id: str
        :return: list of uint64
            assigned ids (in same order as `annotations`)
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].insert_annotations(user_id,
                                                                annotations)

    def delete_annotations(self, user_id, dataset_name, table_name,
                           annotation_ids):
        """ Deletes annotations from the database

        :param dataset_name: str
        :param table_name: str
        :param annotation_ids: list of uint64s
        :param user_id: str
        :return: bool
            success
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return False

        return self._loaded_tables[table_id].delete_annotations(user_id,
                                                                annotation_ids)

    def update_annotations(self, user_id, dataset_name, annotation_type,
                           annotations):
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

        return self._loaded_tables[table_id].update_annotations(user_id,
                                                                annotations)

    def get_annotation_data(self, dataset_name, table_name, annotation_id,
                            time_stamp=None):
        """ Reads the data of a single annotation object

        :param dataset_name: str
        :param table_name: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation(annotation_id,
                                                            time_stamp=time_stamp)[0]

    def get_annotation(self, dataset_name, table_name, annotation_id,
                       time_stamp=None):
        """ Reads the data and sv_ids of a single annotation object

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob, list of np.uint64
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_annotation(annotation_id,
                                                            time_stamp=time_stamp)

    def get_max_annotation_id(self, dataset_name, table_name):
        """ Returns an upper limit on the annotation id in the table

        There is no guarantee that the returned id itself exists. It is only
        guaranteed that no larger id exists.

        :param dataset_name: str
        :param table_name: str
        :return: np.uint64
        """
        table_id = build_table_id(dataset_name, table_name)

        if not self._load_table(table_id):
            print("Cannot load table")
            return None

        return self._loaded_tables[table_id].get_max_annotation_id()
