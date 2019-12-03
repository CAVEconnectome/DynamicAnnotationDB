import numpy as np
import time
import datetime
import os
from sqlalchemy import create_engine
from emannotationschemas import models as em_models
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

    def __init__(self, sql_uri):

        self.sql_uri = sql_uri
        self.engine = create_engine(self.sql_uri, echo=False)
        self._SessionMaker = sessionmaker(
                bind=self.sqlalchemy_engine)
        self._session = self._SessionMaker()

        self._loaded_tables = {}


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
            self._loaded_tables[table_id] = AnnotationDB(sql_uri=self.sql_uri)
            return True
        except:
            if table_id in self.get_existing_tables():
                print("Could not load table")
                return False
            else:
                print("Table id does not exist")
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
            self._loaded_tables[table_id] = AnnotationDB(sql_uri=self.sql_uri)
            self._loaded_tables[table_id].delete_table()
            del self._loaded_tables[table_id]
            return True
        else:
            return False

    def _reset_table(self, dataset_name, table_name, n_retries=20, delay_s=5):
        """ Deletes and creates table

        :param dataset_name: str
        :param table_name: str
        :return: bool
        """

        metadata = self.get_table_metadata(dataset_name, table_name)

        if self._delete_table(dataset_name=dataset_name,
                              table_name=table_name):
            for i_try in range(n_retries):
                time.sleep(delay_s)
                try:
                    if self.create_table(**metadata):
                        return True
                except:
                    time.sleep(delay_s)
            return False
        else:
            return False

    def virtual_table(self, table_id):
        if not self._load_table(table_id):
            raise Exception("Cannot load table")

        return self._loaded_tables[table_id]

    def get_serialized_info(self):
        """ Rerturns dictionary that can be used to load this AnnotationMetaDB

        :return: dict
        """
        amdb_info = {"sql_uri": self.sql_uri}

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
        return self.virtual_table(table_id).metadata

    def get_existing_tables(self, dataset_name=None):
        """ Collects table_ids of existing tables

        Annotation tables start with `anno`

        :return: list
        """
        tables = self._session.query(em_models.AnalysisTable).all()
        
        annotation_tables = []
        for table in tables:
            table_id = table.tablename
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
                     lookup_mip_resolution=[8, 8, 40], materialize_table=False,
                     chunk_size=[512, 512, 128], additional_metadata=None):
        """ Creates new table

        :param user_id: str
        :param dataset_name: str
        :param table_name: str
        :param schema_name: str
        :param lookup_mip_resolution: Tuple(int, int, int)
        :param materialize_table: bool
        :param chunk_size: Tuple(int, int, int)
        :param additional_metadata: bytes or None
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
                                                         lookup_mip_resolution=lookup_mip_resolution,
                                                         user_id=user_id,
                                                         materialize_table=materialize_table,
                                                         additional_metadata=additional_metadata,
                                                         is_new=True)
            return True
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

        return self.virtual_table(table_id).insert_annotations(user_id,
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

        return self.virtual_table(table_id).get_annotation(annotation_id,
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

        return self.virtual_table(table_id).get_annotation(annotation_id,
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

        return self.virtual_table(table_id).get_max_annotation_id()

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

        return self.virtual_table(table_id).delete_annotations(user_id,
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

        return self.virtual_table(table_id).update_annotations(user_id,
                                                               annotations)
