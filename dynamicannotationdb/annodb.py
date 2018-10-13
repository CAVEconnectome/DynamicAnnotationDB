import collections
import numpy as np
import time
import datetime
import os
import pytz

# from . import multiprocessing_utils as mu
from google.api_core.retry import Retry, if_exception_type
from google.api_core.exceptions import Aborted, DeadlineExceeded, \
    ServiceUnavailable
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import TimestampRange, \
    TimestampRangeFilter, ColumnRangeFilter, ValueRangeFilter, RowFilterChain, \
    ColumnQualifierRegexFilter, RowFilterUnion, ConditionalRowFilter, \
    PassAllFilter, BlockAllFilter
from google.cloud.bigtable.column_family import MaxVersionsGCRule

# global variables
HOME = os.path.expanduser("~")
N_DIGITS_UINT64 = len(str(np.iinfo(np.uint64).max))
LOCK_EXPIRED_TIME_DELTA = datetime.timedelta(minutes=3, seconds=00)

# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    HOME + "/.cloudvolume/secrets/google-secret.json"


def serialize_key(key):
    """ Serializes a key to be ingested by a bigtable table row

    :param key: str
    :return: str
    """
    return key.encode("utf-8")


def serialize_node_id(node_id):
    """ Serializes an id to be ingested by a bigtable table row

    :param node_id: int
    :return: str
    """
    return serialize_key("%.20d" % node_id)


def deserialize_node_id(node_id):
    """ De-serializes a node id from a BigTable row

    :param node_id: int
    :return: str
    """
    return int(node_id.decode())


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


class AnnotationDB(object):
    """ Manages annotations from a single annotation type and dataset """

    def __init__(self, table_id, instance_id="pychunkedgraph",
                 project_id="neuromancer-seung-import", client=None,
                 instance=None, credentials=None, is_new=False):

        if client is not None and instance is not None:
            self._client = client
            self._instance = instance
        else:
            self._client = bigtable.Client(project=project_id, admin=True,
                                           credentials=credentials)
            self._instance = self.client.instance(instance_id)

        self._table_id = table_id

        self._table = self.instance.table(self.table_id)

        if is_new:
            self._check_and_create_table()

    @property
    def client(self):
        return self._client

    @property
    def instance(self):
        return self._instance

    @property
    def table(self):
        return self._table

    @property
    def table_id(self):
        return self._table_id

    @property
    def data_family_id(self):
        return "0"

    @property
    def incrementer_family_id(self):
        return "1"

    @property
    def mapping_family_id(self):
        return "2"

    @property
    def log_family_id(self):
        return "3"

    def _check_and_create_table(self):
        """ Checks if table exists and creates new one if necessary """
        table_ids = [t.table_id for t in self.instance.list_tables()]

        if not self.table_id in table_ids:
            self.table.create()
            f_data = self.table.column_family(self.data_family_id)
            f_data.create()

            f_inc = self.table.column_family(self.incrementer_family_id,
                                             gc_rule=MaxVersionsGCRule(1))
            f_inc.create()

            f_map = self.table.column_family(self.mapping_family_id)
            f_map.create()

            f_log = self.table.column_family(self.log_family_id)
            f_log.create()

            print("Table created")

    def _mutate_row(self, row_key, column_family_id, val_dict, time_stamp=None):
        """ Mutates a single row

        :param row_key: serialized bigtable row key
        :param column_family_id: str
            serialized column family id
        :param val_dict: dict
        :param time_stamp: None or datetime
        :return: list
        """
        row = self.table.row(row_key)

        for column, value in val_dict.items():
            row.set_cell(column_family_id=column_family_id, column=column,
                         value=value, timestamp=time_stamp)
        return row

    def _bulk_write(self, rows, annotation_ids=None, operation_id=None,
                    slow_retry=True):
        """ Writes a list of mutated rows in bulk

        WARNING: If <rows> contains the same row (same row_key) and column
        key two times only the last one is effectively written to the BigTable
        (even when the mutations were applied to different columns)
        --> no versioning!

        :param rows: list
            list of mutated rows
        :param annotation_ids: list if uint64
        :param operation_id: str or None
            operation_id (or other unique id) that *was* used to lock the root
            the bulk write is only executed if the root is still locked with
            the same id.
        :param slow_retry: bool
        """
        if slow_retry:
            initial = 5
        else:
            initial = 1

        retry_policy = Retry(
            predicate=if_exception_type((Aborted,
                                         DeadlineExceeded,
                                         ServiceUnavailable)),
            initial=initial,
            maximum=15.0,
            multiplier=2.0,
            deadline=LOCK_EXPIRED_TIME_DELTA.seconds)

        if annotation_ids is not None and operation_id is not None:
            if isinstance(annotation_ids, int):
                annotation_ids = [annotation_ids]

            if not self._check_and_renew_annotation_locks(annotation_ids,
                                                          operation_id):
                return False

        status = self.table.mutate_rows(rows, retry=retry_policy)

        if not any(status):
            raise Exception(status)

        return True

    def _lock_annotation_loop(self, annoation_id, operation_id, max_tries=1,
                              waittime_s=0.5):
        """ Attempts to lock multiple roots at the same time

        :param annoation_id: uint64
        :param operation_id: str
        :param max_tries: int
        :param waittime_s: float
        :return: bool, list of uint64s
            success, latest root ids
        """

        i_try = 0
        while i_try < max_tries:

            # Attempt to lock annoation id
            lock_acquired = self._lock_single_annotation(annoation_id,
                                                         operation_id)

            if lock_acquired:
                return True

            time.sleep(waittime_s)
            i_try += 1
            print(i_try)

        return False

    def _lock_single_annotation(self, annotation_id, operation_id):
        """ Attempts to lock the latest version of a root node

        :param annotation_id: uint64
        :param operation_id: str
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """

        operation_id_b = serialize_key(operation_id)

        lock_key = serialize_key("lock")

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if it is still valid (timestamp younger than
        # LOCK_EXPIRED_TIME_DELTA)

        time_cutoff = datetime.datetime.utcnow() - LOCK_EXPIRED_TIME_DELTA

        # Comply to resolution of BigTables TimeRange
        time_cutoff -= datetime.timedelta(
            microseconds=time_cutoff.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(start=time_cutoff))

        lock_key_filter = ColumnRangeFilter(column_family_id=self.data_family_id,
                                            start_column=lock_key,
                                            end_column=lock_key,
                                            inclusive_start=True,
                                            inclusive_end=True)

        # Combine filters together
        chained_filter = RowFilterChain([time_filter, lock_key_filter])

        # Get conditional row using the chained filter
        annotation_row = self.table.row(serialize_node_id(annotation_id),
                                        filter_=chained_filter)

        # Set row lock if condition returns no results (state == False)
        annotation_row.set_cell(self.data_family_id, lock_key, operation_id_b,
                                state=False)

        # The lock was acquired when set_cell returns False (state)
        lock_acquired = not annotation_row.commit()

        return lock_acquired

    def _unlock_annotation(self, annotation_id, operation_id):
        """ Unlocks a root

        This is mainly used for cases where multiple roots need to be locked and
        locking was not sucessful for all of them

        :param annotation_id: uint64
        :param operation_id: str
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        operation_id_b = serialize_key(operation_id)

        lock_key = serialize_key("lock")

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if it is still valid (timestamp younger than
        # LOCK_EXPIRED_TIME_DELTA) and if the given operation_id is still
        # the active lock holder

        time_cutoff = datetime.datetime.utcnow() - LOCK_EXPIRED_TIME_DELTA

        # Comply to resolution of BigTables TimeRange
        time_cutoff -= datetime.timedelta(
            microseconds=time_cutoff.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(start=time_cutoff))

        column_key_filter = ColumnQualifierRegexFilter(lock_key)

        value_filter = ColumnQualifierRegexFilter(operation_id_b)

        # Chain these filters together
        chained_filter = RowFilterChain([time_filter, column_key_filter,
                                         value_filter])

        # Get conditional row using the chained filter
        root_row = self.table.row(serialize_node_id(annotation_id),
                                  filter_=chained_filter)

        # Delete row if conditions are met (state == True)
        root_row.delete_cell(self.data_family_id, lock_key, state=True)

        root_row.commit()

    def _check_and_renew_annotation_locks(self, annotation_ids, operation_id):
        """ Tests if the roots are locked with the provided operation_id and
        renews the lock to reset the time_stam

        This is mainly used before executing a bulk write

        :param annotation_ids: uint64
        :param operation_id: str
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """

        for annotation_id in annotation_ids:
            if not self._check_and_renew_annotation_lock_single(annotation_id,
                                                                operation_id):
                return False

        return True

    def _check_and_renew_annotation_lock_single(self, root_id, operation_id):
        """ Tests if the root is locked with the provided operation_id and
        renews the lock to reset the time_stam

        This is mainly used before executing a bulk write

        :param root_id: uint64
        :param operation_id: str
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        operation_id_b = serialize_key(operation_id)

        lock_key = serialize_key("lock")
        new_parents_key = serialize_key("new_parents")

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if the given operation_id is still the active lock holder.

        column_key_filter = ColumnQualifierRegexFilter(operation_id_b)
        value_filter = ColumnQualifierRegexFilter(operation_id_b)

        # Chain these filters together
        chained_filter = RowFilterChain([column_key_filter, value_filter])

        # Get conditional row using the chained filter
        root_row = self.table.row(serialize_node_id(root_id),
                                  filter_=chained_filter)

        # Set row lock if condition returns a result (state == True)
        root_row.set_cell(self.data_family_id, lock_key, operation_id_b,
                          state=False)

        # The lock was acquired when set_cell returns True (state)
        lock_acquired = not root_row.commit()

        return lock_acquired

    def _get_unique_annotation_id(self):
        """ Return unique Node ID for given Chunk ID

        atomic counter

        :return: uint64
        """

        # Incrementer row keys start with an "i"
        row_key = serialize_key("iannotations")
        append_row = self.table.row(row_key, append=True)
        append_row.increment_cell_value(self.incrementer_family_id,
                                        serialize_key("counter"), 1)

        # This increments the row entry and returns the value AFTER incrementing
        latest_row = append_row.commit()
        annotation_id = int.from_bytes(
            latest_row[self.incrementer_family_id][serialize_key('counter')][0][
                0], byteorder="big")

        return np.uint64(annotation_id)

    def get_max_annotation_id(self):
        """ Gets maximal annotation id in the table based on atomic counter

        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.

        :return: uint64
        """

        # Incrementer row keys start with an "i"
        row_key = serialize_key("iannotations")
        row = self.table.read_row(row_key)

        # Read incrementer value
        if row is not None:
            annotation_id = int.from_bytes(row.cells[self.incrementer_family_id][serialize_key('counter')][0].value, byteorder="big")
        else:
            annotation_id = 0

        return np.uint64(annotation_id)

    def _get_unique_operation_id(self):
        """ Finds a unique operation id

        atomic counter

        :return: str
        """

        # Incrementer row keys start with an "i"
        row_key = serialize_key("ioperations")
        append_row = self.table.row(row_key, append=True)
        append_row.increment_cell_value(self.incrementer_family_id,
                                        "counter", 1)

        # Commit increments the row entry and returns the value AFTER
        # incrementing
        latest_row = append_row.commit()
        latest_row_entry = latest_row[self.incrementer_family_id][serialize_key(
            'counter')][0][0]

        operation_id = "op%d" % int.from_bytes(latest_row_entry,
                                               byteorder="big")

        return operation_id

    def _create_log_row(self, operation_id, user_id, annotation_id, time_stamp):
        val_dict = {serialize_key("user"): serialize_key(user_id),
                    serialize_key("annotation"):
                        np.array(annotation_id, dtype=np.uint64).tobytes()}

        row = self._mutate_row(serialize_key(operation_id),
                               self.log_family_id, val_dict, time_stamp)

        return row

    def _write_sv_mapping(self, sv_mapping_dict, time_stamp=None, add=True,
                          is_new=True):
        rows = []

        for sv_id in sv_mapping_dict.keys():
            if not is_new:
                mapped_anno_ids = self.get_annotation_ids_from_sv(sv_id)

                relevant_anno_ids = []
                for anno_id in sv_mapping_dict[sv_id]:
                    if add and anno_id not in mapped_anno_ids:
                       relevant_anno_ids.append(anno_id)
                    elif not add and anno_id in mapped_anno_ids:
                       relevant_anno_ids.append(anno_id)
            else:
                relevant_anno_ids = sv_mapping_dict[sv_id]

            relevant_anno_ids_bin = np.array(relevant_anno_ids,
                                             dtype=np.uint64).tobytes()
            rows.append(self._mutate_row(serialize_node_id(sv_id),
                                         self.mapping_family_id,
                                         {"mapped_anno_ids": relevant_anno_ids_bin},
                                         time_stamp=time_stamp))
        return rows

    def _write_annotation_data(self, annotation_id, annotation_data, sv_ids,
                               time_stamp=None):

        m_row = self._mutate_row(serialize_node_id(annotation_id),
                                 self.data_family_id,
                                 {"data": annotation_data,
                                  "sv_ids": sv_ids.tobytes()},
                                 time_stamp=time_stamp)

        return m_row

    def insert_annotations(self, annotations, user_id, bulk_block_size=10):
        """ Inserts new annotations into the database and returns assigned ids

        :param annotations: list
            list of annotations (data)
        :param user_id: str
        :return: list of uint64
            assigned ids (in same order as `annotations`)
        """

        time_stamp = datetime.datetime.utcnow()

        rows = []
        sv_mapping_dict = collections.defaultdict(list)

        anno_ids = []

        for annotation in annotations:
            sv_ids, annotation_data = annotation

            # Get unique id
            annotation_id = self._get_unique_annotation_id()

            # Write data
            rows.append(self._write_annotation_data(annotation_id,
                                                    annotation_data,
                                                    sv_ids,
                                                    time_stamp=time_stamp))

            for sv_id in np.unique(sv_ids):
                sv_mapping_dict[sv_id].append(annotation_id)

            anno_ids.append(annotation_id)

            if len(rows) >= bulk_block_size / 2:
                rows.extend(self._write_sv_mapping(sv_mapping_dict))

                self._bulk_write(rows)

                sv_mapping_dict = collections.defaultdict(list)
                rows = []

        if len(rows) > 0:
            rows.extend(self._write_sv_mapping(sv_mapping_dict))

            self._bulk_write(rows)

        return anno_ids

    def delete_annotations(self, annotation_ids, user_id,
                           bulk_block_size=10000):
        """ Deletes annotations from the database

        :param annotation_ids: list of uint64s
            annotations (key: annotation id)
        :param user_id: str
        :return: bool
            success
        """

        time_stamp = datetime.datetime.utcnow()

        # TODO: lock

        rows = []
        success_marker = []
        sv_mapping_dict = collections.defaultdict(list)

        for annotation_id in annotation_ids:
            old_sv_ids = self.get_annotation_sv_ids(annotation_id)

            if old_sv_ids is None:
                success_marker.append(False)
                continue

            if len(old_sv_ids) == 0:
                success_marker.append(False)
                continue

            for sv_id in np.unique(old_sv_ids):
                sv_mapping_dict[sv_id].append(annotation_id)

            rows = [self._write_annotation_data(annotation_id,
                                                np.array([]).tobytes(),
                                                np.array([]),
                                                time_stamp=time_stamp)]

            success_marker.append(True)

            if len(rows) >= bulk_block_size / 2:
                rows.extend(self._write_sv_mapping(sv_mapping_dict, add=False,
                                                   is_new=False))

                self._bulk_write(rows)

                sv_mapping_dict = collections.defaultdict(list)
                rows = []

        if len(rows) > 0:
            rows.extend(self._write_sv_mapping(sv_mapping_dict, add=False,
                                               is_new=False))

            self._bulk_write(rows)

        return success_marker

    def update_annotations(self, annotations, user_id,
                           bulk_block_size=10000):
        """ Updates existing annotations

        :param annotation_ids: list of uint64s
            annotations (key: annotation id)
        :param user_id: str
        :return: bool
            success
        """

        time_stamp = datetime.datetime.utcnow()

        # TODO: lock

        rows = []
        success_marker = []
        new_sv_mapping_dict = collections.defaultdict(list)
        old_sv_mapping_dict = collections.defaultdict(list)

        for annotation in annotations:
            annotation_id, sv_ids, annotation_data = annotation

            old_sv_ids = self.get_annotation_sv_ids(annotation_id)

            if old_sv_ids is None:
                success_marker.append(False)
                continue

            if len(old_sv_ids) == 0:
                success_marker.append(False)
                continue

            sv_id_mask = old_sv_ids != sv_ids

            for sv_id in np.unique(sv_ids[sv_id_mask]):
                new_sv_mapping_dict[sv_id].append(annotation_id)

            for sv_id in np.unique(old_sv_ids[sv_id_mask]):
                old_sv_mapping_dict[sv_id].append(annotation_id)

            rows = [self._write_annotation_data(annotation_id,
                                                annotation_data,
                                                sv_ids,
                                                time_stamp=time_stamp)]

            success_marker.append(True)

            if len(rows) >= bulk_block_size / 3:
                rows.extend(self._write_sv_mapping(old_sv_mapping_dict,
                                                   add=False,
                                                   is_new=False))
                rows.extend(self._write_sv_mapping(new_sv_mapping_dict,
                                                   add=True,
                                                   is_new=False))

                self._bulk_write(rows)

                new_sv_mapping_dict = collections.defaultdict(list)
                old_sv_mapping_dict = collections.defaultdict(list)
                rows = []

        if len(rows) > 0:
            rows.extend(self._write_sv_mapping(old_sv_mapping_dict,
                                               add=False,
                                               is_new=False))
            rows.extend(self._write_sv_mapping(new_sv_mapping_dict,
                                               add=True,
                                               is_new=False))

            self._bulk_write(rows)

        return success_marker

    def get_annotation_ids_from_sv(self, sv_id, time_stamp=None):
        """ Acquires all annotation ids associated with a supervoxel

        To also read the data of the acquired annotations use
        `get_annotations_from_sv`

        :param sv_id: uint64
        :param time_stamp: None or datetime
        :return: list
            annotation ids
        """

        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        # Adjust time_stamp to bigtable precision
        time_stamp -= datetime.timedelta(
            microseconds=time_stamp.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(end=time_stamp))

        # Read mapped entries with time_stamp
        row = self.table.read_row(
            serialize_node_id(sv_id), filter_=time_filter)

        if row is None:
            return []

        if not self.mapping_family_id in row.cells:
            return []

        anno_id_entries = row.cells[self.mapping_family_id][serialize_key(
            "mapped_anno_ids")]
        anno_ids = []
        for entry in anno_id_entries:
            # print(len(np.frombuffer(entry.value, dtype=np.uint64)))
            anno_ids.extend(np.frombuffer(entry.value, dtype=np.uint64))

        # Resolve changes over time
        anno_ids, c_anno_ids = np.unique(anno_ids, return_counts=True)

        # Every anno_id with number of entries % 2 == 0 was removed
        anno_ids = anno_ids[c_anno_ids % 2 == 1]

        return anno_ids

    def get_annotation(self, annotation_id, time_stamp=None):
        """ Reads the data and sv_ids of a single annotation object

        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob, list of np.uint64
        """

        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        # Adjust time_stamp to bigtable precision
        time_stamp -= datetime.timedelta(
            microseconds=time_stamp.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(end=time_stamp))

        row = self.table.read_row(serialize_node_id(annotation_id),
                                  filter_=time_filter)

        bin_data = row.cells[self.data_family_id][serialize_key("data")][0].value

        if len(bin_data) == 0:
            return None, None

        sv_ids_bin = row.cells[self.data_family_id][serialize_key("sv_ids")][0].value

        if len(sv_ids_bin) == 0:
            return bin_data, None

        sv_ids = np.frombuffer(sv_ids_bin, dtype=np.uint64)

        return bin_data, sv_ids

    def get_annotation_data(self, annotation_id, time_stamp=None):
        """ Reads the data of a single annotation object

        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob
        """

        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        # Adjust time_stamp to bigtable precision
        time_stamp -= datetime.timedelta(
            microseconds=time_stamp.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(end=time_stamp))

        row = self.table.read_row(serialize_node_id(annotation_id),
                                  filter_=time_filter)

        bin_data = row.cells[self.data_family_id][serialize_key("data")][0].value

        if len(bin_data) == 0:
            return None

        return bin_data

    def get_annotation_sv_ids(self, annotation_id, time_stamp=None):
        """ Reads the sv ids belonging to an annotation

        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: list of np.uint64s
        """

        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        # Adjust time_stamp to bigtable precision
        time_stamp -= datetime.timedelta(
            microseconds=time_stamp.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(end=time_stamp))

        row = self.table.read_row(serialize_node_id(annotation_id),
                                  filter_=time_filter)

        if row is None:
            return []

        # for entry in row.cells[self.data_family_id][serialize_key("sv_ids")]:
        #     print(entry.timestamp)

        sv_ids_bin = row.cells[self.data_family_id][serialize_key("sv_ids")][0].value

        if len(sv_ids_bin) == 0:
            return None

        sv_ids = np.frombuffer(sv_ids_bin, dtype=np.uint64)

        return sv_ids

    def get_annotations_from_sv(self, sv_id, time_stamp=None):
        """ Collects the data from all annotations associated with a supervoxel

        This function chains `get_annotation_ids_from_sv` and
        `get_annotation_data`

        :param sv_id: uint64
        :param time_stamp: None or datetime
        :return: dict
            annotations with keys of ids and values of json blobs
        """

        annotation_ids = self.get_annotation_ids_from_sv(sv_id,
                                                         time_stamp=time_stamp)

        annotation_dict = {}
        for annotation_id in annotation_ids:
            annotation_dict[annotation_id] = self.get_annotation_data(annotation_id)

        return annotation_dict
