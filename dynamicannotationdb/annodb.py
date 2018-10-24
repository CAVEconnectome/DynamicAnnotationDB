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
from google.auth import credentials
from google.cloud.bigtable.row_filters import TimestampRange, \
    TimestampRangeFilter, ColumnRangeFilter, ValueRangeFilter, RowFilterChain, \
    ColumnQualifierRegexFilter, RowFilterUnion, ConditionalRowFilter, \
    PassAllFilter, BlockAllFilter
from google.cloud.bigtable.column_family import MaxVersionsGCRule

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from dynamicannotationdb import key_utils, table_info

# global variables
HOME = os.path.expanduser("~")
N_DIGITS_UINT64 = len(str(np.iinfo(np.uint64).max))
LOCK_EXPIRED_TIME_DELTA = datetime.timedelta(minutes=3, seconds=00)

# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    HOME + "/.cloudvolume/secrets/google-secret.json"


class AnnotationDB(object):
    """ Manages annotations from a single annotation type and dataset """

    def __init__(self, table_id: str,
                 instance_id: str = "pychunkedgraph",
                 project_id: str = "neuromancer-seung-import",
                 chunk_size: Tuple[int, int, int] = None,
                 schema_name: str = None,
                 credentials: Optional[credentials.Credentials] = None,
                 client: bigtable.Client = None,
                 is_new: bool = False):

        if client is not None:
            self._client = client
        else:
            self._client = bigtable.Client(project=project_id, admin=True,
                                           credentials=credentials)

        self._instance = self.client.instance(instance_id)
        self._table_id = table_id

        self._table = self.instance.table(self.table_id)

        if is_new:
            self._check_and_create_table()

        self._chunk_size = self.check_and_write_table_parameters("chunk_size",
                                                                 chunk_size)

        self._schema_name = self.check_and_write_table_parameters("schema_name",
                                                                  schema_name)

        self._bits_per_dim = 12


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
    def bsp_family_id(self):
        return "2"

    @property
    def log_family_id(self):
        return "3"

    @property
    def family_ids(self):
        return [self.data_family_id, self.incrementer_family_id,
                self.log_family_id, self.bsp_family_id]

    @property
    def chunk_size(self) -> np.ndarray:
        return self._chunk_size

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def bits_per_dim(self) -> int:
        return self._bits_per_dim

    @property
    def metadata(self):
        return {"schema_name": self.schema_name,
                "chunk_size": self.chunk_size,
                "table_name": key_utils.get_table_name_from_table_id(self.table_id),
                "max_annotation_id": self.get_max_annotation_id()}

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

            f_bsp = self.table.column_family(self.bsp_family_id)
            f_bsp.create()

            f_log = self.table.column_family(self.log_family_id)
            f_log.create()

            print("Table created")

    def check_and_write_table_parameters(self, param_key: str,
                                         value: Optional[np.uint64] = None
                                         ) -> np.uint64:
        """ Checks if a parameter already exists in the table. If it already
        exists it returns the stored value, else it stores the given value. It
        raises an exception if no value is passed and the parameter does not
        exist, yet.

        :param param_key: str
        :param value: np.uint64
        :return: np.uint64
            value
        """
        ser_param_key = key_utils.serialize_key(param_key)
        row = self.table.read_row(key_utils.serialize_key("params"))

        if row is None or ser_param_key not in row.cells[self.data_family_id]:
            assert value is not None

            if param_key in ["schema_name"]:
                val_dict = {param_key: key_utils.serialize_key(value)}
            elif param_key in ["chunk_size"]:
                val_dict = {param_key: np.array(value,
                                                dtype=np.uint64).tobytes()}
            else:
                raise Exception("Unknown type for parameter")

            row = self.mutate_row(key_utils.serialize_key("params"), self.data_family_id,
                                  val_dict)

            self.bulk_write([row])
        else:
            value = row.cells[self.data_family_id][ser_param_key][0].value

            if param_key in ["schema_name"]:
                value = key_utils.deserialize_key(value)
            elif param_key in ["chunk_size"]:
                value = np.frombuffer(value, dtype=np.uint64)
            else:
                raise Exception("Unknown key")

        return value

    def mutate_row(self, row_key: bytes, column_family_id: str, val_dict: dict,
                   time_stamp: Optional[datetime.datetime] = None
                   ) -> bigtable.row.Row:
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


    def bulk_write(self, rows: Iterable[bigtable.row.DirectRow],
                   # root_ids: Optional[Union[np.uint64,
                   #                          Iterable[np.uint64]]] = None,
                   # operation_id: Optional[np.uint64] = None,
                   slow_retry: bool = True,
                   block_size: int = 2000) -> bool:
        """ Writes a list of mutated rows in bulk

        WARNING: If <rows> contains the same row (same row_key) and column
        key two times only the last one is effectively written to the BigTable
        (even when the mutations were applied to different columns)
        --> no versioning!

        :param rows: list
            list of mutated rows
        :param root_ids: list if uint64
        :param operation_id: uint64 or None
            operation_id (or other unique id) that *was* used to lock the root
            the bulk write is only executed if the root is still locked with
            the same id.
        :param slow_retry: bool
        :param block_size: int
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

        # if root_ids is not None and operation_id is not None:
        #     if isinstance(root_ids, int):
        #         root_ids = [root_ids]
        #
        #     if not self.check_and_renew_root_locks(root_ids, operation_id):
        #         return False

        for i_row in range(0, len(rows), block_size):
            status = self.table.mutate_rows(rows[i_row: i_row + block_size],
                                            retry=retry_policy)

            if not all(status):
                raise Exception(status)

        return True

    def get_chunk_coordinates(self, node_or_chunk_id: np.uint64
                              ) -> np.ndarray:
        """ Extract X, Y and Z coordinate from Node ID or Chunk ID

        :param node_or_chunk_id: np.uint64
        :return: Tuple(int, int, int)
        """

        x_offset = 64 - self.bits_per_dim
        y_offset = x_offset - self.bits_per_dim
        z_offset = y_offset - self.bits_per_dim

        x = int(node_or_chunk_id) >> x_offset & 2 ** self.bits_per_dim - 1
        y = int(node_or_chunk_id) >> y_offset & 2 ** self.bits_per_dim - 1
        z = int(node_or_chunk_id) >> z_offset & 2 ** self.bits_per_dim - 1
        return np.array([x, y, z])

    def get_chunk_id(self, node_id: Optional[np.uint64] = None,
                     x: Optional[int] = None,
                     y: Optional[int] = None,
                     z: Optional[int] = None) -> np.uint64:
        """ (1) Extract Chunk ID from Node ID
            (2) Build Chunk ID from X, Y and Z components

        :param node_id: np.uint64
        :param layer: int
        :param x: int
        :param y: int
        :param z: int
        :return: np.uint64
        """
        assert node_id is not None or \
               all(v is not None for v in [x, y, z])

        if node_id is not None:
            chunk_offset = 64 - 3 * self.bits_per_dim
            return np.uint64((int(node_id) >> chunk_offset) << chunk_offset)
        else:

            if not(x < 2 ** self.bits_per_dim and
                   y < 2 ** self.bits_per_dim and
                   z < 2 ** self.bits_per_dim):
                raise Exception("Chunk coordinate is out of range for"
                                "this graph with %d bits/dim."
                                "[%d, %d, %d]; max = %d."
                                % (self.bits_per_dim, x, y, z,
                                   2 ** self.bits_per_dim))

            x_offset = 64 - self.bits_per_dim
            y_offset = x_offset - self.bits_per_dim
            z_offset = y_offset - self.bits_per_dim
            return np.uint64(x << x_offset | y << y_offset | z << z_offset)

    def get_chunk_ids_from_node_ids(self, node_ids: Iterable[np.uint64]
                                    ) -> np.ndarray:
        """ Extract a list of Chunk IDs from a list of Node IDs

        :param node_ids: np.ndarray(dtype=np.uint64)
        :return: np.ndarray(dtype=np.uint64)
        """
        # TODO: measure and improve performance(?)
        return np.array(list(map(lambda x: self.get_chunk_id(node_id=x),
                                 node_ids)), dtype=np.uint64)

    def get_segment_id_limit(self, node_or_chunk_id: np.uint64) -> np.uint64:
        """ Get maximum possible Segment ID for given Node ID or Chunk ID

        :param node_or_chunk_id: np.uint64
        :return: np.uint64
        """
        chunk_offset = 64 - 3 * self.bits_per_dim
        return np.uint64(2 ** chunk_offset - 1)

    def get_segment_id(self, node_id: np.uint64) -> np.uint64:
        """ Extract Segment ID from Node ID

        :param node_id: np.uint64
        :return: np.uint64
        """

        return node_id & self.get_segment_id_limit(node_id)

    def get_node_id(self, segment_id: np.uint64,
                    chunk_id: Optional[np.uint64] = None,
                    x: Optional[int] = None,
                    y: Optional[int] = None,
                    z: Optional[int] = None) -> np.uint64:
        """ (1) Build Node ID from Segment ID and Chunk ID
            (2) Build Node ID from Segment ID, Layer, X, Y and Z components

        :param segment_id: np.uint64
        :param chunk_id: np.uint64
        :param layer: int
        :param x: int
        :param y: int
        :param z: int
        :return: np.uint64
        """

        if chunk_id is not None:
            return chunk_id | segment_id
        else:
            return self.get_chunk_id(x=x, y=y, z=z) | segment_id

    def get_unique_segment_id_range(self, chunk_id: np.uint64 = None,
                                    step: int = 1) -> np.ndarray:
        """ Return unique Segment ID for given Chunk ID

        atomic counter

        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """

        # Incrementer row keys start with an "i" followed by the chunk id
        if chunk_id is None:
            row_key = table_info.annotation_counter_key_s
        else:
            row_key = key_utils.serialize_key("i%s" % key_utils.pad_node_id(chunk_id))

        append_row = self.table.row(row_key, append=True)
        append_row.increment_cell_value(self.incrementer_family_id,
                                        table_info.counter_key_s, step)

        # This increments the row entry and returns the value AFTER incrementing
        latest_row = append_row.commit()
        max_segment_id_b = latest_row[self.incrementer_family_id][table_info.counter_key_s][0][0]
        max_segment_id = int.from_bytes(max_segment_id_b, byteorder="big")

        min_segment_id = max_segment_id + 1 - step
        segment_id_range = np.array(range(min_segment_id, max_segment_id + 1),
                                    dtype=np.uint64)
        return segment_id_range

    def get_unique_segment_id(self, chunk_id: np.uint64 = None) -> np.uint64:
        """ Return unique Segment ID for given Chunk ID

        atomic counter

        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """

        return self.get_unique_segment_id_range(chunk_id=chunk_id, step=1)[0]

    def get_unique_node_id_range(self, chunk_id: np.uint64 = None, step: int = 1
                                 )  -> np.ndarray:
        """ Return unique Node ID range for given Chunk ID

        atomic counter

        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """

        segment_ids = self.get_unique_segment_id_range(chunk_id=chunk_id,
                                                       step=step)

        node_ids = np.array([self.get_node_id(segment_id, chunk_id)
                             for segment_id in segment_ids], dtype=np.uint64)
        return node_ids

    def get_unique_node_id(self, chunk_id: np.uint64 = None) -> np.uint64:
        """ Return unique Node ID for given Chunk ID

        atomic counter

        :param chunk_id: np.uint64
        :return: np.uint64
        """

        return self.get_unique_node_id_range(chunk_id=chunk_id, step=1)[0]

    def get_max_seg_id(self, chunk_id: np.uint64 = None) -> np.uint64:
        """  Gets maximal seg id in a chunk based on the atomic counter

        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.


        :return: uint64
        """

        # Incrementer row keys start with an "i"
        if chunk_id is None:
            row_key = table_info.annotation_counter_key_s
        else:
            row_key = key_utils.serialize_key("i%s" % key_utils.pad_node_id(chunk_id))
        row = self.table.read_row(row_key)

        # Read incrementer value
        if row is not None:
            max_node_id_b = row.cells[self.incrementer_family_id][table_info.counter_key_s][0].value
            max_node_id = int.from_bytes(max_node_id_b, byteorder="big")
        else:
            max_node_id = 0

        return np.uint64(max_node_id)

    def get_max_node_id(self, chunk_id: np.uint64 = None) -> np.uint64:
        """  Gets maximal node id in a chunk based on the atomic counter

        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.


        :return: uint64
        """

        max_seg_id = self.get_max_seg_id(chunk_id)
        return self.get_node_id(segment_id=max_seg_id, chunk_id=chunk_id)

    def get_unique_annotation_id_range(self, step: int = 1):
        """ Return unique Node ID for given Chunk ID

        atomic counter

        :return: uint64
        """

        return self.get_unique_segment_id_range(chunk_id=None, step=step)

    def get_unique_annotation_id(self):
        """ Return unique Node ID for given Chunk ID

        atomic counter

        :return: uint64
        """

        return self.get_unique_annotation_id_range(step=1)

    def get_max_annotation_id(self):
        """ Gets maximal annotation id in the table based on atomic counter

        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.

        :return: uint64
        """

        return self.get_max_seg_id(chunk_id=None)

    def get_unique_operation_id(self) -> np.uint64:
        """ Finds a unique operation id

        atomic counter

        Operations essentially live in layer 0. Even if segmentation ids might
        live in layer 0 one day, they would not collide with the operation ids
        because we write information belonging to operations in a separate
        family id.

        :return: str
        """
        append_row = self.table.row(table_info.operation_counter_key_s, append=True)
        append_row.increment_cell_value(self.incrementer_family_id,
                                        table_info.counter_keys_s, 1)

        # This increments the row entry and returns the value AFTER incrementing
        latest_row = append_row.commit()
        operation_id_b = latest_row[self.incrementer_family_id][table_info.counter_keys_s][0][0]
        operation_id = int.from_bytes(operation_id_b, byteorder="big")

        return np.uint64(operation_id)

    def get_max_operation_id(self) -> np.uint64:
        """  Gets maximal operation id based on the atomic counter

        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.


        :return: uint64
        """
        row = self.table.read_row(table_info.operation_counter_key_s)

        # Read incrementer value
        if row is not None:
            max_operation_id_b = row.cells[self.incrementer_family_id][table_info.counter_key_s][0].value
            max_operation_id = int.from_bytes(max_operation_id_b,
                                              byteorder="big")
        else:
            max_operation_id = 0

        return np.uint64(max_operation_id)

    def _write_annotation_data(self, annotation_id, annotation_data, bsp_dict,
                               time_stamp=None):

        rows = []
        val_dict = {table_info.blob_key_s: annotation_data}
        anno_id_b = np.array(annotation_id, dtype=np.uint64).tobytes()
        for k in bsp_dict:
            val_dict[k] = np.array(bsp_dict[k]["id"], dtype=np.uint64).tobytes()
            coord = np.array(bsp_dict[k]["coordinate"], dtype=np.float32)
            coord_b = coord.tobytes()
            name_b = key_utils.serialize_key(k)

            rows.append(self.mutate_row(key_utils.serialize_uint64(bsp_dict[k]["id"]),
                                        self.bsp_family_id,
                                        {table_info.anno_id_key_s: anno_id_b,
                                         table_info.coordinate_key_s: coord_b,
                                         table_info.bsp_name_key_s: name_b},
                                        time_stamp=time_stamp))

        rows.append(self.mutate_row(key_utils.serialize_uint64(annotation_id),
                                    self.data_family_id,
                                    val_dict, time_stamp=time_stamp))
        return rows

    def insert_annotations(self, user_id, annotations, bulk_block_size=2000):
        """ Inserts new annotations into the database and returns assigned ids

        :param user_id: str
        :param annotations: list
            list of annotations (data)
        :param bulk_block_size: int
        :return: list of uint64
            assigned ids (in same order as `annotations`)
        """

        time_stamp = datetime.datetime.utcnow()

        rows = []

        print("N annotations:", len(annotations))
        id_range = self.get_unique_annotation_id_range(step=len(annotations))

        for i_annotation, annotation in enumerate(annotations):
            bsps, annotation_data = annotation

            # Get unique ids
            annotation_id = id_range[i_annotation]

            bsp_dict = {}
            for bsp_k in bsps:
                bsp_dict[bsp_k] = {}
                bsp_coord = np.array(bsps[bsp_k])
                bsp_dict[bsp_k]['coordinate'] = bsp_coord

                bsp_chunk_coord = bsp_coord / self.chunk_size
                bsp_chunk_coord = bsp_chunk_coord.astype(np.int)
                chunk_id = self.get_chunk_id(x=bsp_chunk_coord[0],
                                             y=bsp_chunk_coord[1],
                                             z=bsp_chunk_coord[2])
                bsp_dict[bsp_k]['id'] = self.get_unique_node_id(chunk_id)

            # Write data
            rows.extend(self._write_annotation_data(annotation_id,
                                                    annotation_data,
                                                    bsp_dict,
                                                    time_stamp=time_stamp))

            if len(rows) >= bulk_block_size:
                self.bulk_write(rows)
                rows = []

        if len(rows) > 0:
            self.bulk_write(rows)

        return id_range

    def delete_annotations(self, user_id, annotation_ids,
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

        for annotation_id in annotation_ids:
            rows = [self._write_annotation_data(annotation_id,
                                                np.array([]).tobytes(),
                                                np.array([]),
                                                time_stamp=time_stamp)]

            success_marker.append(True)

            if len(rows) >= bulk_block_size:
                self.bulk_write(rows)
                rows = []

        if len(rows) >= bulk_block_size:
            self.bulk_write(rows)
            rows = []

        return success_marker


    # def update_annotations(self, annotations, user_id,
    #                        bulk_block_size=10000):
    #     """ Updates existing annotations
    #
    #     :param annotation_ids: list of uint64s
    #         annotations (key: annotation id)
    #     :param user_id: str
    #     :return: bool
    #         success
    #     """
    #
    #     time_stamp = datetime.datetime.utcnow()
    #
    #     # TODO: lock
    #
    #     rows = []
    #     success_marker = []
    #     new_sv_mapping_dict = collections.defaultdict(list)
    #     old_sv_mapping_dict = collections.defaultdict(list)
    #
    #     for annotation in annotations:
    #         annotation_id, sv_ids, annotation_data = annotation
    #
    #         old_sv_ids = self.get_annotation_sv_ids(annotation_id)
    #
    #         if old_sv_ids is None:
    #             success_marker.append(False)
    #             continue
    #
    #         if len(old_sv_ids) == 0:
    #             success_marker.append(False)
    #             continue
    #
    #         sv_id_mask = old_sv_ids != sv_ids
    #
    #         for sv_id in np.unique(sv_ids[sv_id_mask]):
    #             new_sv_mapping_dict[sv_id].append(annotation_id)
    #
    #         for sv_id in np.unique(old_sv_ids[sv_id_mask]):
    #             old_sv_mapping_dict[sv_id].append(annotation_id)
    #
    #         rows = [self._write_annotation_data(annotation_id,
    #                                             annotation_data,
    #                                             sv_ids,
    #                                             time_stamp=time_stamp)]
    #
    #         success_marker.append(True)
    #
    #         if len(rows) >= bulk_block_size / 3:
    #             rows.extend(self._write_sv_mapping(old_sv_mapping_dict,
    #                                                add=False,
    #                                                is_new=False))
    #             rows.extend(self._write_sv_mapping(new_sv_mapping_dict,
    #                                                add=True,
    #                                                is_new=False))
    #
    #             self.bulk_write(rows)
    #
    #             new_sv_mapping_dict = collections.defaultdict(list)
    #             old_sv_mapping_dict = collections.defaultdict(list)
    #             rows = []
    #
    #     if len(rows) > 0:
    #         rows.extend(self._write_sv_mapping(old_sv_mapping_dict,
    #                                            add=False,
    #                                            is_new=False))
    #         rows.extend(self._write_sv_mapping(new_sv_mapping_dict,
    #                                            add=True,
    #                                            is_new=False))
    #
    #         self.bulk_write(rows)
    #
    #     return success_marker

    def get_annotation(self, annotation_id, time_stamp=None):
        """ Reads the data and bsps of a single annotation object

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

        row = self.table.read_row(key_utils.serialize_uint64(annotation_id),
                                  filter_=time_filter)
        cells = row.cells[self.data_family_id]

        bsps = {}
        bin_data = None
        for k in cells:
            value = cells[k][0].value
            if k == table_info.blob_key_s:
                bin_data = value
            else:
                bsps[key_utils.deserialize_key(k)] = np.frombuffer(value, dtype=np.uint64)[0]

        if bin_data is None:
            return None, None

        return bin_data, bsps

    def get_bsp(self, bsp_id, time_stamp=None):
        """ Reads the data and bsps of a single annotation object

        :param bsp_id: uint64
        :param time_stamp: None or datetime
        :return: blob, list of np.uint64
        """

        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        # Adjust time_stamp to bigtable precision
        time_stamp -= datetime.timedelta(
            microseconds=time_stamp.microsecond % 1000)

        time_filter = TimestampRangeFilter(TimestampRange(end=time_stamp))

        row = self.table.read_row(key_utils.serialize_uint64(bsp_id),
                                  filter_=time_filter)
        cells = row.cells[self.bsp_family_id]

        coordinate = np.frombuffer(cells[table_info.coordinate_key_s][0].value, dtype=np.float32)
        annotation_id = np.frombuffer(cells[table_info.anno_id_key_s][0].value, dtype=np.uint64)[0]

        return coordinate, annotation_id
