from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.errors import TableNameNotFoundException
from dynamicannotationdb.key_utils import get_table_name_from_table_id
from typing import List
import json


class DynamicAnnotationClient:
    def __init__(self, aligned_volume: str, sql_base_uri: str):

        sql_base_uri = sql_base_uri.rpartition("/")[0]
        sql_uri = f"{sql_base_uri}/{aligned_volume}"

        self._client = DynamicAnnotationInterface(sql_uri)
        self.aligned_volume = aligned_volume

        self._table = None
        self._cached_schemas = {}

    @property
    def session(self):
        return self._client.cached_session

    @property
    def table(self):
        return self._table

    def load_table(self, table_name: str):
        self._table = self._client.cached_table(table_name)
        return self._table

    def has_table(self, table_name: str) -> bool:
        return table_name in self._client.get_existing_tables()

    def get_existing_tables(self):
        table_ids = self._client.get_existing_tables()
        table_names = [get_table_name_from_table_id(tid) for tid in table_ids]
        return table_names

    def get_aligned_volume_tables(self, aligned_volume: str):
        return self._client.get_dataset_tables(aligned_volume=aligned_volume)

    def get_table_metadata(self,aligned_volume: str, table_name: str) -> dict:
        return self._client.get_table_metadata(aligned_volume, table_name)

    def get_table_schema(self, table_name: str):
        table_metadata = self.get_table_metadata(
            self.aligned_volume, table_name)
        return table_metadata['schema_type']

    def get_existing_tables_metadata(self, aligned_volume: str, table_name: str) -> list:

        return [
            self.get_table_metadata(aligned_volume, table_name=table_name)
            for table_id in self._client.get_dataset_tables(table_name)
        ]

    def get_annotation_table_length(self, aligned_volume: str, table_name: str) -> int:
        return self._client.get_annotation_table_size(aligned_volume, table_name)

    def create_table(self, table_name: str,
                           schema_type: str,
                           metadata_dict: dict):

        return self._client.create_table(self.aligned_volume,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

    def drop_table(self, table_name: str) -> bool:
        return self._client.drop_table(table_name)

    def insert_annotations(self, table_name: str,
                                 schema_type: str,
                                 annotations: list):
        try:
            self._client.insert_annotations(self.aligned_volume,
                                            table_name,
                                            schema_type,
                                            annotations)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

    def get_annotations(self, table_name: str, 
                              schema_type: str,
                              annotation_ids: List[int]):
        try:
            return self._client.get_annotations(self.aligned_volume,
                                                table_name,
                                                schema_type,
                                                annotation_ids)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

    def delete_annotations(self, table_name: str,
                                 schema_type: str,
                                 annotation_ids: int):

        self._client.delete_annotations(self.aligned_volume,
                                       table_name,
                                       annotation_ids)

    def update_annotations(self, table_name: str,
                                 schema_type: str,
                                 anno_id: int,
                                 annotations: dict):
        try:
            self._client.update_annotations(self.aligned_volume,
                                            table_name,
                                            schema_type,
                                            anno_id,
                                            annotations)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

