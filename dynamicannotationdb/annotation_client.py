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
        """Load a table

        Args:
            table_name (str): name of table

        Returns:
            DeclarativeMeta: the sqlalchemy table of that name
        """
        self._table = self._client.cached_table(table_name)
        return self._table

    def has_table(self, table_name: str) -> bool:
        """check if a table exists

        Args:
            table_name (str): name of the table

        Returns:
            bool: whether the table exists
        """
        return table_name in self._client.get_existing_tables()

    def get_existing_tables(self):
        """get the table names of table that exist

        Returns:
            list(str): list of table names that exist
        """
        table_ids = self._client.get_existing_tables()
        table_names = [get_table_name_from_table_id(tid) for tid in table_ids]
        return table_names

    def get_table_metadata(self, table_name: str) -> dict:
        """get the metadata for this table_name

        Args:
            table_name (str): name of the table

        Returns:
            dict: the meta data (from sql) for this table
        """
        return self._client.get_table_metadata(self.aligned_volume, table_name)

    def get_table_schema(self, table_name: str) -> str:
        """get the schema type of this table

        Args:
            table_name (str): name of the table

        Returns:
            str: the schema type of this table
        """
        table_metadata = self.get_table_metadata( table_name)
        return table_metadata['schema_type']

    def get_existing_tables_metadata(self) -> list:
        """get all the metadata for all tables

        Returns:
            list(dict): all table metadata (from sql) that exist
        """
        return [
            self.get_table_metadata(table_name=table_name)
            for table_name in self.get_existing_tables()
        ]

    def get_annotation_table_length(self,  table_name: str) -> int:
        """get the number of annotations in a table

        Args:
            table_name (str): name of table

        Returns:
            int: number of annotations
        """
        return self._client.get_annotation_table_size(self.aligned_volume, table_name)

    def create_table(self, table_name: str,
                           schema_type: str,
                           metadata_dict: dict):
        """create a new table

        Args:
            table_name (str): name of new table
            schema_type (str): type of schema for that table
            metadata_dict (dict): metadata to attach
                {'description': "a string with a human readable explanation \
                                 of what is in the table. Including who made it"
                 'user_id': "user_id"
                 'reference_table': "reference table name, if required by this schema" }
                

        Returns:
            dict: a set of messages about created table
                keys include
                Created Succesfully: whether it was made
                Table Name: name of table that was createsd
                Description: description of table that was created
        """
        # TODO: check that schemas that are reference schemas
        # have a reference_table in their metadata
        return self._client.create_table(self.aligned_volume,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

    def drop_table(self, table_name: str) -> bool:
        """drop a table

        Args:
            table_name (str): name of table to drop

        Returns:
            bool: whether drop was successful
        """
        return self._client.drop_table(table_name)

    def insert_annotations(self, table_name: str,
                                 schema_type: str,
                                 annotations: list):
        """insert some annotations

        Args:
            table_name (str): name of table to insert
            schema_type (str): schema for that table
            annotations (list): a list of dicts with the annotations
                                that meet the schema

        Returns:
           None
        Raises:
            TODO: What kind of exceptions does this raise
        """
        # TODO: the schema type should not be an argument here
        # the table needs to have been created, and therefore has a schema
        # TODO: this should raise an exception for inserting too many
        # not return a dict
        try:
            self._client.insert_annotations(self.aligned_volume,
                                            table_name,
                                            schema_type,
                                            annotations)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

    def get_annotations(self, table_name: str, 
                              schema_type: str,
                              annotation_ids: List[int])->List[dict]:
        """get a set of annotations by ID

        Args:
            table_name (str): name of table
            schema_type (str): schema of table
            annotation_ids (List[int]): list of annotation ids to get

        Returns:
            List[dict]: list of returned annotations
        """
        # TODO: schema_type should not be an argument here
        try:
            return self._client.get_annotations(self.aligned_volume,
                                                table_name,
                                                schema_type,
                                                annotation_ids)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

    def delete_annotations(self, table_name: str,
                                 schema_type: str,
                                 annotation_ids: List[int]):
        """delete annotations by 

        Args:
            table_name (str): name of table to delete from
            schema_type (str): schema of table
            annotation_ids List[int]: list of ids to delete
        Raises:
            TODO: what does this raise
        """
        self._client.delete_annotations(self.aligned_volume,
                                       table_name,
                                       annotation_ids)

    def update_annotation(self, table_name: str,
                                 schema_type: str,
                                 anno_id: int,
                                 annotation: dict):
        """update an annotation.  
        # TODO: schema_type also shouldn't be an argument as above

        Args:
            table_name (str): name of table to update
            schema_type (str): type of schema
            anno_id (int): ID of annotation to update
            annotation (dict): new data for that annotation

        Returns:
            ???
        Raises:
            TODO: What does this raise
        """
        try:
            self._client.update_annotation(self.aligned_volume,
                                            table_name,
                                            schema_type,
                                            anno_id,
                                            annotation)
        except TableNameNotFoundException as e:
            return {f"Error: {e}"}

