from dynamicannotationdb.interface import AnnotationDB
import json

class AnnotationDBMeta:
    def __init__(self, sql_uri: str):
        
        self._client = AnnotationDB(sql_uri)
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
        return self._client.get_existing_tables()

    def get_dataset_tables(self, dataset_name: str):
        return self._client.get_dataset_tables(dataset_name=dataset_name)
          
    def get_table_metadata(self, dataset_name: str, table_name: str) -> dict:
        return self._client.get_table_metadata(dataset_name, table_name)

    def get_existing_tables_metadata(self, dataset_name:str, table_name:str) ->  list:
        
        return [
            self.get_table_metadata(dataset_name, table_name=table_name)
            for table_id in self._client.get_dataset_tables(table_name)
        ]  
    
    def get_annotation_table_length(self, table_id: str) -> int:
        return self._client.get_annotation_table_size(table_id)

    def create_table(self, em_dataset_name:str, 
                           table_name: str, 
                           schema_type:str, 
                           metadata_dict: dict):
        
        return self._client.create_table(em_dataset_name,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

    def drop_table(self, table_name: str) -> bool:
        return self._client.drop_table(table_name)

    def insert_annotations(self, table_id:str, 
                                 schema_name:str, 
                                 annotations: list, 
                                 assign_id: bool=False):
        
        self._client.insert_annotations(table_id,
                                        schema_name,
                                        annotations)
        
    def get_annotation_data(self, table_id: str, schema_name: str, anno_id: int):

        return self._client.get_annotations(table_id,
                                           schema_name,
                                           anno_id)

    def update_annotation_data(self, table_id: str, 
                                     schema_name: str, 
                                     anno_id: int, 
                                     new_annotations: dict):

        self._client.update_annotation(table_id, 
                                       schema_name, 
                                       anno_id, 
                                       new_annotations)

    def delete_annotation(self, table_id: str, anno_id: int):
        self._client.delete_annotation(table_id, anno_id)
