from dynamicannotationdb.interface import AnnotationDB

# Test URI
SQL_URI = "postgres://postgres:annodb@localhost:5432/annodb" 

# Create annotation db instance
client = AnnotationDB(sql_uri=SQL_URI)

dataset_name = 'test'
table_name = 'soma_table'
schema_name = 'microns_func_coreg'

new_table = client.create_table(dataset_name=dataset_name, 
                                schema_type=schema_name,
                                table_name=table_name,
                                metadata_dict = None, 
                                description='Test description',
                                user_id='foo@bar.com')
print(new_table)

table_id = f"{dataset_name}_{table_name}"

# get table info
client.get_model_columns(table_id)

# get tables by dataset
tables = client.get_dataset_tables(dataset_name)
print(tables)


soma_data = {
    'type': 'microns_func_coreg',
    'pt': {'position':  [31, 31, 0]},
    'func_id': 123456,
}

client.insert_annotation(table_id, schema_name, soma_data)


soma_annotation = client.get_annotation(table_id, schema_name, anno_id=1)

print(soma_annotation)