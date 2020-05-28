from dynamicannotationdb.interface import AnnotationDB

# Test URI
SQL_URI = "postgres://postgres:annodb@localhost:5432/annodb" 

# Create annotation db instance
client = AnnotationDB(sql_uri=SQL_URI)

dataset_name = 'minnie'
table_name = 'synapse_test'
schema_name = 'synapse'
table_id = f"{dataset_name}_{table_name}"

test_table_description = "This is an example description for this table"

new_table = client.create_table(dataset_name, 
                                table_name, 
                                schema_name,
                                description=test_table_description,
                                user_id='foo@bar.com')

# get list of tables by em_dataset name
tables = client.get_dataset_tables(dataset_name)

# Lets add a synapse

synapse_data = {
    'id': 65534332,
    'pre_pt': {'position': [121,123,1232], 'supervoxel_id':  2344444, 'root_id': 4},
    'ctr_pt': {'position': [121,123,1232]},
    'post_pt':{'position': [333,555,5555], 'supervoxel_id':  3242424, 'root_id': 5},
    'type': 'synapse',
} 


""" 
insert new data, if assign_id is True, the Database PK will be overriden by the 'id' in
synapse_data
"""
client.insert_annotation(table_id, schema_name, synapse_data, assign_id=False)
# Get the newly inserted row
client.get_annotation(table_id, schema_name, 1)


updated_synapse_data = {
    'id': 65534332,
    'pre_pt': {'position': [121,123,1232], 'supervoxel_id':  2344444, 'root_id': 4},
    'ctr_pt': {'position': [121,123,1232]},
    'post_pt':{'position': [555,555,5555], 'supervoxel_id':  3242424, 'root_id': 5},
    'type': 'synapse',
}  

# Update annotation
client.update_annotation(table_id, schema_name, 1, updated_synapse_data)
