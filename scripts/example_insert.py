from dynamicannotationdb.annotation_client import DynamicAnnotationClient
from dynamicannotationdb.key_utils import build_table_id
# Test URI
SQL_URI = "postgres://postgres:annodb@localhost:5432/annodb" 

# Create annotation db instance


aligned_volume = 'minnie'
table_name = 'synapse_test'
schema_name = 'synapse'
client = DynamicAnnotationClient(aligned_volume=aligned_volume, sql_base_uri=SQL_URI)

test_table_description = "This is an example description for this table"

new_table = client.create_table(table_name, 
                                schema_name,
                                metadata_dict={
                                    'description':test_table_description,
                                    'user_id': 'foo@bar.com'})

# get list of tables in aligned volume
tables = client.get_existing_tables()

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
client.insert_annotations(table_name, schema_name, [synapse_data])
# Get the newly inserted row
anns=client.get_annotations(table_name, schema_name, [1])


updated_synapse_data = {
    'id': 65534332,
    'pre_pt': {'position': [121,123,1232], 'supervoxel_id':  2344444, 'root_id': 4},
    'ctr_pt': {'position': [121,123,1232]},
    'post_pt':{'position': [555,555,5555], 'supervoxel_id':  3242424, 'root_id': 5},
    'type': 'synapse',
}  

# Update annotation
client.update_annotation(table_name, schema_name, 1, updated_synapse_data)
