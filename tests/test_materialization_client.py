from .conftest import TABLE_NAME, PCG_TABLE_NAME
from sqlalchemy.ext.declarative import api
import numpy as np

def test_load_table(materialization_client):
    loaded_table = materialization_client.load_table(TABLE_NAME)
    if isinstance(loaded_table, api.DeclarativeMeta):
        assert loaded_table.__name__ == TABLE_NAME


def test_create_and_attach_seg_table(materialization_client):
    table_added_status = materialization_client.create_and_attach_seg_table(TABLE_NAME, PCG_TABLE_NAME)
    assert table_added_status == {'Created Succesfully': True, 'Table Name': f'{TABLE_NAME}__{PCG_TABLE_NAME}'}


def test_insert_linked_annotations(materialization_client):
    segmentation_data = [{
        'id': 1,
        'pre_pt': {'supervoxel_id':  2344444, 'root_id': 4}, 
        'post_pt': {'supervoxel_id':  3242424, 'root_id': 5}
    }]
    is_inserted = materialization_client.insert_linked_annotations(TABLE_NAME, PCG_TABLE_NAME, segmentation_data)
    assert is_inserted == True


def test_get_linked_annotations(materialization_client):
    annotations = materialization_client.get_linked_annotations(TABLE_NAME, PCG_TABLE_NAME, [1,2])
    print(annotations)
    assert annotations[0]['pre_pt_supervoxel_id'] == 2344444
    assert annotations[0]['pre_pt_root_id'] == 4
    assert annotations[0]['post_pt_supervoxel_id'] == 3242424
    assert annotations[0]['post_pt_root_id'] == 5


# def test_update_linked_annotations(materialization_client):
#     segmentation_data = [{
#         'id': 2,
#         'pre_pt': {'supervoxel_id':  2344444, 'root_id': 7}, 
#         'post_pt': {'supervoxel_id':  3242424, 'root_id': 5}
#     }]
#     assert False


def test_drop_table(materialization_client):
    table_dropped = materialization_client.drop_table(f'{TABLE_NAME}__mat{PCG_TABLE_NAME}')
    assert table_dropped == True