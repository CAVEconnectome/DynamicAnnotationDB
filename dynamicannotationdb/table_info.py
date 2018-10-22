import numpy as np

import dynamicannotationdb.key_utils as key_utils


operation_counter_key = "ioperations"
operation_counter_key_s = key_utils.serialize_key(operation_counter_key)
annotation_counter_key = "iannotations"
annotation_counter_key_s = key_utils.serialize_key(annotation_counter_key)
counter_key = 'counter'
counter_key_s = key_utils.serialize_key(counter_key)
blob_key = 'data'
blob_key_s = key_utils.serialize_key(blob_key)
anno_id_key = 'annotation'
anno_id_key_s = key_utils.serialize_key(anno_id_key)
coordinate_key = 'coordinate'
coordinate_key_s = key_utils.serialize_key(coordinate_key)
bsp_name_key = 'name'
bsp_name_key_s = key_utils.serialize_key(bsp_name_key)

dtype_dict = {}