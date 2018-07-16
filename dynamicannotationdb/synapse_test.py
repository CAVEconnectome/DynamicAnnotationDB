import pandas as pd
import numpy as np
import os

HOME = os.path.expanduser("~")


def load_synapses(path=HOME + "/pinky40_run2_remapped.df"):
    """ Cheap test scenario using real synapses """

    df = pd.read_csv(path)

    sv_ids = np.array(df[["presyn_segid", "postsyn_segid"]])
    data = np.array(df)

    mask = ~np.any(np.isnan(sv_ids), axis=1)
    sv_ids = sv_ids[mask].astype(np.uint64)
    data = data[mask]

    annotations = [(sv_ids[i], data[i].tobytes()) for i in range(len(data))]

    return annotations