import numpy as np
from carpyncho.lib import beamc


COLUMNS = [
    "bm_src_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


S_COLUMNS = [
    "bm_src_id", "pwp_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]

def read_pxt(pxt_path, pwp_id, total, idx, tile_name):
    print("Processing pxt {} of {} (Tile {})".format(idx, total, tile_name))
    arr = np.load(pxt_path)[COLUMNS]

    ids = np.empty(len(arr)).astype(int)
    ids[:] = int(pwp_id)

    extra_cols = [("pwp_id", ids, )]

    arr = beamc.add_columns(arr, extra_cols)[S_COLUMNS]

    return arr
