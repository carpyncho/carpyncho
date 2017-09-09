from corral import db
from carpyncho.models import *
import numpy as np
import sys;sys.exit()


def fix_id_column(tile_name, ps_id, data):

    tile_name = "3" + tile_name[1:]
    ps_name = "3" + str(ps_id).zfill(7)

    def to_int_ps_id(src_id):
        order = src_id.split("_")[-1]
        order = order.rjust(8, "0")
        return (ps_name + order)

    def to_int_bm_id(src_id):
        order = src_id.split("_")[-1]
        order = order.rjust(10, "0")
        return int(tile_name + order)

    dtype = {
        "names": list(data.dtype.names),
        "formats": [e[-1] for e in data.dtype.descr]}

    bm_src_id_idx = dtype["names"].index("bm_src_id")
    pwp_stack_src_id_idx = dtype["names"].index("pwp_stack_src_id")

    dtype["formats"][bm_src_id_idx] = np.int64
    dtype["formats"][pwp_stack_src_id_idx] = np.int64

    ndata = np.empty(len(data), dtype=dtype)
    for name in data.dtype.names:
        if name == dtype["names"][bm_src_id_idx]:
            ndata[name] = np.array(map(to_int_bm_id, data[name]))
        elif name == dtype["names"][pwp_stack_src_id_idx]:
            ndata[name] = np.array(map(to_int_ps_id, data[name]))
        else:
            ndata[name] = data[name]
    return ndata



with db.session_scope() as ses:
    total = ses.query(PawprintStackXTile).count()
    for idx, pxt in enumerate(ses.query(PawprintStackXTile)):
        print pxt, "{}/{}".format(idx, total)
        data = pxt.load_npy_file()
        #~ import ipdb; ipdb.set_trace()
        data = fix_id_column(
            pxt.tile.name,
            pxt.pawprint_stack.id, data)
        #~ a=1
        pxt.store_npy_file(data)
