from corral import db
from carpyncho.models import *
import numpy as np
import sys;sys.exit()

def fix_id_column(tile_name, data):

    tile_name = tile_name[1:]

    def to_int(src_id):
        order = src_id.split("_")[-1]
        order = order.rjust(10, "0")
        return int("3" + tile_name + order)

    #~ fix_id = np.array(map(to_int, data["id"]))


    dtype = {
        "names": list(data.dtype.names),
        "formats": [e[-1] for e in data.dtype.descr]}
    dtype["formats"][0] = np.int64

    ndata = np.empty(len(data), dtype=dtype)
    for name in data.dtype.names:
        if name == dtype["names"][0]:
            ndata[name] = np.array(map(to_int, data[name]))
        else:
            ndata[name] = data[name]
    return ndata



with db.session_scope() as ses:
    for tile in ses.query(Tile):
        print tile
        data = tile.load_npy_file()
        data = fix_id_column(tile.name, data)
        tile.store_npy_file(data)
