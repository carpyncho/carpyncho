from corral import db
from carpyncho.models import *
import numpy as np
import sys;sys.exit()


def fix_id_column(ps_id, data):

    ps_name = "3" + str(ps_id).zfill(7)

    def to_int(src_id):
        order = src_id.split("_")[-1]
        order = order.rjust(8, "0")
        return (ps_name + order)

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
    total = ses.query(PawprintStack).count()
    for idx, ps in enumerate(ses.query(PawprintStack)):
        print ps, "{}/{}".format(idx, total)
        data = ps.load_npy_file()
        data = fix_id_column(ps.id, data)
        ps.store_npy_file(data)
