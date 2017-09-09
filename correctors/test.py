# coding: utf-8

# In[1]:


from corral import db
from carpyncho.models import *
import numpy as np
import pandas as pd


# In[2]:


ses = db.Session()
tiles = ses.query(Tile).all()

columns = [
            "bm_src_id", "pwp_stack_src_hjd",
            "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


def to_int(src_id):
    tile_id, order = src_id.split("_")[1:]
    order = order.rjust(10, "0")
    return int("3278" + order)

def fix_ids(data, u_ids):
        # create dtype
        dtype = {
            "names": list(data.dtype.names),
            "formats": [e[-1] for e in data.dtype.descr]}
        dtype["formats"][0] = np.int64


        #import ipdb; ipdb.set_trace()

        # create an empty array and copy the values
        ndata = np.empty(len(data), dtype=dtype)
        for name in data.dtype.names:
            if name == columns[0]:
                ndata[name] = np.array(map(u_ids.get, data[name]))
            else:
                ndata[name] = data[name]
        return ndata


# In[3]:


b278 = tiles[-2]
b278
#~ ids = b278.load_npy_file()["id"]
#~ u_ids = {src_id: to_int(src_id) for src_id in ids}
#~ del ids


# In[ ]:


files = None
total = len(b278.pxts)
for idx, pxt in enumerate(b278.pxts):
    data = pxt.load_npy_file()[columns]
    #~ data = fix_ids(data, u_ids)

    #data = pd.DataFrame(data)
    #data["bm_src_id"] = data.bm_src_id.apply(u_ids.get)
    #data = data.to_records(index=False).copy()

    #~ import ipdb;ipdb.set_trace()
    if files is None:
        files = data
    else:
        files = np.concatenate((files, data))
    print idx, "/", total

del data





np.save("test3.npy", files, False)
np.load("test3.npy")
print "sucess"



# In[22]:
