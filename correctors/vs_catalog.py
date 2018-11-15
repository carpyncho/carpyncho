import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

def to_recarray(sources):
    """Convert the sources dataframr into recarray to easy storage"""
    # convert to recarray
    sources = sources.to_records(index=False)

    # change dtype by making a whole new array
    descr = sources.dtype.descr
    descr[1] = (descr[1][0], '|S13')
    descr[2] = (descr[2][0], '|S13')
    descr = [(str(n), t) for n, t in descr]
    dt = np.dtype(descr)

    return sources.astype(dt)



def main():
    with db.session_scope() as ses:
        query = ses.query(LightCurves)
        rows = []
        for lc in query.all():
            print lc
            tile = lc.tile

            feats = pd.DataFrame(lc.features)
            if "vs_catalog" in feats.columns:
                print "skiping"
                continue

            srcs = pd.DataFrame(tile.load_npy_file()[["id", "vs_catalog"]])
            srcs = srcs[srcs.id.isin(feats.id)]

            new_feats = pd.merge(srcs, feats, on="id", how="inner")
            new_feats = to_recarray(new_feats)

            lc.tile.ready = False
            lc.features = new_feats
            lc.tile.ready = True


main()
