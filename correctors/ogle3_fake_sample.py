import time
import random
import os

import numpy as np
import pandas as pd
from collections import defaultdict
from corral import db
from carpyncho.models import *

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

o3id_path  =  os.path.join(PATH, "o3id.npy")

o3id = np.load(o3id_path)
o3id = o3id.reshape(1)[0]


def main():
    tiles = "b261 b262 b263 b264 b278".split()
    with db.session_scope() as ses:
        query = ses.query(LightCurves).join(Tile).filter(Tile.name.in_(tiles))
        samples = defaultdict(list)
        for lc in query:
            print lc
            tile = lc.tile

            features = pd.DataFrame(lc.features)

            features = features[features.Mean > 12]
            features = features[features.Mean < 16.5]

            vss = features[features.id.isin(o3id)]
            unk = features[~features.id.isin(o3id)]
            unk["vs_type"] = ""

            for sample_size in [2500, 5000, 20000]:
                samp = unk.sample(sample_size)
                samples[sample_size].append(samp)
                samples[sample_size].append(vss)


    for sample_size, dfs in samples.items():
        fname = "sample_ogle3_{}.pkl".format(sample_size)
        df = pd.concat(dfs, ignore_index=True)
        df.to_pickle(fname)






main()
