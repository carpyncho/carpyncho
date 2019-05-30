import time
import random
import os

import numpy as np
import pandas as pd
from collections import defaultdict
from corral import db
from carpyncho.models import *

PATH = "/home/jbcabral/carpyncho3/correctors/"


def main():
    result = []
    with db.session_scope() as ses:
        query = ses.query(LightCurves).join(Tile)
        for lc in query:
            print lc

            features = pd.DataFrame(lc.features)
            features = features[features.Mean > 12]
            features = features[features.Mean < 16.5]
            sample = features.sample(1000)
            result.append(sample)

        print "Merging"
        result = pd.concat(result, ignore_index=True)

        print "Saving to {}".format("sample_all.pkl")
        result.to_pickle("sample_all.pkl.bz2", compression="bz2")





main()
