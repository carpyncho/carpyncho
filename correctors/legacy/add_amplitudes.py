import time
import random
import os

from joblib import Parallel, delayed, cpu_count

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

CPUS = cpu_count()

tiles = "b262 b263 b261 b264 b220 b278".split()

from carpyncho.lib.beamc import add_columns


def main():
    with db.session_scope() as ses:
        query = ses.query(LightCurves).join(Tile).filter(Tile.name.in_(tiles))
        for lc in query:
            print lc
            feats = lc.features
            if "AmplitudeJH" in feats.dtype.names:
                print "   skip!"
                continue

            columns = [
                ("AmplitudeJH", feats["AmplitudeJ"] - feats["AmplitudeH"]),
                ("AmplitudeJK", feats["AmplitudeJ"] - feats["Amplitude"])]

            feats = add_columns(feats, columns, append=True)


            lc.tile.ready = False
            lc.features = feats
            lc.tile.ready = True

            ses.commit()
            print "   Done"





main()
