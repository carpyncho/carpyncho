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

from read_pxt import read_pxt

CPUS = cpu_count()

COLUMNS = [
    "bm_src_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


def main():
    with db.session_scope() as ses:
        query = ses.query(LightCurves)
        for lc in ses.query(LightCurves).filter(LightCurves.tile.has(name="b396")):
            obs = lc.observations
            tile_name = lc.tile.name
            #~ if "pwp_stack_src_id" in obs.dtype.names and "pwp:
                #~ print "Skiping {}".format(tile_name)
                #~ continue
            pxts = [
                (pxt.npy_file_path, pxt.pawprint_stack_id)
                for pxt in lc.tile.pxts]
            total = len(pxts)

            with Parallel(n_jobs=CPUS) as jobs:
                data = jobs(
                    delayed(read_pxt)(pxt[0], pxt[1], total, idx, tile_name)
                    for idx, pxt in enumerate(pxts))

            #~ for idx, pxt in enumerate(pxts):
                #~
                #~ data = pxt.load_npy_file()[COLUMNS]
                #~ if obs is None:
                    #~ obs = data
                #~ else:
                    #~ obs = np.concatenate((obs, data))
            lc.observations = np.concatenate(data)
            ses.commit()

main()
