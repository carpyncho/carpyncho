#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys;sys.exit()

import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from carpyncho.lib.beamc import (
    extinction, knnfix, add_columns, MIN_BOX_SIZE, SERVER_SOURCES_LIMIT)

from PyAstronomy import pyasl


def main():
    with db.session_scope() as ses:
        for tile in ses.query(Tile):
            npy_arr = tile.load_npy_file()
            lc = tile.lcurves
            feats = lc.features

            npy_arr = npy_arr[np.in1d(npy_arr["id"], feats["id"])]



            columns = [
                ("scls_h", npy_arr["scls_h"]),
                ("scls_j", npy_arr["scls_j"]),
                ("scls_k", npy_arr["scls_k"]),
            ]

            arr = add_columns(feats, columns, append=True)
            #~ import ipdb; ipdb.set_trace()


            tile.ready = False
            lc.features = arr
            tile.ready = True
            #~ tile.store_npy_file(arr)


main()
