#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from carpyncho.lib.beamc import (
    extinction, knnfix, add_columns, MIN_BOX_SIZE, SERVER_SOURCES_LIMIT)
import sys;sys.exit()

def main():
    with db.session_scope() as ses:
        for tile in ses.query(Tile):

            arr = tile.load_npy_file()

            # set the aj, ah of n09 and c89

            arr = add_columns(arr, [
                ('c89_aj_2m', 1.692 * arr["c89_ejk_2m"]),
                ('c89_ah_2m', 1.054 * arr["c89_ejk_2m"]),
                ('n09_aj_2m', 1.526 * arr["n09_ejk_2m"]),
                ('n09_ah_2m', 0.855 * arr["n09_ejk_2m"])], append=True)


            # from here https://arxiv.org/pdf/1711.08805.pdf
            c89_ak_vvv = arr["c89_ak_2m"] + 0.01 * arr["c89_ejk_2m"]
            c89_aj_vvv = arr["c89_aj_2m"] - 0.065 * arr["c89_ejk_2m"]
            c89_ah_vvv = arr["c89_ah_2m"] + 0.032 * (arr["c89_aj_2m"] - arr["c89_ah_2m"])

            c89_jk_color = (arr["mag_j"] - c89_aj_vvv) - (arr["mag_k"] - c89_ak_vvv)
            c89_hk_color = (arr["mag_h"] - c89_ah_vvv) - (arr["mag_k"] - c89_ak_vvv)
            c89_jh_color = (arr["mag_j"] - c89_aj_vvv) - (arr["mag_h"] - c89_ah_vvv)

            n09_ak_vvv = arr["n09_ak_2m"] + 0.01 * arr["n09_ejk_2m"]
            n09_aj_vvv = arr["n09_aj_2m"] - 0.065 * arr["n09_ejk_2m"]
            n09_ah_vvv = arr["n09_ah_2m"] + 0.032 * (arr["n09_aj_2m"] - arr["n09_ah_2m"])

            n09_jk_color = (arr["mag_j"] - n09_aj_vvv) - (arr["mag_k"] - n09_ak_vvv)
            n09_hk_color = (arr["mag_h"] - n09_ah_vvv) - (arr["mag_k"] - n09_ak_vvv)
            n09_jh_color = (arr["mag_j"] - n09_aj_vvv) - (arr["mag_h"] - n09_ah_vvv)

            columns = [
                ('c89_ak_vvv', c89_ak_vvv),
                ('c89_aj_vvv', c89_aj_vvv),
                ('c89_ah_vvv', c89_ah_vvv),
                ('c89_jk_color', c89_jk_color),
                ('c89_hk_color', c89_hk_color),
                ('c89_jh_color', c89_jh_color),
                ('n09_ak_vvv', n09_ak_vvv),
                ('n09_aj_vvv', n09_aj_vvv),
                ('n09_ah_vvv', n09_ah_vvv),
                ('n09_jk_color', n09_jk_color),
                ('n09_hk_color', n09_hk_color),
                ('n09_jh_color', n09_jh_color)]

            arr = add_columns(arr, columns, append=True)
#~
            #~ arr = arr[order]
#~
            tile.store_npy_file(arr)


main()
