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


EPOCHS = 3


SOURCE_DTYPE = {
    'names': [
        'ra_h', 'dec_h', 'ra_j',
        'dec_j', 'ra_k', 'dec_k',
        "mag_h", "mag_j", "mag_k",
        "mag_err_h", "mag_err_j", "mag_err_k"],
    'formats': [
        float, float, float,
        float, float, float,
        float, float, float,
        float, float, float]
}

USECOLS = list(range(len(SOURCE_DTYPE["names"])))


def read_dat(fp):
    arr = np.genfromtxt(
        fp, skip_header=EPOCHS,
        dtype=SOURCE_DTYPE,
        usecols=USECOLS)
    flt = (arr["ra_k"] != -9999.0)
    filtered = arr[flt]
    return filtered, len(filtered)

def main():
    with db.session_scope() as ses:
        for tile in ses.query(Tile):
            with open(tile.raw_file_path) as fp:
                raw_arr = read_dat(fp)[0][["ra_k", "dec_k", "mag_h", "mag_j", "mag_k",
                "mag_err_h", "mag_err_j", "mag_err_k"]]
            npy_arr = tile.load_npy_file()



            columns = [
                ("mag_h", raw_arr["mag_h"]),
                ("mag_j", raw_arr["mag_j"]),
                ("mag_k", raw_arr["mag_k"]),
                ("mag_err_h", raw_arr["mag_err_h"]),
                ("mag_err_j", raw_arr["mag_err_j"]),
                ("mag_err_k", raw_arr["mag_err_k"]),

                ("2m_c89_ejk", npy_arr["c89_ejk"]),
                ("2m_c89_ak", npy_arr["c89_ak"]),
                ("2m_n09_ejk", npy_arr["n09_ejk"]),
                ("2m_n09_ak", npy_arr["n09_ak"])


                ]
            arr = add_columns(npy_arr, columns, append=True)

            order = [
                'id', 'ra_h', 'dec_h', 'ra_j', 'dec_j', 'ra_k', 'dec_k',
                'mag_h', 'mag_j', 'mag_k', 'mag_err_h', 'mag_err_j', 'mag_err_k',
                'ogle3_type', 'ogle3_id',
                '2m_c89_ejk', '2m_c89_ak', '2m_n09_ejk', '2m_n09_ak']

            arr = arr[order]

            tile.store_npy_file(arr)


main()
