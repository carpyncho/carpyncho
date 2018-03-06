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


EPOCHS = 3


SOURCE_DTYPE = {
    'names': [
        'ra_h', 'dec_h', 'ra_j',
        'dec_j', 'ra_k', 'dec_k',
        "mag_h", "mag_j", "mag_k",
        "mag_err_h", "mag_err_j", "mag_err_k",
        "scls_h", "scls_j", "scls_k"],
    'formats': [
        float, float, float,
        float, float, float,
        float, float, float,
        float, float, float,
        int, int, int]
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
                raw_arr = read_dat(fp)[0][["scls_h", "scls_j", "scls_k"]]
            npy_arr = tile.load_npy_file()

            # calculate the hjds
            mjd_h, mjd_j, mjd_k = tile.epochs

            print "h"
            hjd_h = np.fromiter(
                (pyasl.helio_jd(mjd_h, ra, dec) for ra, dec in zip(npy_arr["ra_h"], npy_arr["dec_h"])),
                dtype=float)
            print "j"
            hjd_j = np.fromiter(
                (pyasl.helio_jd(mjd_j, ra, dec) for ra, dec in zip(npy_arr["ra_j"], npy_arr["dec_j"])),
                dtype=float)
            print "k"
            hjd_k = np.fromiter(
                (pyasl.helio_jd(mjd_k, ra, dec) for ra, dec in zip(npy_arr["ra_k"], npy_arr["dec_k"])),
                dtype=float)

            columns = [
                ("scls_h", raw_arr["scls_h"]),
                ("scls_j", raw_arr["scls_j"]),
                ("scls_k", raw_arr["scls_k"]),
                ("hjd_h", hjd_h),
                ("hjd_j", hjd_j),
                ("hjd_k", hjd_k)

            ]





            arr = add_columns(npy_arr, columns, append=True)
            #~ import ipdb; ipdb.set_trace()



            tile.store_npy_file(arr)


main()
