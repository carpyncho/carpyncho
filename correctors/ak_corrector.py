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


def main():
    with db.session_scope() as ses:
        for tile in ses.query(Tile):

            arr = tile.load_npy_file()

            # Cardelli: extition 2mass -> vvv
            vvv_c89_ak = .364 * arr["2m_c89_ejk"]
            vvv_c89_aj = .866 * vvv_c89_ak / .364

            # Cardelli: absolute mags and colors
            amag_c89_k = arr["mag_k"] - vvv_c89_ak
            amag_c89_j = arr["mag_j"] - vvv_c89_aj
            color_n09_jk = amag_c89_j - amag_c89_k

            # Nishiyama: extition 2mass -> vvv
            vvv_n09_ak = .364 * arr["2m_n09_ejk"]
            vvv_n09_aj = .866 * vvv_n09_ak / .364

            # Nishiyama: absolute mags and colors
            amag_n09_k = arr["mag_k"] - vvv_n09_ak
            amag_n09_j = arr["mag_j"] - vvv_n09_aj
            color_n09_jk = amag_n09_j - amag_n09_k


            columns = [
                ('vvv_c89_aj', vvv_c89_aj),
                ('vvv_c89_ak', vvv_c89_ak),
                ('vvv_n09_aj', vvv_n09_aj),
                ('vvv_n09_ak', vvv_n09_ak),

                ('amag_c89_j', amag_c89_j),
                ('amag_c89_k', amag_c89_k),
                ('amag_n09_j', amag_n09_j),
                ('amag_n09_k', amag_n09_k),

                ('color_n09_jk', color_n09_jk),
                ('color_n09_jk', color_n09_jk)]

            arr = add_columns(arr, columns, append=True)

            arr = arr[order]

            tile.store_npy_file(arr)


main()
