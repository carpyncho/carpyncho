#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from carpyncho.lib.beamc import add_columns
import sys;sys.exit()

def main():
    with db.session_scope() as ses:
        for lc in ses.query(LightCurves):
            print lc
            try:
                features = lc.features
            except:
                print "bad", lc, "!!!"
                continue

            if "c89_jk_color" in features.dtype.names:
                print "Ready", lc
                continue

            colors = lc.tile.load_npy_file()[[
                'id', 'c89_jk_color', 'c89_hk_color', 'c89_jh_color',
                'n09_jk_color', 'n09_hk_color', 'n09_jh_color']]



            idxs = np.where(np.in1d(colors["id"], features["id"]))[0]

            colors = colors[idxs]

            columns = [
                ('c89_jk_color', colors['c89_jk_color']),
                ('c89_hk_color', colors['c89_hk_color']),
                ('c89_jh_color', colors['c89_jh_color']),
                ('n09_jk_color', colors['n09_jk_color']),
                ('n09_hk_color', colors['n09_hk_color']),
                ('n09_jh_color', colors['n09_jh_color'])]

            lc.features = add_columns(features, columns, append=True)
            print lc


main()
