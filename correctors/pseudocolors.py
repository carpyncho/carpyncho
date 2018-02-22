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


def main():
    with db.session_scope() as ses:
        for lc in ses.query(LightCurves):
            print lc
            try:
                features = lc.features
            except:
                print "bad", lc, "!!!"
                continue

            #~ if "c89_jk_color" in features.dtype.names:
                #~ print "Ready", lc
                #~ continue

            feats = lc.features
            sources = lc.tile.load_npy_file()[[
                'id', "mag_k", "mag_j", "mag_h",
                'c89_ak_vvv', 'c89_aj_vvv', 'c89_ah_vvv',
                'n09_ak_vvv', 'n09_aj_vvv', 'n09_ah_vvv']]

            # CARDELLI

            c89_mk = sources["mag_k"] - sources["c89_ak_vvv"]
            c89_mj = sources["mag_j"] - sources["c89_aj_vvv"]
            c89_mh = sources["mag_h"] - sources["c89_ah_vvv"]

            # magnitudes
            c89_c_m2 = sources["c89_ah_vvv"] / (
                sources["c89_aj_vvv"] - sources["c89_ak_vvv"])
            c89_m2 = c89_mh - c89_c_m2 * (c89_mj - c89_mk)

            c89_c_m4 = sources["c89_ak_vvv"] / (
                sources["c89_aj_vvv"] - sources["c89_ah_vvv"])
            c89_m4 = c89_mk - c89_c_m4 * (c89_mj - c89_mh)

            # color
            c89_c_c3 = (
                (sources["c89_aj_vvv"] - sources["c89_ah_vvv"]) /
                (sources["c89_ah_vvv"] - sources["c89_ak_vvv"]))
            c89_c3 = (c89_mj - c89_mh) - c89_c_c3 * (c89_mh - c89_mk)

            # NISHIYAMA

            n09_mk = sources["mag_k"] - sources["n09_ak_vvv"]
            n09_mj = sources["mag_j"] - sources["n09_aj_vvv"]
            n09_mh = sources["mag_h"] - sources["n09_ah_vvv"]

            n09_c_m2 = sources["n09_ah_vvv"] / (
                sources["n09_aj_vvv"] - sources["n09_ak_vvv"])
            n09_m2 = n09_mh - n09_c_m2 * (n09_mj - n09_mk)

            n09_c_m4 = sources["n09_ak_vvv"] / (
                sources["n09_aj_vvv"] - sources["n09_ah_vvv"])
            n09_m4 = n09_mk - n09_c_m4 * (n09_mj - n09_mh)

            # color
            n09_c_c3 = (
                (sources["n09_aj_vvv"] - sources["n09_ah_vvv"]) /
                (sources["n09_ah_vvv"] - sources["n09_ak_vvv"]))
            n09_c3 = (n09_mj - n09_mh) - n09_c_c3 * (n09_mh - n09_mk)






            import ipdb; ipdb.set_trace()
            a=1


            #~ idxs = np.where(np.in1d(colors["id"], features["id"]))[0]

            #~ colors = colors[idxs]

            #~ columns = [
                #~ ('c89_jk_color', colors['c89_jk_color']),
                #~ ('c89_hk_color', colors['c89_hk_color']),
                #~ ('c89_jh_color', colors['c89_jh_color']),
                #~ ('n09_jk_color', colors['n09_jk_color']),
                #~ ('n09_hk_color', colors['n09_hk_color']),
                #~ ('n09_jh_color', colors['n09_jh_color'])]

            #~ lc.features = add_columns(features, columns, append=True)
            #~ print lc


main()
