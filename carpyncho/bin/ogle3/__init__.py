#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd

from astropy.coordinates import SkyCoord

import sh
from sh import bzip2

from ...lib.context_managers import cd


# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))


# =============================================================================
# FUNCTIONS
# =============================================================================

def build():
    def unpack():
        bzip2("-f", "-dk", "ogleIII_all.csv.bz2")

    def fix_pos(odf):
        ra = odf["RA"].apply(
                lambda d: d.replace(":", "h", 1).replace(":", "m", 1) + "s")
        dec = odf["Decl"].apply(
                lambda d: d.replace(":", "d", 1).replace(":", "m", 1) + "s")

        coords = SkyCoord(ra, dec, frame='icrs')
        odf['ra_deg'] = pd.Series(coords.ra.deg, index=odf.index)
        odf['dec_deg'] = pd.Series(coords.dec.deg, index=odf.index)
        odf["galactic_l"] = pd.Series(coords.galactic.l, index=odf.index)
        odf["galactic_b"] = pd.Series(coords.galactic.b, index=odf.index)
        return odf

    def preprocess():
        df = pd.read_csv("ogleIII_all.csv")
        df = fix_pos(df)
        df.to_pickle("ogleIII.pkl")

    def clean():
        os.remove("ogleIII_all.csv")

    with cd(PATH):
        unpack()
        preprocess()
        clean()


def load():
    path = os.path.join(PATH, "ogleIII.pkl")
    return pd.read_pickle(path)
