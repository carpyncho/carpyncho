#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import copy

from corral import run

import numpy as np

import pandas as pd

from .. import bin
from ..lib import matcher
from ..models import Tile


# =============================================================================
# CONSTANTS
# =============================================================================




# =============================================================================
# STEP
# =============================================================================

class OGLE3TagTile(run.Step):
    """Set the class of the stars from the OGLE-3 Survey

    """

    model = Tile
    conditions = [model.status == "ready-to-tag", model.name=="b220"]
    groups = ["preprocess"]
    production_procno = 1

    def setup(self):
        df = bin.ogle3.load()
        self.ogle_ra = df.ra_deg.values
        self.ogle_dec = df.dec_deg.values
        self.df = df[["ra_deg", "dec_deg"]]

    def process(self, tile):
        arr = tile.load_npy_file()
        tile_ra, tile_dec = arr["ra_k"], arr["dec_k"]
        tdf = pd.DataFrame(arr)[["ra_k", "dec_k"]]
        import ipdb; ipdb.set_trace()

        matchs = matcher.matchs(tile_ra, self.ogle_ra, tile_dec, self.ogle_dec)
        for tile_idx, ogle_idx in matchs:
            print tile_idx, ogle_idx
        a=1
