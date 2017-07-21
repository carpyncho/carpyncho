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
    conditions = [model.status == "ready-to-tag"]
    groups = ["preprocess"]
    production_procno = 1

    def setup(self):
        self.df = bin.ogle3.load()

    def process(self, tile):
        arr = tile.load_npy_file()
        tile_ra, tile_dec = arr["ra_k"], arr["dec_k"]
        ogle_ra, ogle_dec = self.df.RA.values, self.df.DECL.values

        matchs = matcher.matchs(tile_ra, ogle_ra, tile_dec, ogle_dec)
        for tile_idx, ogle_idx in matchs:
            import ipdb; ipdb.set_trace()
