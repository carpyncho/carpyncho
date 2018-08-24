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
    groups = ["preprocess", "tag"]
    production_procno = 1

    def setup(self):
        df = bin.ogle3.load()
        self.ogle_id = df.ID.values
        self.ogle_cls = df.cls.values
        self.ogle_ra = df.ra_deg.values
        self.ogle_dec = df.dec_deg.values

    def add_columns(self, tile_data):
        # create dtype
        dtype = {
            "names": list(tile_data.dtype.names) + ["ogle3_type", "ogle3_id"],
            "formats": (
                [e[-1] for e in tile_data.dtype.descr] + ["|S13", "|S25"])}

        types = np.chararray(len(tile_data), itemsize=13)
        ids = np.chararray(len(tile_data), itemsize=25)

        types[:], ids[:] = "", ""

        # create an empty array and copy the values
        data = np.empty(len(tile_data), dtype=dtype)
        for name in tile_data.dtype.names:
            if name == "ogle3_type":
                data[name] = types
            if name == "ogle3_id":
                data[name] = ids
            else:
                data[name] = tile_data[name]
        return data

    def process(self, tile):
        tile_data = tile.load_npy_file()
        tile_data = self.add_columns(tile_data)

        tile_ra, tile_dec = tile_data["ra_k"], tile_data["dec_k"]

        matchs = matcher.matchs(tile_ra, self.ogle_ra, tile_dec, self.ogle_dec)
        tile_idxs, ogle_idxs = [], []
        for tile_idx, ogle_idx in matchs:
            tile_idxs.append(tile_idx)
            ogle_idxs.append(ogle_idx)

        if tile_idxs:
            tile_data["ogle3_id"][tile_idxs] = self.ogle_id[ogle_idxs]
            tile_data["ogle3_type"][tile_idxs] = self.ogle_cls[ogle_idxs]

        tile.store_npy_file(tile_data)
        tile.ogle3_tagged_number = len(tile_idxs)
        tile.status = "ready-to-unred"
        yield tile
        self.session.commit()
