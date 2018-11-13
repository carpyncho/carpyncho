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

class VSTagTile(run.Step):
    """Set the class of the stars from the vs catalogs

    """

    model = Tile
    conditions = [model.status == "ready-to-tag"]
    groups = ["preprocess", "tag"]
    production_procno = 1

    def setup(self):
        df = bin.catalogs.load()
        self.vs_id = df.ID.values
        self.vs_cls = df.cls.values
        self.vs_ra = df.ra.values
        self.vs_dec = df.dec.values
        self.vs_catalog = df.catalog.values

    def add_columns(self, tile_data):
        # create dtype
        dtype = {
            "names": list(tile_data.dtype.names) + [
                "vs_type", "vs_id", "vs_catalog"],
            "formats": (
                [e[-1] for e in tile_data.dtype.descr] + [
                    "|S13", "|S25", "|S13"])}

        types = np.chararray(len(tile_data), itemsize=13)
        ids = np.chararray(len(tile_data), itemsize=25)
        catalogs  = np.chararray(len(tile_data), itemsize=13)

        types[:], ids[:] = "", ""

        # create an empty array and copy the values
        data = np.empty(len(tile_data), dtype=dtype)
        for name in tile_data.dtype.names:
            if name == "vs_type":
                data[name] = types
            if name == "vs_id":
                data[name] = ids
            if name == "vs_catalog":
                data[name] = catalogs
            else:
                data[name] = tile_data[name]
        return data

    def process(self, tile):
        tile_data = tile.load_npy_file()
        tile_data = self.add_columns(tile_data)

        tile_ra, tile_dec = tile_data["ra_k"], tile_data["dec_k"]

        matchs = matcher.matchs(tile_ra, self.vs_ra, tile_dec, self.vs_dec)
        tile_idxs, vs_idxs = [], []

        for tile_idx, vs_idx in matchs:
            tile_idxs.append(tile_idx)
            vs_idxs.append(vs_idx)

        if tile_idxs:
            tile_data["vs_id"][tile_idxs] = self.vs_id[vs_idxs]
            tile_data["vs_type"][tile_idxs] = self.vs_cls[vs_idxs]
            tile_data["vs_catalog"][tile_idxs] = self.vs_catalog[vs_idxs]

        tile.store_npy_file(tile_data)
        tile.ogle3_tagged_number = len(tile_idxs)
        tile.status = "ready-to-unred"
        yield tile
        self.session.commit()
