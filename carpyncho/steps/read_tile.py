#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import copy

from corral import run

import numpy as np

from ..models import Tile


# =============================================================================
# CONSTANTS
# =============================================================================

EPOCHS = 3


SOURCE_DTYPE = {
    'names': ['ra_h', 'dec_h', 'ra_j', 'dec_j', 'ra_k', 'dec_k'],
    'formats': [float, float, float, float, float, float]
}

USECOLS = [0, 1, 2, 3, 4, 5]


# =============================================================================
# STEP
# =============================================================================

class ReadTile(run.Step):
    """Convert the tile into a numpy array to be tagged and matched
    again their pawprints

    """

    model = Tile
    conditions = [model.status == "raw"]
    groups = ["preprocess"]
    production_procno = 1

    def read_dat(self, fp):
        arr = np.genfromtxt(
            fp, skip_header=EPOCHS,
            dtype=SOURCE_DTYPE,
            usecols=USECOLS)
        flt = (arr["ra_k"] > -9999.0)
        filtered = arr[flt]
        return filtered, len(filtered)

    def add_columns(self, odata, size, tile_id, dtypes):
        """Add id to existing recarray

        """

        # create ids
        ids = np.fromiter(
            ("tile_{}_{}".format(tile_id, idx + 1) for idx in range(size)),
            dtype="|S20")

        dtype = copy.deepcopy(dtypes)
        dtype["names"].insert(0, "id")
        dtype["formats"].insert(0, "|S20")

        # create an empty array and copy the values
        data = np.empty(len(odata), dtype=dtype)
        for name in data.dtype.names:
            if name == "id":
                data[name] = ids
            else:
                data[name] = odata[name]
        return data

    def process(self, tile):
        with open(tile.raw_file_path) as fp:
            oarr, size = self.read_dat(fp)
        arr = self.add_columns(oarr, size, tile.id, SOURCE_DTYPE)

        tile.store_npy_file(arr)
        tile.size = size
        tile.status = "ready-to-tag"

        self.save(tile)
        self.session.commit()
