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

ZONES = {
    "b": "3",
    "d": "4"
}


# =============================================================================
# STEP
# =============================================================================

class ReadTile(run.Step):
    """Convert the tile into a numpy array to be tagged and matched
    again their pawprints.

    The sources are stored in a numpy record array wiht the orinal data
    plut the id of every source.

    ### Understanding the Sources ID:

    The id are an 14 digits integer with the format `PTTTOOOOOOOOOO` where:

    - **P:** indicate the position of the tile on the VVV (3=bulge, 4=disc).
    - **TTT:** Are the tile number of the VVV.
    - **0000000000:** is a sequential number of the source inside the tile.

    #### Example

    The id "40010000000130" indicate the 130th source inside the tile d001.

    """

    model = Tile
    conditions = [model.status == "raw"]
    groups = ["preprocess", "read"]
    production_procno = 1

    def read_dat(self, fp):
        arr = np.genfromtxt(
            fp, skip_header=EPOCHS,
            dtype=SOURCE_DTYPE,
            usecols=USECOLS)
        flt = (arr["ra_k"] > -9999.0)
        filtered = arr[flt]
        return filtered, len(filtered)

    def add_columns(self, odata, size, tile_name, dtypes):
        """Add id to existing recarray

        """

        tile_name = ZONES[tile_name[0].lower()] + tile_name[1:]

        def get_id(order):
            order = str(order).rjust(10, "0")
            return int(tile_name + order)

        # create ids
        ids = np.fromiter(
            (get_id(idx + 1) for idx in range(size)), dtype=np.int64)

        dtype = copy.deepcopy(dtypes)
        dtype["names"].insert(0, "id")
        dtype["formats"].insert(0, np.int64)

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
        arr = self.add_columns(oarr, size, tile.name, SOURCE_DTYPE)

        tile.store_npy_file(arr)
        tile.size = size
        tile.status = "ready-to-tag"

        self.save(tile)
        self.session.commit()
