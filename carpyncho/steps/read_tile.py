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
    'names': [
        'ra_h', 'dec_h', 'ra_j',
        'dec_j', 'ra_k', 'dec_k',
        "mag_h", "mag_j", "mag_k",
        "mag_err_h", "mag__err_j", "mag_err_k"],
    'formats': [
        float, float, float,
        float, float, float,
        float, float, float,
        float, float, float]
}

USECOLS = list(range(len(SOURCE_DTYPE["names"])))


# =============================================================================
# STEP
# =============================================================================

class ReadTile(run.Step):
    """Convert the tile into a numpy array to be tagged and matched
    again their pawprints.

    """

    model = Tile
    conditions = [] #[model.status == "raw"]
    groups = ["preprocess", "read"]
    production_procno = 1

    def read_dat(self, fp):
        arr = np.genfromtxt(
            fp, skip_header=EPOCHS,
            dtype=SOURCE_DTYPE,
            usecols=USECOLS)
        flt = (arr["ra_k"] != -9999.0)
        filtered = arr[flt]
        return filtered, len(filtered)

    def add_columns(self, odata, size, tile_name, dtypes):
        """Add id to existing recarray

        """

        tile_name = Tile.ZONES[tile_name[0].lower()] + tile_name[1:]

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
        import ipdb; ipdb.set_trace()
        with open(tile.raw_file_path) as fp:
            oarr, size = self.read_dat(fp)
        arr = self.add_columns(oarr, size, tile.name, SOURCE_DTYPE)

        tile.store_npy_file(arr)
        tile.size = size
        tile.status = "ready-to-tag"

        self.save(tile)
        self.session.commit()
