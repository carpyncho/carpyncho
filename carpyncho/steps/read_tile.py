#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

import numpy as np

from PyAstronomy import pyasl

from ..models import Tile

from carpyncho.lib.beamc import add_columns


# =============================================================================
# CONSTANTS
# =============================================================================

EPOCHS = 3


SOURCE_DTYPE = {
    'names': [
        'ra_h', 'dec_h', 'ra_j',
        'dec_j', 'ra_k', 'dec_k',
        "mag_h", "mag_j", "mag_k",
        "mag_err_h", "mag_err_j", "mag_err_k",
        "scls_h", "scls_j", "scls_k"],
    'formats': [
        float, float, float,
        float, float, float,
        float, float, float,
        float, float, float,
        int, int, int]
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
    conditions = [model.status == "raw"]
    groups = ["preprocess", "read"]
    production_procno = 1

    def read_dat(self, fp):
        arr = np.genfromtxt(
            fp, skip_header=EPOCHS,
            dtype=SOURCE_DTYPE,
            usecols=USECOLS)
        flt = (
            (arr["ra_k"] != -9999.0) &
            (arr["ra_j"] != -9999.0) &
            (arr["ra_h"] != -9999.0))
        filtered = arr[flt]
        return filtered, len(filtered)

    def add_columns(self, npy_arr, tile):
        """Add id to existing recarray

        """
        size = len(npy_arr)

        tile_name = Tile.ZONES[tile.name[0].lower()] + tile.name[1:]

        # create ids
        def get_id(order):
            order = str(order).rjust(10, "0")
            return int(tile_name + order)
        ids = np.fromiter(
            (get_id(idx + 1) for idx in range(size)), dtype=np.int64)

        # calculate the hjds
        mjd_h, mjd_j, mjd_k = tile.epochs

        gen_h = (pyasl.helio_jd(mjd_h, ra, dec)
                 for ra, dec in zip(npy_arr["ra_h"], npy_arr["dec_h"]))
        hjd_h = np.fromiter(gen_h, dtype=float)

        gen_j = (pyasl.helio_jd(mjd_j, ra, dec)
                 for ra, dec in zip(npy_arr["ra_j"], npy_arr["dec_j"]))
        hjd_j = np.fromiter(gen_j, dtype=float)

        gen_k = (pyasl.helio_jd(mjd_k, ra, dec)
                 for ra, dec in zip(npy_arr["ra_k"], npy_arr["dec_k"]))
        hjd_k = np.fromiter(gen_k, dtype=float)

        columns = [
            ("id", ids),
            ("hjd_h", hjd_h),
            ("hjd_j", hjd_j),
            ("hjd_k", hjd_k)]
        return add_columns(npy_arr, columns)

    def process(self, tile):
        with open(tile.raw_file_path) as fp:
            oarr, size = self.read_dat(fp)
        arr = self.add_columns(oarr, tile)

        tile.store_npy_file(arr)
        tile.size = len(arr)
        tile.status = "ready-to-tag"

        self.save(tile)
        self.session.commit()
