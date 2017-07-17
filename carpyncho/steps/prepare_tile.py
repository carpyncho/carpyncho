#!/usr/bin/env python
# -*- coding: utf-8 -*-

from corral import run

import numpy as np

from ..models import Tile


EPOCHS = 3


MASTER_SOURCE_DEFAULT_PARAMS = {
    "dtype": {
        'names': (
            'ra_h', 'dec_h', 'ra_j', 'dec_j', 'ra_k', 'dec_k'
        ),
        'formats': (
            float, float, float, float, float, float
        )
    },
    "usecols": [0, 1, 2, 3, 4, 5],
}


class PrepareTile(run.Step):

    model = Tile
    conditions = [model.status == "raw"]
    groups = ["measurement"]
    production_procno = 1

    def read_dat(self, fp):
        arr = np.genfromtxt(
            fp, skip_header=EPOCHS, **MASTER_SOURCE_DEFAULT_PARAMS)
        flt = (arr["ra_k"] > -9999.0)
        filtered = arr[flt]
        return filtered

    def process(self, tile):
        with open(tile.raw_file_path) as fp:
            arr = self.read_dat(fp)

        tile.store_npy_file(arr)
        tile.size = len(arr)
        tile.status = "ready"

        self.save(tile)
        self.session.commit()
