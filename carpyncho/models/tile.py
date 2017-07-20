#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# IMPORTS
# =============================================================================

import os
import shutil

import numpy as np

from corral import db
from corral.conf import settings


# =============================================================================
# TILE
# =============================================================================

class Tile(db.Model):
    """Represent a VVV tile. Can has 3 states:

    - `raw`: The tile is discovery and only a path to the original path
      is added
    - `ready`: All the sources of the tile are stored as binary file

    """

    __tablename__ = "Tile"

    statuses = db.Enum(
        "raw",
        "ready-to-tag",
        "ready-to-match",
        "ready-to-extract-features",
        name="tile_statuses")

    id = db.Column(db.Integer, db.Sequence('tile_id_seq'), primary_key=True)
    name = db.Column(db.String(255), nullable=False, index=True, unique=True)

    _raw_filename = db.Column("raw_filename", db.Text)
    _npy_filename = db.Column("npy_filename", db.Text)

    size = db.Column(db.Integer, nullable=True)
    status = db.Column(statuses, default="raw")

    def __repr__(self):
        return "<Tile '{}'>".format(self.name)

    @property
    def raw_file_path(self):
        if self._raw_filename:
            return os.path.join(
                settings.RAW_TILES_DIR, self._raw_filename)
    @property
    def npy_file_path(self):
        if self._npy_filename:
            return os.path.join(
                settings.NPY_TILES_DIR, self._npy_filename)

    def store_raw_file(self, fpath):
        self._raw_filename = os.path.basename(fpath)
        shutil.copyfile(fpath, self.raw_file_path)

    def store_npy_file(self, arr):
        self._npy_filename = os.path.splitext(self._raw_filename)[0] + ".npy"
        np.save(self.npy_file_path, arr)

    def load_npy_file(self):
        return np.load(self.npy_file_path)
