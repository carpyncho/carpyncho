#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# IMPORTS
# =============================================================================

import os
import shutil
from collections import Counter

import numpy as np

import pandas as pd

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

    The sources are stored in a numpy record array wiht the orinal data
    plut the id of every source.

    ### Understanding the Sources ID:

    The id are an 14 digits integer with the format `PTTTOOOOOOOOOO` where:

    - **P:** indicate the position of the tile on the VVV (3=bulge, 4=disc).
    - **TTT:** Are the tile number of the VVV.
    - **OOOOOOOOOO:** is a sequential number of the source inside the tile.

    #### Example

    The id "40010000000130" (4-0001-0000000130) indicate the 130th source
    inside the tile d001.

    """

    __tablename__ = "Tile"

    ZONES = {
        "b": "3",
        "d": "4"
    }

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

    ogle3_tagged_number = db.Column(db.Integer, nullable=True)

    ready = db.Column(db.Boolean, default=False)

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


class LightCurves(db.Model):
    """Stores the sources of the tile and also their observations
    inside a pawprint. This resume are stores inside an hdf5 for
    eficient access

    """

    __tablename__ = "LightCurves"

    id = db.Column(db.Integer, db.Sequence('lc_id_seq'), primary_key=True)

    tile_id = db.Column(
        db.Integer, db.ForeignKey('Tile.id'), nullable=False, unique=True)
    tile = db.relationship(
        "Tile", backref=db.backref("lcurves", uselist=False), lazy='joined')

    _src_obs_counter = db.Column("src_obs_cnt", db.PickleType, nullable=True)

    def __repr__(self):
        return "<LightCurves of '{}'>".format(self.tile.name)

    def _set_cnt(self, ids):
        cnt = Counter(ids)
        gen = (e for e in cnt.items())
        dtype = [("id", np.int64), ("cnt", int)]
        self._src_obs_counter = np.fromiter(gen, dtype=dtype)

    @property
    def lc_path(self):
        path = os.path.join(settings.LC_DIR, self.tile.name)
        if not os.path.isdir(path):
            os.makedirs(path)
        return path

    @property
    def obs_counter(self):
        return self._src_obs_counter

    @property
    def observations(self):
        fname = "lc_obs_{}.npy".format(self.tile.name)
        path = os.path.join(self.lc_path, fname)
        if os.path.exists(path):
            return np.load(path)

    @observations.setter
    def observations(self, arr):
        self._set_cnt(arr["bm_src_id"])
        fname = "lc_obs_{}.npy".format(self.tile.name)
        path = os.path.join(self.lc_path, fname)
        np.save(path, arr)

    @property
    def features(self):
        fname = "features_{}.npy".format(self.tile.name)
        path = os.path.join(self.lc_path, fname)
        if os.path.exists(path):
            return np.load(path)

    @features.setter
    def features(self, arr):
        fname = "features_{}.npy".format(self.tile.name)
        path = os.path.join(self.lc_path, fname)
        np.save(path, arr)
