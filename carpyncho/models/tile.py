#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# IMPORTS
# =============================================================================

import os
import shutil

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
    tile = db.relationship("Tile", backref=db.backref("lcs"), lazy='joined')

    _hdf_filename = db.Column("hdf_filename", db.Text)

    def __repr__(self):
        return "<LightCurves of '{}'>".format(self.tile.name)

    def filepath(self):
        if not self._hdf_filename:
            self._hdf_filename = "{}.h5".format(self.tile.name)
        return os.path.join(settings.LC_DIR, self._hdf_filename)

    @property
    def hdf_storage(self):
        if not hasattr(self, "_hdf"):
            fpath = self.filepath()
            self._hdf = pd.HDFStore(fpath)
        return self._hdf

    @property
    def sources(self):
        tn = "{}_sources".format(self.tile.name)
        return self.hdf_storage[tn]

    @sources.setter
    def sources(self, df):
        tn = "{}_sources".format(self.tile.name)
        self.hdf_storage.put(tn, df, format='table', data_columns=True)

    @property
    def features(self):
        tn = "{}_features".format(self.tile.name)
        return self.hdf_storage[tn]

    @features.setter
    def features(self, df):
        tn = "{}_features".format(self.tile.name)
        self.hdf_storage.put(tn, df, format='table', data_columns=True)

    def append_obs(self, df):
        tn = "{}_observations".format(self.tile.name)
        if tn not in self.hdf_storage:
            self.hdf_storage.append(
                tn, df, format='table', data_columns=True, min_itemsize=100)
        else:
            self.hdf_storage.append(tn, df, format='table')

    def get_obs(self, ids):
        flt = "bm_src_id in {}".format(list(ids))
        tn = "{}_observations".format(self.tile.name)
        columns = [
            "bm_src_id", "pwp_stack_src_hjd",
            "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]
        obs = self.hdf_storage.select(
            tn, where=flt, columns=columns)
        obs = obs.groupby("bm_src_id")

        groups = np.array(obs.groups.keys())

        for id in ids:
            yield id, obs.get_group(id) if id in groups else None
