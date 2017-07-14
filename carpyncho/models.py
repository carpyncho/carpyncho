#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# DOCS
# =============================================================================

"""carpyncho database models

"""

# =============================================================================
# IMPORTS
# =============================================================================

import os
import shutil

import numpy as np

from corral import db
from corral.conf import settings

from astropy.coordinates import SkyCoord


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

    statuses = db.Enum("raw", "ready", name="tile_statuses")

    ready = db.Column(db.Boolean, default=False)

    id = db.Column(db.Integer, db.Sequence('tile_id_seq'), primary_key=True)
    name = db.Column(db.String(255), nullable=False, index=True, unique=True)

    _raw_filename = db.Column("raw_filename", db.Text)
    _npy_filename = db.Column("npy_filename", db.Text)

    size = db.Column(db.Integer, nullable=True)
    status = db.Column(statuses, default="raw")

    def __repr__(self):
        return "<Tile '{}'>".format(self.name)

    @property
    def raw_file(self):
        if self._raw_filename:
            return os.path.join(
                settings.RAW_TILES_DIR, self._raw_filename)
    @property
    def npy_file(self):
        if self._npy_filename:
            return os.path.join(
                settings.NPY_TILES_DIR, self._npy_filename)

    def store_raw_file(self, fpath):
        self._raw_filename = os.path.basename(fpath)
        shutil.copyfile(fpath, self.raw_file)


class PawprintStack(db.Model):
    """Represent a VVV pawprint stack in some band and some epoch.
    Can has 3 states:

    - `raw`: The tile is discovery and only a path to the original path
      is added
    - `ready`: All the sources of the pawprint are stored as binary file

    """

    __tablename__ = "PawprintStack"

    statuses = db.Enum("raw", "ready", name="pawprint_statuses")

    id = db.Column(
        db.Integer, db.Sequence('pawprint_id_seq'), primary_key=True)

    name = db.Column(db.String(255), nullable=False, unique=True)
    mjd = db.Column(db.Float, nullable=True)
    band = db.Column(db.String(5), nullable=True)

    size = db.Column(db.Integer, nullable=True)

    _raw_filename = db.Column("raw_filename", db.Text)
    _npy_filename = db.Column("npy_filename", db.Text)

    status = db.Column(statuses, default="raw")

    def __repr__(self):
        return "<PawprintStack '{}[{}]'>".format(
            repr(self.name), repr(self.band))

    @property
    def raw_file(self):
        if self._raw_filename:
            yearmonth = self._filename[1:7]
            day = self._filename[7:9]
            return os.path.join(
                settings.RAW_PAWPRINTS_DIR, yearmonth, day, self._raw_filename)

    @property
    def npy_file(self):
        if self._npy_filename:
            yearmonth = self._filename[1:7]
            day = self._filename[7:9]
            return os.path.join(
                settings.NPY_PAWPRINTS_DIR, yearmonth, day, self._npy_filename)

    def store_file(self, fpath):
        self._filename = os.path.basename(fpath)
        file_path = self.file_path()
        file_dir = os.path.dirname(file_path)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        shutil.copyfile(fpath, file_path)
