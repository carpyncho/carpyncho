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

from .tile import Tile
from .pawprint_stack import PawprintStack


# =============================================================================
# PawprintStackXTile
# =============================================================================

class PawprintStackXTile(db.Model):
    """Relation between a pawprint-stack and a tile. Because the virca, overlap
    some pawprints can be in two tiles

    """

    __tablename__ = "PawprintStackXTile"
    __table_args__ = (
        db.UniqueConstraint('pawprint_stack_id', 'tile_id',
                            name='_pawprint_tile_uc'),
    )

    statuses = db.Enum(
        "raw", "ready-to-match", "matched", name="pxt_statuses")

    id = db.Column(db.Integer, db.Sequence('pxt_id_seq'), primary_key=True)

    pawprint_stack_id = db.Column(
        db.Integer, db.ForeignKey('PawprintStack.id'), nullable=False)
    pawprint_stack = db.relationship(
        "PawprintStack", backref=db.backref("pxts"))

    tile_id = db.Column(db.Integer, db.ForeignKey('Tile.id'), nullable=False)
    tile = db.relationship("Tile", backref=db.backref("pxts"))

    _npy_filename = db.Column("npy_filename", db.Text)

    matched_number = db.Column(db.Integer, nullable=True)

    status = db.Column(statuses, default="raw")

    def __repr__(self):
        string = "<PXT '{}: {}'>"
        return string.format(self.tile.name, self.pawprint_stack.name)

    @property
    def npy_file_path(self):
        if self._npy_filename:
            return os.path.join(
                settings.MATCHS_DIR, self.tile.name, self._npy_filename)

    def store_npy_file(self, arr):
        self._npy_filename = "{}_{}.npy".format(
            self.tile.name, self.pawprint_stack.name)
        file_dir = os.path.dirname(self.npy_file_path)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        np.save(self.npy_file_path, arr)

    def load_npy_file(self):
        return np.load(self.npy_file_path)
