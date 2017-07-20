#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import copy

from corral import run

import numpy as np

import pandas as pd

from .. import bin
from ..models import Tile


# =============================================================================
# CONSTANTS
# =============================================================================

OGLE_3_TAG_PATH = bin.get("ogle3.txt")


# =============================================================================
# STEP
# =============================================================================

class Ogle3TagTile(run.Step):
    """Set the class of the stars from the OGLE-3 Survey

    """

    model = Tile
    conditions = [model.status == "ready-to-tag"]
    groups = ["preprocess"]
    production_procno = 1

    def setup(self):
        with open(OGLE_3_TAG_PATH) as fp:
            for idx, line in enumerate(fp):
                if idx == 6:
                    self.columns = line.split()[1:]
                    break
        self.df = pd.read_table(OGLE_3_TAG_PATH, skiprows=7, names=self.columns)

    def process(self, tile):
        import ipdb; ipdb.set_trace()
