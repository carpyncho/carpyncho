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




# =============================================================================
# STEP
# =============================================================================

class OGLE3TagTile(run.Step):
    """Set the class of the stars from the OGLE-3 Survey

    """

    model = Tile
    conditions = [model.status == "ready-to-tag"]
    groups = ["preprocess"]
    production_procno = 1

    def setup(self):
        self.ogle_3_data_path = bin.get("ogle3.txt")
        with open(self.ogle_3_data_path) as fp:
            for idx, line in enumerate(fp):
                if idx == 6:
                    self.columns = line.split()[1:]
                    break
        self.df = pd.read_table(
            self.ogle_3_data_path, skiprows=7, names=self.columns)

    def process(self, tile):
        import ipdb; ipdb.set_trace()
