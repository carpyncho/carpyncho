#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

import numpy as np

import feets

from ..models import LightCurves


# =============================================================================
# STEP
# =============================================================================

class FeaturesExtractor(run.Step):
    """Creates a features tables for every sources in a given Tile

    """

    model = LightCurves
    conditions = [model.tile.has(status="ready-to-extract-features")]
    groups = ["fe"]

    def process(self, lc):
        sources_ids = lc.sources.id.values
        import ipdb; ipdb.set_trace()
