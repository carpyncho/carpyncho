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

    chunk_size = 350

    def process(self, lc):
        sources_ids = lc.sources.id.values

        split_size = int(len(sources_ids) / self.chunk_size)
        chunks = np.array_split(sources_ids, split_size)

        for chunk in chunks:
            # extraer las fuentes

            # ejecutar feets

            # almacenar features

        # cerrar hdf
        # commitear
