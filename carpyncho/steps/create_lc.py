#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import numpy as np

from corral import run

from ..models import Tile, PawprintStackXTile, LightCurves


# =============================================================================
# CONSTANTS
# =============================================================================

COLUMNS = [
    "bm_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


# =============================================================================
# STEP
# =============================================================================

class CreateLightCurves(run.Step):
    """Create a unique files with only the information of time, manitude
    and magnitude error for every source in a tile.
    This file is used for the feature extractor for allow to only retrieve
    a fraction of the information and don't fullfill the memory.

    """

    model = Tile
    conditions = [model.status == "ready-to-match"]
    groups = ["postprocess"]

    def generate(self):
        for tile in super(CreateLightCurves, self).generate():
            query = self.session.query(PawprintStackXTile).filter(
                PawprintStackXTile.tile_id == tile.id)
            not_matched = query.filter(
                PawprintStackXTile.status != "matched").count()
            if tile.lcurves is None and not not_matched:
                yield tile, query

    def validate(self, generated):
        if isinstance(generated, (LightCurves, Tile)):
            return True
        tile, query = generated
        return isinstance(tile, Tile) and hasattr(query, "__iter__")

    def process(self, tile_pxts):
        tile, pxts = tile_pxts

        # new light curve
        lc = LightCurves(tile=tile)

        obs, total = None, pxts.count()
        for idx, pxt in enumerate(pxts):
            print("Processing pxt {} of {} (Tile {})".format(idx, total, tile.name))
            data = pxt.load_npy_file()[COLUMNS]
            if obs is None:
                obs = data
            else:
                obs = np.concatenate((obs, data))

        if obs is not None:
            lc.observations = obs

        tile.status = "ready-to-extract-features"

        yield lc
        yield tile

        self.session.commit()
