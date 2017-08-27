#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import pandas as pd

from corral import run

from ..models import Tile, PawprintStackXTile, LightCurves


# =============================================================================
# STEP
# =============================================================================

class MergeLightCurves(run.Step):
    """Create a one file-per-tile hdf5 file with all the
    information of the matched sources. This file is
    used for the feature extractor for allow to only retrieve
    a fraction of the observations and don't fullfill the memory.

    """

    model = Tile
    conditions = [model.status == "ready-to-match"]
    groups = ["postprocess"]
    production_procno = 1

    def generate(self):
        for tile in super(MergeLightCurves, self).generate():
            query = self.session.query(PawprintStackXTile).filter(
                PawprintStackXTile.tile_id == tile.id)
            not_matched = query.filter(
                PawprintStackXTile.status != "matched").count()
            if not not_matched:
                yield tile, query

    def validate(self, generated):
        if isinstance(generated, (LightCurves, Tile)):
            return True
        tile, query = generated
        return isinstance(tile, Tile) and hasattr(query, "__iter__")

    def get_lcs(self, tile):
        lc = self.session.query(
            LightCurves).filter(LightCurves.tile_id==tile.id).first()
        if lc is None:
            lc = LightCurves(tile=tile)
        return lc

    def process(self, generated):
        tile, pxts = generated

        lc = self.get_lcs(tile)

        sources_df = pd.DataFrame(tile.load_npy_file())
        lc.sources = sources_df
        del sources_df

        pwpx_ids = lc.pwpx_ids
        if pwpx_ids:
            pxts = pxts.filter(
                PawprintStackXTile.pawprint_stack_id.notin_(pwpx_ids))
        for pxt in pxts:
            obs_df = pd.DataFrame(pxt.load_npy_file())
            lc.append_obs(obs_df)
            del obs_df

            pwpx_ids.add(pxt.id)
            lc.pwpx_ids = pwpx_ids

        yield lc

        #~ tile.status = "ready-to-extract-features"
        yield tile

        lc.hdf_storage.close()
        self.session.commit()
