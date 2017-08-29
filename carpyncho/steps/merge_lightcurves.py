#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from collections import Counter

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

    def process(self, tile_pxts):
        tile, pxts = tile_pxts
        cnt = Counter()

        # new light curve
        lc = self.get_lcs(tile)

        # dataframe with all the sources of the band merge
        sources_df = pd.DataFrame(tile.load_npy_file())

        for pxt in pxts:
            # convert the match into a dataframe
            obs_df = pd.DataFrame(pxt.load_npy_file())

            # append the data frame of observations into the
            # existing ones
            lc.append_obs(obs_df)

            # update the obs number
            cnt.update(obs_df["bm_src_id"].values)

            # remove from memory
            del obs_df

        # add a new column with the number of matches of every source
        # in the band-merge
        sources_df["obs_number"] = sources_df.id.apply(lambda e: cnt.get(e, 0))
        lc.sources = sources_df
        del sources_df

        yield lc
        yield tile

        lc.hdf_storage.close()
        self.session.commit()
