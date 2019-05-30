#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import numpy as np

from joblib import Parallel, delayed, cpu_count

from corral import run

from ..models import Tile, PawprintStackXTile, LightCurves
from ..lib import beamc


# =============================================================================
# CONSTANTS
# =============================================================================

COLUMNS = [
    "bm_src_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


S_COLUMNS = [
    "bm_src_id", "pwp_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]

CPUS = cpu_count()


# =============================================================================
# FUNCTION
# =============================================================================

def read_pxt(pxt_path, pwp_id, total, idx, tile_name):
    print("Processing pxt {} of {} (Tile {})".format(idx, total, tile_name))
    arr = np.load(pxt_path)[COLUMNS]

    ids = np.empty(len(arr)).astype(int)
    ids[:] = int(pwp_id)

    extra_cols = [("pwp_id", ids, )]

    arr = beamc.add_columns(arr, extra_cols)[S_COLUMNS]

    return arr


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
            if tile.lcurves is None and not not_matched and query.count():
                yield tile, query

    def validate(self, generated):
        if isinstance(generated, (LightCurves, Tile)):
            return True
        tile, query = generated
        return isinstance(tile, Tile) and hasattr(query, "__iter__")

    def process(self, tile_pxts):
        tile, pxts = tile_pxts
        print tile, "<<" * 40

        # new light curve
        lc = LightCurves(tile=tile)

        # variables for joblib
        tile_name, total = tile.name, pxts.count()
        pxts_paths_ids = [
            (pxt.npy_file_path, pxt.pawprint_stack_id) for pxt in pxts]

        with Parallel(n_jobs=CPUS) as jobs:
            data = jobs(
                delayed(read_pxt)(pxt[0], pxt[1], total, idx, tile_name)
                for idx, pxt in enumerate(pxts_paths_ids))

        lc.observations = np.concatenate(data)
        tile.status = "ready-to-extract-features"

        yield lc
        yield tile

        self.session.commit()
