#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

import numpy as np

from ..lib import matcher

from ..models import PawprintStackXTile


# =============================================================================
# STEP
# =============================================================================

class Match(run.Step):
    """Determine the sources existing in the tile and in the pawprint stacks

    """

    model = PawprintStackXTile
    conditions = [model.status == "ready-to-match"]
    groups = ["match"]
    production_procno = 1

    def iter_matchs(self, pxt, tile_data, pwp_data,
                    tile_ra, tile_dec, pwp_ra, pwp_dec):
        matchs = matcher.matchs(tile_ra, pwp_ra, tile_dec, pwp_dec)
        for idx_ms, idx_pwp in matchs:
            pwp_src = pwp_data[idx_pwp]
            ms = tile_data[idx_ms]
            row = tuple(
                [pxt.tile.name, pxt.tile_id] +
                [ms[n] for n in tile_data.dtype.names] +
                [pxt.pawprint_stack_id, pxt.pawprint_stack.band] +
                [pwp_src[n] for n in pwp_data.dtype.names])
            yield row

    def process(self, pxt):
        tile_data = pxt.tile.load_npy_file()
        pwp_data = pxt.pawprint_stack.load_npy_file()

        # create dtype
        dtype = {
            "names": (
                ["tile_name", "tile_id"] +
                ["bm_src_{}".format(n) for n in tile_data.dtype.names] +
                ["pwp_stack_id", "pwp_stack_band"] +
                ["pwp_stack_src_{}".format(n) for n in pwp_data.dtype.names]),
            "formats": (
                ["|S10", int] + [e[-1] for e in tile_data.dtype.descr] +
                [int, "|S10"] + [e[-1] for e in pwp_data.dtype.descr])
        }

        pwp_ra, pwp_dec = pwp_data["ra_deg"], pwp_data["dec_deg"]
        tile_ra, tile_dec = tile_data["ra_k"], tile_data["dec_k"]

        matchs = self.iter_matchs(
            pxt=pxt, tile_data=tile_data, pwp_data=pwp_data,
            tile_ra=tile_ra, tile_dec=tile_dec,
            pwp_ra=pwp_ra, pwp_dec=pwp_dec)

        arr = np.fromiter(matchs, dtype=dtype)

        pxt.matched_number = len(arr)
        pxt.store_npy_file(arr)
        pxt.status = "matched"

        yield pxt

        self.session.commit()
