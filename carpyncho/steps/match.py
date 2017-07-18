#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

from astropysics import coords

from ..models import PawprintStackXTile


# =============================================================================
# CONSTANTS
# =============================================================================

MAX_MATCH = 3 * 9.2592592592592588e-5


# =============================================================================
# STEP
# =============================================================================

class Match(run.Step):
    """Determine the sources existing in the tile and in the pawprint stacks

    """

    model = PawprintStackXTile
    conditions = [model.status == "ready"]
    groups = ["match"]
    production_procno = 1

    def setup(self):
        self.maxmatch = MAX_MATCH
        self.mode = "nearest"

    def iter_matchs(self, pxt, tile_data, pwp_data,
                    nearestind_pwp, match_pwp,
                    nearestind_ms, match_ms):
        for idx_pwp, idx_ms in enumerate(nearestind_ms):
            if match_ms[idx_pwp] and \
               nearestind_pwp[idx_ms] == idx_pwp and match_pwp[idx_ms]:
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

        pwp_ra, pwp_dec = pwp_data["ra_deg"], pwp_data["dec_deg"]
        tile_ra, tile_dec = tile_data["ra_k"], tile_data["dec_k"]

        nearestind_pwp, _, match_pwp = coords.match_coords(
            tile_ra, tile_dec, pwp_ra, pwp_dec,
            eps=self.maxmatch, mode=self.mode)
        nearestind_ms, _, match_ms = coords.match_coords(
            pwp_ra, pwp_dec, tile_ra, tile_dec,
            eps=self.maxmatch, mode=self.mode)

        matchs = self.iter_matchs(
            pxt=pxt, tile_data=tile_data, pwp_data=pwp_data,
            nearestind_pwp=nearestind_pwp, match_pwp=match_pwp,
            nearestind_ms=nearestind_ms, match_ms=match_ms)
