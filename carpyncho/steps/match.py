#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

import numpy as np

from joblib import Parallel, delayed, cpu_count

from ..lib import matcher

from ..models import PawprintStackXTile

# =============================================================================
# FUNCTIONS
# =============================================================================

CPUS = cpu_count()


def iter_matchs(tile_name, tile_id, pawprint_stack_id, band,
                tile_data, pwp_data, tile_ra, tile_dec, pwp_ra, pwp_dec):
    matchs = matcher.matchs(tile_ra, pwp_ra, tile_dec, pwp_dec)
    for idx_ms, idx_pwp in matchs:
        pwp_src = pwp_data[idx_pwp]
        ms = tile_data[idx_ms]
        row = tuple(
            [tile_name, tile_id] +
            [ms[n] for n in tile_data.dtype.names] +
            [pawprint_stack_id, band] +
            [pwp_src[n] for n in pwp_data.dtype.names])
        yield row


def match(pxt_id, tile_name, tile_id, pawprint_stack_id, band, tile_data, pwp_path):
    pwp_data = np.load(pwp_path)

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

    matchs = iter_matchs(
        tile_name=tile_name, tile_id=tile_id,
        pawprint_stack_id=pawprint_stack_id, band=band,
        tile_data=tile_data, pwp_data=pwp_data,
        tile_ra=tile_ra, tile_dec=tile_dec,
        pwp_ra=pwp_ra, pwp_dec=pwp_dec)

    arr = np.fromiter(matchs, dtype=dtype)
    return arr, pxt_id


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

    def generate(self):
        query = super(Match, self).generate()
        models = list(query)
        number = int(len(models) / CPUS)
        return iter(np.array_split(models, number))

    def validate(self, chunk):
        return isinstance(chunk, np.ndarray)

    def read_arrs(self, chunk):
        tile_buff, reads = {}, []
        for pxt in chunk:

            if pxt.tile.name not in tile_buff:
                print("Reading {}...".format(pxt.tile))
                tile_buff[pxt.tile.name] = pxt.tile.load_npy_file()

            reads.append({
                "pxt_id": pxt.id,
                "tile_name": pxt.tile.name,
                "tile_id": pxt.tile.id,
                "pawprint_stack_id": pxt.pawprint_stack.id,
                "band": pxt.pawprint_stack.band,
                "tile_data": tile_buff[pxt.tile.name],
                "pwp_path":  pxt.pawprint_stack.npy_file_path})
        return reads

    def process(self, chunk):
        chunk_arrs = self.read_arrs(chunk)
        with Parallel(n_jobs=CPUS) as jobs:
            matches = jobs(delayed(match)(**arrs) for arrs in chunk_arrs)
        if len(chunk) != len(matches):
            raise ValueError("We have {} chunks but {} matches".format(
                len(chunk), len(matches)))

        for pxt, mtch in zip(chunk, matches):
            arr, pxt_id = mtch
            if pxt.id != pxt_id:
                raise ValueError("Pxt ID is {} but array ID is {}".format(
                    pxt.id, pxt_id))

            pxt.matched_number = len(arr)
            pxt.store_npy_file(arr)
            pxt.status = "matched"
            yield pxt

        self.session.commit()
