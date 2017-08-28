#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from collections import Counter

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

    chunk_size = 300
    min_observation = 5

    def parse_obs(self, lc, chunk):
        src_ids, obs, lcs = [], [], []
        for src_id, df in lc.get_obs(chunk, self.min_observation):
            import ipdb; ipdb.set_trace()
            time = df.pwp_stack_src_hjd.values
            mag = df.pwp_stack_src_mag3.values
            mag_err = df.pwp_stack_src_mag_err3.values
            sort_mask = time.argsort()

            src_ids.append(src_id)
            obs.append(len(time))
            lcs.append((mag[sort_mask], time[sort_mask], mag_err[sort_mask]))

        return src_ids, obs, lcs

    def setup(self):
        self.fs = feets.FeatureSpace(data=["magnitude", "time", "error"])

    def get_sources(self, lc):
        cnt, total = Counter(), len(lc.tile.pxts)
        print "Retrieving sources counters for {}".format(lc)
        for idx, pxt in enumerate(lc.tile.pxts):
            print "Reading {}/{} match files".format(idx, total)
            data = pxt.load_npy_file()["bm_src_id"]
            cnt.update(data)
        import ipdb; ipdb.set_trace()


    def process(self, lc):
        sources_ids = self.get_sources(lc)
        sources_ids = lc.sources.id.values

        split_size = int(len(sources_ids) / self.chunk_size)
        chunks = np.array_split(sources_ids, split_size)

        for chunk in chunks:
            # extraer las fuentes
            src_ids, obs_number, lcs = self.parse_obs(lc, chunk)
            if not src_ids:
                continue

            # ejecutar feets
            features, values = self.fs.extract(lcs)

            # almacenar features
            import ipdb; ipdb.set_trace()

        # cerrar hdf
        # commitear
