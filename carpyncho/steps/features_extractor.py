#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run, conf

import numpy as np

import pandas as pd

import feets

from ..models import LightCurves


# =============================================================================
# STEP
# =============================================================================

class FeaturesExtractor(run.Step):
    """Creates a features tables for every sources in a given Tile

    """

    model = LightCurves
    conditions = [
        model.tile.has(status="ready-to-extract-features"),
        model.tile.has(ready=False)]
    groups = ["fe"]

    chunk_size = conf.settings.get("FE_CHUNK_SIZE", 10)
    min_observation = conf.settings.get("FE_MIN_OBSERVATION", 30)

    def setup(self):
        self.fs = feets.MPFeatureSpace(data=["magnitude", "time", "error"])

        # TODO DEV ONLY
        import os
        self.data_path = os.path.join(
            os.path.abspath(os.path.dirname(feets.tests.__file__)), "data")
        self.lc_path = os.path.join(self.data_path, "lc_1.3444.614.B_R.npz")
        with np.load(self.lc_path) as npz:
            self.lc = (
                npz['mag'],
                npz['time'],
                npz['error'])

    def parse_obs(self, lc, src_ids):
        lcs = []
        for src_id, df in lc.get_obs(src_ids):
            time = df.pwp_stack_src_hjd.values
            mag = df.pwp_stack_src_mag3.values
            mag_err = df.pwp_stack_src_mag_err3.values
            data = np.vstack((mag, time, mag_err))
            # todo
            data = self.lc

            lcs.append(data)
        return lcs

    def get_sources(self, lc):
        sources = lc.sources
        sources = sources[sources.obs_number >= self.min_observation]
        return sources.to_records()

    def chunk_it(self, sources):
        split_size = int(len(sources) / self.chunk_size)
        chunks = np.array_split(sources, split_size)
        return chunks

    def merge_features(self, srcs, features, values):
        df1 = pd.DataFrame(srcs)[["id", "ogle3_type", "obs_number"]]
        df2 = pd.DataFrame(values, columns=features)
        return pd.concat((df1, df2), axis=1)

    def process(self, lc):
        sources = self.get_sources(lc)
        chunks = self.chunk_it(sources)

        lc_features = None
        for src_chunk in chunks:
            # extraer las fuentes
            src_chunk_ids = src_chunk.id
            lcs = self.parse_obs(lc, src_chunk_ids)

            # ejecutar feets
            features, values = self.fs.extract(lcs)

            # concatenar features
            df = self.merge_features(src_chunk, features, values)
            if lc_features is None:
                lc_features = df
            else:
                lc_features = pd.concat((lc_features, df), ignore_index=True)
                break

        # almacenar features
        lc.features = lc_features
        yield lc

        # cerrar hdf
        lc.hdf_storage.close()

        # commitear
        lc.tile.ready = True
        yield lc.tile
        self.session.commit()
