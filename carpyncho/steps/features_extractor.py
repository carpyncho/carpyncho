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
        print("chunk_size:", self.chunk_size)
        print("min_observation:", self.min_observation)
        self.fs = feets.MPFeatureSpace(
            data=["magnitude", "time", "error"],
            exclude=["SlottedA_length", "StetsonK_AC"])

    def parse_obs(self, lc, src_ids):
        lcs = []
        for src_id, df in lc.get_obs(src_ids):
            time = df.pwp_stack_src_hjd.values
            mag = df.pwp_stack_src_mag3.values
            mag_err = df.pwp_stack_src_mag_err3.values
            data = (mag, time, mag_err)
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
        chunks_n = len(chunks)
        for idx, src_chunk in enumerate(chunks):
            print("Processing chunk {} of {}".format(idx, chunks_n))
            # extraer las fuentes
            src_chunk_ids = src_chunk.id
            lcs = self.parse_obs(lc, src_chunk_ids)

            # ejecutar feets
            features, values = self.fs.extract(lcs)

            # concatenar features
            df = self.merge_features(src_chunk, features, values)
            lc.features_append(df)

        yield lc

        # cerrar hdf
        lc.hdf_storage.close()

        # commitear
        lc.tile.ready = True
        yield lc.tile
        self.session.commit()
