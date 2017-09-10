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
    write_limit = conf.settings.get("FE_WRITE_LIMIT", 500)

    def setup(self):
        print("chunk_size:", self.chunk_size)
        print("min_observation:", self.min_observation)
        print("write_limit:", self.write_limit)
        self.fs = feets.FeatureSpace(
            data=["magnitude", "time", "error"],
            exclude=["SlottedA_length", "StetsonK_AC"])

    def parse_obs(self, lc, src_ids):
        lcs = []
        for src_id, df in lc.get_obs(src_ids):
            time = df.pwp_stack_src_hjd.values
            mag = df.pwp_stack_src_mag3.values
            mag_err = df.pwp_stack_src_mag_err3.values

            sort_mask = time.argsort()
            data = (mag[sort_mask], time[sort_mask], mag_err[sort_mask])
            lcs.append(data)
        return lcs

    def get_sources(self, lc):
        import ipdb; ipdb.set_trace()


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

        stored_features = lc.features
        lc_features = None
        chunks_n = len(chunks)
        for idx, src_chunk in enumerate(chunks):
            print("Processing chunk {} of {}".format(idx, chunks_n))
            # extraer las fuentes
            src_chunk_ids = src_chunk.id
            lcs = self.parse_obs(lc, src_chunk_ids)

            # ejecutar feets
            features, values = self.fs.extract(lcs)
            df = self.merge_features(src_chunk, features, values)

            # concatenar features
            if lc_features is None:
                lc_features = df
            else:
                lc_features = pd.concat([lc_features, df], ignore_index=True)

            # guardamos de ser necesario
            if len(lc_features) >= self.write_limit:
                print "Writing..."
                if stored_features is not None:
                    stored_features = pd.concat(
                        [stored_features, lc_features], ignore_index=True)
                else:
                    stored_features = lc_features
                lc.features = stored_features
                lc_features = None

        if lc_features is not None:
            print "Writing..."
            if stored_features is not None:
                stored_features = pd.concat(
                    [stored_features, lc_features], ignore_index=True)
            else:
                stored_features = lc_features
            lc.features = stored_features
            lc_features = None

        yield lc

        # cerrar hdf
        lc.hdf_storage.close()

        # commitear
        lc.tile.ready = True
        yield lc.tile
        self.session.commit()
