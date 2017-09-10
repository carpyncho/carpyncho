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
        self.fs = feets.FeatureSpace(
            data=["magnitude", "time", "error"],
            only=["PeriodLS", "Std", "CAR_mean"],
            exclude=["SlottedA_length", "StetsonK_AC"])

    def get_sources(self, lc):
        # get the ids of the sources with at least min_observation matches
        cnt = lc.obs_counter
        with_min_obs = pd.DataFrame(cnt[cnt["cnt"] >= self.min_observation])

        # extract ids and classes of ogle
        sources = pd.DataFrame(lc.tile.load_npy_file()[["id", "ogle3_type"]])

        # filter with the filter :D
        sources = sources[sources.id.isin(with_min_obs.id)]

        # add the cnt
        sources = pd.merge(sources, with_min_obs, on="id", how="inner")

        # free memory
        del cnt, with_min_obs

        return sources

    def get_obs(self, lc, sources):
        obs = lc.observations
        obs = obs[np.in1d(obs["bm_src_id"], sources.id)]
        return obs

    def chunk_it(self, sources):
        split_size = int(len(sources) / self.chunk_size)
        chunks = np.array_split(sources, split_size)
        return chunks

    def merge_features(self, srcs, features, values):
        df1 = pd.DataFrame(srcs)[["id", "ogle3_type", "obs_number"]]
        df2 = pd.DataFrame(values, columns=features)
        return pd.concat((df1, df2), axis=1)

    def extract(self, src_id, obs):
        print("Processing {}/{} ({})...".format(
            self._current_proc, self._total, self._tile_name))
        src_obs = obs[obs["bm_src_id"] == src_id]

        time = src_obs["pwp_stack_src_hjd"]
        mag = src_obs["pwp_stack_src_mag3"]
        mag_err = src_obs["pwp_stack_src_mag_err3"]

        sort_mask = time.argsort()
        data = (mag[sort_mask], time[sort_mask], mag_err[sort_mask])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = dict(zip(*self.fs.extract_one(data)))

        self._current_proc += 1
        return pd.Series(result)

    def process(self, lc):
        print("Selecting sources...")
        sources = self.get_sources(lc)
        print("Filtering observarions....")
        obs = self.get_obs(lc, sources)

        self._tile_name = lc.tile.name
        self._total = len(sources)
        self._current_proc = 1

        sources[self.fs.features_as_array_] = (
            sources.id.apply(lambda src_id: self.extract(src_id, obs)))

        # convert to recarray
        sources = sources.to_records(index=False)

        # change dtype by making a whole new array
        descr = sources.dtype.descr
        descr[2] = (descr[2][0], '|S13')
        descr = [(str(n), t) for n, t in descr]
        dt = np.dtype(descr)

        lc.features = sources.astype(dt)
        del sources

        self.session.commit()
