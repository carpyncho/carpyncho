#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import warnings
import uuid

from corral import run, conf

import numpy as np

import pandas as pd

import feets

from ..models import LightCurves
from ..lib.mppandas import mp_apply

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

class Extractor(object):

    def __init__(self, fs, obs, tile_name):
        self._fs = fs
        self._obs = obs
        self._tname = tile_name

    def __call__(self, srcs):
        fs = self._fs
        self._cnt, self._total, self._uid = 1, len(srcs), uuid.uuid4()
        srcs[fs.features_as_array_] = srcs.id.apply(self.extract)
        del self._cnt, self._total, self._uid
        return srcs

    def extract(self, src_id):
        print("[{}-chunk-{}] Extracting Source {}/{}...".format(
            self._tname, self._uid, self._cnt, self._total))
        self._cnt += 1

        fs, obs = self._fs, self._obs
        src_obs = obs[obs["bm_src_id"] == src_id]

        time = src_obs["pwp_stack_src_hjd"]
        mag = src_obs["pwp_stack_src_mag3"]
        mag_err = src_obs["pwp_stack_src_mag_err3"]

        sort_mask = time.argsort()
        data = (mag[sort_mask], time[sort_mask], mag_err[sort_mask])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = dict(zip(*fs.extract_one(data)))

        return pd.Series(result)


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

    min_observation = conf.settings.get("FE_MIN_OBSERVATION", 30)
    mp_cores = conf.settings.get("FE_MP_CORES", None)
    mp_split = conf.settings.get("FE_MP_SPLIT", None)

    def setup(self):
        print("min_observation:", self.min_observation)
        print("mp_cores:", self.mp_cores)
        print("mp_split:", self.mp_split)
        self.fs = feets.FeatureSpace(
            data=["magnitude", "time", "error"],
            exclude=["SlottedA_length", "StetsonK_AC"])
        #~ self.fs = feets.FeatureSpace(
            #~ only=["PeriodLS", "AMP"])

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

    def process(self, lc):
        print("Selecting sources...")
        sources = self.get_sources(lc)
        print("Filtering observarions...")
        obs = self.get_obs(lc, sources)

        extractor = Extractor(self.fs, obs, lc.tile.name)
        sources = mp_apply(
            sources, extractor, procs=self.mp_cores, chunks=self.mp_split)

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
