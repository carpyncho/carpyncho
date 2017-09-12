#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from __future__ import print_function

import warnings
import uuid

from corral import run, conf

import numpy as np

import pandas as pd

import feets

from ..models import LightCurves
from ..lib.mppandas import mp_apply, CORES

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

class Extractor(object):

    def __init__(self, fs, obs, tile_name, chunkn, chunkst):
        self._fs = fs
        self._obs = obs
        self._tname = tile_name
        self._chunkn = chunkn
        self._chunkst = chunkst

    def __call__(self, srcs):
        fs = self._fs
        self._cnt, self._total = 1, len(srcs)
        srcs[fs.features_as_array_] = srcs.id.apply(self.extract)
        del self._cnt, self._total
        return srcs

    def extract(self, src_id):
        print("[{}-chunk-{}/{}] Extracting Source {}/{}...".format(
            self._tname, self._chunkn, self._chunkst, self._cnt, self._total))
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
    chunk_size = conf.settings.get("FE_CHUNK_SIZE", CORES * 10)
    write_limit = conf.settings.get("FE_WRITE_LIMIT", 1000)
    mp_cores = conf.settings.get("FE_MP_CORES", CORES)
    mp_split = conf.settings.get("FE_MP_SPLIT", CORES)

    def setup(self):
        print("min_observation:", self.min_observation)
        print("chunk_size:", self.chunk_size)
        print("write_limit:", self.write_limit)
        print("mp_cores:", self.mp_cores)
        print("mp_split:", self.mp_split)
        self.fs = feets.FeatureSpace(
            data=["magnitude", "time", "error"],
            exclude=["SlottedA_length", "StetsonK_AC"])

    def get_sources(self, lc):
        """Get the sources of the lightcurve"""

        # get the ids of the sources with at least min_observation matches
        cnt = lc.obs_counter
        with_min_obs = pd.DataFrame(cnt[cnt["cnt"] >= self.min_observation])

        # extract ids and classes of ogle
        sources = pd.DataFrame(lc.tile.load_npy_file()[["id", "ogle3_type"]])

        # filter with min obs
        sources = sources[sources.id.isin(with_min_obs.id)]

        # filter the already used sources
        exclude = lc.features
        if exclude is not None:
            sources = sources[~sources.id.isin(exclude["id"])]

        # add the cnt
        sources = pd.merge(sources, with_min_obs, on="id", how="inner")

        # free memory
        del cnt, with_min_obs, exclude

        return sources

    def get_obs(self, obs, sources_ids):
        """Return the observations of the given sources"""
        obs = obs[np.in1d(obs["bm_src_id"], sources_ids)]
        return obs

    def chunk_it(self, sources):
        """Split the source in many parts to low the memory footprint in pandas
        mp_apply

        """
        split_size = int(len(sources) / self.chunk_size)
        chunks = np.array_split(sources, split_size)
        return chunks

    def to_recarray(self, sources):
        # convert to recarray
        sources = sources.to_records(index=False)

        # change dtype by making a whole new array
        descr = sources.dtype.descr
        descr[2] = (descr[2][0], '|S13')
        descr = [(str(n), t) for n, t in descr]
        dt = np.dtype(descr)

        return sources.astype(dt)

    def write_is_needed(self, lc, features):
        if len(features) < self.write_limit:
            return features
        print("Writing {} new features...".format(len(features)))
        stored = lc.features
        if stored is None:
            to_store = features
        else:
            to_store = np.append(stored, features)
        lc.features = to_store
        print("{} FEATURES SET STORED!".format(len(to_store)))
        del stored, to_store

    def process(self, lc):
        print("Selecting sources...")
        all_sources = self.get_sources(lc)
        print("{} SOURCES FOUND!".format(len(all_sources)))

        #~ all_obs = self.get_obs(lc.observations, all_sources.id)

        # chunk all the sources (the rename is for free memory
        chunks = self.chunk_it(all_sources)
        chunkst = len(chunks)

        # free memory
        del all_sources

        features = None
        for chunkn, sources in enumerate(chunks):
            print("Chunk {}/{} START!".format(chunkn + 1, chunkst))

            print("Filtering observarions...")
            obs = self.get_obs(lc.observations, sources.id)

            extractor = Extractor(
                fs=self.fs, obs=obs, tile_name=lc.tile.name,
                chunkn=chunkn + 1, chunkst=chunkst)

            result = mp_apply(
                sources, extractor, procs=self.mp_cores, chunks=self.mp_split)
            result = self.to_recarray(result)

            if features is None:
                features = result
            else:
                features = np.append(features, result)

            features = self.write_is_needed(lc, features)

        features = self.write_is_needed(lc, features)
        self.session.commit()
