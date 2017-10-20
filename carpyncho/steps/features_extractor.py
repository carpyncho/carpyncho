#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from __future__ import print_function

import warnings
import uuid
import datetime as dt
import os
import glob

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
            exclude=["SlottedA_length",
                     "StetsonK_AC",
                     "StructureFunction_index_21",
                     "StructureFunction_index_31",
                     "StructureFunction_index_32"])

    def get_cache_path(self, lc):
        """Return a cache directory for the given lightcurve"""
        cache_path = os.path.join(lc.lc_path, "cache")
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)
        return cache_path

    def get_cached_ids(self, lc):
        """Get only the id column of all the cache files in a single array"""
        cache_path = self.get_cache_path(lc)
        ids = None
        for fpath in glob.glob(os.path.join(cache_path, "cache_*.npy")):
            new_ids = np.load(fpath)["id"]
            if ids is None:
                ids = new_ids
            else:
                ids = np.append(ids, new_ids)
        return ids

    def combine_cache(self, lc):
        """Retrieve al the cache files ina single array"""
        cache_path = self.get_cache_path(lc)
        feats = None
        for fpath in glob.glob(os.path.join(cache_path, "cache_*.npy")):
            new_feats = np.load(fpath)
            if feats is None:
                feats = new_feats
            else:
                feats = np.append(feats, new_feats)
        return feats

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
        exclude = self.get_cached_ids(lc)
        if exclude is not None:
            sources = sources[~sources.id.isin(exclude)]

        # add the cnt
        sources = pd.merge(sources, with_min_obs, on="id", how="inner")

        # free memory
        del cnt, with_min_obs, exclude

        return sources

    def chunk_it(self, sources):
        """Split the source in many parts to low the memory footprint in pandas
        mp_apply

        """
        split_size = int(len(sources) / self.chunk_size)
        chunks = np.array_split(sources, split_size)
        return chunks

    def to_recarray(self, sources):
        """Convert the sources dataframr into recarray to easy storage"""
        # convert to recarray
        sources = sources.to_records(index=False)

        # change dtype by making a whole new array
        descr = sources.dtype.descr
        descr[2] = (descr[2][0], '|S13')
        descr = [(str(n), t) for n, t in descr]
        dt = np.dtype(descr)

        return sources.astype(dt)

    def to_cache(self, lc, features, force=False):
        """Store the features into a cache if its needed"""
        if features is not None and (force or len(features) >= self.write_limit):
            print("Caching {} new features...".format(len(features)))
            cache_path = self.get_cache_path(lc)
            filename = "cache_{}.npy".format(dt.datetime.now().isoformat())
            file_path = os.path.join(cache_path, filename)
            np.save(file_path, features)
            features = None
        return features

    def process(self, lc):
        print("Selecting sources...")
        all_sources = self.get_sources(lc)
        print("{} SOURCES FOUND!".format(len(all_sources)))
        if len(all_sources) == 0:
            yield lc

        all_obs = lc.observations
        all_obs = all_obs[np.in1d(all_obs["bm_src_id"], all_sources.id)]

        # chunk all the sources (the rename is for free memory
        chunks = self.chunk_it(all_sources)
        chunkst = len(chunks)

        # free memory
        del all_sources

        features = None
        for chunkn, sources in enumerate(chunks):
            print("Chunk {}/{} START!".format(chunkn + 1, chunkst))

            print("Filtering observarions...")
            obs = all_obs[np.in1d(all_obs["bm_src_id"], sources.id)]
            all_obs = all_obs[~np.in1d(all_obs["bm_src_id"], sources.id)]

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
            features = self.to_cache(lc, features)

        self.to_cache(lc, features, force=True)
        del features

        if len(all_obs) == 0:
            lc.features = self.combine_cache(lc)
        
        lc.tile.ready = True
        self.session.commit()
