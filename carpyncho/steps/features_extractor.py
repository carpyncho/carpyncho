#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from __future__ import print_function

import warnings
import datetime as dt
import os
import glob
import shutil

from corral import run, conf

import numpy as np

import pandas as pd

import feets

from ..models import LightCurves

from ..lib.mppandas import mp_apply, CORES
from ..lib.beamc import add_columns


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
        print("!!! START:",  src_id)
        self._cnt += 1

        fs, obs = self._fs, self._obs
        src_obs = obs[obs["bm_src_id"] == src_id]

        time = src_obs["pwp_stack_src_hjd"]
        mag = src_obs["pwp_stack_src_mag3"]
        mag_err = src_obs["pwp_stack_src_mag_err3"]

        to_remove = np.unique(
            np.concatenate([
                np.argwhere(np.isinf(mag)),
                np.argwhere(np.isinf(time)),
                np.argwhere(np.isinf(mag_err))
            ]).flatten())

        if to_remove:
            time = np.delete(time, to_remove)
            mag = np.delete(mag, to_remove)
            mag_err = np.delete(mag_err, to_remove)

        sort_mask = time.argsort()
        data = {
            "magnitude": mag[sort_mask],
            "time": time[sort_mask],
            "error": mag_err[sort_mask]}

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            features, values = fs.extract(**data)
            result = dict(zip(features, values))

        series = pd.Series(result)
        print("!!! END:",  src_id)
        return series


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

    def del_cached_ids(self, lc):
        cache_path = self.get_cache_path(lc)
        shutil.rmtree(cache_path)

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

        # extract ids and classes
        sources = pd.DataFrame(lc.tile.load_npy_file()[
            ["id", "vs_catalog", "vs_type", "ra_k", "dec_k"]])

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
        descr[3] = (descr[3][0], '|S13')
        descr = [(str(n), t) for n, t in descr]
        dt = np.dtype(descr)

        return sources.astype(dt)

    def to_cache(self, lc, features, force=False):
        """Store the features into a cache if its needed"""
        write = (
            features is not None and
            (force or len(features) >= self.write_limit))
        if write:
            print("Caching {} new features...".format(len(features)))
            cache_path = self.get_cache_path(lc)
            filename = "cache_{}.npy".format(dt.datetime.now().isoformat())
            file_path = os.path.join(cache_path, filename)
            np.save(file_path, features)
            features = None
        return features

    def add_color(self, features, sources):
        colors = sources[[
            'id', 'c89_jk_color', 'c89_hk_color', 'c89_jh_color',
            'n09_jk_color', 'n09_hk_color', 'n09_jh_color']]

        idxs = np.where(np.in1d(colors["id"], features["id"]))[0]
        colors = colors[idxs]
        columns = [
            ('c89_jk_color', colors['c89_jk_color']),
            ('c89_hk_color', colors['c89_hk_color']),
            ('c89_jh_color', colors['c89_jh_color']),
            ('n09_jk_color', colors['n09_jk_color']),
            ('n09_hk_color', colors['n09_hk_color']),
            ('n09_jh_color', colors['n09_jh_color'])]

        features_colors = add_columns(features, columns, append=True)
        return features_colors

    def add_stellar_classes(self, feats, sources):
        sources = sources[np.in1d(sources["id"], feats["id"])]
        columns = [
            ("scls_h", sources["scls_h"]),
            ("scls_j", sources["scls_j"]),
            ("scls_k", sources["scls_k"]),
        ]
        return add_columns(feats, columns, append=True)

    def add_pseudo_colors_and_amplitude(self, feats, sources):
        sources = sources[np.in1d(sources["id"], feats["id"])]

        # CARDELLI
        c89_mk = sources["mag_k"] - sources["c89_ak_vvv"]
        c89_mj = sources["mag_j"] - sources["c89_aj_vvv"]
        c89_mh = sources["mag_h"] - sources["c89_ah_vvv"]

        # magnitudes
        c89_c_m2 = sources["c89_ah_vvv"] / (
            sources["c89_aj_vvv"] - sources["c89_ak_vvv"])
        c89_m2 = c89_mh - c89_c_m2 * (c89_mj - c89_mk)

        c89_c_m4 = sources["c89_ak_vvv"] / (
            sources["c89_aj_vvv"] - sources["c89_ah_vvv"])
        c89_m4 = c89_mk - c89_c_m4 * (c89_mj - c89_mh)

        # color
        c89_c_c3 = (
            (sources["c89_aj_vvv"] - sources["c89_ah_vvv"]) /
            (sources["c89_ah_vvv"] - sources["c89_ak_vvv"]))
        c89_c3 = (c89_mj - c89_mh) - c89_c_c3 * (c89_mh - c89_mk)

        # NISHIYAMA
        n09_mk = sources["mag_k"] - sources["n09_ak_vvv"]
        n09_mj = sources["mag_j"] - sources["n09_aj_vvv"]
        n09_mh = sources["mag_h"] - sources["n09_ah_vvv"]

        n09_c_m2 = sources["n09_ah_vvv"] / (
            sources["n09_aj_vvv"] - sources["n09_ak_vvv"])
        n09_m2 = n09_mh - n09_c_m2 * (n09_mj - n09_mk)

        n09_c_m4 = sources["n09_ak_vvv"] / (
            sources["n09_aj_vvv"] - sources["n09_ah_vvv"])
        n09_m4 = n09_mk - n09_c_m4 * (n09_mj - n09_mh)

        # color
        n09_c_c3 = (
            (sources["n09_aj_vvv"] - sources["n09_ah_vvv"]) /
            (sources["n09_ah_vvv"] - sources["n09_ak_vvv"]))
        n09_c3 = (n09_mj - n09_mh) - n09_c_c3 * (n09_mh - n09_mk)

        # AMPLITUDES
        ampH = .11 + 1.65 * (feats["Amplitude"] - .18)
        ampJ = -.02 + 3.6 * (feats["Amplitude"] - .18)

        columns = [
            ('c89_m2', c89_m2),
            ('c89_m4', c89_m4),
            ('c89_c3', c89_c3),
            ('n09_m2', n09_m2),
            ('n09_m4', n09_m4),
            ('n09_c3', n09_c3),
            ('AmplitudeH', ampH),
            ('AmplitudeJ', ampJ),
            ("AmplitudeJH", ampJ - ampH),
            ("AmplitudeJK", ampJ - feats["Amplitude"])]
        return add_columns(feats, columns, append=True)

    def add_ppmb(self, feats, sources, obs):
        feats_df = pd.DataFrame(feats[["id", "PeriodLS"]])

        sources = pd.DataFrame(sources[["id", "hjd_h", "hjd_j", "hjd_k"]])
        sources = sources[sources.id.isin(feats_df.id)]

        obs = pd.DataFrame(
            obs[["bm_src_id", "pwp_stack_src_mag3", 'pwp_stack_src_hjd']])
        obs = obs[obs.bm_src_id.isin(feats_df.id)]
        obs = obs.groupby("bm_src_id")
        obs = obs.apply(
            lambda g: g.sort_values(
                "pwp_stack_src_mag3", ascending=False).head(1))
        obs = obs[["bm_src_id", "pwp_stack_src_hjd"]]
        df = pd.merge(
            pd.merge(feats_df, sources, on="id"),
            obs, left_on="id", right_on="bm_src_id")

        del feats_df, obs, sources

        def _ppmb(r):
            t0 = r.pwp_stack_src_hjd
            mb_hjd = np.mean((r.hjd_h, r.hjd_k, r.hjd_j))
            return np.abs(np.modf(mb_hjd - t0)[0]) / r.PeriodLS

        df["ppmb"] = df.apply(_ppmb, axis=1)

        columns = [
            ("ppmb", df.ppmb.values),
        ]
        return add_columns(feats, columns, append=True)

    def process(self, lc):
        print("Selecting sources...")
        all_sources = self.get_sources(lc)
        print("{} SOURCES FOUND!".format(len(all_sources)))
        if len(all_sources) == 0:
            yield lc

        observations = lc.observations

        # chunk all the sources (the rename is for free memory
        if len(all_sources):
            all_obs = observations
            all_obs = all_obs[np.in1d(all_obs["bm_src_id"], all_sources.id)]

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
                    sources, extractor, procs=self.mp_cores,
                    chunks=self.mp_split)
                result = self.to_recarray(result)

                if features is None:
                    features = result
                else:
                    features = np.append(features, result)
                features = self.to_cache(lc, features, force=True)

            self.to_cache(lc, features, force=True)
            del features
        else:
            all_obs = []

        if len(all_obs) == 0:
            sources = lc.tile.load_npy_file()

            print("Combining cache")
            feats = self.combine_cache(lc)

            print("Adding First-Epoch Colors")
            feats = self.add_color(feats, sources)

            print("Adding Stellar Classes")
            feats = self.add_stellar_classes(feats, sources)

            print("Adding Pseudo Colors and Amplitudes")
            feats = self.add_pseudo_colors_and_amplitude(feats, sources)

            print("Adding Multi-Band Pseudo-Phases")
            feats = self.add_ppmb(feats, sources, observations)

            print("Saving")
            lc.features = feats

        lc.tile.ready = True
        self.session.commit()
        self.del_cached_ids(lc)
