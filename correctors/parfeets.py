#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import gc

import pathlib

import numpy as np

import pandas as pd

import tqdm

from carpyncho.lib import feets_patch
from carpyncho.models import *

import joblib

import feets
from feets import preprocess


# =============================================================================
# MULTIPROCESS AND MULTI MACHINE CONFIGURATION
# =============================================================================


HOST_NAME = socket.gethostname()

OUT_FOLDER = pathlib.Path("production_data/cache")


CPUS = joblib.cpu_count()


TILES_BY_HOST = {
    "sersic": [
        'b206',
        'b214',
        #'b216',
        'b220',
        'b228',
        #~ 'b234',
        'b247',
        'b248',

        #~ 'b261',
        'b262',
        'b263',
        'b264',
        #'b277',
        #'b278',
        'b356',
        #~ 'b360',
        'b396'
    ]
}


TILES = TILES_BY_HOST[HOST_NAME]


SPLIT_SIZE = 1000


# =============================================================================
# FEATURES
# =============================================================================

FEATURES_TO_CALULATE = [
    'Amplitude',
    'Autocor_length',
    'Beyond1Std',
    'Con',
    'Eta_e',
    'FluxPercentileRatioMid20',
    'FluxPercentileRatioMid35',
    'FluxPercentileRatioMid50',
    'FluxPercentileRatioMid65',
    'FluxPercentileRatioMid80',
    'Freq1_harmonics_amplitude_0',
    'Freq1_harmonics_amplitude_1',
    'Freq1_harmonics_amplitude_2',
    'Freq1_harmonics_amplitude_3',
    'Freq1_harmonics_rel_phase_0',
    'Freq1_harmonics_rel_phase_1',
    'Freq1_harmonics_rel_phase_2',
    'Freq1_harmonics_rel_phase_3',
    'Freq2_harmonics_amplitude_0',
    'Freq2_harmonics_amplitude_1',
    'Freq2_harmonics_amplitude_2',
    'Freq2_harmonics_amplitude_3',
    'Freq2_harmonics_rel_phase_0',
    'Freq2_harmonics_rel_phase_1',
    'Freq2_harmonics_rel_phase_2',
    'Freq2_harmonics_rel_phase_3',
    'Freq3_harmonics_amplitude_0',
    'Freq3_harmonics_amplitude_1',
    'Freq3_harmonics_amplitude_2',
    'Freq3_harmonics_amplitude_3',
    'Freq3_harmonics_rel_phase_0',
    'Freq3_harmonics_rel_phase_1',
    'Freq3_harmonics_rel_phase_2',
    'Freq3_harmonics_rel_phase_3',
    'Gskew',
    'LinearTrend',
    'MaxSlope',
    'Mean',
    'Meanvariance',
    'MedianAbsDev',
    'MedianBRP',
    'PairSlopeTrend',
    'PercentAmplitude',
    'PercentDifferenceFluxPercentile',
    'PeriodLS',
    'Period_fit',
    'Psi_CS',
    'Psi_eta',
    'Q31',
    'Rcs',
    'Skew',
    'SmallKurtosis',
    'Std',
    'StetsonK']


COLUMNS_NO_FEATURES = [
    'id', 'cnt', 'ra_k', 'dec_k', 'vs_type', 'vs_catalog']


COLUMNS_TO_PRESERVE = COLUMNS_NO_FEATURES + [
    'c89_jk_color', 'c89_hk_color', 'c89_jh_color', 'n09_jk_color',
    'n09_hk_color', 'n09_jh_color', 'c89_m2', 'c89_m4', 'c89_c3',
    'n09_m2', 'n09_m4', 'n09_c3', 'ppmb', "PeriodLS"]


FEATURE_SPACE = feets.FeatureSpace(
    data=["magnitude", "time", "error"],
    only=FEATURES_TO_CALULATE)


# =============================================================================
# FUNCTIONS
# =============================================================================

def sigma_clip(obs):
    time = obs.pwp_stack_src_hjd.values
    magnitude = obs.pwp_stack_src_mag3.values
    error = obs.pwp_stack_src_mag_err3.values

    sort = np.argsort(time)
    time, magnitude, error = time[sort], magnitude[sort], error[sort]

    time, magnitude, error = preprocess.remove_noise(
        time, magnitude, error, std_limit=3)

    return time, magnitude, error


def extract(time, magnitude, error, feats):
    feats = {
        k: v.values()[0] for k, v in feats.to_dict().items()}

    new_feats = dict(
        zip(*FEATURE_SPACE.extract(
            time=time, magnitude=magnitude, error=error)))

    new_feats["ppmb"] = (
        feats["ppmb"] * feats["PeriodLS"] / new_feats["PeriodLS"])

    feats.update(new_feats)

    return feats


def extract_part(result_folder, used_ids_folder, sids, sids_feats, sids_obs):
    results, with_errors = [], []
    for sid in sids:
        obs = sids_obs[sids_obs.bm_src_id == sid]

        time, magnitude, error = sigma_clip(obs)
        cnt = len(time)
        if cnt < 30:
            continue

        feats = sids_feats[sids_feats.id == sid]
        try:
            feats = extract(time, magnitude, error, feats)
            feats["cnt"] = cnt
        except:
            print("With Error {}".format(sid))
            with_errors.append(sid)
            continue

        results.append(feats)

    filehash = hash(sids)

    full_out = result_folder / "part_{}.pkl.bz2".format(filehash)
    df = pd.DataFrame(results)
    df.to_pickle(full_out, compression="bz2")

    cache_out = used_ids_folder / "ids_{}.pkl".format(filehash)
    np.save(str(cache_out), [s for s in sids if s not in with_errors])

    return len(with_errors) == 0




def process(session):
    query = session.query(Tile).filter(Tile.name.in_(TILES))
    for tile in query:
        print(">>> Starting Tile: {}".format(tile.name))

        # prepare the output folder and the done path
        tile_folder = OUT_FOLDER / tile.name
        used_ids_folder = tile_folder / "_uids"
        done_path = OUT_FOLDER / "{}.done".format(tile.name)

        # if the .done file exist skip the entire tile
        if done_path.is_file():
            print("!!! Done file found '{}'. Skiping tile".format(done_path))
            continue

        if not tile_folder.exists():
            tile_folder.mkdir(parents=True)
            used_ids_folder.mkdir(parents=True)


        # read the original features
        lc = tile.lcurves
        feats = pd.DataFrame(lc.features)[COLUMNS_TO_PRESERVE]

        # here we remove the bad colors
        feats = feats[
            feats.c89_hk_color.between(-100, 100) &
            feats.c89_jh_color.between(-100, 100) &
            feats.c89_jk_color.between(-100, 100) &
            feats.n09_hk_color.between(-100, 100) &
            feats.n09_jh_color.between(-100, 100) &
            feats.n09_jk_color.between(-100, 100)]


        # get the already usded ids
        used_ids_files = list(used_ids_folder.iterdir())
        if used_ids_files:

            used_ids = np.concatenate([
                np.load(str(p)) for p in used_ids_folder.iterdir()])

            # remove the used ids
            print("Skiping {} of {}".format(len(used_ids), len(feats)))
            feats = feats[~feats.id.isin(used_ids)]

        # retrieve all the observation for this features
        all_obs = pd.DataFrame(lc.observations)
        all_obs = all_obs[all_obs.bm_src_id.isin(feats.id)]

        # garbage collection
        gc.collect()

        # split the features in a arrays of size SPLIT_SIZE
        n_feats = len(feats)
        number_of_parts = int(n_feats / SPLIT_SIZE) or 1
        split_ids = np.array_split(feats.id.values, number_of_parts)
        split_ids = map(tuple, split_ids)

        # this generate observation for the split_ids part
        feats_and_obs_gen = tqdm.tqdm((
            ids,
            feats[feats.id.isin(ids)],
            all_obs[all_obs.bm_src_id.isin(ids)]
        ) for ids in split_ids)

        # this part generates the progress bar
        feats_and_obs_gen = tqdm.tqdm(
            feats_and_obs_gen, desc=lc.tile.name, total=len(split_ids))

        with joblib.Parallel(n_jobs=CPUS) as parallel:
            results = parallel(
                joblib.delayed(extract_part)(
                    result_folder=tile_folder,
                    used_ids_folder=used_ids_folder,
                    sids=sids,
                    sids_feats=sids_feats,
                    sids_obs=sids_obs)
                for sids, sids_feats, sids_obs in feats_and_obs_gen)

        # if all the results are True write the .done file
        if all(results):
            print("Writing {}".format(done_path))
            done_path.touch()
