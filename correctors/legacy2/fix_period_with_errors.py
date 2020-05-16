import time
import random
import os
import gc
import socket

import tqdm

import diskcache

import numpy as np
import pandas as pd

from corral import db

from carpyncho.models import *
from carpyncho.lib import feets_patch

import joblib

import feets

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

from parfeets import extract

# =============================================================================
# SOME VARS
# =============================================================================

host_name = socket.gethostname()

cache = diskcache.Cache("production_data/_cache" + host_name)

CPUS = joblib.cpu_count()

COLUMNS_TO_REMOVE = [
    'scls_h', 'scls_j', 'scls_k',
    "AndersonDarling", "AmplitudeJ", "AmplitudeH", "AmplitudeJH", "AmplitudeJK",
    'Freq1_harmonics_rel_phase_0', 'Freq2_harmonics_rel_phase_0', 'Freq3_harmonics_rel_phase_0',
    "CAR_mean", "CAR_tau", "CAR_sigma"]

COLUMNS_NO_FEATURES = ['id', 'tile', 'cnt', 'ra_k', 'dec_k', 'vs_type', 'vs_catalog', 'cls']

TILES_BY_HOST = {
    "sersic": ["b278", "b261"],
    "mirta3": ["b234", "b360"],
    "mirta2": [],
}

tile_names = TILES_BY_HOST[host_name]


def to_recarray(sources):
    """Convert the sources dataframr into recarray to easy storage"""
    # convert to recarray
    types = {
        "|O": "|S13"
    }

    sources = sources.to_records(index=False)

    # change dtype by making a whole new array
    descr = sources.dtype.descr
    descr = [(str(n), types.get(t, t)) for n, t in descr]
    dt = np.dtype(descr)

    return sources.astype(dt)


def get_old_feats(feats):
    for sid in feats.id.values:
        row = feats[feats.id == sid]
        row = row.to_dict()
        row = {k: v.values()[0] for k, v in row.items()}
        yield sid, row


def reorder(df):
    print("Reordering")
    features = [c for c in df.columns.values if c not in COLUMNS_NO_FEATURES]
    order = COLUMNS_NO_FEATURES + features
    return df[order]


def remove_bad_color(df):
    df = df[
        df.c89_hk_color.between(-100, 100) &
        df.c89_jh_color.between(-100, 100) &
        df.c89_jk_color.between(-100, 100) &
        df.n09_hk_color.between(-100, 100) &
        df.n09_jh_color.between(-100, 100) &
        df.n09_jk_color.between(-100, 100)]
    return df

def main():
    with db.session_scope() as ses:

        query = ses.query(Tile).filter(Tile.name.in_(tile_names))

        for tile in query:

            if tile.name in cache:
                print "Skiping {}".format(lc.tile.name)
                continue

            lc = tile.lcurves
            feats = pd.DataFrame(lc.features)
            feats = remove_bad_color(feats)  # here we remove the bad colors
            gc.collect()

            obs = pd.DataFrame(lc.observations)
            obs = obs[obs.bm_src_id.isin(feats.id)]
            gc.collect()

            to_proc = tqdm.tqdm(get_old_feats(feats), desc=lc.tile.name)

            with joblib.Parallel(n_jobs=CPUS) as P:
                new_feats = P(
                    joblib.delayed(extract)(  # the exract make the source sigmaclip
                        sid=sid,
                        obs=obs[obs.bm_src_id == sid],
                        old_feats=old_feats)
                    for sid, old_feats in to_proc)

            new_feats = pd.DataFrame(new_feats)[[
                f for f in feats.columns if f not in COLUMNS_TO_REMOVE]]

            new_feats = new_feats[new_feats.cnt >= 30] # herw we remove the to short lc

            new_feats = reorder(new_feats)
            new_feats = to_recarray(new_feats)

            # ~ lc.tile.ready = False
            # ~ lc.features = new_feats
            # ~ lc.tile.ready = True

            gc.collect()
            cache[tile.name] = new_feats


main()
