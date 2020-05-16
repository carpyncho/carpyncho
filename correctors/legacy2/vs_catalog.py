import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *
from carpyncho.libs import feets_patch

import feets

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

def to_recarray(sources):
    """Convert the sources dataframr into recarray to easy storage"""
    # convert to recarray
    sources = sources.to_records(index=False)

    # change dtype by making a whole new array
    descr = sources.dtype.descr
    descr[1] = (descr[1][0], '|S13')
    descr[2] = (descr[2][0], '|S13')
    descr = [(str(n), t) for n, t in descr]
    dt = np.dtype(descr)

    return sources.astype(dt)


fs = feets.FeatureSpace(
    data=["magnitude", "time", "error"],
    only=[
        "PeriodLS", "Period_fit",
        "Psi_CS", "Psi_eta",
        "Freq1_harmonics_amplitude_0", "Freq1_harmonics_amplitude_1",
        "Freq1_harmonics_amplitude_2", "Freq1_harmonics_amplitude_3",
        "Freq2_harmonics_amplitude_0", "Freq2_harmonics_amplitude_1",
        "Freq2_harmonics_amplitude_2", "Freq2_harmonics_amplitude_3",
        "Freq3_harmonics_amplitude_0", "Freq3_harmonics_amplitude_1",
        "Freq3_harmonics_amplitude_2", "Freq3_harmonics_amplitude_3",
        "Freq1_harmonics_rel_phase_0", "Freq1_harmonics_rel_phase_1",
        "Freq1_harmonics_rel_phase_2", "Freq1_harmonics_rel_phase_3",
        "Freq2_harmonics_rel_phase_0", "Freq2_harmonics_rel_phase_1",
        "Freq2_harmonics_rel_phase_2", "Freq2_harmonics_rel_phase_3",
        "Freq3_harmonics_rel_phase_0", "Freq3_harmonics_rel_phase_1",
        "Freq3_harmonics_rel_phase_2", "Freq3_harmonics_rel_phase_3"])

import ipdb; ipdb.set_trace()


def main():
    with db.session_scope() as ses:
        query = ses.query(LightCurves).filter(LightCurves.tile.has(name="b278"))
        rows = []
        for lc in query.all():
            print lc
            tile = lc.tile

            feats = pd.DataFrame(lc.features)
            if "vs_catalog" in feats.columns:
                print "skiping"
                continue

            srcs = pd.DataFrame(tile.load_npy_file()[["id", "vs_catalog"]])
            srcs = srcs[srcs.id.isin(feats.id)]

            new_feats = pd.merge(srcs, feats, on="id", how="inner")
            new_feats = to_recarray(new_feats)

            lc.tile.ready = False
            lc.features = new_feats
            lc.tile.ready = True


main()
