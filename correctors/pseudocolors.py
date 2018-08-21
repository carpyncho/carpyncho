#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import os
import multiprocessing as mp

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from carpyncho.lib.beamc import add_columns

import joblib

#~ from helper import PPMB


class PPMB(mp.Process):

    def __init__(self, srcs, obs, feats):
        super(PPMB, self).__init__()
        self._srcs = srcs
        self._obs = obs
        self._feats = feats
        self._queue = mp.Queue()

    def get_phase(self, src, obs, feats):
        print src["id"]
        sobs = obs[obs["bm_src_id"] == src["id"]]
        max_mag_idx = np.argmax(sobs["pwp_stack_src_mag3"])
        t0 = sobs[max_mag_idx]["pwp_stack_src_hjd"]

        period = feats[feats["id"] == src["id"]]["PeriodLS"][0]

        # multi-band pseudo phase
        mb_hjd = np.mean([src["hjd_h"], src["hjd_j"], src["hjd_k"]])
        return np.abs(np.modf(mb_hjd  - t0)[0]) / period

    def run(self):
        ppmbs = np.empty(len(self._srcs))
        for idx, src in enumerate(self._srcs):
            ppmbs[idx] = self.get_phase(src, self._obs, self._feats)
        self._queue.put(ppmbs)

    def ppmb(self):
        if not self._queue.empty():
            return self._queue.get()


def chunk_it(sources, chunk_size):
    split_size = int(len(sources) / chunk_size)
    chunks = np.array_split(sources, split_size)
    return chunks


def main():
    with db.session_scope() as ses:
        for lc in ses.query(LightCurves):
            print lc
            try:
                features = lc.features
            except:
                print "bad", lc, "!!!"
                continue

            feats = lc.features

            if "ppmb" in feats.dtype.names:
                print "Ready", lc
                continue



            sources = lc.tile.load_npy_file()[[
                'id', "mag_k", "mag_j", "mag_h",
                'c89_ak_vvv', 'c89_aj_vvv', 'c89_ah_vvv',
                'n09_ak_vvv', 'n09_aj_vvv', 'n09_ah_vvv',
                'hjd_h', "hjd_k", "hjd_j"]]

            idxs = np.where(np.in1d(sources["id"], feats["id"]))[0]
            sources = sources[idxs]

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
            ampJ = -.02  + 3.6 * (feats["Amplitude"] - .18)

            # NOW LET CALCULATE THE MULTI EPOCH PHASE
            observations = lc.observations
            njobs = joblib.cpu_count()
            chunks = chunk_it(sources, 100000)
            ppmbs = None
            for chunk in chunks:
                procs = []
                cobs = observations[np.in1d(observations["bm_src_id"], chunk["id"])]
                cfeats = feats[np.in1d(feats["id"], chunk["id"])]
                for mpchunk in np.array_split(chunk, njobs):
                    proc = PPMB(mpchunk, cobs, cfeats)
                    proc.start()
                    procs.append(proc)
                for proc in procs:
                    proc.join()
                    result = proc.ppmb()
                    if result is not None:
                        ppmbs = (
                            result
                            if ppmbs is None else
                            np.append(ppmbs, result))

            columns = [
                ('c89_m2', c89_m2),
                ('c89_m4', c89_m4),
                ('c89_c3', c89_c3),
                ('n09_m2', n09_m2),
                ('n09_m4', n09_m4),
                ('n09_c3', n09_c3),
                ('AmplitudeH', ampH),
                ('AmplitudeJ', ampJ),
                ('ppmb', ppmbs)]

            lc.tile.ready = False
            lc.features = add_columns(feats, columns, append=True)
            lc.tile.ready = True
            ses.commit()
            #~ print lc


main()
