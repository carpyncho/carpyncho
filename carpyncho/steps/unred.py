#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import time
import random
import multiprocessing as mp
import threading as th

from corral import run

import joblib

import numpy as np

from ..lib import matcher

from ..models import Tile


# =============================================================================
# CONSTANTS
# =============================================================================

from ..lib.beamc import (
    extinction, knnfix, add_columns, MIN_BOX_SIZE, SERVER_SOURCES_LIMIT)

import threading as th

COLUMNS = [
    "beamc_success", "beamc_ra", "beamc_dec", "beamc_ak", "beamc_ejk"]

KNN = 100


# =============================================================================
# CLASSES
# =============================================================================

class ExtintionFetcher(th.Thread):

    def __init__(self, tname, sources, idx, total):
        super(ExtintionFetcher, self).__init__()
        self.tname = tname
        self.sources = sources
        self.idx = "[{}] ExtinctionFectcher {}/{}".format(self.tname, idx, total)

    def run(self):
        # lets be gentile with the server
        time.sleep(random.random() + random.randint(0, 2))
        data = self.sources
        ra = data['ra_k']
        dec = data['dec_k']
        self.cardelli = extinction(
            ra, dec, MIN_BOX_SIZE, "Cardelli89")[COLUMNS]
        self.nishiyama = extinction(
            ra, dec, MIN_BOX_SIZE, "Nishiyama09")[COLUMNS]




# =============================================================================
# FUNCTIONS
# =============================================================================

def filter(sources):
    data = sources
    print data.shape
    flt = data[data['ra_k'] != -9999]
    print flt.shape
    return flt


def chunk_it(sources):
    """Split the source

    """
    chunk_sizes =(SERVER_SOURCES_LIMIT - 10000)
    split_size = int(len(sources) / chunk_sizes)
    chunks = np.array_split(sources, split_size)
    return chunks


def fix_missing(data):
    flt = data["beamc_success"] == 0
    idxs = np.where(flt)[0]
    knn_ak, knn_ejk = knnfix(data, idxs, knn=KNN)
    data["beamc_ak"][flt] = knn_ak
    data["beamc_ejk"][flt] = knn_ejk
    return data


# =============================================================================
# STEP
# =============================================================================

CMP = set([
    'n09_aj_2m', 'n09_ah_2m', 'c89_jk_color', 'n09_ak_vvv', 'n09_ejk_2m',
    'c89_aj_vvv', 'c89_ejk_2m', 'c89_hk_color', 'c89_ah_vvv', 'c89_ah_2m',
    'c89_ak_2m', 'c89_jh_color', 'c89_aj_2m', 'n09_ah_vvv', 'n09_aj_vvv',
    'n09_jk_color', 'c89_ak_vvv', 'n09_ak_2m', 'n09_hk_color'])

class Unred(run.Step):
    """Determine the sources existing in the tile and in the pawprint stacks

    """

    model = Tile
    conditions = [model.status == "ready-to-unred"]
    groups = ["preprocess", "unred"]
    production_procno = 1

    def unred(self, tile, tile_sources):
        filtered_tile = filter(tile_sources)
        chunks = chunk_it(filtered_tile)
        total = len(chunks)
        cardelli, nishiyama = None, None
        for idx, data in enumerate(chunks):
            proc = ExtintionFetcher(tile.name, data, idx+1, len(chunks))
            print("{} starting...".format(proc.idx))
            proc.run()
            print("{} DONE".format(proc.idx))

            if idx == 0:
                cardelli = proc.cardelli
                nishiyama = proc.nishiyama
            else:
                cardelli = np.append(cardelli, proc.cardelli)
                nishiyama = np.append(nishiyama, proc.nishiyama)
        import ipdb; ipdb.set_trace()
        print("Fixing missing...")
        cardelli = fix_missing(cardelli)
        nishiyama = fix_missing(nishiyama)

        print("Copying columns...")
        columns = [
            ("c89_ejk_2m", cardelli["beamc_ejk"]),
            ("c89_ak_2m", cardelli["beamc_ak"]),
            ("n09_ejk_2m", nishiyama["beamc_ejk"]),
            ("n09_ak_2m", nishiyama["beamc_ak"])]
        with_columns = add_columns(filtered_tile, columns, append=True)
        return with_columns

    def vvv_colors(self, arr):

        # set the aj, ah of n09 and c89
        arr = add_columns(arr, [
            ('c89_aj_2m', 1.692 * arr["c89_ejk_2m"]),
            ('c89_ah_2m', 1.054 * arr["c89_ejk_2m"]),
            ('n09_aj_2m', 1.526 * arr["n09_ejk_2m"]),
            ('n09_ah_2m', 0.855 * arr["n09_ejk_2m"])], append=True)


        # from here https://arxiv.org/pdf/1711.08805.pdf
        c89_ak_vvv = arr["c89_ak_2m"] + 0.01 * arr["c89_ejk_2m"]
        c89_aj_vvv = arr["c89_aj_2m"] - 0.065 * arr["c89_ejk_2m"]
        c89_ah_vvv = arr["c89_ah_2m"] + 0.032 * (arr["c89_aj_2m"] - arr["c89_ah_2m"])

        c89_jk_color = (arr["mag_j"] - c89_aj_vvv) - (arr["mag_k"] - c89_ak_vvv)
        c89_hk_color = (arr["mag_h"] - c89_ah_vvv) - (arr["mag_k"] - c89_ak_vvv)
        c89_jh_color = (arr["mag_j"] - c89_aj_vvv) - (arr["mag_h"] - c89_ah_vvv)

        n09_ak_vvv = arr["n09_ak_2m"] + 0.01 * arr["n09_ejk_2m"]
        n09_aj_vvv = arr["n09_aj_2m"] - 0.065 * arr["n09_ejk_2m"]
        n09_ah_vvv = arr["n09_ah_2m"] + 0.032 * (arr["n09_aj_2m"] - arr["n09_ah_2m"])

        n09_jk_color = (arr["mag_j"] - n09_aj_vvv) - (arr["mag_k"] - n09_ak_vvv)
        n09_hk_color = (arr["mag_h"] - n09_ah_vvv) - (arr["mag_k"] - n09_ak_vvv)
        n09_jh_color = (arr["mag_j"] - n09_aj_vvv) - (arr["mag_h"] - n09_ah_vvv)

        columns = [
            ('c89_ak_vvv', c89_ak_vvv),
            ('c89_aj_vvv', c89_aj_vvv),
            ('c89_ah_vvv', c89_ah_vvv),
            ('c89_jk_color', c89_jk_color),
            ('c89_hk_color', c89_hk_color),
            ('c89_jh_color', c89_jh_color),
            ('n09_ak_vvv', n09_ak_vvv),
            ('n09_aj_vvv', n09_aj_vvv),
            ('n09_ah_vvv', n09_ah_vvv),
            ('n09_jk_color', n09_jk_color),
            ('n09_hk_color', n09_hk_color),
            ('n09_jh_color', n09_jh_color)]

        arr = add_columns(arr, columns, append=True)
        return arr

    def process(self, tile):
        sources = tile.load_npy_file()
        sources = self.unred(tile, sources)
        sources = self.vvv_colors(sources)
        tile.store_npy_file(sources)
        tile.status = "ready-to-match"
        yield tile
        self.session.commit()
