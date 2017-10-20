#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from carpyncho.lib.beamc import (
    extinction, knnfix, add_columns, MIN_BOX_SIZE, SERVER_SOURCES_LIMIT)

import threading as th

COLUMNS = ["beamc_success", "beamc_ra", "beamc_dec", "beamc_ak", "beamc_ejk"]

KNN = 100


class ExtintionFetcher(th.Thread):

    def __init__(self, tname, sources, idx, total):
        super(ExtintionFetcher, self).__init__()
        self.tname = tname
        self.sources = sources
        self.id = "[{}] ExtinctionFectcher {}/{}".format(self.tname, idx, total)

    def run(self):
        data = self.sources
        ra = data['ra_k']
        dec = data['dec_k']
        self.cardelli = extinction(
            ra, dec, MIN_BOX_SIZE, "Cardelli89")[COLUMNS]
        self.nishiyama = extinction(
            ra, dec, MIN_BOX_SIZE, "Nishiyama09")[COLUMNS]


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


def main():
    with db.session_scope() as ses:
        for tile in ses.query(Tile):
            path = "{}_red.npy".format(tile.name)
            if os.path.exists(path):
                print("Skiping {}".format(tile.name))
                continue
            print("Starting {}".format(tile.name))
            tile_sources = tile.load_npy_file()
            filtered_tile = filter(tile_sources)
            chunks = chunk_it(filtered_tile)

            cardelli, nishiyama = None, None
            for idx, data in enumerate(chunks):
                proc = ExtintionFetcher(tile.name, data, idx+1, len(chunks))
                print("{} starting...".format(proc.id))
                proc.run()
                print("{} DONE".format(proc.id))

                if idx == 0:
                    cardelli = proc.cardelli
                    nishiyama = proc.nishiyama
                else:
                    cardelli = np.append(cardelli, proc.cardelli)
                    nishiyama = np.append(nishiyama, proc.nishiyama)

            cardelli = fix_missing(cardelli)
            nishiyama = fix_missing(nishiyama)

            columns = [
                ("c89_ejk", cardelli["beamc_ejk"]),
                ("c89_ak", cardelli["beamc_ak"]),
                ("n09_ejk", nishiyama["beamc_ejk"]),
                ("n09_ak", nishiyama["beamc_ak"])]
            with_columns = add_columns(filtered_tile, columns, append=True)
            np.save(path, with_columns)








main()
