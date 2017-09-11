#!/usr/bin/env python
# -*- coding: utf-8 -*-

# FROM: http://blog.adeel.io/2016/11/06/parallelize-pandas-map-or-apply/

# =============================================================================
# IMPORTS
# =============================================================================

import multiprocessing as mp

import numpy as np
import pandas as pd


# =============================================================================
# CONSTANTS
# =============================================================================

CORES = mp.cpu_count()


# =============================================================================
# FUNCTIONS
# =============================================================================

def mp_apply(data, func, procs=None, chunks=None):
    procs = procs or CORES
    chunks = chunks or CORES

    data_split = np.array_split(data, chunks)
    pool = mp.Pool(procs)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data
