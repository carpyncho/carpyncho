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

from parfeets import process


def reorder(df):
    print("Reordering")
    features = [c for c in df.columns.values if c not in COLUMNS_NO_FEATURES]
    order = COLUMNS_NO_FEATURES + features
    return df[order]


def main():
    with db.session_scope() as ses:
        process(ses)


main()
