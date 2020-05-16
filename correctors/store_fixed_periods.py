import pandas as pd

import gc

import numpy as np

import pathlib

from corral import db

from carpyncho.models import *


PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)


OUT_FOLDER = pathlib.Path("production_data/cache")


COLUMNS_NO_FEATURES = [
    'id', 'cnt', 'ra_k', 'dec_k', 'vs_type', 'vs_catalog']

def reorder(df):
    print("Reordering")
    features = [c for c in df.columns.values if c not in COLUMNS_NO_FEATURES]
    features.sort()
    order = COLUMNS_NO_FEATURES + features
    return df[order]

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


def main():
    done_tiles = set(e.name.replace(".done", "") for e in OUT_FOLDER.glob("*.done"))
    stored_tiles = set(e.name.replace(".stored", "") for e in OUT_FOLDER.glob("*.stored"))
    to_store = done_tiles.difference(stored_tiles)

    if not to_store:
        return

    with db.session_scope() as session:
        query = session.query(Tile).filter(Tile.name.in_(to_store))

        for tile in query:
            lc = tile.lcurves

            stored_path = OUT_FOLDER / "{}.stored".format(tile.name)

            print(">>> Starting {}".format(tile.name))
            tile_folder = OUT_FOLDER / tile.name

            feats = tuple(
                pd.read_pickle(p) for p in tile_folder.glob("*.pkl.bz2"))
            feats = pd.concat(feats, ignore_index=True)
            feats = reorder(feats)

            feats = to_recarray(feats)

            gc.collect()

            lc.tile.ready = False
            lc.features = feats
            lc.tile.ready = True

            stored_path.touch()


main()
