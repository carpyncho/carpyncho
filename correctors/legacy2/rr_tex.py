import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)


COLUMNS = [
    "bm_src_id", "pwp_stack_src_id", "pwp_stack_src_hjd",
    "pwp_stack_src_mag3", "pwp_stack_src_mag_err3"]


def main():
    with db.session_scope() as ses:
        query = ses.query(LightCurves)
        rows = []
        for tile in ses.query(Tile).filter_by(ready=True).order_by(Tile.name).all():
            if tile.name == "b356":
                     continue
            sources = tile.load_npy_file()
            sources = pd.DataFrame(sources)
            feats = pd.DataFrame(tile.lcurves.features)
            rr = len(sources[sources.vs_type.str.startswith("RRLyr-")])
            util = len(feats[feats.Mean.between(12, 16.5, inclusive=False)])
            rows.append({
                "name": tile.name,
                "size": int(tile.size),
                "util": util,
                "rr": int(rr)
            })
        df = pd.DataFrame(rows)[["name", "size", "util", "rr"]]

        t_resume = df.sum().to_dict()
        t_resume["name"] = "Total"


        p_resume = df.mean().to_dict()
        p_resume["name"] = "Promedio"

        df = df.append(t_resume, ignore_index=True)
        df = df.append(p_resume, ignore_index=True)

        df.columns = ["Nombre", u"Tamaño", "Util", "RR-Lyrae"]
        print df.to_latex(index=False, float_format="%.3f")

main()
