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
            sources = tile.load_npy_file()
            sources = pd.DataFrame(sources)
            rr = len(sources[sources.vs_type.str.startswith("RRLyr-")])
            rows.append({
                "name": tile.name,
                "size": tile.size,
                "rr": rr
            })
        df = pd.DataFrame(rows)[["name", "size", "rr"]]
        resume = df.sum().to_dict()
        resume["name"] = "Total"
        resume = df.mean().to_dict()
        resume["name"] = "Promedio"
        df = df.append(resume, ignore_index=True)
        df.columns = ["Nombre", u"Tamaño", "RR-Lyrae"]
        print df.to_latex(index=False)

main()
