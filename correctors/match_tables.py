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
        for tile in ses.query(
            Tile.name, Tile.size, Tile.ogle3_tagged_number
        ).filter_by(ready=True).order_by(Tile.name).all():
            rows.append({k: getattr(tile, k) for k in tile.keys()})
        df = pd.DataFrame(rows)[["name", "size", "ogle3_tagged_number"]]
        resume = df.sum().to_dict()
        resume["name"] = "Total"
        df = df.append(resume, ignore_index=True)
        df.columns = ["Nombre", u"Tamaño", "Variables"]
        print df.to_latex(index=False)


main()
