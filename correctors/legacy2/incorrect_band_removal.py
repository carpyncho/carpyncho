import time
import random
import os

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from sqlalchemy.orm import joinedload

PATH = "/home/jbcabral/carpyncho3/correctors/"
import sys
sys.path.insert(0, PATH)

def remove(path):
    print "Removing: " + path
    if os.path.exists(path):
        os.remove(path)

def run(ses):
    pxts = ses.query(PawprintStackXTile).options(
        joinedload(PawprintStackXTile.pawprint_stack),
        joinedload(PawprintStackXTile.tile)
    ).filter(~PawprintStackXTile.pawprint_stack.has(band="Ks")).all()
    tiles = set(pxt.tile for pxt in pxts if pxt.pawprint_stack.band != "Ks")
    for pxt in pxts:

        pwp = pxt.pawprint_stack
        if pwp.band == "Ks":
            continue
        remove(pwp.raw_file_path)
        remove(pwp.npy_file_path)

        remove(pxt.npy_file_path)

    for tile in tiles:
        lc = tile.lcurves
        if lc:
            for fname in os.listdir(lc.lc_path):
                remove(os.path.join(lc.lc_path, fname))
        tile.ready = False
        tile.status = "ready-to-match"

    for pxt in pxts:
        pwp = pxt.pawprint_stack
        ses.delete(pxt)
        ses.delete(pwp)



def main():
    with db.session_scope() as ses:
        run(ses)


main()
