#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from corral import db
from carpyncho.models import *

from beamc import extinction, MIN_BOX_SIZE


with db.session_scope() as ses:
    for tile in ses.query(Tile):
        print tile
        data = tile.load_npy_file()
        idxs = np.random.randint(0, len(data), 100)
        data = data[idxs]
        flt = data['ra_k'] != -9999


        ra = data['ra_k'][flt]
        dec = data['dec_k'][flt]
        coso = extinction(ra, dec, MIN_BOX_SIZE, "Cardelli89")
        pd.DataFrame(coso).to_csv("beampy_out.txt",index=False)



        #~ import ipdb; ipdb.set_trace()
        break
