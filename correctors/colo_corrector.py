#!/usr/bin/env python
# -*- coding: utf-8 -*-

from corral import db
from carpyncho.models import *

from beamc import extinction, MIN_BOX_SIZE


with db.session_scope() as ses:
    for tile in ses.query(Tile):
        print tile
        data = tile.load_npy_file()[:10]
        flt = data['ra_k'] != -9999
        ra = data['ra_k'][flt]
        dec = data['dec_k'][flt]
        coso = extinction(ra, dec, MIN_BOX_SIZE, "Cardelli89")
        import ipdb; ipdb.set_trace()
        break
