#!/usr/bin/env python
# -*- coding: utf-8 -*-

from corral import db
from carpyncho.models import LightCurves

with db.session_scope() as session:
    for lc in session.query(LightCurves):
        hdf = lc.hdf_storage
        if "pwpx_ids" in hdf:
            tn = "{}_pwpx_ids".format(lc.tile.name)
            hdf.get_node("pwpx_ids")._f_rename(tn)
            print hdf.keys()
