#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter

import pandas as pd

from corral import db
from carpyncho.models import LightCurves

with db.session_scope() as session:
    for lc in session.query(LightCurves):
        if not lc.tile:
            continue
        sources = lc.sources
        if "obs_number" in sources.columns:
            continue

        cnt = Counter()


        pxts = lc.tile.pxts

        total = len(pxts)
        for idx, pxt in enumerate(pxts):
            print "{} reading {} of {} pxts".format(lc, idx, total)
            obs_df = pd.DataFrame(pxt.load_npy_file())
            cnt.update(obs_df["bm_src_id"].values)


        sources["obs_number"] = sources.id.apply(lambda e: cnt.get(e, 0))
        lc.sources = sources
