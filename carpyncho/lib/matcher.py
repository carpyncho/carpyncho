#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from astropysics import coords


# =============================================================================
# CONSTANTS
# =============================================================================

MAX_MATCH = 3 * 9.2592592592592588e-5

MODE = "nearest"

# =============================================================================
# MANAGERS
# =============================================================================

def matchs(ra0, ra1, dec0, dec1, eps=MAX_MATCH, mode=MODE):
    nearestind1, _, match1 = coords.match_coords(
        ra0, dec0, ra1, dec1, eps=eps, mode=mode)
    nearestind0, _, match0 = coords.match_coords(
        ra1, dec1, ra0, dec0, eps=eps, mode=mode)

    for idx1, idx0 in enumerate(nearestind0):
        if match0[idx1] and nearestind1[idx0] == idx1 and match1[idx0]:
            yield idx0, idx1
