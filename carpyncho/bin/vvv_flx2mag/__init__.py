#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import sh
from sh import gfortran

from ...lib.context_managers import cd


# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))


# =============================================================================
# FUNCTIONS
# =============================================================================

def build():
    with cd(PATH):
        gfortran(
            "vvv_flx2mag.f", "-o", "vvv_flx2mag", "-L", "/sw/lib",
            "-L", "cfitsio", "-l", "cfitsio", "-lm")


def execute(*args, **kwargs):
    vvv_flx2mag = sh(os.path.join(PATH, "vvv_flx2mag"))
    return vvv_flx2mag(*args, **kwargs)
