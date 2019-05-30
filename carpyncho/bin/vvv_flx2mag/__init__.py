#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import sh

from ...lib.context_managers import cd

from corral.conf import settings

# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))

LD_PATH = "/sw/lib"


# =============================================================================
# FUNCTIONS
# =============================================================================

def build():
    with cd(PATH):
        out = sh.gfortran(
            "vvv_flx2mag.f", "-o", "vvv_flx2mag", "-L", LD_PATH,
            "-L", "cfitsio", "-l", "cfitsio", "-lm")



def execute(*args, **kwargs):
    vvv_flx2mag = sh.Command(os.path.join(PATH, "vvv_flx2mag"))
    return vvv_flx2mag(*args, **kwargs)
