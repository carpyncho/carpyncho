#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from corral import core


# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))

COMMANDS = "\n".join([
    l.strip() for l in
    """
        set -e;
        cd {};
        gfortran vvv_flx2mag.f -o vvv_flx2mag -L/sw/lib -Lcfitsio -lcfitsio -lm;
        gfortran tff.f -o tff;
        bzip2 -dk ogle3.txt.bz2;
        cd - > /dev/null;

    """.format(PATH).splitlines()
])


# =============================================================================
# FUNCTIONS
# =============================================================================

def build():
    os.system(COMMANDS)
