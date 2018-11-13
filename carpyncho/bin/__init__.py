#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from corral import core

from . import vvv_flx2mag, catalogs


# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))


# =============================================================================
# FUNCTIONS
# =============================================================================

def build():
    core.logger.info("Compiling vvv_flx2mag...")
    vvv_flx2mag.build()

    core.logger.info("Extracting Catalogs Dataset...")
    catalogs.build()
