#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# DOCS
# =============================================================================

"""Global configuration for carpyncho

"""


# =============================================================================
# IMPORTS
# =============================================================================

import logging
import os

import numpy

from corral import util


# =============================================================================
# CONFIGURATIONS
# =============================================================================

DEBUG_PROCESS = True

PATH = os.path.abspath(os.path.dirname(__file__))

BASE_PATH = os.path.dirname(PATH)


#: Sets the threshold for this logger to lvl. Logging messages which are less
#: severe than lvl will be ignored
LOG_LEVEL = logging.INFO

#: Template of string representation of every log of carpyncho format
#: see: https://docs.python.org/2/library/logging.html#logrecord-attributes
LOG_FORMAT = "[carpyncho-%(name)s-%(levelname)s@%(asctime)-15s] %(message)s"


PIPELINE_SETUP = "carpyncho.pipeline.Pipeline"


#: Database connection string formated ad the URL is an RFC-1738-style string.
#: See: http://docs.sqlalchemy.org/en/latest/core/engines.html
CONNECTION = "sqlite:///carpyncho-dev.db"


# Loader class
LOADER = "carpyncho.load.Loader"


# Pipeline processor steps
STEPS = [
    #~ "carpyncho.steps.measure_tile.MeasureTile",
    #~ "carpyncho.steps.read_tile.ReadTile",
    #~ "carpyncho.steps.xy_master_source.XYMasterSource",
#~
    #~ "carpyncho.steps.mjd_pawprint.MJDPawprint",
    #~ "carpyncho.steps.measure_pawprint.MeasurePawprint",
    #~ "carpyncho.steps.read_pawprint.ReadPawprint",
    #~ "carpyncho.steps.hjd_pawprint_source.HJDPawprintSource",
#~
    #~ "carpyncho.steps.prepare_pawprint_to_sync.PreparePawprintToSync",
    #~ "carpyncho.steps.match.MatchSources",
#~
    #~ "carpyncho.steps.is_tile_ready.IsTileReady",
]


# The alerts
ALERTS = []


# This values are autoimported when you open the shell
SHELL_LOCALS = {
    "np": numpy
}


# SMTP server configuration
EMAIL = {
    "server": "",
    "tls": True,
    "user": "",
    "password": ""
}

MIGRATIONS_SETTINGS = os.path.join(PATH, "migrations", "alembic.ini")

from carpyncho.local_settings import *  # noqa


# =============================================================================
# AFTER LOCAL SETTINGS
# =============================================================================

DATA_PATH = os.path.abspath(DATA_PATH)

RAW_TILES_DIR = os.path.join(DATA_PATH, "raw_tiles")
NPY_TILES_DIR = os.path.join(DATA_PATH, "npy_tiles")

RAW_PAWPRINTS_DIR = os.path.join(DATA_PATH, "raw_pawprints")
NPY_PAWPRINTS_DIR = os.path.join(DATA_PATH, "npy_pawprints")

DEBUG = DEBUG_PROCESS
