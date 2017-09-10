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
import pandas

import colored_traceback.auto  # noqa

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
    "carpyncho.steps.read_tile.ReadTile",
    "carpyncho.steps.tag_tile.OGLE3TagTile",

    "carpyncho.steps.read_pawprint_stack.ReadPawprintStack",

    "carpyncho.steps.prepare_for_match.PrepareForMatch",
    "carpyncho.steps.match.Match",

    "carpyncho.steps.create_lc.CreateLightCurves",

    "carpyncho.steps.features_extractor.FeaturesExtractor"
]


# The alerts
ALERTS = []


# This values are autoimported when you open the shell
SHELL_LOCALS = {
    "np": numpy,
    "pd": pandas
}


# SMTP server configuration
EMAIL = {
    "server": "",
    "tls": True,
    "user": "",
    "password": ""
}

MIGRATIONS_SETTINGS = os.path.join(PATH, "migrations", "alembic.ini")

try:
    from carpyncho.local_settings import *  # noqa
except ImportError:
    print "local_settings.py not found"
    DEBUG_PROCESS = True
    INPUT_PATH = os.path.abspath(os.path.join(PATH, "..", "_input_data"))
    DATA_PATH = os.path.abspath(os.path.join(PATH, "..", "_data"))


# =============================================================================
# AFTER LOCAL SETTINGS
# =============================================================================

DATA_PATH = os.path.abspath(DATA_PATH)

RAW_TILES_DIR = os.path.join(DATA_PATH, "raw_tiles")
NPY_TILES_DIR = os.path.join(DATA_PATH, "npy_tiles")

RAW_PAWPRINTS_DIR = os.path.join(DATA_PATH, "raw_pawprints")
NPY_PAWPRINTS_DIR = os.path.join(DATA_PATH, "npy_pawprints")

MATCHS_DIR = os.path.join(DATA_PATH, "matchs")

LC_DIR = os.path.join(DATA_PATH, "light_curves")

DEBUG = DEBUG_PROCESS

# =============================================================================
# WARNINGS DISABLE
# =============================================================================

import warnings
warnings.simplefilter("ignore", FutureWarning)
