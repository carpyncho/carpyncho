#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import sh
from sh import bzip2

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
        bzip2("-dk", "ogle3.txt.bz2")


def load():
    pass
