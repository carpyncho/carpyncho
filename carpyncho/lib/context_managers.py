#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import os
from contextlib import contextmanager


# =============================================================================
# MANAGERS
# =============================================================================

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    try:
        os.chdir(os.path.expanduser(newdir))
        yield
    finally:
        os.chdir(prevdir)
