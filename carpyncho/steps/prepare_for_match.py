#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

from ..models import PawprintStackXTile


# =============================================================================
# STEP
# =============================================================================

class PrepareForMatch(run.Step):
    """If the status of tile and a linked pawprint-stack are ready
    set the link as ready for match

    """

    model = PawprintStackXTile
    conditions = [
        model.status == "raw",
        model.tile.has(status="ready"),
        model.pawprint_stack.has(status="ready")]
    groups = ["preprocess"]
    production_procno = 1

    def process(self, pxt):
        pxt.status = "ready"
        yield pxt
