#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import copy
import os
from collections import Counter

from corral import run

import numpy as np

from ..models import LightCurves

from psutil import virtual_memory

from corral.conf import settings


# =============================================================================
# CONSTANTS
# =============================================================================

SAMPLING = {
    "2500_rrVSunk": dict(
        sample_size=2500, sample_all_class=[], min_proportion=0.01)
}


# =============================================================================
# STEP
# =============================================================================

class Sample(run.Step):
    """Extract a sampling of features

    """

    model = LightCurves
    conditions = [model.tile.has(ready=True)]
    groups = ["sample"]

    def check_memory(self):
        min_memory, mem = int(32e+9), virtual_memory()
        if mem.total >= min_memory:
            return True
        min_memory_gb = min_memory / 1e+9
        total_gb = mem.total / 1e+9
        msg = "You need at least {}GB of memory. Found {}GB"
        print(msg.format(min_memory_gb, total_gb))
        return False


    def sample(self, lc, output, sample_size, sample_all_class, min_proportion):
        if min_proportion > 1 or min_proportion < 0:
            msg = "min-proportion must be betwen [0, 1]"
            print(msg)

        features = lc.features

        print "[Tile {}] Creating Filters...".format(lc.tile.name)
        if sample_all_class:
            var_filter = np.in1d(features["ogle3_type"], sample_all_class)
        else:
            var_filter = features["ogle3_type"] != ''

        print "[Tile {}] Picking variables...".format(lc.tile.name)
        variables = features[var_filter]

        print "[Tile {}] Sampling unknow sources...".format(lc.tile.name)
        unknow = np.random.choice(
            features[~var_filter], sample_size, replace=False)

        print "[Tile {}] Merging...".format(lc.tile.name)
        sample = np.concatenate((variables, unknow))

        print "[Tile {}] Checking sampling...".format(lc.tile.name)
        selected_cls = Counter(features["ogle3_type"])
        min_selection = sample_size * min_proportion
        remove_unk = 0
        for cls in np.unique(features["ogle3_type"]):
            if cls not in selected_cls or selected_cls[cls] < min_selection:
                full_cls = features[features["ogle3_type"] == cls]
                cls_sample_size = np.min([full_cls.size, min_selection])
                if cls_sample_size > 0:
                    sample = np.concatenate((
                        np.random.choice(full_cls, cls_sample_size, replace=False), sample))
                    remove_unk += int(cls_sample_size)

        print "[Tile {}] Removing class unknow...".format(lc.tile.name)
        if remove_unk > 0:
            where_unk = np.where(sample[sample["ogle3_type"] == ''])[0]

            remove_unk_size = np.min([where_unk.size, remove_unk])
            to_remove = np.random.choice(
                where_unk, remove_unk_size, replace=False)
            if len(to_remove):
                sample = np.delete(sample, to_remove)

        print "[Tile {}] Saving...".format(lc.tile.name)
        np.save(output, sample)


    def process(self, lc):
        base_path = settings.SAMPLES_DIR
        self.check_memory()
        for name, conf in SAMPLING.items():
            outdir = os.path.join(base_path, "smp_{}".format(name))
            if not os.path.exists(outdir):
                os.makedirs(outdir)

            output = os.path.join(outdir, lc.tile.name + ".npy")
            if True or not os.path.exists(output):
                self.sample(lc, output, **conf)
