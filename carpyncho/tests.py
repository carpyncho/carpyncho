#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2017-07-14T17:38:57.152637 by corral 0.2.7


# =============================================================================
# DOCS
# =============================================================================

"""carpyncho tests

"""


# =============================================================================
# IMPORTS
# =============================================================================

import tempfile
import os
import shutil
import sh

import pandas as pd

import numpy as np

from corral import qa, conf

from . import models

from .load import Loader
from .steps.read_tile import ReadTile
from .steps.tag_tile import VSTagTile
from .steps.unred import Unred
from .steps.read_pawprint_stack import ReadPawprintStack
from .steps.prepare_for_match import PrepareForMatch
from .steps.match import Match
from .steps.create_lc import CreateLightCurves
from .steps.features_extractor import FeaturesExtractor

from .lib.beamc import add_columns


# =============================================================================
# LOADER
# =============================================================================

class CarpynchoTestMixin(object):

    run_before = []

    TAR_DATA_PATH = os.path.join(
        conf.settings.BASE_PATH, "res", "example_data.tar.bz2")
    EXAMPLE_DATA_PATH = os.path.join(
        conf.settings.BASE_PATH, "example_data")

    def run_another_tests(self, cases):
        runned = []
        for Case in cases:
            case = Case(
                self.conn, Case.get_subject())
            case._TestCase__patch = self.patch
            case.execute()
            runned.append(case)
        return tuple(runned)

    def setup(self):
        self.work_dir = tempfile.mkdtemp("carpyncho_loader_test")
        self.teardown()

        if not os.path.exists(self.EXAMPLE_DATA_PATH):
            os.makedirs(self.EXAMPLE_DATA_PATH)
            sh.tar(
                j=True, x=True,
                f=self.TAR_DATA_PATH,
                directory=self.EXAMPLE_DATA_PATH)

        shutil.copytree(self.EXAMPLE_DATA_PATH, self.work_dir)
        self.input_path = os.path.join(self.work_dir, "example_data")
        self.data_path = os.path.join(self.work_dir, "stored")
        self.test_cache = os.path.join(self.work_dir, "test_cache")

        dirs = {
            "INPUT_PATH": self.input_path,
            "DATA_PATH": self.data_path,
            "RAW_TILES_DIR": os.path.join(self.data_path, "raw_tiles"),
            "NPY_TILES_DIR": os.path.join(self.data_path, "npy_tiles"),
            "RAW_PAWPRINTS_DIR": os.path.join(self.data_path, "raw_pawprints"),
            "NPY_PAWPRINTS_DIR": os.path.join(self.data_path, "npy_pawprints"),
            "MATCHS_DIR": os.path.join(self.data_path, "matchs"),
            "LC_DIR": os.path.join(self.data_path, "light_curves"),
            "SAMPLES_DIR": os.path.join(self.data_path, "samples")}

        for k, d in dirs.items():
            if not os.path.exists(d):
                os.makedirs(d)
            self.patch("corral.conf.settings.{}".format(k), d)

        self.runned = self.run_another_tests(self.run_before)

    def teardown(self):
        shutil.rmtree(self.work_dir)


# =============================================================================
# TESTS
# =============================================================================

class LoaderTestCase(CarpynchoTestMixin, qa.TestCase):

    subject = Loader

    def validate(self):
        self.assertStreamHas(
            models.Tile, models.Tile.name=="b202")
        self.assertStreamCount(1, models.PawprintStackXTile)
        self.assertStreamCount(1, models.PawprintStack)


class ReadPawprintStackTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = ReadPawprintStack

    def validate(self):
        pwp = self.session.query(models.PawprintStack).one()
        arr = pwp.load_npy_file()
        names = (
            'id', 'hjd', 'ra_deg', 'dec_deg', 'ra_h', 'ra_m', 'ra_s', 'dec_d',
            'dec_m', 'dec_s', 'x', 'y', 'mag1', 'mag_err1', 'mag2', 'mag_err2',
            'mag3', 'mag_err3', 'mag4', 'mag_err4', 'mag5', 'mag_err5', 'mag6',
            'mag_err6', 'mag7', 'mag_err7', 'chip_nro', 'stel_cls', 'elip',
            'pos_ang', 'confidence')
        for name in names:
            self.assertIn(name, arr.dtype.names)
        self.assertEquals(pwp.status, "ready-to-match")


class ReadTileTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = ReadTile

    def validate(self):
        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        arr = tile.load_npy_file()
        names = (
            'id', 'hjd_h', 'hjd_j', 'hjd_k', 'ra_h', 'dec_h', 'ra_j', 'dec_j',
            'ra_k', 'dec_k', 'mag_h', 'mag_j', 'mag_k', 'mag_err_h',
            'mag_err_j', 'mag_err_k', 'scls_h', 'scls_j', 'scls_k')
        for name in names:
            self.assertIn(name, arr.dtype.names)
        self.assertEquals(tile.status, "ready-to-tag")


class VSTagTileTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = VSTagTile

    def setup(self):
        super(VSTagTileTestCase, self).setup()

        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        tile.status = "ready-to-tag"
        self.save(tile)

        rtt_arr_path = os.path.join(self.test_cache, "ready-to-tag.npy")
        self.patch(
            "carpyncho.models.tile.Tile.npy_file_path", rtt_arr_path)

    def validate(self):
        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        arr = tile.load_npy_file()
        names = ('vs_id', 'vs_catalog')
        for name in names:
            self.assertIn(name, arr.dtype.names)
        self.assertEquals(tile.status, "ready-to-unred")


class UnredTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = Unred

    def setup(self):
        super(UnredTestCase, self).setup()

        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        tile.status = "ready-to-unred"
        self.save(tile)

        arr_path = os.path.join(self.test_cache, "ready-to-unred.npy")
        self.patch(
            "carpyncho.models.tile.Tile.npy_file_path", arr_path)
        self.patch("carpyncho.steps.unred.Unred.unred", self.unred)

    def unred(self, tile, sources):
        size = len(sources)
        columns = [
            ("c89_ejk_2m", np.ones(size)),
            ("c89_ak_2m", np.ones(size)),
            ("n09_ejk_2m", np.ones(size)),
            ("n09_ak_2m", np.ones(size))]
        with_columns = add_columns(sources, columns, append=True)
        return with_columns

    def validate(self):
        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        arr = tile.load_npy_file()
        names = (
            'c89_ejk_2m', 'c89_ak_2m', 'n09_ejk_2m', 'n09_ak_2m', 'c89_aj_2m',
            'c89_ah_2m', 'n09_aj_2m', 'n09_ah_2m', 'c89_ak_vvv',
            'c89_aj_vvv', 'c89_ah_vvv', 'c89_jk_color', 'c89_hk_color',
            'c89_jh_color', 'n09_ak_vvv', 'n09_aj_vvv', 'n09_ah_vvv',
            'n09_jk_color', 'n09_hk_color', 'n09_jh_color')
        for name in names:
            self.assertIn(name, arr.dtype.names)
        self.assertEquals(tile.status, "ready-to-match")


class PrepareForMatchTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = PrepareForMatch

    def setup(self):
        super(PrepareForMatchTestCase, self).setup()

        tile = self.session.query(models.Tile).filter_by(name="b202").one()
        tile.status = "ready-to-match"
        self.save(tile)

        pwp = self.session.query(models.PawprintStack).one()
        pwp.status = "ready-to-match"
        self.save(pwp)

    def validate(self):
        pxt = self.session.query(models.PawprintStackXTile).one()
        self.assertEquals(pxt.status, "ready-to-match")


class MatchTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = Match

    def setup(self):
        super(MatchTestCase, self).setup()
        pxt = self.session.query(models.PawprintStackXTile).one()
        pxt.status = "ready-to-match"
        self.save(pxt)

        arr_path = os.path.join(self.test_cache, "tile_ready-to-match.npy")
        self.patch(
            "carpyncho.models.tile.Tile.npy_file_path", arr_path)

        arr_path = os.path.join(self.test_cache, "pwp_ready-to-match.npy")
        self.patch(
            "carpyncho.models.pawprint_stack.PawprintStack.npy_file_path",
            arr_path)

    def validate(self):
        pxt = self.session.query(models.PawprintStackXTile).one()
        arr = pxt.load_npy_file()
        names = (
            'tile_name', 'tile_id', 'bm_src_id', 'bm_src_hjd_h',
            'bm_src_hjd_j', 'bm_src_hjd_k', 'bm_src_ra_h', 'bm_src_dec_h',
            'bm_src_ra_j', 'bm_src_dec_j', 'bm_src_ra_k', 'bm_src_dec_k',
            'bm_src_mag_h', 'bm_src_mag_j', 'bm_src_mag_k', 'bm_src_mag_err_h',
            'bm_src_mag_err_j', 'bm_src_mag_err_k', 'bm_src_scls_h',
            'bm_src_scls_j', 'bm_src_scls_k', 'bm_src_vs_type', 'bm_src_vs_id',
            'bm_src_vs_catalog', 'bm_src_c89_ejk_2m', 'bm_src_c89_ak_2m',
            'bm_src_n09_ejk_2m', 'bm_src_n09_ak_2m', 'bm_src_c89_aj_2m',
            'bm_src_c89_ah_2m', 'bm_src_n09_aj_2m', 'bm_src_n09_ah_2m',
            'bm_src_c89_ak_vvv', 'bm_src_c89_aj_vvv', 'bm_src_c89_ah_vvv',
            'bm_src_c89_jk_color', 'bm_src_c89_hk_color',
            'bm_src_c89_jh_color', 'bm_src_n09_ak_vvv', 'bm_src_n09_aj_vvv',
            'bm_src_n09_ah_vvv', 'bm_src_n09_jk_color', 'bm_src_n09_hk_color',
            'bm_src_n09_jh_color', 'pwp_stack_id', 'pwp_stack_band',
            'pwp_stack_src_id', 'pwp_stack_src_hjd', 'pwp_stack_src_ra_deg',
            'pwp_stack_src_dec_deg', 'pwp_stack_src_ra_h',
            'pwp_stack_src_ra_m', 'pwp_stack_src_ra_s', 'pwp_stack_src_dec_d',
            'pwp_stack_src_dec_m', 'pwp_stack_src_dec_s', 'pwp_stack_src_x',
            'pwp_stack_src_y', 'pwp_stack_src_mag1', 'pwp_stack_src_mag_err1',
            'pwp_stack_src_mag2', 'pwp_stack_src_mag_err2',
            'pwp_stack_src_mag3', 'pwp_stack_src_mag_err3',
            'pwp_stack_src_mag4', 'pwp_stack_src_mag_err4',
            'pwp_stack_src_mag5', 'pwp_stack_src_mag_err5',
            'pwp_stack_src_mag6', 'pwp_stack_src_mag_err6',
            'pwp_stack_src_mag7', 'pwp_stack_src_mag_err7',
            'pwp_stack_src_chip_nro', 'pwp_stack_src_stel_cls',
            'pwp_stack_src_elip', 'pwp_stack_src_pos_ang',
            'pwp_stack_src_confidence')
        for name in names:
            self.assertIn(name, arr.dtype.names)
        self.assertEquals(pxt.status, "matched")


class CreateLightCurvesTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = CreateLightCurves

    def setup(self):
        super(CreateLightCurvesTestCase, self).setup()

        pxt = self.session.query(models.PawprintStackXTile).one()
        pxt.status = "matched"
        pxt.tile.status = "ready-to-match"
        self.save(pxt)

        arr_path = os.path.join(self.test_cache, "matched.npy")
        self.patch(
            "carpyncho.models.psxt.PawprintStackXTile.npy_file_path", arr_path)

    def validate(self):
        self.assertStreamCount(1, models.LightCurves)


class FeaturesExtractorTestCase(CarpynchoTestMixin, qa.TestCase):

    run_before = [LoaderTestCase]
    subject = FeaturesExtractor
    features = np.array(["Amplitude", "PeriodLS"])
    sample = 100

    def extract(self, *args, **kwargs):
        return np.copy(self.features), np.ones(len(self.features))

    def sample_with_min_obs(self, ids):
        min_obs = FeaturesExtractor.min_observation
        tr = np.random.choice(ids, self.sample)
        repeated = np.tile(tr, min_obs)
        return np.concatenate((ids, repeated))

    def setup(self):
        super(FeaturesExtractorTestCase, self).setup()
        arr_path = os.path.join(self.test_cache, "tile_ready-to-match.npy")
        self.patch(
            "carpyncho.models.tile.Tile.npy_file_path", arr_path)

        self.patch("feets.core.FeatureSpace.extract", self.extract)
        self.patch("feets.core.FeatureSpace.features_as_array_", self.features)

        arr_path = os.path.join(self.test_cache, "observations.npy")
        arr = np.load(arr_path)
        self.patch(
            "carpyncho.models.tile.LightCurves.observations", arr)

        tile = self.session.query(models.Tile).one()
        tile.status = "ready-to-extract-features"

        lc = models.LightCurves(tile=tile)
        ids = self.sample_with_min_obs(arr["bm_src_id"])
        lc._set_cnt(ids)

        self.save(lc)
        self.save(tile)

        fe_conf = {
            "chunk_size": self.sample,
            "write_limit": len(arr)}

        tpl = "carpyncho.steps.features_extractor.FeaturesExtractor.{}"
        for attribute_name, value in fe_conf.items():
            tp = tpl.format(attribute_name)
            self.patch(tp, value)

    def validate(self):
        tile = self.session.query(models.Tile).one()
        features = tile.lcurves.features
        self.assertEquals(len(features), self.sample)
        self.assertTrue(tile.ready)
