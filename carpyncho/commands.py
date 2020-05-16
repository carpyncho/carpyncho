#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created at 2015-12-07T20:41:54.110455 by corral 0.0.1


# =============================================================================
# DOCS
# =============================================================================

"""Command line commands for carpyncho

"""


# =============================================================================
# IMPORTS
# =============================================================================

import os

from psutil import virtual_memory

from texttable import Texttable

from corral import cli, conf, db, core

from carpyncho import bin
from carpyncho.models import (
    Tile, PawprintStack, PawprintStackXTile, LightCurves)


# =============================================================================
# HELPER
# =============================================================================

def log2critcal():
        import logging
        level = logging.CRITICAL
        logging.getLogger('sqlalchemy.engine').setLevel(level)


# =============================================================================
# COMMANDS
# =============================================================================

class Paths(cli.BaseCommand):
    """Show the paths of carpyncho"""

    def handle(self):
        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("Name", "Path"))
        table.add_row(("Input Data", conf.settings.INPUT_PATH))
        table.add_row(("Storage", conf.settings.DATA_PATH))
        print(table.draw())


class BuildBin(cli.BaseCommand):
    """Build the bin executables needed to run carpyncho"""

    def handle(self):
        core.logger.info("Building bin extensions...")
        bin.build()
        core.logger.info("Done")


class LSTile(cli.BaseCommand):
    """List all registered tiles"""

    def _bool(self, e):
        return e.lower() not in ("0", "", "false")

    def setup(self):
        self.parser.add_argument(
            "-st", "--status", dest="status", action="store",
            choices=Tile.statuses, nargs="+",
            help="Show only the given status")

    def handle(self, status):
        log2critcal()

        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("Tile", "Status", "VS Tags", "Size", "Ready"))
        cnt = 0
        with db.session_scope() as session:
            query = session.query(
                Tile.name, Tile.status,
                Tile.ogle3_tagged_number, Tile.size,
                Tile.ready)
            if status:
                query = query.filter(Tile.status.in_(status))
            map(table.add_row, query)
            cnt = query.count()
        print(table.draw())
        print("Count: {}".format(cnt))


class LSPawprint(cli.BaseCommand):
    """List all registered pawprint stacks"""

    def setup(self):
        self.parser.add_argument(
            "-st", "--status", dest="status", action="store",
            choices=PawprintStack.statuses.enums, nargs="+",
            help="Show only the given status")
        self.parser.add_argument(
            "-t", "--tile", dest="tiles", action="store", nargs="+",
            help="Show only the given tile/s")

    def handle(self, status, tiles):
        log2critcal()

        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("Pawprint", "Status", "Band", "MJD", "Size"))
        cnt = 0
        with db.session_scope() as session:
            query = session.query(
                PawprintStack.name, PawprintStack.status,
                PawprintStack.band, PawprintStack.mjd, PawprintStack.size)
            if tiles:
                ids = [
                    r[0] for r in
                    session.query(PawprintStackXTile.pawprint_stack_id).join(Tile).filter(Tile.name.in_(tiles))]
                query = query.filter(PawprintStack.id.in_(ids))

            if status:
                query = query.filter(PawprintStack.status.in_(status))
            map(table.add_row, query)
            cnt = query.count()
        print(table.draw())
        print("Count: {}".format(cnt))


class LSSync(cli.BaseCommand):
    """List the status of every pawprint-stack and their tile"""

    def setup(self):
        self.parser.add_argument(
            "-st", "--status", dest="status", action="store",
            choices=PawprintStackXTile.statuses.enums, nargs="+",
            help="Show only the given status")

    def handle(self, status):
        log2critcal()

        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("Tile", "Pawprint", "Matched N.", "Status"))

        with db.session_scope() as session:
            query = session.query(PawprintStackXTile)
            if status:
                query = query.filter(PawprintStackXTile.status.in_(status))
            for pxt in query:
                table.add_row([pxt.tile.name,
                               pxt.pawprint_stack.name,
                               pxt.matched_number,
                               pxt.status])
            cnt = query.count()
        print(table.draw())
        print("Count: {}".format(cnt))


class SetTileStatus(cli.BaseCommand):
    """Set the status of the given tile"""

    options = {
        "title": "set-tile-status"}

    def setup(self):
        self.parser.add_argument(
            "tnames", action="store", nargs="+",
            help="Name of the tile to change the status")
        self.parser.add_argument(
            "--status", action="store", dest="status", choices=Tile.statuses,
            help="New status")

    def handle(self, tnames, status):
        log2critcal()
        with db.session_scope() as session:
            for tile in session.query(Tile).filter(Tile.name.in_(tnames)):
                tile.status = status
                print("[SUCCESS] Tile '{}' -> {}".format(tile.name, status))


class SampleFeatures(cli.BaseCommand):
    """Create a sample of features of the given tile name.

    This sample contains all the know variable stars and a subset of unknow
    sources.

    """

    options = {"title": "sample"}

    def setup(self):
        self.parser.add_argument(
            "tnames", action="store", nargs="+",
            help="name of the tiles to sample")

        self.parser.add_argument(
            "--output", "-o", dest="output", required=True,
            help="path of the sample file")

        self.parser.add_argument(
            "--cone-search", "-cs", dest="cone_search",  nargs=3,
            type=float, metavar=('RA', 'DEC', 'RADIUS'), help=(
                "Describes sky position and an angular distance, defining a "
                "cone on the sky. The response returns a list of astronomical "
                "sources from the catalog whose positions lie within the cone"))

        self.parser.add_argument(
            "--ucls-size", "-u", dest="no_cls_size", default=2500,
            type=(lambda v: int(v) if v not in ("ALL", "O2O") else v),
            help=(
                "sample size of unknow sources by tile."
                "Use 'ALL' to use all the unknow sources "
                "or 'O2O' to sample the same number of variable stars"))

        self.parser.add_argument(
            "--no-variable-stars", "-nvs", dest="include_vs",
            default=True, action="store_false",
            help="Remove all tagged variable stars")

        self.parser.add_argument(
            "--variable-stars-type", "-vst", dest="vs_type",
            action="store", type=str, default=None,
            help="Filter the variable stars by a given regex")

        self.parser.add_argument(
            "--no-saturated", "-ns", dest="no_saturated",
            default=False, action="store_true",
            help="Remove all satured sources (Mean magnitude <= 12)")

        self.parser.add_argument(
            "--no-faint", "-nf", dest="no_faint", default=False,
            action="store_true",
            help="Remove all saturated sources (Mean magnitude >= 16.5)")

        self.parser.add_argument(
            "--ignore-memory", "-i", dest="memory_check", default=True,
            action="store_false",
            help="ignore the memory che before run the command")

    def cone_search(self, features, ra_c, dec_c, sr_c):
        import numpy as np

        # sometimes the ra_k cames in str...
        features.ra_k = features.ra_k.astype(float)
        features.dec_k = features.dec_k.astype(float)

        # the conesearch
        # based on:
        #   http://www.g-vo.org/pmwiki/Products/HEALPixIndexing
        # ORIGINAL:
        # SELECT *
        # FROM RASS_PHOTONS
        # WHERE 2 * ASIN(
        #   SQRT(
        #       SIN(($DEC_C-DEC)/2) *
        #       SIN(($DEC_C-DEC)/2) +
        #       COS($DEC_C) * COS(DEC) *
        #       SIN(($RA_C - RA)/2) *
        #       SIN(($RA_C - RA)/2))) <= $SR_C
        query = 2. * np.arcsin(
            np.sqrt(
                np.sin((dec_c - features.dec_k) / 2.) *
                np.sin((dec_c - features.dec_k) / 2.) +
                np.cos(dec_c) * np.cos(features.dec_k) *
                np.sin((ra_c - features.ra_k) / 2.) *
                np.sin((ra_c - features.ra_k) / 2.))) <= sr_c

        # filtering
        features = features[query]
        return features


    def handle(
        self, tnames, output, cone_search, no_cls_size, no_saturated,
        no_faint, include_vs, memory_check, vs_type):
        min_memory, mem = int(32e+9), virtual_memory()
        if memory_check and mem.total < min_memory:
            min_memory_gb = min_memory / 1e+9
            total_gb = mem.total / 1e+9
            msg = "You need at least {}GB of memory. Found {}GB"
            raise MemoryError(msg.format(min_memory_gb, total_gb))

        if no_cls_size == "O2O" and not include_vs:
            self.parser.error(
                "You can't set the parameter '--ucls-size/-u' to 'O2O' "
                "if you set the flag '--no-variable-stars/-nvs'")

        if vs_type and not include_vs:
            self.parser.error(
                "You can't set the parameter '--variable-stars-type/-vst' "
                "if you set the flag '--no-variable-stars/-nvs'")

        import pandas as pd

        result = []
        with db.session_scope() as session:
            query = session.query(
                LightCurves).join(Tile).filter(Tile.name.in_(tnames))
            for lc in query:
                print "Reading features of tile {}...".format(lc.tile.name)

                features = pd.DataFrame(lc.features)
                print "Sources {}".format(len(features))

                if no_saturated:
                    print "No saturated <-"
                    features = features[features.Mean > 12]

                if no_faint:
                    print "No Faint <-"
                    features = features[features.Mean < 16.5]

                if cone_search:
                    ra, dec, radius = cone_search
                    print "ConeSearch({}, {}, {}) <-".format(ra, dec, radius)
                    features = self.cone_search(
                        features=features, ra_c=ra, dec_c=dec, sr_c=radius)

                if include_vs:
                    print "Retrieving '{}' VS <-".format(vs_type or all)
                    vss = features[features.vs_type != ""]
                    if vs_type:
                        vss = vss[vss.vs_type.str.contains(vs_type)]
                    if len(vss):
                        result.append(vss)

                print "Sampling Unk Src <-"
                unk = features[features.vs_type == ""]
                if no_cls_size == "ALL":
                    sample_size = len(unk)
                elif no_cls_size == "O2O":
                    sample_size = len(vss)
                    if sample_size == 0:
                        continue
                else:
                    sample_size = no_cls_size
                unk = unk.sample(sample_size)
                result.append(unk)

        print "Merging"
        result = pd.concat(result, ignore_index=True)
        print("Total Size {}".format(len(result)))

        print "Saving to {}".format(output)
        ext = os.path.splitext(output)[-1]
        if ext == ".csv":
            result.to_csv(output, index=False)
        elif ext == ".pkl":
            result.to_pickle(output)
        elif ext == ".bz2":
            result.to_pickle(output, compression="bz2")
        else:
            msg = "unknow type {}".format(output)
            raise ValueError(msg)
