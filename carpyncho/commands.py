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

    def handle(self, status):
        log2critcal()

        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("Pawprint", "Status", "Band", "MJD", "Size"))
        cnt = 0
        with db.session_scope() as session:
            query = session.query(
                PawprintStack.name, PawprintStack.status,
                PawprintStack.band, PawprintStack.mjd, PawprintStack.size)
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
            "--ucls-size", "-u", dest="no_cls_size", default=2500,
            type=int, help="sample size of unknow sources by tile")
        self.parser.add_argument(
            "--no-saturated", "-ns", dest="no_saturated", default=False,
            action="store_true",
            help="Remove all satured sources (Mean magnitude < 12)")
        self.parser.add_argument(
            "--no-faint", "-nf", dest="no_faint", default=False,
            action="store_true",
            help="Remove all satured sources (Mean magnitude > 16.5)")
        self.parser.add_argument(
            "--ignore-memory", "-i", dest="cm", default=True,
            action="store_false",
            help="ignore the memory che before run the command")

    def handle(self, tnames, output, no_cls_size, no_saturated, no_faint, cm):
        min_memory, mem = int(32e+9), virtual_memory()
        if cm and mem.total < min_memory:
            min_memory_gb = min_memory / 1e+9
            total_gb = mem.total / 1e+9
            msg = "You need at least {}GB of memory. Found {}GB"
            raise MemoryError(msg.format(min_memory_gb, total_gb))

        import pandas as pd

        result = []
        with db.session_scope() as session:
            query = session.query(
                LightCurves).join(Tile).filter(Tile.name.in_(tnames))
            for lc in query:
                print "Reading features of tile {}...".format(lc.tile.name)

                features = pd.DataFrame(lc.features)
                if no_saturated:
                    features = features[features.Mean > 12]
                if no_faint:
                    features = features[features.Mean < 16.5]

                vss = features[features.vs_type != ""]
                unk = features[features.vs_type == ""].sample(no_cls_size)
                result.extend([vss, unk])

        print "Merging"
        result = pd.concat(result, ignore_index=True)

        print "Saving to {}".format(output)
        ext = os.path.splitext(output)[-1]
        if ext == ".csv":
            result.to_csv(output, index=False)
        elif ext == ".pkl":
            result.to_pickle(output)
        else:
            msg = "unknow type {}".format(output)
            raise ValueError(msg)
