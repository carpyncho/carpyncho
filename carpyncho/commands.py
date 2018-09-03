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

import sys
import os
import shutil
import argparse
import json
from pprint import pprint
from collections import Counter


from psutil import virtual_memory

import sh

import numpy as np

from texttable import Texttable

from sqlalchemy.engine import url

from sqlalchemy_utils import database_exists, create_database, drop_database

from corral import cli, conf, db, core

from carpyncho import bin
from carpyncho.models import Tile, PawprintStack, PawprintStackXTile, LightCurves


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

if conf.settings.DEBUG:

    class FreshLoad(cli.BaseCommand):
        """Reset the database and run the loader class (only in debug)"""

        def setup(self):
            self.conn = conf.settings.CONNECTION

            urlo = url.make_url(self.conn)
            self.backend = urlo.get_backend_name()
            self.db = urlo.database

        def recreate_pg(self):
            if database_exists(self.conn):
                drop_database(self.conn)
            create_database(self.conn)

        def recreate_sqlite(self):
            try:
                os.remove(self.db)
            except OSError:
                pass

        def handle(self):
            if self.backend == "postgresql":
                self.recreate_pg()
            elif self.backend == "sqlite":
                self.recreate_sqlite()

            try:
                shutil.rmtree("_input_data")
            except OSError:
                pass

            try:
                shutil.rmtree("_data")
            except OSError:
                pass

            if not os.path.exists("example_data"):
                os.system("tar jxf res/example_data.tar.bz2")

            shutil.copytree("example_data", "_input_data")
            os.system("python in_corral.py createdb --noinput")
            os.system("python in_corral.py load")


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
        table.header(("Tile", "Status", "OGLE-3 Tags", "Size", "Ready"))
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

    This sample contains all the OGLE3-variable stars and a subset of unknow
    sources.

    """

    options = {"title": "sample"}

    def setup(self):
        self.parser.add_argument(
            "tname", action="store", help="name of the tile to sample")
        self.parser.add_argument(
            "--output", "-o", dest="output", required=True,
            help="path of the sample file")
        self.parser.add_argument(
            "--sample-size", "-s", dest="sample_size", default=20e+3,
            type=int, help="sample size of unknow sources")
        self.parser.add_argument(
            "--sample-all-class", "-sac", nargs="+",
            dest="sample_all_class", default=[], action="store",
            help="The list of positive class to be sampled completely")
        self.parser.add_argument(
            "--ignore-memory", "-i", dest="check_memory", default=True,
            action="store_false",
            help="ignore the memory che before run the command")
        self.parser.add_argument(
            "--min-proportion", "-mp", dest="min_proportion", default=0.01,
            action="store", type=float,
            help="The minimun percentage allowed by a sampling class")

    def handle(self, tname, output, sample_size, sample_all_class, check_memory, min_proportion):
        min_memory, mem = int(32e+9), virtual_memory()
        if check_memory and mem.total < min_memory:
            min_memory_gb = min_memory / 1e+9
            total_gb = mem.total / 1e+9
            msg = "You need at least {}GB of memory. Found {}GB"
            raise MemoryError(msg.format(min_memory_gb, total_gb))
        if min_proportion > 1 or min_proportion < 0:
            msg = "min-proportion must be betwen [0, 1]"
            raise ValueError(msg)

        with db.session_scope() as session:
            lc = session.query(LightCurves).filter(
                LightCurves.tile.has(name=tname)).first()
            print "Reading..."
            features = lc.features

        print "Creating Filters..."
        if sample_all_class:
            var_filter = np.in1d(features["ogle3_type"], sample_all_class)
        else:
            var_filter = features["ogle3_type"] != ''

        print "Picking variables..."
        variables = features[var_filter]

        print "Sampling unknow sources..."
        unknow = np.random.choice(features[~var_filter], sample_size)

        print "Merging..."
        sample = np.concatenate((variables, unknow))

        print "Checking sampling..."
        selected_cls = Counter(features["ogle3_type"])
        min_selection = sample_size * min_proportion
        remove_unk = 0
        for cls in np.unique(features["ogle3_type"]):
            if cls not in selected_cls or selected_cls[cls] < min_selection:
                full_cls = features[features["ogle3_type"] == cls]
                cls_sample_size = np.min([full_cls.size, min_selection])
                if cls_sample_size > 0:
                    sample = np.concatenate((
                        np.random.choice(full_cls, cls_sample_size), sample))
                    remove_unk += int(cls_sample_size)

        print "Removing class unknow..."
        if remove_unk > 0:
            where_unk = np.where(sample[sample["ogle3_type"] == ''])[0]

            remove_unk_size = np.min([where_unk.size, remove_unk])
            to_remove = np.random.choice(where_unk, remove_unk_size)
            if len(to_remove):
                sample = np.delete(sample, to_remove)

        print "Saving..."
        np.save(output, sample)


class LSClasses(cli.BaseCommand):
    """List the objective classes in the given tile"""

    def setup(self):
        self.parser.add_argument(
            "tname", action="store", help="name of the tile to list classes")

    def handle(self, tname):
        log2critcal()

        with db.session_scope() as session:
            tile = session.query(Tile).filter(Tile.name==tname).first()
            classes = Counter(tile.load_npy_file()["ogle3_type"])

        table = Texttable(max_width=0)
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
        table.header(("OGLE3 Type", "Count"))

        for cls, cnt in sorted(classes.items()):
            table.add_row([cls or "''", cnt])

        print(table.draw())
        print("Count: {}".format(np.sum(classes.values())))


class DumpDB(cli.BaseCommand):
    """Dump the database to a JSON file"""

    def setup(self):
        self.parser.add_argument(
            "dump_file", action="store", type=argparse.FileType(mode="w"))

    def handle(self, dump_file):
        raise NotImplementedError()
        log2critcal()
        models = (Tile, PawprintStack, PawprintStackXTile, LightCurves)
        data = {}
        with db.session_scope() as session:
            setup_schema(db.Model, session)
            for model in models:
                model_data = []
                schema = model.__marshmallow__()
                for obj in session.query(model):
                    model_data.append(schema.dump(obj).data)
                data[model.__name__] = model_data
        json.dump(data, dump_file, indent=2)


class LoadDB(cli.BaseCommand):
    """Load the database from a JSON file"""

    def setup(self):
        self.parser.add_argument(
            "load_file", action="store", type=argparse.FileType())

    def handle(self, load_file):
        raise NotImplementedError()
        log2critcal()
        models = (Tile, PawprintStack, PawprintStackXTile, LightCurves)
        data = json.load(load_file)
        with db.session_scope() as session:
            setup_schema(db.Model, session)
            for model in models:
                model_data = data[model.__name__]
                schema = model.__marshmallow__()
                for row in model_data:
                    import ipdb; ipdb.set_trace()
