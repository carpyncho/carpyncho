#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd

from astropy.coordinates import SkyCoord
from astropy.io.votable import parse

import sh
from sh import bzip2

from ...lib.context_managers import cd


# =============================================================================
# CONSTANTS
# =============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))

CATALOG_PATH = os.path.join(PATH, "carpyncho_catalog.pkl")


# =============================================================================
# BUILD
# =============================================================================

def get_ogle_3_resume():
    with cd(PATH):
        bzip2("-f", "-dk", "ogleIII_all.csv.bz2")
        df = pd.read_csv("ogleIII_all.csv")

        ra = df["RA"].apply(
                lambda d: d.replace(":", "h", 1).replace(":", "m", 1) + "s")
        dec = df["Decl"].apply(
                lambda d: d.replace(":", "d", 1).replace(":", "m", 1) + "s")

        coords = SkyCoord(ra, dec, frame='icrs')
        df['ra'] = pd.Series(coords.ra.deg, index=df.index)
        df['dec'] = pd.Series(coords.dec.deg, index=df.index)
        df["cls"] = df["Type"] + "-" + df["Subtype"]

        df = df[["ID", "ra", "dec", "cls"]]
        df["catalog"] = pd.Series("OGLE-3", index=df.index)
        os.remove("ogleIII_all.csv")
        return df


def get_ogle_4_resume():
    with cd(PATH):
        bzip2("-f", "-dk", "ogle4.csv.bz2")
        df = pd.read_csv("ogle4.csv")

        ra = df["ra"].apply(
                lambda d: d.replace(":", "h", 1).replace(":", "m", 1).replace(":", ".") + "s")
        dec = df["dec"].apply(
                lambda d: d.replace(":", "d", 1).replace(":", "m", 1).replace(":", ".") + "s")

        coords = SkyCoord(ra, dec, frame='icrs')
        df['ra'] = pd.Series(coords.ra.deg, index=df.index)
        df['dec'] = pd.Series(coords.dec.deg, index=df.index)
        df["ID"] = df["id"]

        df = df[["ID", "ra", "dec", "cls"]]
        df["catalog"] = pd.Series("OGLE-4", index=df.index)
        os.remove("ogle4.csv")
        return df


def get_vizier_resume():
    with cd(PATH):
        bzip2("-f", "-dk", "vizier_votable.vot.bz2")
        votable = parse("vizier_votable.vot")
        table = votable.get_first_table().to_table(use_names_over_ids=True)
        df = table.to_pandas()

        del votable, table

        df = df[["OID", "RAJ2000", "DEJ2000", "Type"]].copy()
        df.columns = "ID", "ra", "dec", "cls"
        df["catalog"] = "vizier"

        # solo las RRLyrae
        flt = 'RRAB', 'RRC', 'RRD'
        df = df[df.cls.isin(flt)]

        def change_type(t):
            subpart = {
                'RRAB': "RRab",
                'RRC': "RRc",
                'RRD': "RRd"
            }[t]
            return "RRLyr-" + subpart
        df["cls"] = df.cls.apply(change_type)

        os.remove("vizier_votable.vot")
        return df


def build():
    print("Bulding Vizier")
    vizier = get_vizier_resume()
    print("Building OGLE III")
    ogle3 = get_ogle_3_resume()
    print("Building OGLE IV")
    ogle4 = get_ogle_4_resume()

    print("Merging")
    catalog = pd.concat((ogle3, ogle4, vizier), ignore_index=True)

    print("Saving catalog")
    catalog.to_pickle(CATALOG_PATH)


# =============================================================================
# LOAD
# =============================================================================

def load():
    return pd.read_pickle(CATALOG_PATH)
