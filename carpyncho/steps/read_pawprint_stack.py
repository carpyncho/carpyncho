# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

import shutil
import tempfile
import os
import copy

import numpy as np

from corral import run

from astropy.io import fits

from PyAstronomy import pyasl

from six.moves import zip, range

from .. import bin
from ..lib.context_managers import cd
from ..models import PawprintStack


# =============================================================================
# CONSTANTS
# =============================================================================

PAWPRINT_DTYPE = {
    "names": [
        'ra_h', 'ra_m', 'ra_s', 'dec_d', 'dec_m', 'dec_s', 'x', 'y',
        'mag1', 'mag_err1', 'mag2', 'mag_err2',
        'mag3', 'mag_err3', 'mag4', 'mag_err4',
        'mag5', 'mag_err5', 'mag6', 'mag_err6', 'mag7', 'mag_err7',
        'chip_nro', 'stel_cls', 'elip', 'pos_ang', 'confidence',
    ],
    "formats": [
        int, int, float, int, int, float, float, float,
        float, float, float, float,
        float, float, float, float,
        float, float, float, float, float, float,
        int, int, float, float, float
    ]
}


# =============================================================================
# STEPS
# =============================================================================

class ReadPawprintStack(run.Step):
    """Convert the pawprint into a numpy array
    ans also set the mjd and band metadata. This makes the pawprint-stack ready
    to be matched again their tiles.

    """

    model = PawprintStack
    conditions = [model.status == "raw"]
    groups = ["preprocess", "read"]

    # =========================================================================
    # STEP SETUP & TEARDOWN
    # =========================================================================

    def setup(self):
        self.vvv_flx2mag = bin.vvv_flx2mag.execute
        self.temp_directory = tempfile.mkdtemp(suffix="_carpyncho_ppstk")

    def teardown(self, *args, **kwargs):
        if not os.path.exists(self.temp_directory):
            shutil.rmtree(self.temp_directory)

    # =========================================================================
    # EXTRACT HEADER
    # =========================================================================

    def extract_headers(self, hdulist):
        mjd = hdulist[0].header["MJD-OBS"]
        band = hdulist[0].header["ESO INS FILT1 NAME"].strip()
        return band, mjd

    # =========================================================================
    # TO ARRAY
    # =========================================================================

    def load_fit(self, pawprint):
        to_cd = os.path.dirname(pawprint)
        basename = os.path.basename(pawprint)
        asciiname = os.path.splitext(basename)[0] + ".txt"
        asciipath = os.path.join(self.temp_directory, asciiname)

        # create the ascii table
        with cd(to_cd):
            self.vvv_flx2mag(basename, asciipath)

        # read ascii table
        odata = np.genfromtxt(asciipath, PAWPRINT_DTYPE)
        os.remove(asciipath)
        return odata, len(odata)

    def add_columns(self, odata, size, pwp_id, mjd, dtypes):
        """Add id, hjds, ra_deg and dec_deg columns to existing recarray

        """

        # calculate the ra and the dec columns
        radeg = 15 * (odata['ra_h'] +
                      odata['ra_m'] / 60.0 +
                      odata['ra_s'] / 3600.0)

        decdeg = np.sign(odata['dec_d']) * (np.abs(odata['dec_d']) +
                                            odata['dec_m'] / 60.0 +
                                            odata['dec_s'] / 3600.0)

        # calculate the hjds
        hjds = np.fromiter(
            (pyasl.helio_jd(mjd, ra, dec) for ra, dec in zip(radeg, decdeg)),
            dtype=float)

        # create ids
        ps_name = "3" + str(pwp_id).zfill(7)

        def get_id(order):
            order = str(order).rjust(8, "0")
            return (ps_name + order)

        ids = np.fromiter(
            (get_id(idx + 1) for idx in range(size)), dtype=np.int64)

        # create a new dtype to store the ra and dec as degrees
        dtype = copy.deepcopy(dtypes)
        dtype["names"].insert(0, "dec_deg")
        dtype["names"].insert(0, "ra_deg")
        dtype["names"].insert(0, "hjd")
        dtype["names"].insert(0, "id")

        dtype["formats"].insert(0, float)
        dtype["formats"].insert(0, float)
        dtype["formats"].insert(0, float)
        dtype["formats"].insert(0, np.int64)

        # create an empty array and copy the values
        data = np.empty(len(odata), dtype=dtype)
        for name in data.dtype.names:
            if name == "id":
                data[name] = ids
            elif name == "ra_deg":
                data[name] = radeg
            elif name == "dec_deg":
                data[name] = decdeg
            elif name == "hjd":
                data[name] = hjds
            else:
                data[name] = odata[name]
        return data

    def to_array(self, pwp_stk):
        original_array, size = self.load_fit(pwp_stk.raw_file_path)
        arr = self.add_columns(
            odata=original_array, size=size, pwp_id=pwp_stk.id,
            mjd=pwp_stk.mjd, dtypes=PAWPRINT_DTYPE)
        return arr, size

    # =========================================================================
    # STEP FUNCTIONS
    # =========================================================================

    def process(self, pwp):

        with fits.open(pwp.raw_file_path) as hdulist:
            pwp.band, pwp.mjd = self.extract_headers(hdulist)

        arr, size = self.to_array(pwp)

        pwp.size = size
        pwp.store_npy_file(arr)
        pwp.status = "ready-to-match"

        yield pwp
        self.session.commit()
