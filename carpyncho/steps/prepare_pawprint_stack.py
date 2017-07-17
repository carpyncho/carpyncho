# -*- coding: utf-8 -*-

# =============================================================================
# IMPORTS
# =============================================================================

from corral import run

from astropy.io import fits

from ..models import PawprintStack


# =============================================================================
# STEPS
# =============================================================================

class PreparePawprintStack(run.Step):
    """Convert the pawprint into a numpy array
    ans also set the mjd and band metadata

    """

    model = PawprintStack
    conditions = [model.status == "raw"]
    groups = ["preprocess"]

    def extract_headers(self, hdulist:
        mjd = hdulist[0].header["MJD-OBS"]
        band = hdulist[0].header["ESO INS FILT1 NAME"].strip()
        return band, mjd

    def process(self, pwp):
        with fits.open(pwp.raw_file_path) as hdulist:
            pwp.band, pwp.mjd = self.extract_headers(hdulist)
        arr = tile.store_npy_file(arr)
