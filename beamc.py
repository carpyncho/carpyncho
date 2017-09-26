#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Redistribution and use in source and binary forms, "with or without
#  modification, "are permitted provided that the following conditions are
#  met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, "this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, "this list of conditions and the following disclaimer
#    in the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of the  nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, "INCLUDING, "BUT NOT
#  LIMITED TO, "THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, "INDIRECT, "INCIDENTAL,
#  SPECIAL, "EXEMPLARY, "OR CONSEQUENTIAL DAMAGES (INCLUDING, "BUT NOT
#  LIMITED TO, "PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, "OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, "WHETHER IN CONTRACT, "STRICT LIABILITY, "OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, "EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

# =============================================================================
# DOCS
# =============================================================================

"""Client of VVV and 2MASS Bulge Extinction And Metallicity Calculator

This is a simply python wrapper arround the project located at:
http://mill.astro.puc.cl/BEAM/calculator.php

"""

# =============================================================================
# IMPORTS
# =============================================================================

from numbers import Number

import requests

import numpy as np

from astropy.coordinates import SkyCoord
from astropy import units as u

from six import string_types, StringIO


# =============================================================================
# CONSTANTS
# =============================================================================

BEAM_URL = "http://mill.astro.puc.cl/BEAM/calculator.php"

EXTINCTION_LAWS = {
    "Cardelli89": 1,
    "Nishiyama09": 2}

EXTINCTION_LAWS_TO_STR = dict((v, k) for k, v in EXTINCTION_LAWS.items())

SERVER_SOURCES_LIMIT = 40000

MIN_BOX_SIZE = 1.001

FORMS = {
    "extinction": {
        "formats": ['%10.8f', '%10.8f', '%4.3f', '%i'],
        "file_name": "ext_file",
        "form_name": "ext_fileform"}
}


# =============================================================================
# FUNTIONS
# =============================================================================

def to_latlon(coord):
    """Here we convert all stars to galactic coords and anything with
    -10.0 <= gal_lon <= 10.2 we subtract 360 (for beaminput) and feed the
    positions into arrays in the next block for beamin processing

    """
    l = np.asarray(coord.galactic.l.value).flatten()
    b = np.asarray(coord.galactic.b.value).flatten()

    to_big = np.where(l > 10.2)[0]
    l[to_big] = l[to_big] - 360

    return l, b


def prepare_data(l, b, box_size):
    """Parse the data to be feeded into extintion and metallicity

    Parameters
    ----------

    l : int or arry-like
      Galactic longitude in degrees. If is a number ``b`` must be a number to;
      if is an array ``b`` must be an array and the two must has the same
      length. Also ``-10 <= l <= +10.2``.
    b : int or arry-like
      Galactic latitude in degrees. If is a number `l` must be a number to;
      if is an array `l` must be an array and the two must has the same length.
      also  ``-10 <= b <= +5``
    box_size : int or arry-like
        Can be a number >= MIN_BOX_SIZE o an array-like with the same length
        in l and b; also every element must be a number >= MIN_BOX_SIZE.

    Returns
    -------

    l: np.ndarray
        The galactic longitude in a numpy array
    b: np.ndarray
        The galactic latitude in a numpy array
    box_size: np.ndarray
        The box size in a numpy array. If the input its only a number, "this
        create a new array with the same value in all the positions

    """
    # parsing latitude and longitude
    l, b = np.asarray(l).flatten(), np.asarray(b).flatten()

    if len(l) != len(b):
        raise ValueError("'l' and 'b' must have the same size")
    if len(l) > SERVER_SOURCES_LIMIT:
        msg = 'The server can only deal with {} sources at the time'
        raise ValueError(msg.format(SERVER_SOURCES_LIMIT))

    box_size = (
        np.zeros(len(l)) + box_size
        if isinstance(box_size, Number) else
        np.asarray(box_size))
    if len(box_size) != len(l):
        raise ValueError("'l', 'b' and 'box_size' must have the same size")
    elif np.any(box_size < MIN_BOX_SIZE):
        msg = "Boxsize too small, must be >= {}"
        raise ValueError(msg.format(MIN_BOX_SIZE))

    return l, b, box_size


def beamc_post(data, form):
    """Excecute the post sending the array to beam calculator"""
    form_params = FORMS[form]
    formats = form_params["formats"]
    file_name = form_params["file_name"]
    form_name = form_params["form_name"]

    stream = StringIO()
    np.savetxt(stream, data, fmt=formats)
    response = requests.post(
        BEAM_URL,
        files={file_name:  stream.getvalue()},
        data={form_name: "Upload"})
    return response


def post_process(beamc_data, extra_cols):
    """Add extra columns to the output of beamc

    """
    exclude_cols = ("beamc_ext_law", )

    dtype = (
        [(k, v.dtype) for k, v in extra_cols] +
        [(n, f) for n, f in beamc_data.dtype.descr if n not in exclude_cols])

    extra_cols = dict(extra_cols)

    # create an empty array and copy the values
    data = np.empty(len(beamc_data), dtype=dtype)
    for name in data.dtype.names:
        if name in extra_cols:
            data[name] = extra_cols[name]
        else:
            data[name] = beamc_data[name]

    return data


def extinction(ra, dec, box_size, law, interpolation="linear",
               inframe="fk5", prepare_data_kwargs=None):
    """Calculates the mean EXTINCTION Ak based on the method described in
    Gonzalez et al. 2011 and Gonzalez et al. 2012 . As described in the
    article, All extinctions are calculated using coefficients from
    Cardelli et al. 1989 and the user should use those to remain consistent.
    E(J-Ks) values are also returned so that the user can adopt a different
    extinction law if required. Aks values using Nishiyama et al 2009 can also
    be obtained. Be aware that extinction maps have a maximum resolution of
    2 arcmin and are insensitive to variations on smaller scales.

    A description of the extinction law and corresponding coefficients:
    http://mill.astro.puc.cl/BEAM/coffinfo.php

    All regions now have open access as published in
    Gonzalez et al. 2012: -10≤l≤+10.2 and -10≤b≤+5

    """

    ra = np.array([ra]) if isinstance(ra, Number) else np.asarray(ra)
    dec = np.array([dec]) if isinstance(dec, Number) else np.asarray(dec)
    in_coord = SkyCoord(ra=ra * u.degree, dec=dec * u.degree, frame=inframe)

    params = to_latlon(in_coord) + (box_size,)

    prepare_data_kwargs = (
        {} if prepare_data_kwargs is None else prepare_data_kwargs)
    l, b, box_size = prepare_data(*params, **prepare_data_kwargs)

    law = (
        np.zeros(len(l), dtype=int) + EXTINCTION_LAWS[law]
        if isinstance(law, string_types) else
        np.fromiter((EXTINCTION_LAWS[sl] for sl in law), dtype=int))
    if len(law) != len(l):
        raise ValueError("'l', 'b' and 'law' must have the same size")

    input_data = np.vstack((l, b, box_size, law)).T

    response = beamc_post(data=input_data, form="extinction")

    dtype = [
        ('beamc_l', float), ('beamc_b', float), ('beamc_box', float),
        ('beamc_ext_law', int), ('beamc_ejk', float),
        ('beamc_ak', float), ('beamc_err_ejk', float)]
    beamc_data = np.loadtxt(StringIO(response.text), dtype=dtype)
    if beamc_data.ndim == 0:
        beamc_data = beamc_data.flatten()

    beamc_coord = SkyCoord(
        l=beamc_data['beamc_l'] * u.degree,
        b=beamc_data['beamc_b'] * u.degree,
        frame='galactic'
    ).transform_to(inframe)

    beamc_ra = np.asarray(beamc_coord.ra.value).flatten()
    beamc_dec = np.asarray(beamc_coord.dec.value).flatten()
    beamc_separation = in_coord.separation(beamc_coord)
    import ipdb; ipdb.set_trace()

    beamc_law = np.asarray(map(EXTINCTION_LAWS_TO_STR.get, law))

    extra_cols = [
        ("ra", ra),
        ("dec", dec),
        ("beamc_ra", beamc_ra),
        ("beamc_dec", beamc_dec),
        ("beamc_separation", beamc_separation),
        ("beamc_law", beamc_law)]

    output = post_process(beamc_data, extra_cols)
    return output
