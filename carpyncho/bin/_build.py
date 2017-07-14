#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

PATH = os.path.abspath(os.path.dirname(__file__))

commands = "\n".join([
    l.strip() for l in
    """
        set -e;
        cd {};
        gfortran vvv_flx2mag.f -o vvv_flx2mag -L/sw/lib -Lcfitsio -lcfitsio -lm;
        gfortran tff.f -o tff;
        cd - > /dev/null;

    """.format(PATH).splitlines()
]).strip()


def build():
    os.system(commands)
