#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
Print version of current DIRAC installation

Usage::

  dirac-version [option]

Example::

  $ dirac-version

"""
from __future__ import print_function

__RCSID__ = "$Id$"

import argparse

parser = argparse.ArgumentParser(usage=__doc__)
parser.parse_known_args()

import DIRAC
print(DIRAC.version)
