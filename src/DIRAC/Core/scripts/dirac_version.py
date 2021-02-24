#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
Print version of current DIRAC installation

Usage:
  dirac-version [option]

Example:
  $ dirac-version

"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import argparse

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.parse_known_args()

  print(DIRAC.version)


if __name__ == "__main__":
  main()
