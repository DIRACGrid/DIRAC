#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-adler
########################################################################
"""
Calculate alder32 of the supplied file

Usage:

  dirac-utils-file-adler [option|cfgfile] ... File ...

Arguments:

  File:     File Name

Example:

  $ dirac-utils-file-adler Example.tgz
  Example.tgz 88b4ca8b
"""
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=False)
files = Script.getPositionalArgs()
if len(files) == 0:
  Script.showHelp()

exitCode = 0

for fa in files:
  adler = fileAdler(fa)
  if adler:
    print(fa.rjust(100), adler.ljust(10))  # pylint: disable=no-member
  else:
    print('ERROR %s: Failed to get adler' % fa)
    exitCode = 2

DIRAC.exit(exitCode)