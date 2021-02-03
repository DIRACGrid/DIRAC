#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-md5
# Author :
########################################################################
"""
Calculate md5 of the supplied file

Usage:

  dirac-utils-file-md5 [option|cfgfile] ... File ...

Arguments:

  File:     File Name

Example:

  $ dirac-utils-file-md5 Example.tgz
  Example.tgz 5C1A1102-EAFD-2CBA-25BD-0EFCCFC3623E
"""
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=False)
files = Script.getPositionalArgs()
if len(files) == 0:
  Script.showHelp()

exitCode = 0

for file in files:
  try:
    md5 = makeGuid(file)
    if md5:
      print(file.rjust(100), md5.ljust(10))
    else:
      print('ERROR %s: Failed to get md5' % file)
      exitCode = 2
  except Exception as x:
    print('ERROR %s: Failed to get md5' % file, str(x))
    exitCode = 2

DIRAC.exit(exitCode)
