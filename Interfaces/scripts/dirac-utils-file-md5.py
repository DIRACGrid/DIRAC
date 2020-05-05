#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-utils-file-adler
# Author :
########################################################################
"""
  Calculate md5 of the supplied file
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... File ...' % Script.scriptName,
                                  'Arguments:',
                                  '  File:     File Name']))
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
