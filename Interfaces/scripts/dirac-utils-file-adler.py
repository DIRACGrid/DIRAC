#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-adler
########################################################################
"""
  Calculate alder32 of the supplied file
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.Adler import fileAdler
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

for fa in files:
  adler = fileAdler(fa)
  if adler:
    print(fa.rjust(100), adler.ljust(10))  # pylint: disable=no-member
  else:
    print('ERROR %s: Failed to get adler' % fa)
    exitCode = 2

DIRAC.exit(exitCode)
