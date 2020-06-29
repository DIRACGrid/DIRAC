#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-delete
# Author :  Stuart Paterson
########################################################################
"""
  Peek StdOut of the given DIRAC job
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  '  JobID:    DIRAC Job ID']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
dirac = Dirac()
exitCode = 0
errorList = []

for job in parseArguments(args):

  result = dirac.peekJob(job, printOutput=True)
  if not result['OK']:
    errorList.append((job, result['Message']))
    exitCode = 2

for error in errorList:
  print("ERROR %s: %s" % error)

DIRAC.exit(exitCode)
