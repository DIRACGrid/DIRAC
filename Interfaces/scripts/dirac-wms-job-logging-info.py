#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-logging-info
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve history of transitions for a DIRAC job
"""
from __future__ import print_function
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

  result = dirac.getJobLoggingInfo(job, printOutput=True)
  if not result['OK']:
    errorList.append((job, result['Message']))
    exitCode = 2

for error in errorList:
  print("ERROR %s: %s" % error)

DIRAC.exit(exitCode)
