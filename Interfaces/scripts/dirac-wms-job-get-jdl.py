#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-jdl
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve the current JDL of a DIRAC job
"""
from __future__ import print_function
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

original = False
Script.registerSwitch('O', 'Original', 'Gets the original JDL')
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  '  JobID:    DIRAC Job ID']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'Original' or switch[0] == 'O':
    original = True

for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'Original':
    original = True

if len(args) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
dirac = Dirac()
exitCode = 0
errorList = []

for job in parseArguments(args):

  result = dirac.getJobJDL(job, original=original, printOutput=True)
  if not result['OK']:
    errorList.append((job, result['Message']))
    exitCode = 2

for error in errorList:
  print("ERROR %s: %s" % error)

DIRAC.exit(exitCode)
