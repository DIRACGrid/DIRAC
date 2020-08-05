#!/usr/bin/env python
########################################################################
# File :    dirac-production-job-get-input
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve input sandbox for DIRAC Job
"""
from __future__ import print_function
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
import os

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  '  JobID:    DIRAC Job ID']))
Script.registerSwitch("D:", "Dir=", "Store the output in this directory")
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
dirac = Dirac()
exitCode = 0
errorList = []

outputDir = None
for sw, v in Script.getUnprocessedSwitches():
  if sw in ('D', 'Dir'):
    outputDir = v

for job in parseArguments(args):

  result = dirac.getInputSandbox(job, outputDir=outputDir)
  if result['OK']:
    if os.path.exists('InputSandbox%s' % job):
      print('Job input sandbox retrieved in InputSandbox%s/' % (job))
  else:
    errorList.append((job, result['Message']))
    exitCode = 2

for error in errorList:
  print("ERROR %s: %s" % error)

DIRAC.exit(exitCode)
