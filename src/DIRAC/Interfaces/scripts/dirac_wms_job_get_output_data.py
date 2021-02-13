#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-output-data
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the output data files of a DIRAC job

Usage:
  dirac-wms-job-get-output-data [options] ... JobID ...

Arguments:
  JobID:    DIRAC Job ID
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerSwitch("D:", "Dir=", "Store the output in this directory")
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp(exitCode=1)

  from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
  dirac = Dirac()
  exitCode = 0
  errorList = []

  outputDir = ''
  for sw, v in Script.getUnprocessedSwitches():
    if sw in ('D', 'Dir'):
      outputDir = v

  for job in parseArguments(args):

    result = dirac.getJobOutputData(job, destinationDir=outputDir)
    if result['OK']:
      print('Job %s output data retrieved' % (job))
    else:
      errorList.append((job, result['Message']))
      exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
