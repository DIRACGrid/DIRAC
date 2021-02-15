#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-job-pilot-output
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the output of the pilot that executed a given job

Usage:
  dirac-admin-get-job-pilot-output [options] ... JobID ...

Arguments:
  JobID:    DIRAC ID of the Job

Example:
  $ dirac-admin-get-job-pilot-output 34
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp()

  from DIRAC import exit as DIRACExit
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  for job in args:

    try:
      job = int(job)
    except Exception as x:
      errorList.append(('Expected integer for JobID', job))
      exitCode = 2
      continue

    result = diracAdmin.getJobPilotOutput(job)
    if not result['OK']:
      errorList.append((job, result['Message']))
      exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRACExit(exitCode)


if __name__ == "__main__":
  main()
