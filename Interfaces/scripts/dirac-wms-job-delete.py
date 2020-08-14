#!/usr/bin/env python
########################################################################
# File : dirac-wms-job-delete
# Author : Stuart Paterson
########################################################################
"""
Delete DIRAC job from WMS, if running it will be killed
"""

from __future__ import print_function

__RCSID__ = "$Id$"

import os.path

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  ' %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  ' JobID: DIRAC Job ID']))

Script.registerSwitch("f:", "File=", "Get output for jobs with IDs from the file")
Script.registerSwitch("g:", "JobGroup=", "Get output for jobs in the given group")

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()


if __name__ == "__main__":

  from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
  from DIRAC.Core.Utilities.Time import toString, date, day
  dirac = Dirac()

  jobs = []
  for sw, value in Script.getUnprocessedSwitches():
    if sw.lower() in ('f', 'file'):
      if os.path.exists(value):
        jFile = open(value)
        jobs += jFile.read().split()
        jFile.close()
    elif sw.lower() in ('g', 'jobgroup'):
      group = value
      jobDate = toString(date() - 30 * day)
      result = dirac.selectJobs(jobGroup=value, date=jobDate)
      if not result['OK']:
        if "No jobs selected" not in result['Message']:
          print("Error:", result['Message'])
          DIRAC.exit(-1)
      else:
        jobs += result['Value']

  for arg in parseArguments(args):
    jobs.append(arg)

  if not jobs:
    print("Warning: no jobs selected")
    Script.showHelp()
    DIRAC.exit(0)

  result = dirac.deleteJob(jobs)
  if result['OK']:
    print('Deleted jobs %s' % ','.join([str(j) for j in result['Value']]))
    exitCode = 0
  else:
    print(result['Message'])
    exitCode = 2

  DIRAC.exit(exitCode)
