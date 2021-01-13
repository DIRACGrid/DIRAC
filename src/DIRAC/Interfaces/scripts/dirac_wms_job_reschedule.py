#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-delete
# Author :  Stuart Paterson
########################################################################
"""
  Reschedule the given DIRAC job
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
  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                    'Arguments:',
                                    '  JobID:    DIRAC Job ID']))
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp(exitCode=1)

  from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
  dirac = Dirac()
  exitCode = 0
  errorList = []

  result = dirac.rescheduleJob(parseArguments(args))
  if result['OK']:
    print('Rescheduled job %s' % ','.join([str(j) for j in result['Value']]))
  else:
    errorList.append((j, result['Message']))
    print(result['Message'])
    exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
