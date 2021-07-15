#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-reschedule
# Author :  Stuart Paterson
########################################################################
"""
Reschedule the given DIRAC job

Example:
  $ dirac-wms-job-reschedule 1
  Rescheduled job 1
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
  # Registering arguments will automatically add their description to the help menu
  Script.registerArgument(["JobID:    DIRAC Job ID"])
  _, args = Script.parseCommandLine(ignoreErrors=True)

  from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
  dirac = Dirac()
  exitCode = 0
  errorList = []

  result = dirac.rescheduleJob(parseArguments(args))
  if result['OK']:
    print('Rescheduled job %s' % ','.join([str(j) for j in result['Value']]))
  else:
    errorList.append((result['Value'][-1], result['Message']))
    print(result['Message'])
    exitCode = 2

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
