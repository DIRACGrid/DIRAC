#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-kill
# Author :  Stuart Paterson
########################################################################
"""
Issue a kill signal to a running DIRAC job

Usage:
  dirac-wms-job-kill [options] ... JobID ...

Arguments:
  JobID:    DIRAC Job ID

Example:
  $ dirac-wms-job-kill 1918
  Killed job 1918

.. Note::

  - jobs will not disappear from JobDB until JobCleaningAgent has deleted them
  - jobs will be deleted "immediately" if they are in the status 'Deleted'
  - USER jobs will be deleted after a grace period if they are in status Killed, Failed, Done

  What happens when you hit the "kill job" button

  - if the job is in status 'Running', 'Matched', 'Stalled' it will be properly killed, and then its
    status will be marked as 'Killed'
  - otherwise, it will be marked directly as 'Killed'.
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
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp(exitCode=1)

  from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

  result = Dirac().killJob(parseArguments(args))
  if result['OK']:
    print('Killed jobs %s' % ','.join([str(j) for j in result['Value']]))
    exitCode = 0
  else:
    print('ERROR', result['Message'])
    exitCode = 2

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
