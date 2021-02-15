#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-jdl
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the current JDL of a DIRAC job

Usage:
  dirac-wms-job-get-jdl [options] ... JobID ...

Arguments:
  JobID:    DIRAC Job ID

Example:
  $ dirac-wms-job-get-jdl 1
  {'Arguments': '-ltrA',
   'CPUTime': '86400',
   'DIRACSetup': 'EELA-Production',
   'Executable': '/bin/ls',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobRequirements': '[OwnerDN = /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar;
                        OwnerGroup = eela_user;
                        Setup = EELA-Production;
                        UserPriority = 1;
                        CPUTime = 0 ]',
   'OutputSandbox': ['std.out', 'std.err'],
   'Owner': 'vhamar',
   'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
   'OwnerGroup': 'eela_user',
   'OwnerName': 'vhamar',
   'Priority': '1'}
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
  original = False
  Script.registerSwitch('O', 'Original', 'Gets the original JDL')
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Original' or switch[0] == 'O':
      original = True

  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Original':
      original = True

  if len(args) < 1:
    Script.showHelp(exitCode=1)

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


if __name__ == "__main__":
  main()
