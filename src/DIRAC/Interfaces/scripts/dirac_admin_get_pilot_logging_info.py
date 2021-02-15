#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-pilot-logging-info.py
# Author :  Stuart Paterson
########################################################################
"""
Retrieve logging info of a Grid pilot

Usage:
  dirac-admin-get-pilot-logging-info [options] ... PilotID ...

Arguments:
  PilotID:  Grid ID of the pilot

Example:
  $ dirac-admin-get-pilot-logging-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  Pilot Reference: dirac-admin-get-pilot-logging-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  ===================== glite-job-logging-info Success =====================
  LOGGING INFORMATION:
  Printing info for the Job : https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
      ---
  Event: RegJob
  - Arrived   =  Mon Feb 21 13:27:50 2011 CET
  - Host      =  marwms.in2p3.fr
  - Jobtype   =  SIMPLE
  - Level     =  SYSTEM
  - Ns        =  https://marwms.in2p3.fr:7443/glite_wms_wmproxy_server
  - Nsubjobs  =  0
  - Parent    =  https://marlb.in2p3.fr:9000/WQHVOB1mI4oqrlYz2ZKtgA
  - Priority  =  asynchronous
  - Seqcode   =  UI=000000:NS=0000000001:WM=000000:BH=0000000000:JSS=000000:LM=000000:LRMS=000000:APP=000000:LBS=000000
  - Source    =  NetworkServer
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position
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

  for gridID in args:

    result = diracAdmin.getPilotLoggingInfo(gridID)
    if not result['OK']:
      errorList.append((gridID, result['Message']))
      exitCode = 2
    else:
      print('Pilot Reference: %s', gridID)
      print(result['Value'])
      print()

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRACExit(exitCode)


if __name__ == "__main__":
  main()
