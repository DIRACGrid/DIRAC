#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-pilot-info
# Author :  Ricardo Graciani
########################################################################
"""
Retrieve available info about the given pilot

Usage:
  dirac-admin-get-pilot-info [options] ... PilotID ...

Arguments:
  PilotID:  Grid ID of the pilot

Example:
  $ dirac-admin-get-pilot-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  {'https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw': {'AccountingSent': 'False',
                                                          'BenchMark': 0.0,
                                                          'Broker': 'marwms.in2p3.fr',
                                                          'DestinationSite': 'cclcgceli01.in2p3.fr',
                                                          'GridSite': 'LCG.IN2P3.fr',
                                                          'GridType': 'gLite',
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 49, 14),
                                                          'OutputReady': 'False',
                                                          'OwnerDN': '/O=GRID/C=FR/O=CNRS/OU=LPC/CN=Sebastien Guizard',
                                                          'OwnerGroup': '/biomed',
                                                          'ParentID': 0L,
                                                          'PilotID': 2241L,
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/2KHFrQjkw',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52),
                                                          'TaskQueueID': 399L}}
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

extendedPrint = False


def setExtendedPrint(_arg):
  global extendedPrint
  extendedPrint = True


@DIRACScript()
def main():
  global extendedPrint
  Script.registerSwitch('e', 'extended', 'Get extended printout', setExtendedPrint)
  Script.parseCommandLine(ignoreErrors=True)

  from DIRAC import exit as DIRACExit
  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp()

  diracAdmin = DiracAdmin()
  dirac = Dirac()
  exitCode = 0
  errorList = []

  for gridID in args:
    result = diracAdmin.getPilotInfo(gridID)
    if not result['OK']:
      errorList.append((gridID, result['Message']))
      exitCode = 2
    else:
      res = result['Value'][gridID]
      if extendedPrint:
        tab = ''
        for key in [
            'PilotJobReference',
            'Status',
            'OwnerDN',
            'OwnerGroup',
            'SubmissionTime',
            'DestinationSite',
            'GridSite',
        ]:
          if key in res:
            diracAdmin.log.notice('%s%s: %s' % (tab, key, res[key]))
            if not tab:
              tab = '  '
        diracAdmin.log.notice('')
        for jobID in res['Jobs']:
          tab = '  '
          result = dirac.getJobAttributes(int(jobID))
          if not result['OK']:
            errorList.append((gridID, result['Message']))
            exitCode = 2
          else:
            job = result['Value']
            diracAdmin.log.notice('%sJob ID: %s' % (tab, jobID))
            tab += '  '
            for key in ['OwnerDN', 'OwnerGroup', 'JobName', 'Status', 'StartExecTime', 'LastUpdateTime', 'EndExecTime']:
              if key in job:
                diracAdmin.log.notice('%s%s:' % (tab, key), job[key])
        diracAdmin.log.notice('')
      else:
        print(diracAdmin.pPrint.pformat({gridID: res}))

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRACExit(exitCode)


if __name__ == "__main__":
  main()
