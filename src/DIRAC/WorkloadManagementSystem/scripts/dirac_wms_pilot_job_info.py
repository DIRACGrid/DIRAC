#!/usr/bin/env python
########################################################################
# File :    dirac-wms-pilot-job-info
# Author :  Philippe Charpentier
########################################################################
"""
Retrieve info about jobs run by the given pilot

Usage:
  dirac-wms-pilot-job-info [options] ... PilotID ...

Arguments:
  PilotID:  Grid ID of the pilot
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


def _stringInList(subStr, sList):
  resList = []
  for s in sList:
    if subStr.lower() in s.lower():
      resList.append(s)
  return resList


@DIRACScript()
def main():
  parameters = ['OwnerDN', 'StartExecTime', 'EndExecTime']
  Script.registerSwitch('', 'Parameters=', '   List of strings to be matched by job parameters or attributes')
  Script.parseCommandLine(ignoreErrors=True)
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Parameters':
      parameters += [par for par in switch[1].split(',')]
  parameters = [(i, par.lower()) for i, par in enumerate(parameters) if par]
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  from DIRAC.Interfaces.API.Dirac import Dirac
  diracAdmin = DiracAdmin()
  dirac = Dirac()
  errorList = []

  for gridID in args:

    result = {}
    res = diracAdmin.getPilotInfo(gridID)
    if not res['OK']:
      errorList.append((gridID, res['Message']))
    else:
      jobIDs = set([int(jobID) for jobID in res['Value'][gridID]['Jobs']])
      totCPU = 0
      totWall = 0
      effRequested = False
      for jobID in sorted(jobIDs):
        result.setdefault(jobID, {})
        for func in (dirac.getJobParameters, dirac.getJobAttributes):
          res = func(jobID)
          if not res['OK']:
            errorList.append(('Job %d' % jobID, res['Message']))
          else:
            params = res['Value']
            if 'TotalCPUTime(s)' in params:
              totCPU += float(params['TotalCPUTime(s)'])
              totWall += float(params['WallClockTime(s)'])
              params['CPUEfficiency'] = '%s %%' % (
                  100. * float(params['TotalCPUTime(s)']) / float(params['WallClockTime(s)']))
            for i, par in parameters:
              for param in [p for p in _stringInList(str(par), str(params))
                            if not _stringInList(str(p), str(result[jobID]))]:
                if param == 'CPUEfficiency':
                  effRequested = True
                result[jobID]['%d.%s' % (i, param)] = params[param]
        if effRequested:
          result['CPUEfficiency'] = '%s %%' % (100. * totCPU / totWall)
      print(diracAdmin.pPrint.pformat({gridID: result}))

  for error in errorList:
    print("ERROR %s: %s" % error)


if __name__ == "__main__":
  main()
