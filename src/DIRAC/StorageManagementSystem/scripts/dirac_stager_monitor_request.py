#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-request
# Author :  Andrew C. Smith
########################################################################
"""
Report the summary of the stage task from the DB.

Usage:
  dirac-stager-monitor-request [options] ... Request ...

Arguments:
  Request:  ID of the Stage request in the StorageManager
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=False)

  args = Script.getPositionalArgs()

  if not len(args) == 1:
    Script.showHelp()

  from DIRAC import exit as DIRACExit, gLogger

  try:
    taskID = int(args[0])
  except Exception:
    gLogger.fatal('Stage requestID must be an integer')
    DIRACExit(2)

  from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
  client = StorageManagerClient()

  res = client.getTaskSummary(taskID)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRACExit(2)
  taskInfo = res['Value']['TaskInfo']
  replicaInfo = res['Value']['ReplicaInfo']
  outStr = "%s: %s" % ('TaskID'.ljust(20), taskID)
  outStr += "\n%s: %s" % ('Status'.ljust(20), taskInfo[taskID]['Status'])
  outStr += "\n%s: %s" % ('Source'.ljust(20), taskInfo[taskID]['Source'])
  outStr += "\n%s: %s" % ('SourceTaskID'.ljust(20), taskInfo[taskID]['SourceTaskID'])
  outStr += "\n%s: %s" % ('CallBackMethod'.ljust(20), taskInfo[taskID]['CallBackMethod'])
  outStr += "\n%s: %s" % ('SubmitTime'.ljust(20), taskInfo[taskID]['SubmitTime'])
  outStr += "\n%s: %s" % ('CompleteTime'.ljust(20), taskInfo[taskID]['CompleteTime'])
  for lfn, metadata in replicaInfo.items():
    outStr += "\n"
    outStr += "\n\t%s: %s" % ('LFN'.ljust(8), lfn.ljust(100))
    outStr += "\n\t%s: %s" % ('SE'.ljust(8), metadata['StorageElement'].ljust(100))
    outStr += "\n\t%s: %s" % ('PFN'.ljust(8), str(metadata['PFN']).ljust(100))
    outStr += "\n\t%s: %s" % ('Size'.ljust(8), str(metadata['FileSize']).ljust(100))
    outStr += "\n\t%s: %s" % ('Status'.ljust(8), metadata['Status'].ljust(100))
    outStr += "\n\t%s: %s" % ('Reason'.ljust(8), str(metadata['Reason']).ljust(100))
  gLogger.notice(outStr)


if __name__ == "__main__":
  main()
