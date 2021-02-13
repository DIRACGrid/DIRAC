#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-file
# Author :  Daniela Remenska
########################################################################
"""
Give monitoring information regarding a staging file uniquely identified with (LFN,SE)

- status
- last update
- jobs requesting this file to be staged
- SRM requestID
- pin expiry time
- pin length

Usage:
  dirac-stager-monitor-file LFN SE ...

Arguments:
  LFN: LFN of the staging file
  SE: Storage Element for the staging file
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()
  if len(args) < 2:
    Script.showHelp()

  from DIRAC import exit as DIRACExit, gLogger

  lfn = args[0]
  se = args[1]

  from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
  client = StorageManagerClient()
  res = client.getCacheReplicas({'LFN': lfn, 'SE': se})
  if not res['OK']:
    gLogger.error(res['Message'])
  cacheReplicaInfo = res['Value']
  if cacheReplicaInfo:
    replicaID = list(cacheReplicaInfo)[0]
    outStr = "\n--------------------"
    outStr += "\n%s: %s" % ('LFN'.ljust(8), cacheReplicaInfo[replicaID]['LFN'].ljust(100))
    outStr += "\n%s: %s" % ('SE'.ljust(8), cacheReplicaInfo[replicaID]['SE'].ljust(100))
    outStr += "\n%s: %s" % ('PFN'.ljust(8), cacheReplicaInfo[replicaID]['PFN'].ljust(100))
    outStr += "\n%s: %s" % ('Status'.ljust(8), cacheReplicaInfo[replicaID]['Status'].ljust(100))
    outStr += "\n%s: %s" % ('LastUpdate'.ljust(8), str(cacheReplicaInfo[replicaID]['LastUpdate']).ljust(100))
    outStr += "\n%s: %s" % ('Reason'.ljust(8), str(cacheReplicaInfo[replicaID]['Reason']).ljust(100))

    resTasks = client.getTasks({'ReplicaID': replicaID})

    if resTasks['OK']:
      # print resTasks['Message']
      outStr += '\nJob IDs requesting this file to be staged:'.ljust(8)
      tasks = resTasks['Value']
      for tid in tasks.keys():
        outStr += ' %s ' % (tasks[tid]['SourceTaskID'])

    resStageRequests = client.getStageRequests({'ReplicaID': replicaID})

    if not resStageRequests['OK']:
      gLogger.error(resStageRequests['Message'])

    if resStageRequests['Records']:
      stageRequests = resStageRequests['Value']
      outStr += "\n------SRM staging request info--------------"
      for info in stageRequests.values():
        outStr += "\n%s: %s" % ('SRM RequestID'.ljust(8), info['RequestID'].ljust(100))
        outStr += "\n%s: %s" % ('SRM StageStatus'.ljust(8), info['StageStatus'].ljust(100))
        outStr += "\n%s: %s" % ('SRM StageRequestSubmitTime'.ljust(8), str(info['StageRequestSubmitTime']).ljust(100))
        outStr += "\n%s: %s" % ('SRM StageRequestCompletedTime'.ljust(8),
                                str(info['StageRequestCompletedTime']).ljust(100))
        outStr += "\n%s: %s" % ('SRM PinExpiryTime'.ljust(8), str(info['PinExpiryTime']).ljust(100))
        outStr += "\n%s: %s sec" % ('SRM PinLength'.ljust(8), str(info['PinLength']).ljust(100))
    else:
      outStr += '\nThere are no staging requests submitted to the site yet.'.ljust(8)
  else:
    outStr = "\nThere is no such file requested for staging. Check for typo's!"
    # Script.showHelp()
  gLogger.notice(outStr)

  DIRACExit(0)


if __name__ == "__main__":
  main()


''' Example:
dirac-stager-monitor-file.py /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst GRIDKA-RDST
--------------------
LFN     : /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst
SE      : GRIDKA-RDST
PFN     : srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst
Status  : StageSubmitted
LastUpdate: 2013-06-11 18:13:40
Reason  : None
Jobs requesting this file to be staged: 48518896
------SRM staging request info--------------
SRM RequestID: -1768636375
SRM StageStatus: StageSubmitted
SRM StageRequestSubmitTime: 2013-06-11 18:13:38
SRM StageRequestCompletedTime: None
SRM PinExpiryTime: None
SRM PinLength: 43200
'''  # NOQA
