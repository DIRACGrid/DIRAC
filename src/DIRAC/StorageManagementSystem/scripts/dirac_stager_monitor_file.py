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

Example:
  $ dirac-stager-monitor-file.py /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/0_1.full.dst GRIDKA-RDST
  --------------------
  LFN     : /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/0_1.full.dst
  SE      : GRIDKA-RDST
  PFN     : srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/LHCb/Collision12/FULL.DST/00020846/0005/0_1.full.dst
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
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("LFN: LFN of the staging file")
    Script.registerArgument("SE: Storage Element for the staging file")
    Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit, gLogger

    lfn, se = Script.getPositionalArgs(group=True)

    from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

    client = StorageManagerClient()
    res = client.getCacheReplicas({"LFN": lfn, "SE": se})
    if not res["OK"]:
        gLogger.error(res["Message"])
    cacheReplicaInfo = res["Value"]
    if cacheReplicaInfo:
        replicaID = list(cacheReplicaInfo)[0]
        outStr = "\n--------------------"
        outStr += "\n{}: {}".format("LFN".ljust(8), cacheReplicaInfo[replicaID]["LFN"].ljust(100))
        outStr += "\n{}: {}".format("SE".ljust(8), cacheReplicaInfo[replicaID]["SE"].ljust(100))
        outStr += "\n{}: {}".format("PFN".ljust(8), cacheReplicaInfo[replicaID]["PFN"].ljust(100))
        outStr += "\n{}: {}".format("Status".ljust(8), cacheReplicaInfo[replicaID]["Status"].ljust(100))
        outStr += "\n{}: {}".format("LastUpdate".ljust(8), str(cacheReplicaInfo[replicaID]["LastUpdate"]).ljust(100))
        outStr += "\n{}: {}".format("Reason".ljust(8), str(cacheReplicaInfo[replicaID]["Reason"]).ljust(100))

        resTasks = client.getTasks({"ReplicaID": replicaID})

        if resTasks["OK"]:
            # print resTasks['Message']
            outStr += "\nJob IDs requesting this file to be staged:".ljust(8)
            tasks = resTasks["Value"]
            for tid in tasks.keys():
                outStr += " %s " % (tasks[tid]["SourceTaskID"])

        resStageRequests = client.getStageRequests({"ReplicaID": replicaID})

        if not resStageRequests["OK"]:
            gLogger.error(resStageRequests["Message"])

        if resStageRequests["Records"]:
            stageRequests = resStageRequests["Value"]
            outStr += "\n------SRM staging request info--------------"
            for info in stageRequests.values():
                outStr += "\n{}: {}".format("SRM RequestID".ljust(8), info["RequestID"].ljust(100))
                outStr += "\n{}: {}".format("SRM StageStatus".ljust(8), info["StageStatus"].ljust(100))
                outStr += "\n{}: {}".format(
                    "SRM StageRequestSubmitTime".ljust(8),
                    str(info["StageRequestSubmitTime"]).ljust(100),
                )
                outStr += "\n{}: {}".format(
                    "SRM StageRequestCompletedTime".ljust(8),
                    str(info["StageRequestCompletedTime"]).ljust(100),
                )
                outStr += "\n{}: {}".format("SRM PinExpiryTime".ljust(8), str(info["PinExpiryTime"]).ljust(100))
                outStr += "\n{}: {} sec".format("SRM PinLength".ljust(8), str(info["PinLength"]).ljust(100))
        else:
            outStr += "\nThere are no staging requests submitted to the site yet.".ljust(8)
    else:
        outStr = "\nThere is no such file requested for staging. Check for typo's!"
        # Script.showHelp()
    gLogger.notice(outStr)

    DIRACExit(0)


if __name__ == "__main__":
    main()
