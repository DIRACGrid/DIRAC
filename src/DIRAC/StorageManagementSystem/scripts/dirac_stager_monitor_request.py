#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-request
# Author :  Andrew C. Smith
########################################################################
"""
Report the summary of the stage task from the DB.
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("Request:  ID of the Stage request in the StorageManager")
    Script.parseCommandLine(ignoreErrors=False)

    args = Script.getPositionalArgs()

    if not len(args) == 1:
        Script.showHelp()

    from DIRAC import exit as DIRACExit, gLogger

    try:
        taskID = int(args[0])
    except Exception:
        gLogger.fatal("Stage requestID must be an integer")
        DIRACExit(2)

    from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

    client = StorageManagerClient()

    res = client.getTaskSummary(taskID)
    if not res["OK"]:
        gLogger.error(res["Message"])
        DIRACExit(2)
    taskInfo = res["Value"]["TaskInfo"]
    replicaInfo = res["Value"]["ReplicaInfo"]
    outStr = "{}: {}".format("TaskID".ljust(20), taskID)
    outStr += "\n{}: {}".format("Status".ljust(20), taskInfo[taskID]["Status"])
    outStr += "\n{}: {}".format("Source".ljust(20), taskInfo[taskID]["Source"])
    outStr += "\n{}: {}".format("SourceTaskID".ljust(20), taskInfo[taskID]["SourceTaskID"])
    outStr += "\n{}: {}".format("CallBackMethod".ljust(20), taskInfo[taskID]["CallBackMethod"])
    outStr += "\n{}: {}".format("SubmitTime".ljust(20), taskInfo[taskID]["SubmitTime"])
    outStr += "\n{}: {}".format("CompleteTime".ljust(20), taskInfo[taskID]["CompleteTime"])
    for lfn, metadata in replicaInfo.items():
        outStr += "\n"
        outStr += "\n\t{}: {}".format("LFN".ljust(8), lfn.ljust(100))
        outStr += "\n\t{}: {}".format("SE".ljust(8), metadata["StorageElement"].ljust(100))
        outStr += "\n\t{}: {}".format("PFN".ljust(8), str(metadata["PFN"]).ljust(100))
        outStr += "\n\t{}: {}".format("Size".ljust(8), str(metadata["FileSize"]).ljust(100))
        outStr += "\n\t{}: {}".format("Status".ljust(8), metadata["Status"].ljust(100))
        outStr += "\n\t{}: {}".format("Reason".ljust(8), str(metadata["Reason"]).ljust(100))
    gLogger.notice(outStr)


if __name__ == "__main__":
    main()
