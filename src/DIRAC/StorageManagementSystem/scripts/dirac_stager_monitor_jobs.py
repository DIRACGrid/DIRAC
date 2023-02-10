#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-jobs
# Author :  Daniela Remenska
########################################################################
"""
Report the summary of the staging progress of jobs

Example:
  $ dirac-stager-monitor-jobs.py 5688643 5688644

  JobID               : 5688643
  Status              : Offline
  SubmitTime          : 2013-06-10 15:21:03
  CompleteTime        : None
  Staging files for this job:
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00003705_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00003705_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00001918_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00001918_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00002347_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00002347_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00003701_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00003701_1.sdst
      Status  : Offline
      Reason  : None
  ----------------------
  JobID               : 5688644
  Status              : Offline
  SubmitTime          : 2013-06-10 15:21:07
  CompleteTime        : None
  Staging files for this job:
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00005873_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00005873_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00004468_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00004468_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00000309_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00000309_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00005911_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00005911_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
      LFN     : /lhcb/LHCb/Collision10/SDST/01/0000/01_00003296_1.sdst
      SE      : IN2P3-RDST
      PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/01/0000/01_00003296_1.sdst
      Status  : Offline
      Reason  : None
      --------------------
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    Script.parseCommandLine(ignoreErrors=False)

    args = Script.getPositionalArgs()

    if len(args) < 1:
        Script.showHelp()

    from DIRAC import exit as DIRACExit, gLogger

    try:
        jobIDs = [int(arg) for arg in args]
    except Exception:
        gLogger.fatal("DIRAC Job IDs must be integers")
        DIRACExit(2)

    from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

    client = StorageManagerClient()

    outStr = "\n"
    for jobID in jobIDs:
        res = client.getTaskSummary(jobID)
        if not res["OK"]:
            gLogger.error(res["Message"])
            continue
        if not res["Value"]:
            gLogger.notice(f"No info for job {jobID}, probably gone from the stager...")
            continue
        taskInfo = res["Value"]["TaskInfo"]
        replicaInfo = res["Value"]["ReplicaInfo"]
        outStr = f"{'JobID'.ljust(20)}: {jobID}"
        outStr += f"\n{'Status'.ljust(20)}: {taskInfo[str(jobID)]['Status']}"
        outStr += f"\n{'SubmitTime'.ljust(20)}: {taskInfo[str(jobID)]['SubmitTime']}"
        outStr += f"\n{'CompleteTime'.ljust(20)}: {taskInfo[str(jobID)]['CompleteTime']}"
        outStr += "\nStaging files for this job:"
        if not res["Value"]["ReplicaInfo"]:
            gLogger.notice(f"No info on files for the job = {jobID}, that is odd")
            continue
        else:
            for lfn, metadata in replicaInfo.items():
                outStr += "\n\t--------------------"
                outStr += f"\n\t{'LFN'.ljust(8)}: {lfn.ljust(100)}"
                outStr += f"\n\t{'SE'.ljust(8)}: {metadata['StorageElement'].ljust(100)}"
                outStr += f"\n\t{'PFN'.ljust(8)}: {str(metadata['PFN']).ljust(100)}"
                outStr += f"\n\t{'Status'.ljust(8)}: {metadata['Status'].ljust(100)}"
                outStr += f"\n\t{'Reason'.ljust(8)}: {str(metadata['Reason']).ljust(100)}"
                outStr += f"\n{'LastUpdate'.ljust(8)}: {str(metadata['LastUpdate']).ljust(100)}"
            outStr += "\n----------------------"
        gLogger.notice(outStr)
    DIRACExit(0)


if __name__ == "__main__":
    main()
