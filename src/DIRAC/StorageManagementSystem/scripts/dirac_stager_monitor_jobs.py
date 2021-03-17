#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-jobs
# Author :  Daniela Remenska
########################################################################
"""
Report the summary of the staging progress of jobs

Usage:
  dirac-stager-monitor-jobs jobID [jobID] [jobID] ...

Arguments:
  JobID: DIRAC job ID
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

  if len(args) < 1:
    Script.showHelp()

  from DIRAC import exit as DIRACExit, gLogger

  try:
    jobIDs = [int(arg) for arg in args]
  except Exception:
    gLogger.fatal('DIRAC Job IDs must be integers')
    DIRACExit(2)

  from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
  client = StorageManagerClient()

  outStr = "\n"
  for jobID in jobIDs:
    res = client.getTaskSummary(jobID)
    if not res['OK']:
      gLogger.error(res['Message'])
      continue
    if not res['Value']:
      gLogger.notice('No info for job %s, probably gone from the stager...' % jobID)
      continue
    taskInfo = res['Value']['TaskInfo']
    replicaInfo = res['Value']['ReplicaInfo']
    outStr = "%s: %s" % ('JobID'.ljust(20), jobID)
    outStr += "\n%s: %s" % ('Status'.ljust(20), taskInfo[str(jobID)]['Status'])
    outStr += "\n%s: %s" % ('SubmitTime'.ljust(20), taskInfo[str(jobID)]['SubmitTime'])
    outStr += "\n%s: %s" % ('CompleteTime'.ljust(20), taskInfo[str(jobID)]['CompleteTime'])
    outStr += "\nStaging files for this job:"
    if not res['Value']['ReplicaInfo']:
      gLogger.notice('No info on files for the job = %s, that is odd' % jobID)
      continue
    else:
      for lfn, metadata in replicaInfo.items():
        outStr += "\n\t--------------------"
        outStr += "\n\t%s: %s" % ('LFN'.ljust(8), lfn.ljust(100))
        outStr += "\n\t%s: %s" % ('SE'.ljust(8), metadata['StorageElement'].ljust(100))
        outStr += "\n\t%s: %s" % ('PFN'.ljust(8), str(metadata['PFN']).ljust(100))
        outStr += "\n\t%s: %s" % ('Status'.ljust(8), metadata['Status'].ljust(100))
        outStr += "\n\t%s: %s" % ('Reason'.ljust(8), str(metadata['Reason']).ljust(100))
        outStr += "\n%s: %s" % ('LastUpdate'.ljust(8), str(metadata['LastUpdate']).ljust(100))
      outStr += "\n----------------------"
    gLogger.notice(outStr)
  DIRACExit(0)


if __name__ == "__main__":
  main()


''' Example:
dirac-stager-monitor-jobs.py 5688643 5688644

JobID               : 5688643
Status              : Offline
SubmitTime          : 2013-06-10 15:21:03
CompleteTime        : None
Staging files for this job:
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003705_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003705_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00001918_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00001918_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00002347_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00002347_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003701_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003701_1.sdst
    Status  : Offline
    Reason  : None
----------------------
JobID               : 5688644
Status              : Offline
SubmitTime          : 2013-06-10 15:21:07
CompleteTime        : None
Staging files for this job:
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00005873_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00005873_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00004468_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00004468_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00000309_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00000309_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00005911_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00005911_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
    LFN     : /lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003296_1.sdst
    SE      : IN2P3-RDST
    PFN     : srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision10/SDST/00010270/0000/00010270_00003296_1.sdst
    Status  : Offline
    Reason  : None
    --------------------
'''  # NOQA
