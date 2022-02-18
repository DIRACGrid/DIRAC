#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-normalized-queue-length.py
# Author :  Ricardo Graciani
########################################################################
"""
Report Normalized CPU length of queue

This script was used by the dirac-pilot script to set the CPUTime limit for
the matching but now this is no more the case.

Example:
  $ dirac-wms-get-normalized-queue-length  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long
  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long 857400.0
"""
import DIRAC
from DIRAC.Core.Base.Script import Script
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import queueNormalizedCPU


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["Queue: GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    exitCode = 0

    for ceUniqueID in args:

        normCPU = queueNormalizedCPU(ceUniqueID)

        if not normCPU["OK"]:
            print("ERROR %s:" % ceUniqueID, normCPU["Message"])
            exitCode = 2
            continue
        print(ceUniqueID, normCPU["Value"])

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
