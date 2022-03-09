#!/usr/bin/env python
"""
Show details of currently active Task Queues

Example:
  $ dirac-admin-show-task-queues
  Getting TQs..
  * TQ 401
          CPUTime: 360
             Jobs: 3
          OwnerDN: /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
       OwnerGroup: dirac_user
         Priority: 1.0
            Setup: Dirac-Production
"""

import sys

from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities.PrettyPrint import printTable

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.Core.Base.Script import Script

verbose = False


def setVerbose(optVal):
    global verbose
    verbose = True
    return S_OK()


taskQueueID = 0


def setTaskQueueID(optVal):
    global taskQueueID
    taskQueueID = int(optVal)
    return S_OK()


@Script()
def main():
    global verbose
    global taskQueueID
    Script.registerSwitch("v", "verbose", "give max details about task queues", setVerbose)
    Script.registerSwitch("t:", "taskQueue=", "show this task queue only", setTaskQueueID)
    Script.parseCommandLine(initializeMonitor=False)

    result = MatcherClient().getActiveTaskQueues()
    if not result["OK"]:
        gLogger.error(result["Message"])
        sys.exit(1)

    tqDict = result["Value"]

    if not verbose:
        fields = [
            "TaskQueue",
            "Jobs",
            "CPUTime",
            "Owner",
            "OwnerGroup",
            "Sites",
            "Platforms",
            "Priority",
        ]
        records = []

        for tqId in sorted(tqDict):
            if taskQueueID and tqId != taskQueueID:
                continue
            record = [str(tqId)]
            tqData = tqDict[tqId]
            for key in fields[1:]:
                if key == "Owner":
                    value = tqData.get("OwnerDN", "-")
                    if value != "-":
                        result = getUsernameForDN(value)
                        if not result["OK"]:
                            value = "Unknown"
                        else:
                            value = result["Value"]
                else:
                    value = tqData.get(key, "-")
                if isinstance(value, list):
                    if len(value) > 1:
                        record.append(str(value[0]) + "...")
                    else:
                        record.append(str(value[0]))
                else:
                    record.append(str(value))
            records.append(record)

        printTable(fields, records)
    else:
        fields = ["Key", "Value"]
        for tqId in sorted(tqDict):
            if taskQueueID and tqId != taskQueueID:
                continue
            gLogger.notice("\n==> TQ %s" % tqId)
            records = []
            tqData = tqDict[tqId]
            for key in sorted(tqData):
                value = tqData[key]
                if isinstance(value, list):
                    records.append([key, {"Value": value, "Just": "L"}])
                else:
                    value = str(value)
                    records.append([key, {"Value": value, "Just": "L"}])

            printTable(fields, records, numbering=False)


if __name__ == "__main__":
    main()
