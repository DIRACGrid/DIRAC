#! /usr/bin/env python
########################################################################
# File :    dirac-stager-monitor-requests
# Author :  Daniela Remenska
########################################################################
"""
Report the details of file staging requests, based on selection filters

WARNING: Query may be heavy, please use --limit switch!

Example:
  $ dirac-stager-show-requests.py --status=Staged --se=GRIDKA-RDST --limit=10 --showJobs=YES
  Query limited to 10 entries

  Status          LastUpdate           LFN              SE       Reason     Jobs       PinExpiryTime        PinLength
  Staged   2013-06-05 20:10:50 /lhcb/LHCb/5.full.dst GRIDKA-RDST None    ['48498752']  2013-06-05 22:10:50  86400
  Staged   2013-06-06 15:54:29 /lhcb/LHCb/1.full.dst GRIDKA-RDST None    ['48516851']  2013-06-06 16:54:29  43200
  Staged   2013-06-07 02:35:41 /lhcb/LHCb/3.full.dst GRIDKA-RDST None    ['48520736']  2013-06-07 03:35:41  43200
  Staged   2013-06-06 04:16:50 /lhcb/LHCb/3.full.dst GRIDKA-RDST None    ['48510852']  2013-06-06 06:16:50  86400
  Staged   2013-06-07 03:44:04 /lhcb/LHCb/3.full.dst GRIDKA-RDST None    ['48520737']  2013-06-07 04:44:04  43200
  Staged   2013-06-05 23:37:46 /lhcb/LHCb/3.full.dst GRIDKA-RDST None    ['48508687']  2013-06-06 01:37:46  86400
  Staged   2013-06-10 08:50:09 /lhcb/LHCb/5.full.dst GRIDKA-RDST None    ['48518896']  2013-06-10 09:50:09  43200
  Staged   2013-06-06 11:03:25 /lhcb/LHCb/2.full.dst GRIDKA-RDST None    ['48515583']  2013-06-06 12:03:25  43200
  Staged   2013-06-06 11:11:50 /lhcb/LHCb/2.full.dst GRIDKA-RDST None    ['48515072']  2013-06-06 12:11:50  43200
  Staged   2013-06-07 03:19:26 /lhcb/LHCb/2.full.dst GRIDKA-RDST None    ['48515600']  2013-06-07 04:19:26  43200
"""
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger, exit as DIRACExit

subLogger = None


@Script()
def main():
    def registerSwitches():
        """
        Registers all switches that can be used while calling the script from the
        command line interface.
        """

        switches = (
            (
                "status=",
                "Filter per file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged)."
                "\n                                 If not used, all status values will be taken into account",
            ),
            ("se=", "Filter per Storage Element. If not used, all storage elements will be taken into account."),
            ("limit=", "Limit the number of entries returned."),
            ("showJobs=", "Whether to ALSO list the jobs asking for these files to be staged"),
        )

        for switch in switches:
            Script.registerSwitch("", switch[0], switch[1])

    def parseSwitches():
        """
        Parses the arguments passed by the user
        """

        Script.parseCommandLine(ignoreErrors=True)
        args = Script.getPositionalArgs()
        if args:
            subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
            subLogger.error("Please, check documentation below")
            Script.showHelp(exitCode=1)

        switches = dict(Script.getUnprocessedSwitches())

        for key in ("status", "se", "limit"):
            if key not in switches:
                subLogger.warn("You're not using switch --%s, query may take long!" % key)

        if "status" in switches and switches["status"] not in (
            "New",
            "Offline",
            "Waiting",
            "Failed",
            "StageSubmitted",
            "Staged",
        ):
            subLogger.error('Found "%s" as Status value. Incorrect value used!' % switches["status"])
            subLogger.error("Please, check documentation below")
            Script.showHelp(exitCode=1)

        subLogger.debug("The switches used are:")
        map(subLogger.debug, switches.items())

        return switches

    # ...............................................................................

    def run():
        global subLogger

        from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

        client = StorageManagerClient()
        queryDict = {}

        if "status" in switchDict:
            queryDict["Status"] = str(switchDict["status"])

        if "se" in switchDict:
            queryDict["SE"] = str(switchDict["se"])

        # weird: if there are no switches (dictionary is empty), then the --limit is ignored!!
        # must FIX that in StorageManagementDB.py!
        # ugly fix:
        newer = "1903-08-02 06:24:38"  # select newer than
        if "limit" in switchDict:
            gLogger.notice("Query limited to %s entries" % switchDict["limit"])
            res = client.getCacheReplicas(queryDict, None, newer, None, None, int(switchDict["limit"]))
        else:
            res = client.getCacheReplicas(queryDict)

        if not res["OK"]:
            gLogger.error(res["Message"])
        outStr = "\n"
        if res["Records"]:
            replicas = res["Value"]
            outStr += " %s" % ("Status".ljust(15))
            outStr += " %s" % ("LastUpdate".ljust(20))
            outStr += " %s" % ("LFN".ljust(80))
            outStr += " %s" % ("SE".ljust(10))
            outStr += " %s" % ("Reason".ljust(10))
            if "showJobs" in switchDict:
                outStr += " %s" % ("Jobs".ljust(10))
            outStr += " %s" % ("PinExpiryTime".ljust(15))
            outStr += " %s" % ("PinLength(sec)".ljust(15))
            outStr += "\n"

            for crid, info in replicas.items():
                outStr += " %s" % (info["Status"].ljust(15))
                outStr += " %s" % (str(info["LastUpdate"]).ljust(20))
                outStr += " %s" % (info["LFN"].ljust(30))
                outStr += " %s" % (info["SE"].ljust(15))
                outStr += " %s" % (str(info["Reason"]).ljust(10))

                # Task info
                if "showJobs" in switchDict:
                    resTasks = client.getTasks({"ReplicaID": crid})
                    if resTasks["OK"]:
                        if resTasks["Value"]:
                            tasks = resTasks["Value"]
                            jobs = []
                            for tid in tasks:
                                jobs.append(tasks[tid]["SourceTaskID"])
                            outStr += " %s " % (str(jobs).ljust(10))
                    else:
                        outStr += " %s " % (" --- ".ljust(10))
                # Stage request info
                # what if there's no request to the site yet?
                resStageRequests = client.getStageRequests({"ReplicaID": crid})
                if not resStageRequests["OK"]:
                    gLogger.error(resStageRequests["Message"])
                if resStageRequests["Records"]:
                    stageRequests = resStageRequests["Value"]
                    for info in stageRequests.values():
                        outStr += " %s" % (str(info["PinExpiryTime"]).ljust(20))
                        outStr += " %s" % (str(info["PinLength"]).ljust(10))
                outStr += "\n"

            gLogger.notice(outStr)
        else:
            gLogger.notice("No entries")

    subLogger = gLogger.getSubLogger(__file__)

    registerSwitches()

    switchDict = parseSwitches()
    run()

    # Bye
    DIRACExit(0)


if __name__ == "__main__":
    main()
