#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-wn
# Author :  Philippe Charpentier
########################################################################
"""
Get WNs for a selection of jobs
"""
import datetime
from functools import cmp_to_key

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    site = "BOINC.World.org"
    status = ["Running"]
    minorStatus = None
    workerNodes = None
    since = None
    date = "today"
    full = False
    until = None
    batchIDs = None
    Script.registerSwitch("", "Site=", f"   Select site (default: {site})")
    Script.registerSwitch("", "Status=", f"   Select status (default: {status})")
    Script.registerSwitch("", "MinorStatus=", "   Select minor status")
    Script.registerSwitch("", "WorkerNode=", "  Select WN")
    Script.registerSwitch("", "BatchID=", "  Select batch jobID")
    Script.registerSwitch("", "Since=", "   Date since when to select jobs, or number of days (default: today)")
    Script.registerSwitch("", "Date=", "   Specify the date (check for a full day)")
    Script.registerSwitch("", "Full", "   Printout full list of job (default: False except if --WorkerNode)")

    Script.parseCommandLine()
    from DIRAC import gLogger
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

    switches = Script.getUnprocessedSwitches()
    for switch in switches:
        if switch[0] == "Site":
            site = switch[1]
        elif switch[0] == "MinorStatus":
            minorStatus = switch[1]
        elif switch[0] == "Status":
            if switch[1].lower() == "all":
                status = [None]
            else:
                status = switch[1].split(",")
        elif switch[0] == "WorkerNode":
            workerNodes = switch[1].split(",")
        elif switch[0] == "BatchID":
            try:
                batchIDs = [int(id) for id in switch[1].split(",")]
            except Exception:
                gLogger.error("Invalid jobID", switch[1])
                DIRAC.exit(1)
        elif switch[0] == "Full":
            full = True
        elif switch[0] == "Date":
            since = switch[1].split()[0]
            until = str(datetime.datetime.strptime(since, "%Y-%m-%d") + datetime.timedelta(days=1)).split()[0]
        elif switch[0] == "Since":
            date = switch[1].lower()
            if date == "today":
                since = None
            elif date == "yesterday":
                since = 1
            elif date == "ever":
                since = 2 * 365
            elif date.isdigit():
                since = int(date)
                date += " days"
            else:
                since = date
            if isinstance(since, int):
                since = str(datetime.datetime.now() - datetime.timedelta(days=since)).split()[0]

    if workerNodes or batchIDs:
        # status = [None]
        full = True

    monitoring = JobMonitoringClient()
    dirac = Dirac()

    # Get jobs according to selection
    jobs = set()
    for stat in status:
        res = dirac.selectJobs(site=site, date=since, status=stat, minorStatus=minorStatus)
        if not res["OK"]:
            gLogger.error("Error selecting jobs", res["Message"])
            DIRAC.exit(1)
        allJobs = {int(job) for job in res["Value"]}
        if until:
            res = dirac.selectJobs(site=site, date=until, status=stat)
            if not res["OK"]:
                gLogger.error("Error selecting jobs", res["Message"])
                DIRAC.exit(1)
            allJobs -= {int(job) for job in res["Value"]}
        jobs.update(allJobs)
    if not jobs:
        gLogger.always("No jobs found...")
        DIRAC.exit(0)
    # res = monitoring.getJobsSummary( jobs )
    # print eval( res['Value'] )[jobs[0]]

    allJobs = set()
    result = {}
    wnJobs = {}
    gLogger.always(f"{len(jobs)} jobs found")
    # Get host name
    for job in jobs:
        res = monitoring.getJobParameter(job, "HostName")
        node = res.get("Value", {}).get("HostName", "Unknown")
        res = monitoring.getJobParameter(job, "LocalJobID")
        batchID = res.get("Value", {}).get("LocalJobID", "Unknown")
        if workerNodes:
            if not [wn for wn in workerNodes if node.startswith(wn)]:
                continue
            allJobs.add(job)
        if batchIDs:
            if batchID not in batchIDs:
                continue
            allJobs.add(job)
        if full or status == [None]:
            allJobs.add(job)
        result.setdefault(job, {})["Status"] = status
        result[job]["Node"] = node
        result[job]["LocalJobID"] = batchID
        wnJobs[node] = wnJobs.setdefault(node, 0) + 1

    # If necessary get jobs' status
    statusCounters = {}
    if allJobs:
        allJobs = sorted(allJobs, reverse=True)
        res = monitoring.getJobsStates(allJobs)
        if not res["OK"]:
            gLogger.error("Error getting job parameter", res["Message"])
        else:
            jobStates = res["Value"]
            for job in allJobs:
                stat = (
                    jobStates.get(job, {}).get("Status", "Unknown")
                    + "; "
                    + jobStates.get(job, {}).get("MinorStatus", "Unknown")
                    + "; "
                    + jobStates.get(job, {}).get("ApplicationStatus", "Unknown")
                )
                result[job]["Status"] = stat
                statusCounters[stat] = statusCounters.setdefault(stat, 0) + 1
    elif not workerNodes and not batchIDs:
        allJobs = sorted(jobs, reverse=True)

    # Print out result
    if workerNodes or batchIDs:
        gLogger.always(f"Found {len(allJobs)} jobs at {site}, WN {workerNodes} (since {date}):")
        if allJobs:
            gLogger.always("List of jobs:", ",".join([str(job) for job in allJobs]))
    else:
        if status == [None]:
            gLogger.always(f"Found {len(allJobs)} jobs at {site} (since {date}):")
            for stat in sorted(statusCounters):
                gLogger.always("%d jobs %s" % (statusCounters[stat], stat))
        else:
            gLogger.always(f"Found {len(allJobs)} jobs {status} at {site} (since {date}):")
        gLogger.always(
            "List of WNs:",
            ",".join(
                [
                    "%s (%d)" % (node, wnJobs[node])
                    for node in sorted(wnJobs, key=cmp_to_key(lambda n1, n2: (wnJobs[n2] - wnJobs[n1])))
                ]
            ),
        )
    if full:
        if workerNodes or batchIDs:
            nodeJobs = {}
            for job in allJobs:
                status = result[job]["Status"]
                node = result[job]["Node"].split(".")[0]
                jobID = result[job].get("LocalJobID")
                nodeJobs.setdefault(node, []).append((jobID, job, status))
            if not workerNodes:
                workerNodes = sorted(nodeJobs)
            for node in workerNodes:
                for job in nodeJobs.get(node.split(".")[0], []):
                    gLogger.always(f"{node} " + "(%s): %s - %s" % job)
        else:
            for job in allJobs:
                status = result[job]["Status"]
                node = result[job]["Node"]
                jobID = result[job].get("LocalJobID")
                gLogger.always(f"{node} ({jobID}): {job} - {status}")


if __name__ == "__main__":
    main()
