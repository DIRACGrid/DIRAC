#!/bin/env python
"""
Show request given its ID, a jobID or a transformation and a task
"""
import datetime
import os
from DIRAC.Core.Base.Script import Script


def convertDate(date):
    """Get the date of 24 hours ago"""
    try:
        value = datetime.datetime.strptime(date, "%Y-%m-%d")
        return value
    except Exception:
        pass
    try:
        value = datetime.datetime.utcnow() - datetime.timedelta(hours=int(24 * float(date)))
    except Exception:
        from DIRAC import gLogger

        gLogger.fatal("Invalid date", date)
        value = None
    return value


@Script()
def main():
    """
    Main executive code
    """
    Script.registerSwitch("", "Job=", "   JobID[,jobID2,...]")
    Script.registerSwitch("", "Transformation=", "   transformation ID")
    Script.registerSwitch("", "Tasks=", "      Associated to --Transformation, list of taskIDs")
    Script.registerSwitch("", "Verbose", "   Print more information")
    Script.registerSwitch("", "Terse", "   Only print request status")
    Script.registerSwitch("", "Full", "   Print full request content")
    Script.registerSwitch("", "Status=", "   Select all requests in a given status")
    Script.registerSwitch(
        "", "Since=", "      Associated to --Status, start date yyyy-mm-dd or nb of days (default= -one day"
    )
    Script.registerSwitch("", "Until=", "      Associated to --Status, end date (default= now")
    Script.registerSwitch("", "Maximum=", "      Associated to --Status, max number of requests ")
    Script.registerSwitch("", "Reset", "   Reset Failed files to Waiting if any")
    Script.registerSwitch("", "Force", "   Force reset even if not Failed")
    Script.registerSwitch(
        "", "All", "      (if --Status Failed) all requests, otherwise exclude irrecoverable failures"
    )
    Script.registerSwitch("", "FixJob", "   Set job Done if the request is Done")
    Script.registerSwitch("", "Cancel", "   Cancel the request")
    Script.registerSwitch("", "ListJobs", " List the corresponding jobs")
    Script.registerSwitch("", "TargetSE=", " Select request only if that SE is in the targetSEs")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        (
            "file:     a file containing a list of requests (Comma-separated on each line)",
            "request:  a request ID or a unique request name",
        ),
        mandatory=False,
    )
    Script.registerArgument(["request:  a request ID or a unique request name"], mandatory=False)
    Script.parseCommandLine()

    import DIRAC
    from DIRAC import gLogger

    jobs = []
    requestID = 0
    transID = None
    taskIDs = None
    tasks = None
    requests = []
    full = False
    verbose = False
    status = None
    until = None
    since = None
    terse = False
    allR = False
    reset = False
    fixJob = False
    maxRequests = 999999999999
    cancel = False
    listJobs = False
    force = False
    targetSE = set()
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "Job":
            jobs = []
            job = "Unknown"
            try:
                for arg in switch[1].split(","):
                    if os.path.exists(arg):
                        with open(arg) as fp:
                            lines = fp.readlines()
                        for line in lines:
                            for job in line.split(","):
                                jobs += [int(job.strip())]
                        gLogger.notice(f"Found {len(jobs)} jobs in file {arg}")
                    else:
                        jobs.append(int(arg))
            except TypeError:
                gLogger.fatal("Invalid jobID", job)
        elif switch[0] == "Transformation":
            try:
                transID = int(switch[1])
            except Exception:
                gLogger.fatal("Invalid transID", switch[1])
        elif switch[0] == "Tasks":
            try:
                taskIDs = [int(task) for task in switch[1].split(",")]
            except Exception:
                gLogger.fatal("Invalid tasks", switch[1])
        elif switch[0] == "Full":
            full = True
        elif switch[0] == "Verbose":
            verbose = True
        elif switch[0] == "Terse":
            terse = True
        elif switch[0] == "All":
            allR = True
        elif switch[0] == "Reset":
            reset = True
        elif switch[0] == "Force":
            force = True
        elif switch[0] == "Status":
            status = switch[1].capitalize()
        elif switch[0] == "Since":
            since = convertDate(switch[1])
        elif switch[0] == "Until":
            until = convertDate(switch[1])
        elif switch[0] == "FixJob":
            fixJob = True
        elif switch[0] == "Cancel":
            cancel = True
        elif switch[0] == "ListJobs":
            listJobs = True
        elif switch[0] == "Maximum":
            try:
                maxRequests = int(switch[1])
            except Exception:
                pass
        elif switch[0] == "TargetSE":
            targetSE = set(switch[1].split(","))

    if reset and not force:
        status = "Failed"
    if fixJob:
        status = "Done"
    if terse:
        verbose = True
    if status:
        if not until:
            until = datetime.datetime.utcnow()
        if not since:
            since = until - datetime.timedelta(hours=24)
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest, recoverableRequest

    reqClient = ReqClient()
    if transID:
        if not taskIDs:
            gLogger.fatal("If Transformation is set, a list of Tasks should also be set")
            Script.showHelp(exitCode=2)
        # In principle, the task name is unique, so the request name should be unique as well
        # If ever this would not work anymore, we would need to use the transformationClient
        # to fetch the ExternalID
        requests = ["%08d_%08d" % (transID, task) for task in taskIDs]
        allR = True

    elif not jobs:
        requests = []
        # Get full list of arguments, with and without comma
        for arg in [x.strip() for ar in Script.getPositionalArgs() for x in ar.split(",")]:
            if os.path.exists(arg):
                lines = open(arg).readlines()
                requests += [reqID.strip() for line in lines for reqID in line.split(",")]
                gLogger.notice(f"Found {len(requests)} requests in file")
            else:
                requests.append(arg)
            allR = True
    else:
        res = reqClient.getRequestIDsForJobs(jobs)
        if not res["OK"]:
            gLogger.fatal("Error getting request for jobs", res["Message"])
            DIRAC.exit(2)
        if res["Value"]["Failed"]:
            gLogger.error(f"No request found for jobs {','.join(sorted(str(job) for job in res['Value']['Failed']))}")
        requests = sorted(res["Value"]["Successful"].values())
        if requests:
            allR = True
        else:
            DIRAC.exit(0)

    if status and not requests:
        allR = allR or status != "Failed"
        res = reqClient.getRequestIDsList([status], limit=maxRequests, since=since, until=until)

        if not res["OK"]:
            gLogger.error("Error getting requests:", res["Message"])
            DIRAC.exit(2)
        requests = [reqID for reqID, _st, updTime in res["Value"] if updTime > since and updTime <= until and reqID]
        gLogger.notice(f"Obtained {len(requests)} requests {status} between {since} and {until}")
    if not requests:
        gLogger.notice("No request selected....")
        Script.showHelp(exitCode=2)
    okRequests = []
    jobIDList = []
    for reqID in requests:
        # We allow reqID to be the requestName if it is unique
        try:
            # PEP-515 allows for underscore in numerical literals
            # So a request name 00123_00456
            # is interpreted as a requestID 12300456
            # Using an exception here for non-string is not an option
            if isinstance(reqID, str) and not reqID.isdigit():
                raise ValueError()

            requestID = int(reqID)
        except (ValueError, TypeError):
            requestID = reqClient.getRequestIDForName(reqID)
            if not requestID["OK"]:
                gLogger.notice(requestID["Message"])
                continue
            requestID = requestID["Value"]

        request = reqClient.peekRequest(requestID)
        if not request["OK"]:
            gLogger.error(request["Message"])
            DIRAC.exit(-1)

        request = request["Value"]
        if not request:
            gLogger.error(f"no such request {requestID}")
            continue
        # If no operation as the targetSE, skip
        if targetSE:
            found = False
            for op in request:
                if op.TargetSE and targetSE.intersection(op.TargetSE.split(",")):
                    found = True
                    break
            if not found:
                continue
        # keep a list of jobIDs if requested
        if request.JobID and listJobs:
            jobIDList.append(request.JobID)

        if status and request.Status != status:
            gLogger.notice(
                "Request {} is not in requested status {}{}".format(
                    reqID, status, " (cannot be reset)" if reset else ""
                )
            )
            continue

        if fixJob and request.Status == "Done" and request.JobID:
            # The request is for a job and is Done, verify that the job is in the proper status
            result = reqClient.finalizeRequest(request.RequestID, request.JobID, useCertificates=False)
            if not result["OK"]:
                gLogger.error("Error finalizing job", result["Message"])
            else:
                gLogger.notice("Job %d updated to %s" % (request.JobID, result["Value"]))
            continue

        if cancel:
            if request.Status not in ("Done", "Failed"):
                ret = reqClient.cancelRequest(requestID)
                if not ret["OK"]:
                    gLogger.error(f"Error canceling request {reqID}", ret["Message"])
                else:
                    gLogger.notice(f"Request {reqID} cancelled")
            else:
                gLogger.notice(f"Request {reqID} is in status {request.Status}, not cancelled")

        elif allR or recoverableRequest(request):
            okRequests.append(str(requestID))
            if reset:
                gLogger.notice(f"============ Request {requestID} =============")
                ret = reqClient.resetFailedRequest(requestID, allR=allR)
                if not ret["OK"]:
                    gLogger.error(f"Error resetting request {requestID}", ret["Message"])
            else:
                if len(requests) > 1:
                    gLogger.notice("\n===================================")
                dbStatus = reqClient.getRequestStatus(requestID).get("Value", "Unknown")
                printRequest(request, status=dbStatus, full=full, verbose=verbose, terse=terse)

    if listJobs:
        gLogger.notice(f"List of {len(jobIDList)} jobs:\n", ",".join(str(jobID) for jobID in jobIDList))

    if status and okRequests:
        from DIRAC.Core.Utilities.List import breakListIntoChunks

        gLogger.notice(f"\nList of {len(okRequests)} selected requests:")
        for reqs in breakListIntoChunks(okRequests, 100):
            gLogger.notice(",".join(reqs))


if __name__ == "__main__":
    main()
