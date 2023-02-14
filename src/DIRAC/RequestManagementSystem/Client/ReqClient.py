"""
:mod:  ReqClient

.. module:  ReqClient
  :synopsis: implementation of client for RequestDB using DISET framework

"""
import os
import time
import random
import json
import datetime

# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient


@createClient("RequestManagement/ReqManager")
class ReqClient(Client):
    """ReqClient is a class manipulating and operation on Requests."""

    __requestProxiesDict = {}
    __requestValidator = None

    def __init__(self, url=None, **kwargs):
        """c'tor

        :param self: self reference
        :param url: url of the ReqManager
        :param kwargs: forwarded to the Base Client class
        """

        super().__init__(**kwargs)
        self.serverURL = "RequestManagement/ReqManager" if not url else url

        self.log = gLogger.getSubLogger(f"RequestManagement/ReqClient/pid_{os.getpid()}")

    def requestProxies(self, timeout=120):
        """get request proxies dict"""
        # Forward all the connection options to the requestClient
        # (e.g. the userDN to use)
        kwargs = self.getClientKWArgs()
        kwargs["timeout"] = timeout

        if not self.__requestProxiesDict:
            self.__requestProxiesDict = {}
            proxiesURLs = fromChar(PathFinder.getServiceURL("RequestManagement/ReqProxyURLs"))
            if not proxiesURLs:
                self.log.warn("CS option RequestManagement/ReqProxyURLs is not set!")
            for proxyURL in proxiesURLs:
                self.log.debug(f"creating RequestProxy for url = {proxyURL}")
                pc = Client(**kwargs)
                pc.setServer(proxyURL)
                self.__requestProxiesDict[proxyURL] = pc

        return self.__requestProxiesDict

    def requestValidator(self):
        """get request validator"""
        if not self.__requestValidator:
            self.__requestValidator = RequestValidator()
        return self.__requestValidator

    def putRequest(self, request, useFailoverProxy=True, retryMainService=0):
        """Put request to RequestManager

        :param self: self reference
        :param ~Request.Request request: Request instance
        :param bool useFailoverProxy: if False, will not attempt to forward the request to ReqProxies
        :param int retryMainService: Amount of time we retry on the main ReqHandler in case of failures

        :return: S_OK/S_ERROR
        """
        errorsDict = {"OK": False}
        valid = self.requestValidator().validate(request)
        if not valid["OK"]:
            self.log.error("putRequest: request not valid", f"{valid['Message']}")
            return valid
        # # dump to json
        requestJSON = request.toJSON()
        if not requestJSON["OK"]:
            return requestJSON
        requestJSON = requestJSON["Value"]

        retryMainService += 1

        while retryMainService:
            retryMainService -= 1
            setRequestMgr = self._getRPC().putRequest(requestJSON)
            if setRequestMgr["OK"]:
                return setRequestMgr
            errorsDict["RequestManager"] = setRequestMgr["Message"]
            # sleep a bit
            time.sleep(random.randint(1, 5))

        self.log.warn(
            f"putRequest: unable to set request '{request.RequestName}' at RequestManager", setRequestMgr["Message"]
        )
        proxies = self.requestProxies() if useFailoverProxy else {}
        for proxyURL in randomize(proxies.keys()):
            proxyClient = proxies[proxyURL]
            self.log.debug(f"putRequest: trying RequestProxy at {proxyURL}")
            setRequestProxy = proxyClient.putRequest(requestJSON)
            if setRequestProxy["OK"]:
                if setRequestProxy["Value"]["set"]:
                    self.log.info(
                        "putRequest: request '%s' successfully set using RequestProxy %s"
                        % (request.RequestName, proxyURL)
                    )
                elif setRequestProxy["Value"]["saved"]:
                    self.log.info(
                        "putRequest: request '%s' successfully forwarded to RequestProxy %s"
                        % (request.RequestName, proxyURL)
                    )
                return setRequestProxy
            else:
                self.log.warn(
                    "putRequest: unable to set request using RequestProxy %s: %s"
                    % (proxyURL, setRequestProxy["Message"])
                )
                errorsDict[f"RequestProxy({proxyURL})"] = setRequestProxy["Message"]
        # # if we're here neither requestManager nor requestProxy were successful
        self.log.error("putRequest: unable to set request", f"'{request.RequestName}'")
        errorsDict["Message"] = f"ReqClient.putRequest: unable to set request '{request.RequestName}'"
        return errorsDict

    def getRequest(self, requestID=0):
        """Get request from RequestDB

        :param self: self reference
        :param int requestID: ID of the request. If 0, choice is made for you

        :return: S_OK( Request instance ) or S_OK() or S_ERROR
        """
        self.log.debug("getRequest: attempting to get request.")
        getRequest = self._getRPC().getRequest(requestID)
        if not getRequest["OK"]:
            self.log.error("getRequest: unable to get request", f"'{requestID}' {getRequest['Message']}")
            return getRequest
        if not getRequest["Value"]:
            return getRequest
        return S_OK(Request(getRequest["Value"]))

    @ignoreEncodeWarning
    def getBulkRequests(self, numberOfRequest=10, assigned=True):
        """get bulk requests from RequestDB

        :param self: self reference
        :param str numberOfRequest: size of the bulk (default 10)

        :return: S_OK( Successful : { requestID, RequestInstance }, Failed : message  ) or S_ERROR
        """
        self.log.debug("getRequests: attempting to get request.")
        getRequests = self._getRPC().getBulkRequests(numberOfRequest, assigned)
        if not getRequests["OK"]:
            self.log.error(f"getRequests: unable to get '{numberOfRequest}' requests: {getRequests['Message']}")
            return getRequests
        # No Request returned
        if not getRequests["Value"]:
            return getRequests
        # No successful Request
        if not getRequests["Value"]["Successful"]:
            return getRequests

        jsonReq = getRequests["Value"]["Successful"]
        # Do not forget to cast back str keys to int
        reqInstances = {int(rId): Request(jsonReq[rId]) for rId in jsonReq}
        failed = strToIntDict(getRequests["Value"]["Failed"])
        return S_OK({"Successful": reqInstances, "Failed": failed})

    def peekRequest(self, requestID):
        """peek request"""
        self.log.debug("peekRequest: attempting to get request.")
        peekRequest = self._getRPC().peekRequest(int(requestID))
        if not peekRequest["OK"]:
            self.log.error("peekRequest: unable to peek request", f"request: '{requestID}' {peekRequest['Message']}")
            return peekRequest
        if not peekRequest["Value"]:
            return peekRequest
        return S_OK(Request(peekRequest["Value"]))

    def deleteRequest(self, requestID):
        """delete request given it's ID

        :param self: self reference
        :param str requestID: request ID
        """
        requestID = int(requestID)
        self.log.debug(f"deleteRequest: attempt to delete '{requestID}' request")
        deleteRequest = self._getRPC().deleteRequest(requestID)
        if not deleteRequest["OK"]:
            self.log.error(
                "deleteRequest: unable to delete request",
                f"'{requestID}' request: {deleteRequest['Message']}",
            )
        return deleteRequest

    def getRequestIDsList(self, statusList=None, limit=None, since=None, until=None, getJobID=False):
        """get at most :limit: request ids with statuses in :statusList:"""
        statusList = statusList if statusList else list(Request.FINAL_STATES)
        limit = limit if limit else 100
        since = since.strftime("%Y-%m-%d") if since else ""
        until = until.strftime("%Y-%m-%d") if until else ""

        return self._getRPC().getRequestIDsList(statusList, limit, since, until, getJobID)

    def getScheduledRequest(self, operationID):
        """get scheduled request given its scheduled OperationID"""
        self.log.debug("getScheduledRequest: attempt to get scheduled request...")
        scheduled = self._getRPC().getScheduledRequest(operationID)
        if not scheduled["OK"]:
            self.log.error("getScheduledRequest failed", scheduled["Message"])
            return scheduled
        if scheduled["Value"]:
            return S_OK(Request(scheduled["Value"]))
        return scheduled

    def getDBSummary(self):
        """Get the summary of requests in the RequestDBs."""
        self.log.debug("getDBSummary: attempting to get RequestDB summary.")
        dbSummary = self._getRPC().getDBSummary()
        if not dbSummary["OK"]:
            self.log.error("getDBSummary: unable to get RequestDB summary", dbSummary["Message"])
        return dbSummary

    def getDigest(self, requestID):
        """Get the request digest given a request ID.

        :param self: self reference
        :param str requestID: request id
        """
        self.log.debug(f"getDigest: attempting to get digest for '{requestID}' request.")
        digest = self._getRPC().getDigest(int(requestID))
        if not digest["OK"]:
            self.log.error("getDigest: unable to get digest for request", f"request: '{requestID}' {digest['Message']}")

        return digest

    def getRequestStatus(self, requestID):
        """Get the request status given a request id.

        :param self: self reference
        :param int requestID: id of the request
        """
        if isinstance(requestID, str):
            requestID = int(requestID)
        self.log.debug("getRequestStatus: attempting to get status for '%d' request." % requestID)
        requestStatus = self._getRPC().getRequestStatus(requestID)
        if not requestStatus["OK"]:
            self.log.error(
                "getRequestStatus: unable to get status for request",
                ": '%d' %s" % (requestID, requestStatus["Message"]),
            )
        return requestStatus

    #   def getRequestName( self, requestID ):
    #     """ get request name for a given requestID """
    #     return self._getRPC().getRequestName( requestID )

    def getRequestInfo(self, requestID):
        """The the request info given a request id.

        :param self: self reference
        :param int requestID: request nid
        """
        self.log.debug(f"getRequestInfo: attempting to get info for '{requestID}' request.")
        requestInfo = self._getRPC().getRequestInfo(int(requestID))
        if not requestInfo["OK"]:
            self.log.error(
                "getRequestInfo: unable to get status for request",
                f"request: '{requestID}' {requestInfo['Message']}",
            )
        return requestInfo

    def getRequestFileStatus(self, requestID, lfns):
        """Get file status for request given a request id.

        :param self: self reference
        :param int requestID: request id
        :param lfns: list of LFNs
        :type lfns: python:list
        """
        self.log.debug(f"getRequestFileStatus: attempting to get file statuses for '{requestID}' request.")
        fileStatus = self._getRPC().getRequestFileStatus(int(requestID), lfns)
        if not fileStatus["OK"]:
            self.log.verbose(
                "getRequestFileStatus: unable to get file status for request",
                f"request: '{requestID}' {fileStatus['Message']}",
            )
        return fileStatus

    def finalizeRequest(self, requestID, jobID, useCertificates=True):
        """check request status and perform finalization if necessary
            update the request status and the corresponding job parameter

        :param self: self reference
        :param str requestID: request id
        :param int jobID: job id
        """

        stateServer = JobStateUpdateClient(useCertificates=useCertificates)

        # Checking if to update the job status - we should fail here, so it will be re-tried later
        # Checking the state, first
        res = self.getRequestStatus(requestID)
        if not res["OK"]:
            self.log.error("finalizeRequest: failed to get request", f"request: {requestID} status: {res['Message']}")
            return res
        if res["Value"] != "Done":
            return S_ERROR(
                "The request %s isn't 'Done' but '%s', this should never happen, why are we here?"
                % (requestID, res["Value"])
            )

        # The request is 'Done', let's update the job status. If we fail, we should re-try later

        monitorServer = JobMonitoringClient(useCertificates=useCertificates)
        res = monitorServer.getJobSummary(int(jobID))
        if not res["OK"]:
            self.log.error("finalizeRequest: Failed to get job status", "JobID: %d" % jobID)
            return res
        elif not res["Value"]:
            self.log.info("finalizeRequest: job %d does not exist (anymore): finalizing" % jobID)
            return S_OK()
        else:
            jobStatus = res["Value"]["Status"]
            jobMinorStatus = res["Value"]["MinorStatus"]
            jobAppStatus = ""
            newJobStatus = ""
            if jobStatus == JobStatus.STALLED:
                # If job is stalled, find the previous status from the logging info
                res = monitorServer.getJobLoggingInfo(int(jobID))
                if not res["OK"]:
                    self.log.error("finalizeRequest: Failed to get job logging info", "JobID: %d" % jobID)
                    return res
                # Check the last status was Stalled and get the one before
                if len(res["Value"]) >= 2 and res["Value"][-1][0] == JobStatus.STALLED:
                    jobStatus, jobMinorStatus, jobAppStatus = res["Value"][-2][:3]
                    newJobStatus = jobStatus

            # update the job pending request digest in any case since it is modified
            self.log.info("finalizeRequest: Updating request digest for job %d" % jobID)

            digest = self.getDigest(requestID)
            if digest["OK"]:
                digest = digest["Value"]
                self.log.verbose(digest)
                res = stateServer.setJobParameter(jobID, "PendingRequest", digest)
                if not res["OK"]:
                    self.log.info("finalizeRequest: Failed to set job %d parameter: %s" % (jobID, res["Message"]))
                    return res
            else:
                self.log.error(f"finalizeRequest: Failed to get request digest for {requestID}: {digest['Message']}")
            if jobStatus == JobStatus.COMPLETED:
                # What to do? Depends on what we have in the minorStatus
                if jobMinorStatus == JobMinorStatus.PENDING_REQUESTS:
                    newJobStatus = JobStatus.DONE
                elif jobMinorStatus == JobMinorStatus.APP_ERRORS:
                    newJobStatus = JobStatus.FAILED
                elif jobMinorStatus == JobMinorStatus.MARKED_FOR_TERMINATION:
                    # If the job has been Killed, set it Killed
                    newJobStatus = JobStatus.KILLED
                else:
                    self.log.error(
                        "finalizeRequest: Unexpected jobMinorStatus", "for %d (got %s)" % (jobID, jobMinorStatus)
                    )
                    return S_ERROR("Unexpected jobMinorStatus")

            if newJobStatus:
                self.log.info(
                    "finalizeRequest: Updating job status",
                    "for %d to '%s/%s'" % (jobID, newJobStatus, JobMinorStatus.REQUESTS_DONE),
                )
            else:
                self.log.info(
                    "finalizeRequest: Updating job minor status",
                    "for %d to '%s' (current status is %s)" % (jobID, JobMinorStatus.REQUESTS_DONE, jobStatus),
                )
            stateUpdate = stateServer.setJobStatus(jobID, newJobStatus, JobMinorStatus.REQUESTS_DONE, "RMS")
            if jobAppStatus and stateUpdate["OK"]:
                stateUpdate = stateServer.setJobApplicationStatus(jobID, jobAppStatus, "RMS")
            if not stateUpdate["OK"]:
                self.log.error(
                    "finalizeRequest: Failed to set job status",
                    "JobID: %d, error: %s" % (jobID, stateUpdate["Message"]),
                )
                return stateUpdate

        return S_OK(newJobStatus)

    @ignoreEncodeWarning
    def getRequestIDsForJobs(self, jobIDs):
        """get the request ids for the supplied jobIDs.

        :param self: self reference
        :param list jobIDs: list of job IDs (integers)
        :return: S_ERROR or S_OK( "Successful": { jobID1: reqID1, jobID2: requID2, ... },
                                  "Failed" : { jobIDn: errMsg, jobIDm: errMsg, ...}  )
        """
        self.log.verbose("getRequestIDsForJobs: attempt to get request(s) for jobs", f"(n={len(jobIDs)})")
        res = self._getRPC().getRequestIDsForJobs(jobIDs)
        if not res["OK"]:
            self.log.error("getRequestIDsForJobs: unable to get request(s) for jobs", f"{jobIDs}: {res['Message']}")
            return res

        # Cast the JobIDs back to int
        successful = strToIntDict(res["Value"]["Successful"])
        failed = strToIntDict(res["Value"]["Failed"])

        return S_OK({"Successful": successful, "Failed": failed})

    @ignoreEncodeWarning
    def readRequestsForJobs(self, jobIDs):
        """read requests for jobs

        :param jobIDs: list with jobIDs
        :type jobIDs: python:list
        :return: S_OK( { "Successful" : { jobID1 : Request, ... },
                         "Failed" : { jobIDn : "Fail reason" } } )
        """
        readReqsForJobs = self._getRPC().readRequestsForJobs(jobIDs)
        if not readReqsForJobs["OK"]:
            return readReqsForJobs
        ret = readReqsForJobs["Value"]
        # # create Requests out of JSONs for successful reads
        # Do not forget to cast back str keys to int
        successful = {int(jobID): Request(jsonReq) for jobID, jsonReq in ret["Successful"].items()}
        failed = strToIntDict(ret["Failed"])

        return S_OK({"Successful": successful, "Failed": failed})

    def resetFailedRequest(self, requestID, allR=False):
        """Reset a failed request to "Waiting" status"""

        # # we can safely only peek the request as it is Failed and therefore not owned by an agent
        res = self.peekRequest(requestID)
        if not res["OK"]:
            return res
        req = res["Value"]
        if allR or recoverableRequest(req):
            # Only reset requests that can be recovered
            if req.Status != "Failed":
                gLogger.notice(f"Reset NotBefore time, was {str(req.NotBefore)}")
            else:
                for i, op in enumerate(req):
                    op.Error = ""
                    if op.Status == "Failed":
                        printOperation((i, op), onlyFailed=True)
                    for fi in op:
                        if fi.Status == "Failed":
                            fi.Attempt = 1
                            fi.Error = ""
                            fi.Status = "Waiting"
                    if op.Status == "Failed":
                        op.Status = "Waiting"

            # Reset also NotBefore
            req.NotBefore = datetime.datetime.utcnow().replace(microsecond=0)
            return self.putRequest(req)
        return S_OK("Not reset")


# ============= Some useful functions to be shared ===========


output = ""


def prettyPrint(mainItem, key="", offset=0):
    global output
    if key:
        key += ": "
    blanks = offset * " "
    if mainItem and isinstance(mainItem, dict):
        output += f"{blanks}{key}{{\n" if blanks or key else ""
        for key in sorted(mainItem):
            prettyPrint(mainItem[key], key=key, offset=offset)
        output += f"{blanks}}}\n" if blanks else ""
    elif mainItem and isinstance(mainItem, list) or isinstance(mainItem, tuple):
        output += f"{blanks}{key}{'[' if isinstance(mainItem, list) else '('}\n"
        for item in mainItem:
            prettyPrint(item, offset=offset + 2)
        output += f"{blanks}{']' if isinstance(mainItem, list) else ')'}\n"
    elif isinstance(mainItem, str):
        if "\n" in mainItem:
            prettyPrint(mainItem.strip("\n").split("\n"), offset=offset)
        else:
            output += f"{blanks}{key}'{mainItem}'\n"
    else:
        output += f"{blanks}{key}{str(mainItem)}\n"
    output = (
        output.replace("[\n%s{" % blanks, "[{")
        .replace("}\n%s]" % blanks, "}]")
        .replace("(\n%s{" % blanks, "({")
        .replace("}\n%s)" % blanks, "})")
        .replace(f"(\n{blanks}(", "((")
        .replace(f")\n{blanks})", "))")
        .replace(f"(\n{blanks}[", "[")
        .replace(f"]\n{blanks})", "]")
    )


def printFTSJobs(request):
    """Prints the FTSJobs associated to a request

    :param request: Request object
    """

    try:
        if request.RequestID:
            # We try first the new FTS3 system

            from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client

            fts3Client = FTS3Client()
            res = fts3Client.ping()

            if res["OK"]:
                associatedFTS3Jobs = []
                for op in request:
                    res = fts3Client.getOperationsFromRMSOpID(op.OperationID)
                    if res["OK"]:
                        for fts3Op in res["Value"]:
                            associatedFTS3Jobs.extend(fts3Op.ftsJobs)
                if associatedFTS3Jobs:
                    # Display the direct url and the status
                    gLogger.always(
                        "\n\nFTS3 jobs associated: \n%s"
                        % "\n".join(
                            "%s/fts3/ftsmon/#/job/%s (%s)"
                            % (
                                job.ftsServer.replace(":8446", ":8449"),  # Submission port is 8446, web port is 8449
                                job.ftsGUID,
                                job.status,
                            )
                            for job in associatedFTS3Jobs
                        )
                    )
                return

    # AttributeError can be thrown because the deserialization will not have
    # happened correctly on the new fts3 (CC7 typically), and the error is not
    # properly propagated
    except AttributeError as err:
        gLogger.debug("Could not instantiate FtsClient because of Exception", repr(err))


def printRequest(request, status=None, full=False, verbose=True, terse=False):
    global output

    if full:
        output = ""
        prettyPrint(json.loads(request.toJSON()["Value"]))
        gLogger.always(output)
    else:
        if not status:
            status = request.Status
        gLogger.always(
            "Request name='%s' ID=%s Status='%s'%s%s%s"
            % (
                request.RequestName,
                request.RequestID if hasattr(request, "RequestID") else "(not set yet)",
                request.Status,
                " ('%s' in DB)" % status if status != request.Status else "",
                (" Error='%s'" % request.Error) if request.Error and request.Error.strip() else "",
                f" Job={request.JobID}" if request.JobID else "",
            )
        )
        gLogger.always(
            "Created %s, Updated %s%s"
            % (
                request.CreationTime,
                request.LastUpdate,
                (", NotBefore %s" % request.NotBefore) if request.NotBefore else "",
            )
        )
        if request.OwnerDN:
            gLogger.always(f"Owner: '{request.OwnerDN}', Group: {request.OwnerGroup}")
        for indexOperation in enumerate(request):
            op = indexOperation[1]
            if not terse or op.Status == "Failed":
                printOperation(indexOperation, verbose, onlyFailed=terse)
    if not terse:
        printFTSJobs(request)


def printOperation(indexOperation, verbose=True, onlyFailed=False):
    global output
    i, op = indexOperation
    prStr = ""
    if op.SourceSE:
        prStr += f"SourceSE: {op.SourceSE}"
    if op.TargetSE:
        prStr += (" - " if prStr else "") + f"TargetSE: {op.TargetSE}"
    if prStr:
        prStr += " - "
    prStr += f"Created {op.CreationTime}, Updated {op.LastUpdate}"
    if op.Type == "ForwardDISET" and op.Arguments:
        from DIRAC.Core.Utilities import DEncode

        decode, _length = DEncode.decode(op.Arguments)
        if verbose:
            output = ""
            prettyPrint(decode, offset=10)
            prStr += "\n      Arguments:\n" + output.strip("\n")
        else:
            prStr += f"\n      Service: {decode[0][0]}"
    gLogger.always(
        "  [%s] Operation Type='%s' ID=%s Order=%s Status='%s'%s%s"
        % (
            i,
            op.Type,
            op.OperationID if hasattr(op, "OperationID") else "(not set yet)",
            op.Order,
            op.Status,
            (" Error='%s'" % op.Error) if op.Error and op.Error.strip() else "",
            f" Catalog={op.Catalog}" if op.Catalog else "",
        )
    )
    if prStr:
        gLogger.always(f"      {prStr}")
    for indexFile in enumerate(op):
        if not onlyFailed or indexFile[1].Status == "Failed":
            printFile(indexFile)


def printFile(indexFile):
    ind, fi = indexFile
    gLogger.always(
        "    [%02d] ID=%s LFN='%s' Status='%s'%s%s%s"
        % (
            ind + 1,
            fi.FileID if hasattr(fi, "FileID") else "(not set yet)",
            fi.LFN,
            fi.Status,
            (" Checksum='%s'" % fi.Checksum) if fi.Checksum or (fi.Error and "checksum" in fi.Error.lower()) else "",
            f" Error='{fi.Error}'" if fi.Error and fi.Error.strip() else "",
            (" Attempts=%d" % fi.Attempt) if fi.Attempt > 1 else "",
        )
    )


def recoverableRequest(request):
    excludedErrors = (
        "File does not exist",
        "No such file or directory",
        "sourceSURL equals to targetSURL",
        "Max attempts limit reached",
        "Max attempts reached",
    )
    operationErrorsOK = ("is banned for", "Failed to perform exists from any catalog")
    for op in request:
        if op.Status == "Failed" and (
            not op.Error or not [errStr for errStr in operationErrorsOK if errStr in op.Error]
        ):
            for fi in op:
                if fi.Status == "Failed":
                    if [errStr for errStr in excludedErrors if errStr in fi.Error]:
                        return False
                    return True
    return True
