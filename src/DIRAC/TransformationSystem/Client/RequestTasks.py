import time

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.JEncode import decode

from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.TransformationSystem.Client import TransformationFilesStatus
from DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody import BaseBody
from DIRAC.TransformationSystem.Client.TaskManager import TaskBase


class StopTaskIteration(Exception):
    """Utility Exception to stop creating
    a Request for the current worked on task
    """

    pass


class RequestTasks(TaskBase):
    """
    Class for handling tasks for the RMS
    """

    def __init__(
        self,
        transClient=None,
        logger=None,
        requestClient=None,
        requestClass=None,
        requestValidator=None,
        ownerDN=None,
        ownerGroup=None,
    ):
        """c'tor

        the requestClass is by default Request.
        If extensions want to use an extended type, they can pass it as a parameter.
        This is the same behavior as WorfkloTasks and jobClass
        """

        if not logger:
            logger = gLogger.getSubLogger(self.__class__.__name__)

        super().__init__(transClient, logger)
        useCertificates = True if (bool(ownerDN) and bool(ownerGroup)) else False

        if not requestClient:
            self.requestClient = ReqClient(
                useCertificates=useCertificates, delegatedDN=ownerDN, delegatedGroup=ownerGroup
            )
        else:
            self.requestClient = requestClient

        if not requestClass:
            self.requestClass = Request
        else:
            self.requestClass = requestClass

        if not requestValidator:
            self.requestValidator = RequestValidator()
        else:
            self.requestValidator = requestValidator

    def prepareTransformationTasks(
        self, transBody, taskDict, owner="", ownerGroup="", ownerDN="", bulkSubmissionFlag=False
    ):
        """Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB"""
        if not taskDict:
            return S_OK({})

        if (not owner) or (not ownerGroup):
            res = getProxyInfo(False, False)
            if not res["OK"]:
                return res
            proxyInfo = res["Value"]
            owner = proxyInfo["username"]
            ownerGroup = proxyInfo["group"]

        if not ownerDN:
            res = getDNForUsername(owner)
            if not res["OK"]:
                return res
            ownerDN = res["Value"][0]

        try:
            transJson, _decLen = decode(transBody)

            if isinstance(transJson, BaseBody):
                self._bodyPlugins(transJson, taskDict, ownerDN, ownerGroup)
            else:
                self._multiOperationsBody(transJson, taskDict, ownerDN, ownerGroup)
        except ValueError:  # #json couldn't load
            self._singleOperationsBody(transBody, taskDict, ownerDN, ownerGroup)

        return S_OK(taskDict)

    def _multiOperationsBody(self, transJson, taskDict, ownerDN, ownerGroup):
        """Deal with a Request that has multiple operations

        :param transJson: list of lists of string and dictionaries, e.g.:

          .. code :: python

            body = [ ( "ReplicateAndRegister", { "SourceSE":"FOO-SRM", "TargetSE":"TASK:TargetSE" }),
                     ( "RemoveReplica", { "TargetSE":"FOO-SRM" } ),
                   ]

            If a value of an operation parameter in the body starts with ``TASK:``,
            we take it from the taskDict.
            For example ``TASK:TargetSE`` is replaced with ``task['TargetSE']``

        :param dict taskDict: dictionary of tasks, modified in this function
        :param str ownerDN: certificate DN used for the requests
        :param str onwerGroup: dirac group used for the requests

        :returns: None
        """
        for taskID, task in list(taskDict.items()):
            try:
                transID = task["TransformationID"]
                if not task.get("InputData"):
                    raise StopTaskIteration("No input data")
                files = []

                oRequest = Request()
                if isinstance(task["InputData"], list):
                    files = task["InputData"]
                elif isinstance(task["InputData"], str):
                    files = task["InputData"].split(";")

                # create the operations from the json structure
                for operationTuple in transJson:
                    op = Operation()
                    op.Type = operationTuple[0]
                    for parameter, value in operationTuple[1].items():
                        # Here we massage a bit the body to replace some parameters
                        # with what we have in the task.
                        try:
                            taskKey = value.split("TASK:")[1]
                            value = task[taskKey]
                        # Either the attribute is not a string (AttributeError)
                        # or it does not start with 'TASK:' (IndexError)
                        except (AttributeError, IndexError):
                            pass
                        # That happens when the requested substitution is not
                        # a key in the task, and that's a problem
                        except KeyError:
                            raise StopTaskIteration("Parameter %s does not exist in taskDict" % taskKey)

                        setattr(op, parameter, value)

                    for lfn in files:
                        opFile = File()
                        opFile.LFN = lfn
                        op.addFile(opFile)

                    oRequest.addOperation(op)

                result = self._assignRequestToTask(oRequest, taskDict, transID, taskID, ownerDN, ownerGroup)
                if not result["OK"]:
                    raise StopTaskIteration("Could not assign request to task: %s" % result["Message"])
            except StopTaskIteration as e:
                self._logError("Error creating request for task", f"{taskID}, {e}", transID=transID)
                taskDict.pop(taskID)

    def _singleOperationsBody(self, transBody, taskDict, ownerDN, ownerGroup):
        """deal with a Request that has just one operation, as it was sofar

        :param transBody: string, can be an empty string
        :param dict taskDict: dictionary of tasks, modified in this function
        :param str ownerDN: certificate DN used for the requests
        :param str onwerGroup: dirac group used for the requests

        :returns: None
        """

        requestOperation = "ReplicateAndRegister"
        if transBody:
            try:
                _requestType, requestOperation = transBody.split(";")
            except AttributeError:
                pass
        failedTasks = []
        # Do not remove sorted, we might pop elements in the loop
        for taskID, task in taskDict.items():

            transID = task["TransformationID"]

            oRequest = Request()
            transfer = Operation()
            transfer.Type = requestOperation
            transfer.TargetSE = task["TargetSE"]

            # If there are input files
            if task.get("InputData"):
                if isinstance(task["InputData"], list):
                    files = task["InputData"]
                elif isinstance(task["InputData"], str):
                    files = task["InputData"].split(";")
                for lfn in files:
                    trFile = File()
                    trFile.LFN = lfn

                    transfer.addFile(trFile)

            oRequest.addOperation(transfer)
            result = self._assignRequestToTask(oRequest, taskDict, transID, taskID, ownerDN, ownerGroup)
            if not result["OK"]:
                failedTasks.append(taskID)
        # Remove failed tasks
        for taskID in failedTasks:
            taskDict.pop(taskID)

    def _bodyPlugins(self, bodyObj, taskDict, ownerDN, ownerGroup):
        """Deal with complex body object"""
        for taskID, task in list(taskDict.items()):
            try:
                transID = task["TransformationID"]
                if not task.get("InputData"):
                    raise StopTaskIteration("No input data")

                oRequest = bodyObj.taskToRequest(taskID, task, transID)
                result = self._assignRequestToTask(oRequest, taskDict, transID, taskID, ownerDN, ownerGroup)
                if not result["OK"]:
                    raise StopTaskIteration("Could not assign request to task: %s" % result["Message"])
            except StopTaskIteration as e:
                self._logError("Error creating request for task", f"{taskID}, {e}", transID=transID)
                taskDict.pop(taskID)

    def _assignRequestToTask(self, oRequest, taskDict, transID, taskID, ownerDN, ownerGroup):
        """set ownerDN and group to request, and add the request to taskDict if it is
        valid, otherwise remove the task from the taskDict

        :param oRequest: Request
        :param dict taskDict: dictionary of tasks, modified in this function
        :param int transID: Transformation ID
        :param int taskID: Task ID
        :param str ownerDN: certificate DN used for the requests
        :param str onwerGroup: dirac group used for the requests

        :returns: None
        """

        oRequest.RequestName = self._transTaskName(transID, taskID)
        oRequest.OwnerDN = ownerDN
        oRequest.OwnerGroup = ownerGroup

        isValid = self.requestValidator.validate(oRequest)
        if not isValid["OK"]:
            self._logError("Error creating request for task", f"{taskID} {isValid}", transID=transID)
            return S_ERROR("Error creating request")
        taskDict[taskID]["TaskObject"] = oRequest
        return S_OK()

    def submitTransformationTasks(self, taskDict):
        """Submit requests one by one"""
        submitted = 0
        failed = 0
        startTime = time.time()
        method = "submitTransformationTasks"
        for task in taskDict.values():
            # transID is the same for all tasks, so pick it up every time here
            transID = task["TransformationID"]
            if not task["TaskObject"]:
                task["Success"] = False
                failed += 1
                continue
            res = self.submitTaskToExternal(task["TaskObject"])
            if res["OK"]:
                task["ExternalID"] = res["Value"]
                task["Success"] = True
                submitted += 1
            else:
                self._logError("Failed to submit task to RMS", res["Message"], transID=transID)
                task["Success"] = False
                failed += 1
        if submitted:
            self._logInfo(
                "Submitted %d tasks to RMS in %.1f seconds" % (submitted, time.time() - startTime),
                transID=transID,
                method=method,
            )
        if failed:
            self._logWarn("Failed to submit %d tasks to RMS." % (failed), transID=transID, method=method)
        return S_OK(taskDict)

    def submitTaskToExternal(self, oRequest):
        """
        Submits a request to RMS
        """
        if isinstance(oRequest, self.requestClass):
            return self.requestClient.putRequest(oRequest, useFailoverProxy=False, retryMainService=2)
        return S_ERROR("Request should be a Request object")

    def updateTransformationReservedTasks(self, taskDicts):
        requestNameIDs = {}
        noTasks = []
        for taskDict in taskDicts:
            requestName = self._transTaskName(taskDict["TransformationID"], taskDict["TaskID"])
            reqID = taskDict["ExternalID"]
            if reqID and int(reqID):
                requestNameIDs[requestName] = reqID
            else:
                noTasks.append(requestName)
        return S_OK({"NoTasks": noTasks, "TaskNameIDs": requestNameIDs})

    def getSubmittedTaskStatus(self, taskDicts):
        """
        Check if tasks changed status, and return a list of tasks per new status
        """
        updateDict = {}
        badRequestID = 0
        for taskDict in taskDicts:
            oldStatus = taskDict["ExternalStatus"]
            # ExternalID is normally a string
            if taskDict["ExternalID"] and int(taskDict["ExternalID"]):
                newStatus = self.requestClient.getRequestStatus(taskDict["ExternalID"])
                if not newStatus["OK"]:
                    log = self._logVerbose if "not exist" in newStatus["Message"] else self._logWarn
                    log(
                        "getSubmittedTaskStatus: Failed to get requestID for request",
                        newStatus["Message"],
                        transID=taskDict["TransformationID"],
                    )
                else:
                    newStatus = newStatus["Value"]
                    # We don't care updating the tasks to Assigned while the request is being processed
                    if newStatus != oldStatus and newStatus != "Assigned":
                        updateDict.setdefault(newStatus, []).append(taskDict["TaskID"])
            else:
                badRequestID += 1
        if badRequestID:
            self._logWarn("%d requests have identifier 0" % badRequestID)
        return S_OK(updateDict)

    def getSubmittedFileStatus(self, fileDicts):
        """
        Check if transformation files changed status, and return a list of taskIDs per new status
        """
        # Don't try and get status of not submitted tasks!
        transID = None
        taskFiles = {}
        for fileDict in fileDicts:
            # There is only one transformation involved, get however the transID in the loop
            transID = fileDict["TransformationID"]
            taskID = int(fileDict["TaskID"])
            taskFiles.setdefault(taskID, []).append(fileDict["LFN"])
        # Should not happen, but just in case there are no files, return
        if transID is None:
            return S_OK({})

        res = self.transClient.getTransformationTasks({"TransformationID": transID, "TaskID": list(taskFiles)})
        if not res["OK"]:
            return res
        requestFiles = {}
        for taskDict in res["Value"]:
            taskID = taskDict["TaskID"]
            externalID = taskDict["ExternalID"]
            # Only consider tasks that are submitted, ExternalID is a string
            if taskDict["ExternalStatus"] != "Created" and externalID and int(externalID):
                requestFiles[externalID] = taskFiles[taskID]

        updateDict = {}
        for requestID, lfnList in requestFiles.items():
            statusDict = self.requestClient.getRequestFileStatus(requestID, lfnList)
            if not statusDict["OK"]:
                log = self._logVerbose if "not exist" in statusDict["Message"] else self._logWarn
                log(
                    "Failed to get files status for request",
                    statusDict["Message"],
                    transID=transID,
                    method="getSubmittedFileStatus",
                )
            else:
                for lfn, newStatus in statusDict["Value"].items():
                    if newStatus == "Done":
                        updateDict[lfn] = TransformationFilesStatus.PROCESSED
                    elif newStatus == "Failed":
                        updateDict[lfn] = TransformationFilesStatus.PROBLEMATIC
        return S_OK(updateDict)
