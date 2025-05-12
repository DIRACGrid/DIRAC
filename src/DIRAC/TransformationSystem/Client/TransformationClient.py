"""Class that contains client access to the transformation DB handler."""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise
from DIRAC.Core.Utilities.JEncode import decode as jdecode, strToIntDict
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.TransformationSystem.Client import TransformationStatus
from DIRAC.TransformationSystem.Client import TransformationFilesStatus


@createClient("Transformation/TransformationManager")
class TransformationClient(Client):
    """Exposes the functionality available in the DIRAC/TransformationManagerHandler

    This inherits the DIRAC base Client for direct execution of server functionality.
    The following methods are available (although not visible here).

    Transformation (table) manipulation

        deleteTransformation(transName)
        getTransformationParameters(transName,paramNames)
        getTransformationWithStatus(status)
        setTransformationParameter(transName,paramName,paramValue)
        deleteTransformationParameter(transName,paramName)

    TransformationFiles table manipulation

        addFilesToTransformation(transName,lfns)
        addTaskForTransformation(transName,lfns=[],se='Unknown')
        getTransformationStats(transName)

    TransformationTasks table manipulation

        setTaskStatus(transName, taskID, status)
        setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)
        getTransformationTaskStats(transName)
        deleteTasks(transName, taskMin, taskMax)
        extendTransformation( transName, nTasks)
        getTasksToSubmit(transName,numTasks,site='')

    TransformationLogging table manipulation

        getTransformationLogging(transName)

    File/directory manipulation methods (the remainder of the interface can be found below)

        getFileSummary(lfns)
        exists(lfns)
    """

    def __init__(self, **kwargs):
        """Simple constructor"""

        super().__init__(**kwargs)
        opsH = Operations()
        self.maxResetCounter = opsH.getValue("Transformations/FilesMaxResetCounter", 10)
        if "url" not in kwargs:
            self.setServer("Transformation/TransformationManager")

    def setServer(self, url):
        self.serverURL = url

    def getCounters(self, table, attrList, condDict, older=None, newer=None, timeStamp=None):
        rpcClient = self._getRPC()
        return rpcClient.getCounters(table, attrList, condDict, older, newer, timeStamp)

    def addTransformation(
        self,
        transName,
        description,
        longDescription,
        transType,
        plugin,
        agentType,
        fileMask,
        transformationGroup="General",
        groupSize=1,
        inheritedFrom=0,
        body="",
        maxTasks=0,
        eventsPerTask=0,
        addFiles=True,
        inputMetaQuery=None,
        outputMetaQuery=None,
        timeout=1800,
    ):
        """add a new transformation"""
        rpcClient = self._getRPC(timeout=timeout)
        return rpcClient.addTransformation(
            transName,
            description,
            longDescription,
            transType,
            plugin,
            agentType,
            fileMask,
            transformationGroup,
            groupSize,
            inheritedFrom,
            body,
            maxTasks,
            eventsPerTask,
            addFiles,
            inputMetaQuery,
            outputMetaQuery,
        )

    def getTransformations(
        self,
        condDict=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        limit=100,
        extraParams=False,
        columns=None,
    ):
        """gets all the transformations in the system, incrementally. "limit" here is just used to determine the offset."""
        rpcClient = self._getRPC()

        transformations = []
        if condDict is None:
            condDict = {}
        if timeStamp is None:
            timeStamp = "CreationDate"
        # getting transformations - incrementally
        offsetToApply = 0
        while True:
            res = rpcClient.getTransformations(
                condDict, older, newer, timeStamp, orderAttribute, limit, extraParams, offsetToApply, columns
            )
            if not res["OK"]:
                return res
            else:
                gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res["Value"])))
                if res["Value"]:
                    transformations = transformations + res["Value"]
                    offsetToApply += limit
                if len(res["Value"]) < limit:
                    break
        return S_OK(transformations)

    def getTransformation(self, transName, extraParams=False):
        rpcClient = self._getRPC()
        return rpcClient.getTransformation(transName, extraParams)

    def getTransformationFiles(
        self,
        condDict=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        limit=None,
        timeout=1800,
        offset=0,
        maxfiles=None,
        columns=None,
    ):
        """gets all the transformation files for a transformation, incrementally.
        "limit" here is just used to determine the offset.
        """
        rpcClient = self._getRPC(timeout=timeout)
        transformationFiles = []
        if condDict is None:
            condDict = {}
        if timeStamp is None:
            timeStamp = "LastUpdate"

        if "LFN" not in condDict:
            res = rpcClient.getTransformationFiles(
                condDict, older, newer, timeStamp, orderAttribute, offset, maxfiles, columns
            )
            if not res["OK"]:
                return res
            res, _ = jdecode(res["Value"])
            # TransformationDB.getTransformationFiles includes a "Records"/"ParameterNames"
            # that we don't want to return to the client so explicitly return S_OK with the value
            if not res["OK"]:
                return res
            return S_OK(res["Value"])

        # If LFNs requested, request in small batches, because...
        # Probably not needed? Because this should always be a list
        if isinstance(condDict["LFN"], str):
            lfnList = [condDict["LFN"]]
        else:
            lfnList = sorted(condDict["LFN"])
        # If a list of LFNs is given, default to chunks of 1000 only
        limit = limit if limit else 1000

        transID = condDict.get("TransformationID", "Unknown")
        offsetToApply = offset
        retries = 5
        while True:
            # If list is exhausted, exit
            if offsetToApply >= len(lfnList):
                break
            # Apply the offset to the list of LFNs
            condDict["LFN"] = lfnList[offsetToApply : offsetToApply + limit]
            # No limit and no offset as the list is limited already
            res = rpcClient.getTransformationFiles(
                condDict, older, newer, timeStamp, orderAttribute, None, None, columns
            )
            if not res["OK"]:
                return res
            res, _ = jdecode(res["Value"])
            if not res["OK"]:
                gLogger.error(
                    "Error getting files for transformation %s (offset %d), %s"
                    % (str(transID), offsetToApply, ("retry %d times" % retries) if retries else "give up"),
                    res["Message"],
                )
                retries -= 1
                if retries:
                    continue
                return res
            gLogger.verbose(f"Result for limit {limit}, offset {offsetToApply}: {len(res['Value'])} files")
            if res["Value"]:
                transformationFiles += res["Value"]
                # Limit the number of files returned
                if maxfiles and len(transformationFiles) >= maxfiles:
                    transformationFiles = transformationFiles[:maxfiles]
                    break
            # Less data than requested, exit only if LFNs were not given
            if not lfnList and len(res["Value"]) < limit:
                break
            offsetToApply += limit
            # Reset number of retries for next chunk
            retries = 5
        return S_OK(transformationFiles)

    def getTransformationTasks(
        self, condDict=None, older=None, newer=None, timeStamp=None, orderAttribute=None, limit=10000, inputVector=False
    ):
        """gets all the transformation tasks for a transformation, incrementally.
        "limit" here is just used to determine the offset.
        """
        rpcClient = self._getRPC()
        transformationTasks = []
        if condDict is None:
            condDict = {}
        if timeStamp is None:
            timeStamp = "CreationTime"
        # getting transformationFiles - incrementally
        offsetToApply = 0
        while True:
            res = rpcClient.getTransformationTasks(
                condDict, older, newer, timeStamp, orderAttribute, limit, inputVector, offsetToApply
            )
            if not res["OK"]:
                return res
            else:
                gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res["Value"])))
                if res["Value"]:
                    transformationTasks = transformationTasks + res["Value"]
                    offsetToApply += limit
                if len(res["Value"]) < limit:
                    break
        return S_OK(transformationTasks)

    def completeTransformation(self, transID):
        """Complete the transformation, and set the status parameter (doing it here, for easier extensibility)"""
        # Complete
        rpcClient = self._getRPC()
        res = rpcClient.completeTransformation(transID)
        if not res["OK"]:
            return res
        # Setting the status
        return self.setTransformationParameter(transID, "Status", TransformationStatus.COMPLETED)

    def cleanTransformation(self, transID):
        """Clean the transformation, and set the status parameter (doing it here, for easier extensibility)"""
        # Cleaning
        rpcClient = self._getRPC()
        res = rpcClient.cleanTransformation(transID)
        if not res["OK"]:
            return res
        # Setting the status
        return self.setTransformationParameter(transID, "Status", TransformationStatus.TRANSFORMATIONCLEANED)

    # Add methods to handle transformation status

    def startTransformation(self, transID):
        """Start the transformation"""
        res = self.setTransformationParameter(transID, "Status", TransformationStatus.ACTIVE)
        if not res["OK"]:
            gLogger.error(f"Failed to start transformation {transID}: {res['Message']}")
            return res
        else:
            res = self.setTransformationParameter(transID, "AgentType", "Automatic")
            if not res["OK"]:
                gLogger.error(f"Failed to set AgentType to transformation {transID}: {res['Message']}")

        return res

    def stopTransformation(self, transID):
        """Stop the transformation"""
        res = self.setTransformationParameter(transID, "Status", TransformationStatus.STOPPED)
        if not res["OK"]:
            gLogger.error(f"Failed to stop transformation {transID}: {res['Message']}")
            return res
        else:
            res = self.setTransformationParameter(transID, "AgentType", "Manual")
            if not res["OK"]:
                gLogger.error(f"Failed to set AgentType to transformation {transID}: {res['Message']}")

        return res

    def moveFilesToDerivedTransformation(self, transDict, resetUnused=True):
        """move files input to a transformation, to the derived one"""
        prod = transDict["TransformationID"]
        parentProd = int(transDict.get("InheritedFrom", 0))
        movedFiles = {}
        log = gLogger.getLocalSubLogger("[None] [%d] .moveFilesToDerivedTransformation:" % prod)
        if not parentProd:
            log.warn("Transformation was not derived...")
            return S_OK((parentProd, movedFiles))
        # get the lfns in status Unused/MaxReset of the parent production
        res = self.getTransformationFiles(
            condDict={
                "TransformationID": parentProd,
                "Status": [TransformationFilesStatus.UNUSED, TransformationFilesStatus.MAX_RESET],
            }
        )
        if not res["OK"]:
            log.error(" Error getting Unused files from transformation", "%d: %s" % (parentProd, res["Message"]))
            return res
        parentFiles = res["Value"]
        lfns = [lfnDict["LFN"] for lfnDict in parentFiles]
        if not lfns:
            log.info(" No files found to be moved from transformation", parentProd)
            return S_OK((parentProd, movedFiles))
        # get the lfns of the derived production that were Unused/MaxReset in the parent one
        res = self.getTransformationFiles(condDict={"TransformationID": prod, "LFN": lfns})
        if not res["OK"]:
            log.error(" Error getting files from derived transformation:", res["Message"])
            return res
        derivedFiles = res["Value"]
        derivedStatusDict = {derivedDict["LFN"]: derivedDict["Status"] for derivedDict in derivedFiles}
        newStatusFiles = {}
        parentStatusFiles = {}
        badStatusFiles = {}
        for parentDict in parentFiles:
            lfn = parentDict["LFN"]
            derivedStatus = derivedStatusDict.get(lfn)
            if derivedStatus:
                parentStatus = parentDict["Status"]
                # By default move to the parent status (which is Unused or MaxReset)
                status = parentStatus
                moveStatus = parentStatus
                # For MaxReset, set Unused if requested
                if parentStatus == TransformationFilesStatus.MAX_RESET:
                    if resetUnused:
                        status = TransformationFilesStatus.UNUSED
                        moveStatus = "Unused from MaxReset"
                    else:
                        status = "MaxReset-inherited"
                if derivedStatus.endswith("-inherited"):
                    # This is the general case
                    newStatusFiles.setdefault((status, parentStatus), []).append(lfn)
                    movedFiles[moveStatus] = movedFiles.setdefault(moveStatus, 0) + 1
                else:
                    badStatusFiles[derivedStatus] = badStatusFiles.setdefault(derivedStatus, 0) + 1
                if parentStatus == TransformationFilesStatus.UNUSED:
                    # If the file was Unused, set it NotProcessed in parent
                    parentStatusFiles.setdefault("NotProcessed", []).append(lfn)
                else:
                    parentStatusFiles.setdefault("Moved", []).append(lfn)

        for status, count in badStatusFiles.items():  # can be an iterator
            log.warn(
                "Files found in an unexpected status in derived transformation",
                ": %d files in status %s" % (count, status),
            )
        # Set the status in the parent transformation first
        for status, lfnList in parentStatusFiles.items():  # can be an iterator
            for lfnChunk in breakListIntoChunks(lfnList, 5000):
                res = self.setFileStatusForTransformation(parentProd, status, lfnChunk)
                if not res["OK"]:
                    log.error(
                        " Error setting status in transformation",
                        "%d: status %s for %d files - %s" % (parentProd, status, len(lfnList), res["Message"]),
                    )

        # Set the status in the new transformation
        for (status, oldStatus), lfnList in newStatusFiles.items():  # can be an iterator
            for lfnChunk in breakListIntoChunks(lfnList, 5000):
                res = self.setFileStatusForTransformation(prod, status, lfnChunk)
                if not res["OK"]:
                    log.debug(
                        " Error setting status in transformation",
                        "%d: status %s for %d files; resetting them %s. %s"
                        % (parentProd, status, len(lfnChunk), oldStatus, res["Message"]),
                    )
                    res = self.setFileStatusForTransformation(parentProd, oldStatus, lfnChunk)
                    if not res["OK"]:
                        log.error(
                            " Error setting status in transformation",
                            " %d: status %s for %d files: %s" % (parentProd, oldStatus, len(lfnChunk), res["Message"]),
                        )
                else:
                    log.info("Successfully moved files", f": {len(lfnChunk)} files from {oldStatus} to {status}")

        # If files were Assigned or Unused at the time of derivation, try and update them as jobs may have run since then
        res = self.getTransformationFiles(
            condDict={
                "TransformationID": prod,
                "Status": [TransformationFilesStatus.ASSIGNED_INHERITED, TransformationFilesStatus.UNUSED_INHERITED],
            }
        )
        if res["OK"]:
            assignedFiles = res["Value"]
            if assignedFiles:
                lfns = [lfnDict["LFN"] for lfnDict in assignedFiles]
                res = self.getTransformationFiles(condDict={"TransformationID": parentProd, "LFN": lfns})
                if res["OK"]:
                    parentFiles = res["Value"]
                    processedLfns = [
                        lfnDict["LFN"]
                        for lfnDict in parentFiles
                        if lfnDict["Status"] == TransformationFilesStatus.PROCESSED
                    ]
                    if processedLfns:
                        res = self.setFileStatusForTransformation(
                            prod, TransformationFilesStatus.PROCESSED_INHERITED, processedLfns
                        )
                        if res["OK"]:
                            log.info(
                                "Successfully set files status",
                                ": %d files to status %s"
                                % (len(processedLfns), TransformationFilesStatus.PROCESSED_INHERITED),
                            )
        if not res["OK"]:
            log.error("Error setting status for Assigned derived files", res["Message"])

        return S_OK((parentProd, movedFiles))

    def setFileStatusForTransformation(self, transName, newLFNsStatus=None, lfns=None, force=False):
        """Sets the file status for LFNs of a transformation

        For backward compatibility purposes, the status and LFNs can be passed in 2 ways:

          - newLFNsStatus is a dictionary with the form:
            {'/this/is/an/lfn1.txt': 'StatusA', '/this/is/an/lfn2.txt': 'StatusB',  ... }
            and at this point lfns is not considered
          - newLFNStatus is a string, that applies to all the LFNs in lfns

        """
        # create dictionary in case newLFNsStatus is a string
        if isinstance(newLFNsStatus, str):
            if not lfns:
                return S_OK({})
            if isinstance(lfns, str):
                lfns = [lfns]
            newLFNsStatus = dict.fromkeys(lfns, newLFNsStatus)
        if not newLFNsStatus:
            return S_OK({})

        rpcClient = self._getRPC()
        # gets current status, errorCount and fileID
        tsFiles = self.getTransformationFiles({"TransformationID": transName, "LFN": list(newLFNsStatus)})
        if not tsFiles["OK"]:
            return tsFiles
        tsFiles = tsFiles["Value"]
        newStatuses = {}
        if tsFiles:
            # for convenience, makes a small dictionary out of the tsFiles, with the lfn as key
            tsFilesAsDict = {
                tsFile["LFN"]: [tsFile["Status"], tsFile["ErrorCount"], tsFile["FileID"]] for tsFile in tsFiles
            }

            # applying the state machine to the proposed status
            newStatuses = self._applyTransformationFilesStateMachine(tsFilesAsDict, newLFNsStatus, force)

            if newStatuses:  # if there's something to update
                # Key to the service is fileIDs
                # The value is a tuple with the new status and a flag that says if ErrorCount should be incremented
                newStatusForFileIDs = {
                    tsFilesAsDict[lfn][2]: [
                        newStatuses[lfn],
                        self._wasFileInError(newStatuses[lfn], tsFilesAsDict[lfn][0]),
                    ]
                    for lfn in newStatuses
                }
                res = rpcClient.setFileStatusForTransformation(transName, newStatusForFileIDs)
                if not res["OK"]:
                    return res

        return S_OK(newStatuses)

    def _wasFileInError(self, newStatus, currentStatus):
        """Tells whether the file was Assigned and failed, i.e. was not Processed"""
        return currentStatus == TransformationFilesStatus.ASSIGNED and newStatus != TransformationFilesStatus.PROCESSED

    def _applyTransformationFilesStateMachine(self, tsFilesAsDict, dictOfProposedLFNsStatus, force):
        """For easier extension, here we apply the state machine of the production files.
        VOs might want to replace the standard here with something they prefer.

        tsFiles is a dictionary with the lfn as key and as value a list of [Status, ErrorCount, FileID]
        dictOfNewLFNsStatus is a dictionary with the proposed status
        force is a boolean

        It returns a dictionary with the status updates
        """
        newStatuses = {}

        for lfn, newStatus in dictOfProposedLFNsStatus.items():  # can be an iterator
            if lfn in tsFilesAsDict:
                currentStatus = tsFilesAsDict[lfn][0]
                # Apply optional corrections
                if (
                    currentStatus == TransformationFilesStatus.PROCESSED
                    and newStatus != TransformationFilesStatus.PROCESSED
                ):
                    # Processed files should be in a final status unless forced
                    if not force:
                        newStatus = TransformationFilesStatus.PROCESSED
                elif currentStatus == TransformationFilesStatus.MAX_RESET:
                    # MaxReset files can go to any status except Unused (unless forced)
                    if newStatus == TransformationFilesStatus.UNUSED and not force:
                        newStatus = TransformationFilesStatus.MAX_RESET
                elif newStatus == TransformationFilesStatus.UNUSED:
                    errorCount = tsFilesAsDict[lfn][1]
                    # every 10 retries (by default) the file cannot be reset Unused any longer
                    if errorCount and ((errorCount % self.maxResetCounter) == 0) and not force:
                        newStatus = TransformationFilesStatus.MAX_RESET

                # Only worth changing status if it is different
                if newStatus != currentStatus:
                    newStatuses[lfn] = newStatus

        return newStatuses

    def setTransformationParameter(self, transID, paramName, paramValue, force=False, currentStatus=None):
        """Sets a transformation parameter. There's a special case when coming to setting the status of a transformation.

        :param currentStatus: if set, make sure the status did not change in the DB before setting it
        """
        rpcClient = self._getRPC()

        if paramName.lower() == "status":
            # get transformation Type
            transformation = self.getTransformation(transID)
            if not transformation["OK"]:
                return transformation
            transformationType = transformation["Value"]["Type"]

            # get status as of today
            originalStatus = self.getTransformationParameters(transID, "Status")
            if not originalStatus["OK"]:
                return originalStatus
            originalStatus = originalStatus["Value"]

            if currentStatus and currentStatus != originalStatus:
                return S_ERROR(f"Status changed in the DB: {originalStatus}")

            transIDAsDict = {transID: [originalStatus, transformationType]}
            dictOfProposedstatus = {transID: paramValue}
            # applying the state machine to the proposed status
            value = self._applyTransformationStatusStateMachine(transIDAsDict, dictOfProposedstatus, force)
        else:
            value = paramValue

        return rpcClient.setTransformationParameter(transID, paramName, value)

    def _applyTransformationStatusStateMachine(self, transIDAsDict, dictOfProposedstatus, force):
        """For easier extension, here we apply the state machine of the transformation status.
        VOs might want to replace the standard here with something they prefer.

        transIDAsDict is a dictionary with the transID as key and as value a list with [Status, Type]
        dictOfProposedstatus is a dictionary with the proposed status
        force is a boolean

        It returns the new status (the standard is just doing nothing: everything is possible)
        """
        return list(dictOfProposedstatus.values())[0]

    def isOK(self):
        return self.valid

    def addDirectory(self, path, force=False):
        rpcClient = self._getRPC()
        return rpcClient.addDirectory(path, force)

    @convertToReturnValue
    def getTransformationFilesCount(self, transName, field, selection=None, **kwargs):
        if not selection:
            selection = {}
        rpcClient = self._getRPC(**kwargs)

        files_count = returnValueOrRaise(rpcClient.getTransformationFilesCount(transName, field, selection))
        # Some of these fields are LHCb specific, however:
        # * it seems only LHCb uses this method
        # * it will not be necessary with diracx & pydantic anymore
        if field in ["TransformationID", "FileID", "TaskID", "ErrorCount", "RunNumber", "Size"]:
            total = files_count.pop("Total")
            files_count = strToIntDict(files_count)
            files_count["Total"] = total
        return files_count
