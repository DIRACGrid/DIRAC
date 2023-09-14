""" Service for interacting with TransformationDB
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security.Properties import SecurityProperty
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.TransformationSystem.Client import TransformationFilesStatus
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation


TASKS_STATE_NAMES = ["TotalCreated", "Created"] + sorted(
    set(JobStatus.JOB_STATES) | set(Request.ALL_STATES) | set(Operation.ALL_STATES)
)
FILES_STATE_NAMES = ["PercentProcessed", "Total"] + TransformationFilesStatus.TRANSFORMATION_FILES_STATES


class TransformationManagerHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB object"""

        try:
            result = ObjectLoader().loadObject("TransformationSystem.DB.TransformationDB", "TransformationDB")
            if not result["OK"]:
                return result
            cls.transformationDB = result["Value"]()

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to TransformationDB: {excp}")

        return S_OK()

    def checkPermissions(self, transName: str):
        """
        checks if remote user has permission to access to a given transformation

        :param str transName: Name of the transformation to check

        :return: S_ERROR if user does not have permission or if transformation does not exist
                 S_OK otherwise
        """
        credDict = self.getRemoteCredentials()
        groupProperties = credDict.get("properties", [])
        if SecurityProperty.PRODUCTION_MANAGEMENT in groupProperties:
            return S_OK()
        tfDetails = self.transformationDB.getTransformation(transName)
        if not tfDetails["OK"]:
            return S_ERROR(f"Could not retrieve transformation {transName} details for permissions check.")
        authorGroup = tfDetails["Value"]["AuthorGroup"]
        authorDN = tfDetails["Value"]["AuthorDN"]
        if SecurityProperty.PRODUCTION_SHARING in groupProperties:
            if authorGroup == credDict["group"]:
                return S_OK()
        if SecurityProperty.PRODUCTION_USER in groupProperties:
            if authorDN == credDict["DN"]:
                return S_OK()
        return S_ERROR(f"You do not have permissions for transformation {transName}")

    types_getCounters = [str, list, dict]

    @classmethod
    def export_getCounters(cls, table, attrList, condDict, older=None, newer=None, timeStamp=None):
        return cls.transformationDB.getCounters(
            table, attrList, condDict, older=older, newer=newer, timeStamp=timeStamp
        )

    ####################################################################
    #
    # These are the methods to manipulate the transformations table
    #

    types_addTransformation = [str, str, str, str, str, str, str]

    def export_addTransformation(
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
    ):
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        authorGroup = credDict.get("group")
        groupProperties = credDict.get("properties", [])
        if (
            SecurityProperty.PRODUCTION_MANAGEMENT not in groupProperties
            and SecurityProperty.PRODUCTION_SHARING not in groupProperties
            and SecurityProperty.PRODUCTION_USER not in groupProperties
        ):
            return S_ERROR("You do not have permission to add a Transformation")
        res = self.transformationDB.addTransformation(
            transName,
            description,
            longDescription,
            authorDN,
            authorGroup,
            transType,
            plugin,
            agentType,
            fileMask,
            transformationGroup=transformationGroup,
            groupSize=groupSize,
            inheritedFrom=inheritedFrom,
            body=body,
            maxTasks=maxTasks,
            eventsPerTask=eventsPerTask,
            addFiles=addFiles,
            inputMetaQuery=inputMetaQuery,
            outputMetaQuery=outputMetaQuery,
        )
        if res["OK"]:
            self.log.info("Added transformation", res["Value"])
        return res

    types_deleteTransformation = [[int, str]]

    def export_deleteTransformation(self, transName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.deleteTransformation(transName, author=authorDN)

    types_completeTransformation = [[int, str]]

    def export_completeTransformation(self, transName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.setTransformationParameter(transName, "Status", "Completed", author=authorDN)

    types_cleanTransformation = [[int, str]]

    def export_cleanTransformation(self, transName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.cleanTransformation(transName, author=authorDN)

    types_setTransformationParameter = [[int, str], str]

    def export_setTransformationParameter(self, transName, paramName, paramValue):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.setTransformationParameter(transName, paramName, paramValue, author=authorDN)

    types_deleteTransformationParameter = [[int, str], str]

    def export_deleteTransformationParameter(self, transName, paramName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        return self.transformationDB.deleteTransformationParameter(transName, paramName)

    types_getTransformations = []

    @classmethod
    def export_getTransformations(
        cls,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="CreationDate",
        orderAttribute=None,
        limit=None,
        extraParams=False,
        offset=None,
        columns=None,
    ):
        if not condDict:
            condDict = {}
        return cls.transformationDB.getTransformations(
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            extraParams=extraParams,
            offset=offset,
            columns=columns,
        )

    types_getTransformation = [[int, str]]

    def export_getTransformation(self, transName, extraParams=False):
        # check first if transformation exists to avoid returning permissions error for non-existing transformation
        tfDetails = self.transformationDB.getTransformation(transName, extraParams=extraParams)
        if not tfDetails["OK"]:
            return tfDetails
        return tfDetails

    types_getTransformationParameters = [[int, str], [str, list]]

    def export_getTransformationParameters(self, transName, parameters):
        return self.transformationDB.getTransformationParameters(transName, parameters)

    types_getTransformationWithStatus = [[str, list, tuple]]

    def export_getTransformationWithStatus(self, status):
        return self.transformationDB.getTransformationWithStatus(status)

    ####################################################################
    #
    # These are the methods to manipulate the TransformationFiles tables
    #

    types_addFilesToTransformation = [[int, str], [list, tuple]]

    def export_addFilesToTransformation(self, transName, lfns):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        return self.transformationDB.addFilesToTransformation(transName, lfns)

    types_addTaskForTransformation = [[int, str]]

    def export_addTaskForTransformation(self, transName, lfns=[], se="Unknown"):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        return self.transformationDB.addTaskForTransformation(transName, lfns=lfns, se=se)

    types_setFileStatusForTransformation = [[int, str], dict]

    @ignoreEncodeWarning
    def export_setFileStatusForTransformation(self, transName, dictOfNewFilesStatus):
        """Sets the file status for the transformation.

        The dictOfNewFilesStatus is a dictionary with the form:
        {12345: ('StatusA', errorA), 6789: ('StatusB',errorB),  ... } where the keys are fileIDs
        The tuple may be a string with only the status if the client was from an older version
        """
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        if not dictOfNewFilesStatus:
            return S_OK({})

        statusSample = list(dictOfNewFilesStatus.values())[0]
        if isinstance(statusSample, (list, tuple)) and len(statusSample) == 2:
            newStatusForFileIDs = dictOfNewFilesStatus
        else:
            return S_ERROR("Status field should be two values")

        res = self.transformationDB._getConnectionTransID(False, transName)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        transID = res["Value"]["TransformationID"]

        return self.transformationDB.setFileStatusForTransformation(transID, newStatusForFileIDs, connection=connection)

    types_getTransformationStats = [[int, str]]

    def export_getTransformationStats(self, transName):
        return self.transformationDB.getTransformationStats(transName)

    types_getTransformationFilesCount = [[int, str], str]

    def export_getTransformationFilesCount(self, transName, field, selection={}):
        return self.transformationDB.getTransformationFilesCount(transName, field, selection=selection)

    types_getTransformationFiles = []

    def export_getTransformationFiles(
        self,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="LastUpdate",
        orderAttribute=None,
        limit=None,
        offset=None,
    ):
        if not condDict:
            condDict = {}
        return self.transformationDB.getTransformationFiles(
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            offset=offset,
            connection=False,
        )

    ####################################################################
    #
    # These are the methods to manipulate the TransformationTasks table
    #

    types_getTransformationTasks = []

    @classmethod
    def export_getTransformationTasks(
        cls,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="CreationTime",
        orderAttribute=None,
        limit=None,
        inputVector=False,
        offset=None,
    ):
        if not condDict:
            condDict = {}
        return cls.transformationDB.getTransformationTasks(
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            inputVector=inputVector,
            offset=offset,
        )

    types_setTaskStatus = [[int, str], [list, int], str]

    def export_setTaskStatus(self, transName, taskID, status):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        return self.transformationDB.setTaskStatus(transName, taskID, status)

    types_setTaskStatusAndWmsID = [[int, str], int, str, str]

    def export_setTaskStatusAndWmsID(self, transName, taskID, status, taskWmsID):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        return self.transformationDB.setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)

    types_getTransformationTaskStats = [[int, str]]

    def export_getTransformationTaskStats(self, transName):
        return self.transformationDB.getTransformationTaskStats(transName)

    types_deleteTasks = [[int, str], int, int]

    def export_deleteTasks(self, transName, taskMin, taskMax):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.deleteTasks(transName, taskMin, taskMax, author=authorDN)

    types_extendTransformation = [[int, str], int]

    def export_extendTransformation(self, transName, nTasks):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        # authorDN = self._clientTransport.peerCredentials['DN']
        return self.transformationDB.extendTransformation(transName, nTasks, author=authorDN)

    types_getTasksToSubmit = [[int, str], int]

    def export_getTasksToSubmit(self, transName, numTasks, site=""):
        """Get information necessary for submission for a given number of tasks for a given transformation"""
        res = self.transformationDB.getTransformation(transName)
        if not res["OK"]:
            return res
        transDict = res["Value"]
        submitDict = {}
        res = self.transformationDB.getTasksForSubmission(
            transName, numTasks=numTasks, site=site, statusList=["Created"]
        )
        if not res["OK"]:
            return res
        tasksDict = res["Value"]
        for taskID, taskDict in tasksDict.items():
            res = self.transformationDB.reserveTask(transName, int(taskID))
            if not res["OK"]:
                return res
            else:
                submitDict[taskID] = taskDict
        transDict["JobDictionary"] = submitDict
        return S_OK(transDict)

    ####################################################################
    #
    # These are the methods for TransformationMetaQueries table. It replaces methods
    # for the old TransformationInputDataQuery table
    #

    types_createTransformationMetaQuery = [[int, str], dict, str]

    def export_createTransformationMetaQuery(self, transName, queryDict, queryType):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.createTransformationMetaQuery(transName, queryDict, queryType, author=authorDN)

    types_deleteTransformationMetaQuery = [[int, str], str]

    def export_deleteTransformationMetaQuery(self, transName, queryType):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        authorDN = credDict.get("DN", credDict.get("CN"))
        return self.transformationDB.deleteTransformationMetaQuery(transName, queryType, author=authorDN)

    types_getTransformationMetaQuery = [[int, str], str]

    def export_getTransformationMetaQuery(self, transName, queryType):
        return self.transformationDB.getTransformationMetaQuery(transName, queryType)

    ####################################################################
    #
    # These are the methods for transformation logging manipulation
    #

    types_getTransformationLogging = [[int, str]]

    def export_getTransformationLogging(self, transName):
        return self.transformationDB.getTransformationLogging(transName)

    ####################################################################
    #
    # These are the methods for transformation additional parameters
    #

    types_getAdditionalParameters = [[int, str]]

    def export_getAdditionalParameters(self, transName):
        return self.transformationDB.getAdditionalParameters(transName)

    ####################################################################
    #
    # These are the methods for file manipulation
    #

    types_getFileSummary = [list]

    @classmethod
    def export_getFileSummary(cls, lfns):
        return cls.transformationDB.getFileSummary(lfns)

    types_addDirectory = [str]

    @classmethod
    def export_addDirectory(cls, path, force=False):
        return cls.transformationDB.addDirectory(path, force=force)

    types_exists = [list]

    @classmethod
    def export_exists(cls, lfns):
        return cls.transformationDB.exists(lfns)

    types_addFile = [[list, dict, str]]

    @classmethod
    def export_addFile(cls, fileDicts, force=False):
        """Interface provides { LFN1 : { PFN1, SE1, ... }, LFN2 : { PFN2, SE2, ... } }"""
        return cls.transformationDB.addFile(fileDicts, force=force)

    types_removeFile = [[list, dict]]

    @classmethod
    def export_removeFile(cls, lfns):
        """Interface provides [ LFN1, LFN2, ... ]"""
        if isinstance(lfns, dict):
            lfns = list(lfns)
        return cls.transformationDB.removeFile(lfns)

    types_setMetadata = [str, dict]

    @classmethod
    def export_setMetadata(cls, path, querydict):
        """Set metadata to a file or to a directory (path)"""
        return cls.transformationDB.setMetadata(path, querydict)

    ####################################################################
    #
    # These are the methods used for web monitoring
    #

    # TODO Get rid of this (talk to Matvey)
    types_getDistinctAttributeValues = [str, dict]

    @classmethod
    def export_getDistinctAttributeValues(cls, attribute, selectDict):
        res = cls.transformationDB.getTableDistinctAttributeValues("Transformations", [attribute], selectDict)
        if not res["OK"]:
            return res
        return S_OK(res["Value"][attribute])

    types_getTableDistinctAttributeValues = [str, list, dict]

    @classmethod
    def export_getTableDistinctAttributeValues(cls, table, attributes, selectDict):
        return cls.transformationDB.getTableDistinctAttributeValues(table, attributes, selectDict)

    types_getTransformationStatusCounters = []

    @classmethod
    def export_getTransformationStatusCounters(cls):
        res = cls.transformationDB.getCounters("Transformations", ["Status"], {})
        if not res["OK"]:
            return res
        statDict = {}
        for attrDict, count in res["Value"]:
            statDict[attrDict["Status"]] = count
        return S_OK(statDict)

    types_getTransformationSummary = []

    def export_getTransformationSummary(self):
        """Get the summary of the currently existing transformations"""
        res = self.transformationDB.getTransformations()
        if not res["OK"]:
            return res
        transList = res["Value"]
        resultDict = {}
        for transDict in transList:
            transID = transDict["TransformationID"]
            res = self.transformationDB.getTransformationTaskStats(transID)
            if not res["OK"]:
                self.log.warn("Failed to get job statistics for transformation", transID)
                continue
            transDict["JobStats"] = res["Value"]
            res = self.transformationDB.getTransformationStats(transID)
            if not res["OK"]:
                transDict["NumberOfFiles"] = -1
            else:
                transDict["NumberOfFiles"] = res["Value"]["Total"]
            resultDict[transID] = transDict
        return S_OK(resultDict)

    types_getTabbedSummaryWeb = [str, dict, dict, list, int, int]

    def export_getTabbedSummaryWeb(self, table, requestedTables, selectDict, sortList, startItem, maxItems):
        tableDestinations = {
            "Transformations": {
                "TransformationFiles": ["TransformationID"],
                "TransformationTasks": ["TransformationID"],
            },
            "TransformationFiles": {
                "Transformations": ["TransformationID"],
                "TransformationTasks": ["TransformationID", "TaskID"],
            },
            "TransformationTasks": {
                "Transformations": ["TransformationID"],
                "TransformationFiles": ["TransformationID", "TaskID"],
            },
        }

        tableSelections = {
            "Transformations": ["TransformationID", "AgentType", "Type", "TransformationGroup", "Plugin"],
            "TransformationFiles": ["TransformationID", "TaskID", "Status", "UsedSE", "TargetSE"],
            "TransformationTasks": ["TransformationID", "TaskID", "ExternalStatus", "TargetSE"],
        }

        tableTimeStamps = {
            "Transformations": "CreationDate",
            "TransformationFiles": "LastUpdate",
            "TransformationTasks": "CreationTime",
        }

        tableStatusColumn = {
            "Transformations": "Status",
            "TransformationFiles": "Status",
            "TransformationTasks": "ExternalStatus",
        }

        resDict = {}
        res = self.__getTableSummaryWeb(
            table,
            selectDict,
            sortList,
            startItem,
            maxItems,
            selectColumns=tableSelections[table],
            timeStamp=tableTimeStamps[table],
            statusColumn=tableStatusColumn[table],
        )
        if not res["OK"]:
            self.log.error("Failed to get Summary for table", f"{table} {res['Message']}")
            return res
        resDict[table] = res["Value"]
        selections = res["Value"]["Selections"]
        tableSelection = {}
        for destination in tableDestinations[table].keys():
            tableSelection[destination] = {}
            for parameter in tableDestinations[table][destination]:
                tableSelection[destination][parameter] = selections.get(parameter, [])

        for table, paramDict in requestedTables.items():
            sortList = paramDict.get("SortList", [])
            startItem = paramDict.get("StartItem", 0)
            maxItems = paramDict.get("MaxItems", 50)
            res = self.__getTableSummaryWeb(
                table,
                tableSelection[table],
                sortList,
                startItem,
                maxItems,
                selectColumns=tableSelections[table],
                timeStamp=tableTimeStamps[table],
                statusColumn=tableStatusColumn[table],
            )
            if not res["OK"]:
                self.log.error("Failed to get Summary for table", f"{table} {res['Message']}")
                return res
            resDict[table] = res["Value"]
        return S_OK(resDict)

    types_getTransformationsSummaryWeb = [dict, list, int, int]

    def export_getTransformationsSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        return self.__getTableSummaryWeb(
            "Transformations",
            selectDict,
            sortList,
            startItem,
            maxItems,
            selectColumns=["TransformationID", "AgentType", "Type", "Group", "Plugin"],
            timeStamp="CreationDate",
            statusColumn="Status",
        )

    types_getTransformationTasksSummaryWeb = [dict, list, int, int]

    def export_getTransformationTasksSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        return self.__getTableSummaryWeb(
            "TransformationTasks",
            selectDict,
            sortList,
            startItem,
            maxItems,
            selectColumns=["TransformationID", "ExternalStatus", "TargetSE"],
            timeStamp="CreationTime",
            statusColumn="ExternalStatus",
        )

    types_getTransformationFilesSummaryWeb = [dict, list, int, int]

    def export_getTransformationFilesSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        return self.__getTableSummaryWeb(
            "TransformationFiles",
            selectDict,
            sortList,
            startItem,
            maxItems,
            selectColumns=["TransformationID", "Status", "UsedSE", "TargetSE"],
            timeStamp="LastUpdate",
            statusColumn="Status",
        )

    def __getTableSummaryWeb(
        self, table, selectDict, sortList, startItem, maxItems, selectColumns=[], timeStamp=None, statusColumn="Status"
    ):
        fromDate = selectDict.get("FromDate", None)
        if fromDate:
            del selectDict["FromDate"]
        # if not fromDate:
        #  fromDate = last_update
        toDate = selectDict.get("ToDate", None)
        if toDate:
            del selectDict["ToDate"]
        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = sortList[0][0] + ":" + sortList[0][1]
        else:
            orderAttribute = None
        # Get the columns that match the selection
        fcn = None
        fcnName = f"get{table}"
        if hasattr(self.transformationDB, fcnName) and callable(getattr(self.transformationDB, fcnName)):
            fcn = getattr(self.transformationDB, fcnName)
        if not fcn:
            return S_ERROR(f"Unable to invoke gTransformationDB.{fcnName}, it isn't a member function")
        res = fcn(condDict=selectDict, older=toDate, newer=fromDate, timeStamp=timeStamp, orderAttribute=orderAttribute)
        if not res["OK"]:
            return res

        # The full list of columns in contained here
        allRows = res["Records"]
        # Prepare the standard structure now within the resultDict dictionary
        resultDict = {}
        # Create the total records entry
        resultDict["TotalRecords"] = len(allRows)
        # Create the ParameterNames entry
        resultDict["ParameterNames"] = res["ParameterNames"]
        # Find which element in the tuple contains the requested status
        if statusColumn not in resultDict["ParameterNames"]:
            return S_ERROR("Provided status column not present")
        statusColumnIndex = resultDict["ParameterNames"].index(statusColumn)

        # Get the rows which are within the selected window
        if resultDict["TotalRecords"] == 0:
            return S_OK(resultDict)
        ini = startItem
        last = ini + maxItems
        if ini >= resultDict["TotalRecords"]:
            return S_ERROR("Item number out of range")
        if last > resultDict["TotalRecords"]:
            last = resultDict["TotalRecords"]
        selectedRows = allRows[ini:last]
        resultDict["Records"] = selectedRows

        # Generate the status dictionary
        statusDict = {}
        for row in selectedRows:
            status = row[statusColumnIndex]
            statusDict[status] = statusDict.setdefault(status, 0) + 1
        resultDict["Extras"] = statusDict

        # Obtain the distinct values of the selection parameters
        res = self.transformationDB.getTableDistinctAttributeValues(
            table, selectColumns, selectDict, older=toDate, newer=fromDate
        )
        distinctSelections = zip(selectColumns, [])
        if res["OK"]:
            distinctSelections = res["Value"]
        resultDict["Selections"] = distinctSelections

        return S_OK(resultDict)

    types_getTransformationSummaryWeb = [dict, list, int, int]

    def export_getTransformationSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        """Get the summary of the transformation information for a given page in the generic format"""

        # Obtain the timing information from the selectDict
        last_update = selectDict.get("CreationDate", None)
        if last_update:
            del selectDict["CreationDate"]
        fromDate = selectDict.get("FromDate", None)
        if fromDate:
            del selectDict["FromDate"]
        if not fromDate:
            fromDate = last_update
        toDate = selectDict.get("ToDate", None)
        if toDate:
            del selectDict["ToDate"]
        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = []
            for i in sortList:
                orderAttribute += [i[0] + ":" + i[1]]
        else:
            orderAttribute = None

        # Get the transformations that match the selection
        res = self.transformationDB.getTransformations(
            condDict=selectDict, older=toDate, newer=fromDate, orderAttribute=orderAttribute
        )
        if not res["OK"]:
            return res

        ops = Operations()
        # Prepare the standard structure now within the resultDict dictionary
        resultDict = {}
        trList = res["Records"]
        # Create the total records entry
        nTrans = len(trList)
        resultDict["TotalRecords"] = nTrans
        # Create the ParameterNames entry
        # As this list is a reference to the list in the DB, we cannot extend it, therefore copy it
        resultDict["ParameterNames"] = list(res["ParameterNames"])
        # Add the job states to the ParameterNames entry
        taskStateNames = TASKS_STATE_NAMES + ops.getValue("Transformations/AdditionalTaskStates", [])
        resultDict["ParameterNames"] += ["Jobs_" + x for x in taskStateNames]
        # Add the file states to the ParameterNames entry
        fileStateNames = FILES_STATE_NAMES + ops.getValue("Transformations/AdditionalFileStates", [])
        resultDict["ParameterNames"] += ["Files_" + x for x in fileStateNames]

        # Get the transformations which are within the selected window
        if nTrans == 0:
            return S_OK(resultDict)
        ini = startItem
        last = ini + maxItems
        if ini >= nTrans:
            return S_ERROR("Item number out of range")
        if last > nTrans:
            last = nTrans
        transList = trList[ini:last]

        statusDict = {}
        extendableTranfs = ops.getValue("Transformations/ExtendableTransfTypes", ["Simulation", "MCsimulation"])
        givenUpFileStatus = ops.getValue("Transformations/GivenUpFileStatus", ["MissingInFC"])
        problematicStatuses = ops.getValue("Transformations/ProblematicStatuses", ["Problematic"])
        # Add specific information for each selected transformation
        for trans in transList:
            transDict = dict(zip(resultDict["ParameterNames"], trans))

            # Update the status counters
            status = transDict["Status"]
            statusDict[status] = statusDict.setdefault(status, 0) + 1

            # Get the statistics on the number of jobs for the transformation
            transID = transDict["TransformationID"]
            res = self.transformationDB.getTransformationTaskStats(transID)
            taskDict = {}
            if res["OK"] and res["Value"]:
                taskDict = res["Value"]
            for state in taskStateNames:
                trans.append(taskDict.get(state, 0))

            # Get the statistics for the number of files for the transformation
            fileDict = {}
            transType = transDict["Type"]
            if transType.lower() in extendableTranfs:
                fileDict["PercentProcessed"] = "-"
            else:
                res = self.transformationDB.getTransformationStats(transID)
                if res["OK"]:
                    fileDict = res["Value"]
                    total = fileDict["Total"]
                    for stat in givenUpFileStatus:
                        total -= fileDict.get(stat, 0)
                    processed = fileDict.get(TransformationFilesStatus.PROCESSED, 0)
                    fileDict["PercentProcessed"] = f"{int(processed * 1000.0 / total) / 10.0:.1f}" if total else 0.0
            problematic = 0
            for stat in problematicStatuses:
                problematic += fileDict.get(stat, 0)
            fileDict["Problematic"] = problematic
            for state in fileStateNames:
                trans.append(fileDict.get(state, 0))

        resultDict["Records"] = transList
        resultDict["Extras"] = statusDict
        return S_OK(resultDict)


class TransformationManagerHandler(TransformationManagerHandlerMixin, RequestHandler):
    pass
