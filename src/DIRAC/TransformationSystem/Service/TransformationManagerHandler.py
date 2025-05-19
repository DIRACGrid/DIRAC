""" Service for interacting with TransformationDB
"""
import datetime

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security.Properties import SecurityProperty
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import encode as jencode
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


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
        author = tfDetails["Value"]["Author"]
        if SecurityProperty.PRODUCTION_SHARING in groupProperties:
            if authorGroup == credDict.get("group", None):
                return S_OK()
        if SecurityProperty.PRODUCTION_USER in groupProperties:
            if author == credDict.get("username", None):
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
        author = credDict.get("username")
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
            author,
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
        author = credDict.get("username")
        return self.transformationDB.deleteTransformation(transName, author=author)

    types_completeTransformation = [[int, str]]

    def export_completeTransformation(self, transName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.transformationDB.setTransformationParameter(transName, "Status", "Completed", author=author)

    types_cleanTransformation = [[int, str]]

    def export_cleanTransformation(self, transName):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.transformationDB.cleanTransformation(transName, author=author)

    types_setTransformationParameter = [[int, str], str]

    def export_setTransformationParameter(self, transName, paramName, paramValue):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.transformationDB.setTransformationParameter(transName, paramName, paramValue, author=author)

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

    @classmethod
    def export_getTransformationWithStatus(cls, status):
        return cls.transformationDB.getTransformationWithStatus(status)

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
        columns=None,
    ):
        if not condDict:
            condDict = {}
        result = self.transformationDB.getTransformationFiles(
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            offset=offset,
            connection=False,
            columns=columns,
        )

        # DEncode cannot cope with nested structures of multiple millions items.
        # Encode everything as a json string, that DEncode can then transmit faster.

        return S_OK(jencode(result))

    types_getTransformationFilesAsJsonString = types_getTransformationFiles

    @deprecated("Use getTransformationFiles instead")
    def export_getTransformationFilesAsJsonString(self, *args, **kwargs):
        """
        Deprecated call -- redirect to getTransformationFiles
        """
        return self.export_getTransformationFiles(*args, **kwargs)

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
        author = credDict.get("username")
        return self.transformationDB.deleteTasks(transName, taskMin, taskMax, author=author)

    types_extendTransformation = [[int, str], int]

    def export_extendTransformation(self, transName, nTasks):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.transformationDB.extendTransformation(transName, nTasks, author=author)

    types_getTasksToSubmit = [[int, str], int]

    def export_getTasksToSubmit(self, transName, numTasks, site=""):
        """
        Retrieve the necessary information for the submission of a specified number of tasks
        for a given transformation. This includes reserving tasks to avoid race conditions.

        :param int | str transName: Name of the transformation
        :param int numTasks: Number of tasks to retrieve for submission
        :param str site: Optional site specification
        :return: S_OK Dictionary containing transformation and task submission details
        """
        # Get the transformation details
        res = self.transformationDB.getTransformation(transName)
        if not res["OK"]:
            return res
        transDict = res["Value"]

        submitDict = {}

        # Apply a delay to avoid race conditions
        older = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)

        # Retrieve tasks that are ready for submission
        res = self.transformationDB.getTasksForSubmission(
            transName, numTasks=numTasks, site=site, statusList=["Created"], older=older
        )
        if not res["OK"]:
            return res
        tasksDict = res["Value"]

        # Reserve each task for submission
        for taskID, taskDict in tasksDict.items():
            res = self.transformationDB.reserveTask(transName, int(taskID))
            if not res["OK"]:
                return res
            # Add reserved task to the submission dictionary
            submitDict[taskID] = taskDict

        # Add the job dictionary to the transformation details
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
        author = credDict.get("username")
        return self.transformationDB.createTransformationMetaQuery(transName, queryDict, queryType, author=author)

    types_deleteTransformationMetaQuery = [[int, str], str]

    def export_deleteTransformationMetaQuery(self, transName, queryType):
        if not (result := self.checkPermissions(transName))["OK"]:
            return result
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.transformationDB.deleteTransformationMetaQuery(transName, queryType, author=author)

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

    types_getTableDistinctAttributeValues = [str, list, dict]

    @classmethod
    def export_getTableDistinctAttributeValues(cls, table, attributes, selectDict):
        return cls.transformationDB.getTableDistinctAttributeValues(table, attributes, selectDict)

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


class TransformationManagerHandler(TransformationManagerHandlerMixin, RequestHandler):
    pass
