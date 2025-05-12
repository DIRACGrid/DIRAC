"""A generic client for creating and managing transformations.

See the information about transformation parameters below.
"""
import json

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.API import API
from DIRAC.Core.Utilities.JEncode import encode
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody import BaseBody
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


class Transformation(API):
    #############################################################################
    def __init__(self, transID=0, transClient=None):
        """c'tor"""
        super().__init__()

        self.paramTypes = {
            "TransformationID": int,
            "TransformationName": str,
            "Status": str,
            "Description": str,
            "LongDescription": str,
            "Type": str,
            "Plugin": str,
            "AgentType": str,
            "FileMask": str,
            "TransformationGroup": str,
            "GroupSize": (
                int,
                float,
            ),
            "InheritedFrom": int,
            "Body": str,
            "MaxNumberOfTasks": int,
            "EventsPerTask": int,
        }
        self.paramValues = {
            "TransformationID": 0,
            "TransformationName": "",
            "Status": "New",
            "Description": "",
            "LongDescription": "",
            "Type": "",
            "Plugin": "Standard",
            "AgentType": "Manual",
            "FileMask": "",
            "TransformationGroup": "General",
            "GroupSize": 1,
            "InheritedFrom": 0,
            "Body": "",
            "MaxNumberOfTasks": 0,
            "EventsPerTask": 0,
        }

        # the metaquery parameters are neither part of the transformation parameters nor the additional parameters, so
        # special treatment is necessary
        self.inputMetaQuery = None
        self.outputMetaQuery = None

        self.ops = Operations()
        self.supportedPlugins = self.ops.getValue(
            "Transformations/AllowedPlugins", ["Broadcast", "Standard", "BySize", "ByShare"]
        )
        if not transClient:
            self.transClient = TransformationClient()
        else:
            self.transClient = transClient
        self.serverURL = self.transClient.getServer()
        self.exists = False
        if transID:
            self.paramValues["TransformationID"] = transID
            res = self.getTransformation()
            if res["OK"]:
                self.exists = True
            elif res["Message"] == "Transformation does not exist":
                raise AttributeError("TransformationID %d does not exist" % transID)
            else:
                self.paramValues["TransformationID"] = 0
                gLogger.fatal("Failed to get transformation from database", f"{transID} @ {self.transClient.serverURL}")

    def getServer(self):
        return self.serverURL

    def reset(self, transID=0):
        self.__init__(transID)
        self.transClient.setServer(self.serverURL)
        return S_OK()

    def setTargetSE(self, seList):
        return self.__setSE("TargetSE", seList)

    def setSourceSE(self, seList):
        return self.__setSE("SourceSE", seList)

    def setBody(self, body):
        """check that the body is a string, or using the proper syntax for multiple operations,
        or is a BodyPlugin object

        :param body: transformation body, for example

          .. code :: python

            body = [ ( "ReplicateAndRegister", { "SourceSE":"FOO-SRM", "TargetSE":"BAR-SRM" }),
                     ( "RemoveReplica", { "TargetSE":"FOO-SRM" } ),
                   ]

        :type body: string or list of tuples (or lists) of string and dictionaries or a Body plugin (:py:class:`DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody.BaseBody`)
        :raises TypeError: If the structure is not as expected
        :raises ValueError: If unknown attribute for the :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
                            is used
        :returns: S_OK, S_ERROR
        """
        self.item_called = "Body"

        # Simple single operation body case
        if isinstance(body, str):
            return self.__setParam(body)

        # BodyPlugin case
        elif isinstance(body, BaseBody):
            return self.__setParam(encode(body))

        if not isinstance(body, (list, tuple)):
            raise TypeError(f"Expected list or string, but {body!r} is {type(body)}")

        # MultiOperation body case
        for tup in body:
            if not isinstance(tup, (tuple, list)):
                raise TypeError(f"Expected tuple or list, but {tup!r} is {type(tup)}")
            if len(tup) != 2:
                raise TypeError(f"Expected 2-tuple, but {tup!r} is length {len(tup)}")
            if not isinstance(tup[0], str):
                raise TypeError(f"Expected string, but first entry in tuple {tup!r} is {type(tup[0])}")
            if not isinstance(tup[1], dict):
                raise TypeError(f"Expected dictionary, but second entry in tuple {tup!r} is {type(tup[0])}")
            for par, val in tup[1].items():
                if not isinstance(par, str):
                    raise TypeError(f"Expected string, but key in dictionary {par!r} is {type(par)}")
                if par not in Operation.ATTRIBUTE_NAMES:
                    raise ValueError(f"Unknown attribute for Operation: {par}")
                if not isinstance(val, (str, int, float, list, tuple, dict)):
                    raise TypeError(f"Cannot encode {val!r}, in json")
        return self.__setParam(json.dumps(body))

    def setInputMetaQuery(self, query):
        """Set the input meta query.

        :param dict query: dictionary to use for input meta query
        """
        self.inputMetaQuery = query
        return S_OK()

    def setOutputMetaQuery(self, query):
        """Set the output meta query.

        :param dict query: dictionary to use for output meta query
        """
        self.outputMetaQuery = query
        return S_OK()

    def __setSE(self, seParam, seList):
        if isinstance(seList, str):
            try:
                seList = eval(seList)
            except Exception:
                seList = seList.split(",")
        elif isinstance(seList, (list, dict, tuple)):
            seList = list(seList)
        else:
            return S_ERROR("Bad argument type")
        res = self.__checkSEs(seList)
        if not res["OK"]:
            return res
        self.item_called = seParam
        return self.__setParam(seList)

    def __getattr__(self, name):
        if name.find("get") == 0:
            item = name[3:]
            self.item_called = item
            return self.__getParam
        if name.find("set") == 0:
            item = name[3:]
            self.item_called = item
            return self.__setParam
        raise AttributeError(name)

    def __getParam(self):
        if self.item_called == "Available":
            return S_OK(list(self.paramTypes))
        if self.item_called == "Parameters":
            return S_OK(self.paramValues)
        if self.item_called in self.paramValues:
            return S_OK(self.paramValues[self.item_called])
        raise AttributeError(f"Unknown parameter for transformation: {self.item_called}")

    def __setParam(self, value):
        change = False
        if self.item_called in self.paramTypes:
            if self.paramValues[self.item_called] != value:
                if isinstance(value, self.paramTypes[self.item_called]):
                    change = True
                else:
                    raise TypeError(
                        "%s %s %s expected one of %s"
                        % (self.item_called, value, type(value), self.paramTypes[self.item_called])
                    )
        else:
            if self.item_called not in self.paramValues:
                change = True
            else:
                if self.paramValues[self.item_called] != value:
                    change = True
        if not change:
            gLogger.verbose(f"No change of parameter {self.item_called} required")
        else:
            gLogger.verbose(f"Parameter {self.item_called} to be changed")
            transID = self.paramValues["TransformationID"]
            if self.exists and transID:
                res = self.transClient.setTransformationParameter(transID, self.item_called, value)
                if not res["OK"]:
                    return res
            self.paramValues[self.item_called] = value
        return S_OK()

    def getTransformation(self, printOutput=False):
        transID = self.paramValues["TransformationID"]
        if not transID:
            gLogger.fatal("No TransformationID known")
            return S_ERROR()
        res = self.transClient.getTransformation(transID, extraParams=True)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        transParams = res["Value"]
        for paramName, paramValue in transParams.items():
            setter = None
            setterName = f"set{paramName}"
            if hasattr(self, setterName) and callable(getattr(self, setterName)):
                setter = getattr(self, setterName)
            if not setterName:
                gLogger.error(f"Unable to invoke setter {setterName}, it isn't a member function")
                continue
            setter(paramValue)
        if printOutput:
            gLogger.info("No printing available yet")
        return S_OK(transParams)

    def getTransformationLogging(self, printOutput=False):
        transID = self.paramValues["TransformationID"]
        if not transID:
            gLogger.fatal("No TransformationID known")
            return S_ERROR()
        res = self.transClient.getTransformationLogging(transID)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        loggingList = res["Value"]
        if printOutput:
            self._printFormattedDictList(
                loggingList, ["Message", "MessageDate", "Author"], "MessageDate", "MessageDate"
            )
        return S_OK(loggingList)

    def extendTransformation(self, nTasks, printOutput=False):
        return self.__executeOperation("extendTransformation", nTasks, printOutput=printOutput)

    def cleanTransformation(self, printOutput=False):
        res = self.__executeOperation("cleanTransformation", printOutput=printOutput)
        if res["OK"]:
            self.paramValues["Status"] = "Cleaned"
        return res

    def deleteTransformation(self, printOutput=False):
        res = self.__executeOperation("deleteTransformation", printOutput=printOutput)
        if res["OK"]:
            self.reset()
        return res

    def addFilesToTransformation(self, lfns, printOutput=False):
        return self.__executeOperation("addFilesToTransformation", lfns, printOutput=printOutput)

    def setFileStatusForTransformation(self, status, lfns, printOutput=False):
        return self.__executeOperation("setFileStatusForTransformation", status, lfns, printOutput=printOutput)

    def getTransformationTaskStats(self, printOutput=False):
        return self.__executeOperation("getTransformationTaskStats", printOutput=printOutput)

    def getTransformationStats(self, printOutput=False):
        return self.__executeOperation("getTransformationStats", printOutput=printOutput)

    def deleteTasks(self, taskMin, taskMax, printOutput=False):
        return self.__executeOperation("deleteTasks", taskMin, taskMax, printOutput=printOutput)

    def addTaskForTransformation(self, lfns=[], se="Unknown", printOutput=False):
        return self.__executeOperation("addTaskForTransformation", lfns, se, printOutput=printOutput)

    def setTaskStatus(self, taskID, status, printOutput=False):
        return self.__executeOperation("setTaskStatus", taskID, status, printOutput=printOutput)

    def __executeOperation(self, operation, *parms, **kwds):
        transID = self.paramValues["TransformationID"]
        if not transID:
            gLogger.fatal("No TransformationID known")
            return S_ERROR()
        printOutput = kwds.pop("printOutput")
        fcn = None
        if hasattr(self.transClient, operation) and callable(getattr(self.transClient, operation)):
            fcn = getattr(self.transClient, operation)
        if not fcn:
            return S_ERROR("Unable to invoke %s, it isn't a member funtion of TransformationClient")
        res = fcn(transID, *parms, **kwds)
        if printOutput:
            self._prettyPrint(res)
        return res

    def getTransformationFiles(
        self,
        fileStatus=[],
        lfns=[],
        outputFields=[
            "FileID",
            "LFN",
            "Status",
            "TaskID",
            "TargetSE",
            "UsedSE",
            "ErrorCount",
            "InsertedTime",
            "LastUpdate",
        ],
        orderBy="FileID",
        printOutput=False,
    ):
        condDict = {"TransformationID": self.paramValues["TransformationID"]}
        if fileStatus:
            condDict["Status"] = fileStatus
        if lfns:
            condDict["LFN"] = lfns
        res = self.transClient.getTransformationFiles(condDict=condDict)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        if printOutput:
            if not outputFields:
                gLogger.info(f"Available fields are: {res['ParameterNames'].join(' ')}")
            elif not res["Value"]:
                gLogger.info("No tasks found for selection")
            else:
                self._printFormattedDictList(res["Value"], outputFields, "FileID", orderBy)
        return res

    def getTransformationTasks(
        self,
        taskStatus=[],
        taskIDs=[],
        outputFields=[
            "TransformationID",
            "TaskID",
            "ExternalStatus",
            "ExternalID",
            "TargetSE",
            "CreationTime",
            "LastUpdateTime",
        ],
        orderBy="TaskID",
        printOutput=False,
    ):
        condDict = {"TransformationID": self.paramValues["TransformationID"]}
        if taskStatus:
            condDict["ExternalStatus"] = taskStatus
        if taskIDs:
            condDict["TaskID"] = taskIDs
        res = self.transClient.getTransformationTasks(condDict=condDict)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        if printOutput:
            if not outputFields:
                gLogger.info(f"Available fields are: {res['ParameterNames'].join(' ')}")
            elif not res["Value"]:
                gLogger.info("No tasks found for selection")
            else:
                self._printFormattedDictList(res["Value"], outputFields, "TaskID", orderBy)
        return res

    #############################################################################
    def getTransformations(
        self,
        transID=[],
        transStatus=[],
        outputFields=["TransformationID", "Status", "AgentType", "TransformationName", "CreationDate"],
        orderBy="TransformationID",
        printOutput=False,
    ):
        condDict = {}
        if transID:
            condDict["TransformationID"] = transID
        if transStatus:
            condDict["Status"] = transStatus
        res = self.transClient.getTransformations(condDict=condDict)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        if printOutput:
            if not outputFields:
                gLogger.info(f"Available fields are: {res['ParameterNames'].join(' ')}")
            elif not res["Value"]:
                gLogger.info("No tasks found for selection")
            else:
                self._printFormattedDictList(res["Value"], outputFields, "TransformationID", orderBy)
        return res

    #############################################################################
    def getTransformationsByUser(
        self,
        userName="",
        transID=[],
        transStatus=[],
        outputFields=["TransformationID", "Status", "AgentType", "TransformationName", "CreationDate", "Author"],
        orderBy="TransformationID",
        printOutput=False,
    ):
        condDict = {}
        gLogger.info(f"Will list transformations created by user '{userName}' with status '{transStatus}'")
        condDict["Author"] = userName
        if transID:
            condDict["TransformationID"] = transID
        if transStatus:
            condDict["Status"] = transStatus
        res = self.transClient.getTransformations(condDict=condDict)
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res

        if printOutput:
            if not outputFields:
                gLogger.info(f"Available fields are: {res['ParameterNames'].join(' ')}")
            elif not res["Value"]:
                gLogger.info("No tasks found for selection")
            else:
                self._printFormattedDictList(res["Value"], outputFields, "TransformationID", orderBy)
        return res

    #############################################################################
    def getSummaryTransformations(self, transID=[]):
        """Show the summary for a list of Transformations

        Fields starting with 'F' ('J')  refers to files (jobs).
        Proc. stand for processed.
        """
        condDict = {"TransformationID": transID}
        orderby = []
        start = 0
        maxitems = len(transID)
        paramShowNames = [
            "TransformationID",
            "Type",
            "Status",
            "Files_Total",
            "Files_PercentProcessed",
            "Files_Processed",
            "Files_Unused",
            "Jobs_TotalCreated",
            "Jobs_Waiting",
            "Jobs_Running",
            "Jobs_Done",
            "Jobs_Failed",
            "Jobs_Stalled",
        ]
        # Below, the header used for each field in the printing: short to fit in one line
        paramShowNamesShort = [
            "TransID",
            "Type",
            "Status",
            "F_Total",
            "F_Proc.(%)",
            "F_Proc.",
            "F_Unused",
            "J_Created",
            "J_Wait",
            "J_Run",
            "J_Done",
            "J_Fail",
            "J_Stalled",
        ]
        dictList = []

        result = WebAppClient().getTransformationSummaryWeb(condDict, orderby, start, maxitems)
        if not result["OK"]:
            self._prettyPrint(result)
            return result

        if result["Value"]["TotalRecords"] > 0:
            try:
                paramNames = result["Value"]["ParameterNames"]
                for paramValues in result["Value"]["Records"]:
                    paramShowValues = map(lambda pname: paramValues[paramNames.index(pname)], paramShowNames)
                    showDict = dict(zip(paramShowNamesShort, paramShowValues))
                    dictList.append(showDict)

            except Exception as x:
                print(f"Exception {str(x)} ")

        if not dictList:
            gLogger.error("No found transformations satisfying input condition")
            return S_ERROR("No found transformations satisfying input condition")

        print(
            self._printFormattedDictList(dictList, paramShowNamesShort, paramShowNamesShort[0], paramShowNamesShort[0])
        )

        return S_OK(dictList)

    #############################################################################
    def addTransformation(self, addFiles=True, printOutput=False):
        """Add transformation to the transformation system.

        Sets all parameters currently assigned to the transformation.

        :param bool addFiles: if True, immediately perform input data query
        :param bool printOutput: if True, print information about transformation
        """
        res = self._checkCreation()
        if not res["OK"]:
            return self._errorReport(res, "Failed transformation sanity check")
        if printOutput:
            gLogger.info("Will attempt to create transformation with the following parameters")
            self._prettyPrint(self.paramValues)

        res = self.transClient.addTransformation(
            self.paramValues["TransformationName"],
            self.paramValues["Description"],
            self.paramValues["LongDescription"],
            self.paramValues["Type"],
            self.paramValues["Plugin"],
            self.paramValues["AgentType"],
            self.paramValues["FileMask"],
            transformationGroup=self.paramValues["TransformationGroup"],
            groupSize=self.paramValues["GroupSize"],
            inheritedFrom=self.paramValues["InheritedFrom"],
            body=self.paramValues["Body"],
            maxTasks=self.paramValues["MaxNumberOfTasks"],
            eventsPerTask=self.paramValues["EventsPerTask"],
            addFiles=addFiles,
            inputMetaQuery=self.inputMetaQuery,
            outputMetaQuery=self.outputMetaQuery,
        )
        if not res["OK"]:
            if printOutput:
                self._prettyPrint(res)
            return res
        transID = res["Value"]
        self.exists = True
        self.setTransformationID(transID)
        gLogger.notice("Created transformation %d" % transID)
        for paramName, paramValue in self.paramValues.items():
            if paramName not in self.paramTypes:
                res = self.transClient.setTransformationParameter(transID, paramName, paramValue)
                if not res["OK"]:
                    gLogger.error("Failed to add parameter", f"{paramName} {res['Message']}")
                    gLogger.notice("To add this parameter later please execute the following.")
                    gLogger.notice("oTransformation = Transformation(%d)" % transID)
                    gLogger.notice(f"oTransformation.set{paramName}(...)")
        return S_OK(transID)

    def _checkCreation(self):
        """Few checks"""
        if self.paramValues["TransformationID"]:
            gLogger.info("You are currently working with an active transformation definition.")
            gLogger.info("If you wish to create a new transformation reset the TransformationID.")
            gLogger.info("oTransformation.reset()")
            return S_ERROR()

        requiredParameters = ["TransformationName", "Description", "LongDescription", "Type"]
        for parameter in requiredParameters:
            if not self.paramValues[parameter]:
                gLogger.info(f"{parameter} is not defined for this transformation. This is required...")
                self.paramValues[parameter] = input("Please enter the value of " + parameter + " ")

        plugin = self.paramValues["Plugin"]
        if plugin:
            if plugin not in self.supportedPlugins:
                gLogger.info(f"The selected Plugin ({plugin}) is not known to the transformation agent.")
                res = self.__promptForParameter("Plugin", choices=self.supportedPlugins, default="Standard")
                if not res["OK"]:
                    return res
                self.paramValues["Plugin"] = res["Value"]

        plugin = self.paramValues["Plugin"]

        return S_OK()

    def _checkBySizePlugin(self):
        return self._checkStandardPlugin()

    def _checkBySharePlugin(self):
        return self._checkStandardPlugin()

    def _checkStandardPlugin(self):
        groupSize = self.paramValues["GroupSize"]
        if groupSize <= 0:
            gLogger.info("The GroupSize was found to be less than zero. It has been set to 1.")
            res = self.setGroupSize(1)
            if not res["OK"]:
                return res
        return S_OK()

    def _checkBroadcastPlugin(self):
        gLogger.info(
            f"The Broadcast plugin requires the following parameters be set: {', '.join(['SourceSE', 'TargetSE'])}"
        )
        requiredParams = ["SourceSE", "TargetSE"]
        for requiredParam in requiredParams:
            if not self.paramValues.get(requiredParam):
                paramValue = input("Please enter " + requiredParam + " ")
                setter = None
                setterName = f"set{requiredParam}"
                if hasattr(self, setterName) and callable(getattr(self, setterName)):
                    setter = getattr(self, setterName)
                if not setter:
                    return S_ERROR(f"Unable to invoke {setterName}, this function hasn't been implemented.")
                ses = paramValue.replace(",", " ").split()
                res = setter(ses)
                if not res["OK"]:
                    return res
        return S_OK()

    def __checkSEs(self, seList):
        res = gConfig.getSections("/Resources/StorageElements")
        if not res["OK"]:
            return self._errorReport(res, "Failed to get possible StorageElements")
        missing = set(seList) - set(res["Value"])
        if missing:
            for se in missing:
                gLogger.error(f"StorageElement {se} is not known")
            return S_ERROR(f"{len(missing)} StorageElements not known")
        return S_OK()

    def __promptForParameter(self, parameter, choices=[], default="", insert=True):
        res = promptUser(f"Please enter {parameter}", choices=choices, default=default)
        if not res["OK"]:
            return self._errorReport(res)
        gLogger.notice(f"{parameter} will be set to '{res['Value']}'")
        paramValue = res["Value"]
        if insert:
            setter = None
            setterName = f"set{parameter}"
            if hasattr(self, setterName) and callable(getattr(self, setterName)):
                setter = getattr(self, setterName)
            if not setter:
                return S_ERROR("Unable to invoke %s, it isn't a member function of Transformation!")
            res = setter(paramValue)
            if not res["OK"]:
                return res
        return S_OK(paramValue)
