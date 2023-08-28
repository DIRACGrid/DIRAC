""" Executor Module

    Just provides a number of functions used by all executors
"""
import os
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, rootPath
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure


class ExecutorModule:
    """All executors should inherit from this module"""

    @classmethod
    def _ex_initialize(cls, exeName, loadName):
        cls.__properties = {
            "fullName": exeName,
            "loadName": loadName,
            "section": PathFinder.getExecutorSection(exeName),
            "loadSection": PathFinder.getExecutorSection(loadName),
            "messagesProcessed": 0,
            "reconnects": 0,
            "setup": gConfig.getValue("/DIRAC/Setup", "Unknown"),
        }
        cls.__defaults = {}
        cls.__defaults["MonitoringEnabled"] = True
        cls.__defaults["Enabled"] = True
        cls.__defaults["ControlDirectory"] = os.path.join(rootPath, "control", *exeName.split("/"))
        cls.__defaults["WorkDirectory"] = os.path.join(rootPath, "work", *exeName.split("/"))
        cls.__defaults["ReconnectRetries"] = 10
        cls.__defaults["ReconnectSleep"] = 5
        cls.__defaults["shifterProxy"] = ""
        cls.__defaults["shifterProxyLocation"] = os.path.join(cls.__defaults["WorkDirectory"], ".shifterCred")
        cls.__properties["shifterProxy"] = ""
        cls.__properties["shifterProxyLocation"] = os.path.join(cls.__defaults["WorkDirectory"], ".shifterCred")
        cls.__mindName = False
        cls.__mindExtraArgs = False
        cls.__freezeTime = 0
        cls.__fastTrackEnabled = True
        cls.log = gLogger.getSubLogger(exeName)

        try:
            result = cls.initialize()  # pylint: disable=no-member
        except Exception as excp:
            gLogger.exception(f"Exception while initializing {loadName}", lException=excp)
            return S_ERROR(f"Exception while initializing: {str(excp)}")
        if not isReturnStructure(result):
            return S_ERROR(f"Executor {loadName} does not return an S_OK/S_ERROR after initialization")
        return result

    def __installShifterProxy(self):
        shifterProxy = self.ex_getProperty("shifterProxy")
        if not shifterProxy:
            return S_OK()
        location = f"{self.ex_getProperty('shifterProxyLocation')}-{shifterProxy}"
        result = setupShifterProxyInEnv(shifterProxy, location)
        if not result["OK"]:
            self.log.error(f"Cannot set shifter proxy: {result['Message']}")
        return result

    @classmethod
    def ex_setOption(cls, optName, value):
        cls.__defaults[optName] = value

    @classmethod
    def ex_getOption(cls, optName, defaultValue=None):
        if defaultValue is None:
            if optName in cls.__defaults:
                defaultValue = cls.__defaults[optName]
        if optName and optName[0] == "/":
            return gConfig.getValue(optName, defaultValue)
        for section in (cls.__properties["section"], cls.__properties["loadSection"]):
            result = gConfig.getOption(f"{section}/{optName}", defaultValue)
            if result["OK"]:
                return result["Value"]
        return defaultValue

    @classmethod
    def ex_setProperty(cls, optName, value):
        cls.__properties[optName] = value

    @classmethod
    def ex_getProperty(cls, optName):
        return cls.__properties[optName]

    @classmethod
    def ex_enabled(cls):
        return cls.ex_getOption("Enabled")

    @classmethod
    def ex_setMind(cls, mindName, **extraArgs):
        cls.__mindName = mindName
        cls.__mindExtraArgs = extraArgs

    @classmethod
    def ex_getMind(cls):
        return cls.__mindName

    @classmethod
    def ex_getExtraArguments(cls):
        return cls.__mindExtraArgs

    def __serialize(self, taskId, taskObj):
        try:
            result = self.serializeTask(taskObj)
        except Exception as excp:
            gLogger.exception(f"Exception while serializing task {taskId}", lException=excp)
            return S_ERROR(f"Cannot serialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("serializeTask does not return a return structure")
        return result

    def __deserialize(self, taskId, taskStub):
        try:
            result = self.deserializeTask(taskStub)
        except Exception as excp:
            gLogger.exception(f"Exception while deserializing task {taskId}", lException=excp)
            return S_ERROR(f"Cannot deserialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("deserializeTask does not return a return structure")
        return result

    def _ex_processTask(self, taskId, taskStub):
        self.__properties["shifterProxy"] = self.ex_getOption("shifterProxy")
        self.__freezeTime = 0
        self.__fastTrackEnabled = True
        self.log.verbose(f"Task {str(taskId)}: Received")
        result = self.__deserialize(taskId, taskStub)
        if not result["OK"]:
            self.log.error("Can not deserialize task", f"Task {str(taskId)}: {result['Message']}")
            return result
        taskObj = result["Value"]
        # Shifter proxy?
        result = self.__installShifterProxy()
        if not result["OK"]:
            return result
        # Execute!
        result = self.processTask(taskId, taskObj)
        if not isReturnStructure(result):
            raise Exception("processTask does not return a return structure")
        if not result["OK"]:
            return result
        # If there's a result, serialize it again!
        if result["Value"]:
            taskObj = result["Value"]
        # Serialize again
        result = self.__serialize(taskId, taskObj)
        if not result["OK"]:
            self.log.verbose(f"Task {str(taskId)}: Cannot serialize: {result['Message']}")
            return result
        taskStub = result["Value"]
        # Try fast track
        fastTrackType = False
        if not self.__freezeTime and self.__fastTrackEnabled:
            result = self.fastTrackDispatch(taskId, taskObj)
            if not result["OK"]:
                self.log.error("FastTrackDispatch failed for job", f"{taskId}: {result['Message']}")
            else:
                fastTrackType = result["Value"]

        # EOP
        return S_OK((taskStub, self.__freezeTime, fastTrackType))

    ####
    # Callable functions
    ####

    def freezeTask(self, freezeTime):
        self.__freezeTime = freezeTime

    def isTaskFrozen(self):
        return self.__freezeTime

    def disableFastTrackForTask(self):
        self.__fastTrackEnabled = True

    ###
    #  Fast-track tasks
    ###

    def fastTrackDispatch(self, taskId, taskObj):
        return S_OK()

    ####
    # Need to overwrite this functions
    ####

    def serializeTask(self, taskObj):
        raise Exception("Method serializeTask has to be coded!")

    def deserializeTask(self, taskStub):
        raise Exception("Method deserializeTask has to be coded!")

    def processTask(self, taskId, taskObj):
        raise Exception("Method processTask has to be coded!")
