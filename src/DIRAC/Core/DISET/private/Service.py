"""
  Service class implements the server side part of the DISET protocol
  There are 2 main parts in this class:

  - All useful functions for initialization
  - All useful functions to handle the requests
"""
# pylint: skip-file
# __searchInitFunctions gives RuntimeError: maximum recursion depth exceeded

import os
import time
import datetime
import threading
import psutil

from concurrent.futures import ThreadPoolExecutor

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.DISET.private.TransportPool import getGlobalTransportPool
from DIRAC.Core.DISET.private.MessageBroker import MessageBroker, MessageSender
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.RequestHandler import getServiceOption
from DIRAC.Core.Utilities import Network, TimeUtilities
from DIRAC.Core.Utilities.DErrno import ENOAUTH
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.FrameworkSystem.Client.SecurityLogClient import SecurityLogClient


class Service:

    SVC_VALID_ACTIONS = {"RPC": "export", "FileTransfer": "transfer", "Message": "msg", "Connection": "Message"}
    SVC_SECLOG_CLIENT = SecurityLogClient()

    def __init__(self, serviceData):
        """
        Init the variables for the service

        :param serviceData: dict with modName, standalone, loadName, moduleObj, classObj. e.g.:
          {'modName': 'Framework/serviceName',
          'standalone': True,
          'loadName': 'Framework/serviceName',
          'moduleObj': <module 'serviceNameHandler' from '/home/DIRAC/FrameworkSystem/Service/serviceNameHandler.pyo'>,
          'classObj': <class 'serviceNameHandler.serviceHandler'>}

        """
        self._svcData = serviceData
        self._name = serviceData["modName"]
        self._startTime = datetime.datetime.utcnow()
        self._validNames = [serviceData["modName"]]
        if serviceData["loadName"] not in self._validNames:
            self._validNames.append(serviceData["loadName"])
        self._cfg = ServiceConfiguration(list(self._validNames))
        self._standalone = serviceData["standalone"]
        self.__monitorLastStatsUpdate = time.time()
        self._stats = {"queries": 0, "connections": 0}
        self._authMgr = AuthManager("%s/Authorization" % PathFinder.getServiceSection(serviceData["loadName"]))
        self._transportPool = getGlobalTransportPool()
        self.__cloneId = 0
        self.__maxFD = 0
        self.activityMonitoring = False
        # Check if monitoring is enabled
        if "Monitoring" in Operations().getMonitoringBackends(monitoringType="ServiceMonitoring"):
            self.activityMonitoring = True

    def setCloneProcessId(self, cloneId):
        self.__cloneId = cloneId

    def _isMetaAction(self, action):
        referedAction = Service.SVC_VALID_ACTIONS[action]
        if referedAction in Service.SVC_VALID_ACTIONS:
            return referedAction
        return False

    def initialize(self):
        # Build the URLs
        self._url = self._cfg.getURL()
        if not self._url:
            return S_ERROR("Could not build service URL for %s" % self._name)
        gLogger.verbose("Service URL is %s" % self._url)
        # Load handler
        result = self._loadHandlerInit()
        if not result["OK"]:
            return result
        self._handler = result["Value"]
        # Initialize lock manager
        self._lockManager = LockManager(self._cfg.getMaxWaitingPetitions())
        self._threadPool = ThreadPoolExecutor(max(0, self._cfg.getMaxThreads()))
        self._msgBroker = MessageBroker("%sMSB" % self._name, threadPool=self._threadPool)
        # Create static dict
        self._serviceInfoDict = {
            "serviceName": self._name,
            "serviceSectionPath": PathFinder.getServiceSection(self._name),
            "URL": self._cfg.getURL(),
            "messageSender": MessageSender(self._name, self._msgBroker),
            "validNames": self._validNames,
            "csPaths": [PathFinder.getServiceSection(svcName) for svcName in self._validNames],
        }
        self.securityLogging = Operations().getValue("EnableSecurityLogging", False) and getServiceOption(
            self._serviceInfoDict, "EnableSecurityLogging", False
        )

        # Initialize Monitoring
        # The import needs to be here because of the CS must be initialized before importing
        # this class (see https://github.com/DIRACGrid/DIRAC/issues/4793)
        from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

        self.activityMonitoringReporter = MonitoringReporter(monitoringType="ServiceMonitoring")

        # Call static initialization function
        try:
            self._handler["class"]._rh__initializeClass(
                dict(self._serviceInfoDict), self._lockManager, self._msgBroker, self.activityMonitoringReporter
            )
            if self._handler["init"]:
                for initFunc in self._handler["init"]:
                    gLogger.verbose("Executing initialization function")
                    try:
                        result = initFunc(dict(self._serviceInfoDict))
                    except Exception as excp:
                        gLogger.exception("Exception while calling initialization function", lException=excp)
                        return S_ERROR("Exception while calling initialization function: %s" % str(excp))
                    if not isReturnStructure(result):
                        return S_ERROR("Service initialization function %s must return S_OK/S_ERROR" % initFunc)
                    if not result["OK"]:
                        return S_ERROR("Error while initializing {}: {}".format(self._name, result["Message"]))
        except Exception as e:
            errMsg = "Exception while initializing %s" % self._name
            gLogger.exception(e)
            gLogger.exception(errMsg)
            return S_ERROR(errMsg)
        if self.activityMonitoring:
            gThreadScheduler.addPeriodicTask(30, self.__reportActivity)
            gThreadScheduler.addPeriodicTask(100, self.__activityMonitoringReporting)

        # Load actions after the handler has initialized itself
        result = self._loadActions()
        if not result["OK"]:
            return result
        self._actions = result["Value"]

        return S_OK()

    def __searchInitFunctions(self, handlerClass, currentClass=None):
        if not currentClass:
            currentClass = handlerClass
        initFuncs = []
        ancestorHasInit = False
        for ancestor in currentClass.__bases__:
            initFuncs += self.__searchInitFunctions(handlerClass, ancestor)
            if "initializeHandler" in dir(ancestor):
                ancestorHasInit = True
        if ancestorHasInit:
            initFuncs.append(super(currentClass, handlerClass).initializeHandler)
        if currentClass == handlerClass and "initializeHandler" in dir(handlerClass):
            initFuncs.append(handlerClass.initializeHandler)
        return initFuncs

    def _loadHandlerInit(self):
        handlerClass = self._svcData["classObj"]
        handlerName = handlerClass.__name__
        handlerInitMethods = self.__searchInitFunctions(handlerClass)
        try:
            handlerInitMethods.append(getattr(self._svcData["moduleObj"], "initialize%s" % handlerName))
        except AttributeError:
            gLogger.verbose("Not found global initialization function for service")

        if handlerInitMethods:
            gLogger.info("Found %s initialization methods" % len(handlerInitMethods))

        handlerInfo = {}
        handlerInfo["name"] = handlerName
        handlerInfo["module"] = self._svcData["moduleObj"]
        handlerInfo["class"] = handlerClass
        handlerInfo["init"] = handlerInitMethods

        return S_OK(handlerInfo)

    def _loadActions(self):

        handlerClass = self._handler["class"]

        authRules = {}
        typeCheck = {}
        methodsList = {}
        for actionType in Service.SVC_VALID_ACTIONS:
            if self._isMetaAction(actionType):
                continue
            authRules[actionType] = {}
            typeCheck[actionType] = {}
            methodsList[actionType] = []
        handlerAttributeList = dir(handlerClass)
        for actionType in Service.SVC_VALID_ACTIONS:
            if self._isMetaAction(actionType):
                continue
            methodPrefix = "%s_" % Service.SVC_VALID_ACTIONS[actionType]
            for attribute in handlerAttributeList:
                if attribute.find(methodPrefix) != 0:
                    continue
                exportedName = attribute[len(methodPrefix) :]
                methodsList[actionType].append(exportedName)
                gLogger.verbose(f"+ Found {actionType} method {exportedName}")
                # Create lock for method
                self._lockManager.createLock(
                    f"{actionType}/{exportedName}", self._cfg.getMaxThreadsForMethod(actionType, exportedName)
                )
                # Look for type and auth rules
                if actionType == "RPC":
                    typeAttr = "types_%s" % exportedName
                    authAttr = "auth_%s" % exportedName
                else:
                    typeAttr = f"types_{Service.SVC_VALID_ACTIONS[actionType]}_{exportedName}"
                    authAttr = f"auth_{Service.SVC_VALID_ACTIONS[actionType]}_{exportedName}"
                if typeAttr in handlerAttributeList:
                    obj = getattr(handlerClass, typeAttr)
                    gLogger.verbose(f"|- Found type definition {typeAttr}: {str(obj)}")
                    typeCheck[actionType][exportedName] = obj
                if authAttr in handlerAttributeList:
                    obj = getattr(handlerClass, authAttr)
                    gLogger.verbose(f"|- Found auth rules {authAttr}: {str(obj)}")
                    authRules[actionType][exportedName] = obj

        for actionType in Service.SVC_VALID_ACTIONS:
            referedAction = self._isMetaAction(actionType)
            if not referedAction:
                continue
            gLogger.verbose(f"Action {actionType} is a meta action for {referedAction}")
            authRules[actionType] = []
            for method in authRules[referedAction]:
                for prop in authRules[referedAction][method]:
                    if prop not in authRules[actionType]:
                        authRules[actionType].append(prop)
            gLogger.verbose(f"Meta action {actionType} props are {authRules[actionType]}")

        return S_OK({"methods": methodsList, "auth": authRules, "types": typeCheck})

    def __reportActivity(self):
        initialWallTime, initialCPUTime, mem = self.__startReportToMonitoring()
        pendingQueries = self._threadPool._work_queue.qsize()
        activeQuereies = len(self._threadPool._threads)
        percentage = self.__endReportToMonitoring(initialWallTime, initialCPUTime)
        self.activityMonitoringReporter.addRecord(
            {
                "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
                "Host": Network.getFQDN(),
                "ServiceName": "_".join(self._name.split("/")),
                "Location": self._cfg.getURL(),
                "MemoryUsage": mem,
                "CpuPercentage": percentage,
                "PendingQueries": pendingQueries,
                "ActiveQueries": activeQuereies,
                "RunningThreads": threading.active_count(),
                "MaxFD": self.__maxFD,
            }
        )
        self.__maxFD = 0

    def getConfig(self):
        return self._cfg

    # End of initialization functions

    def handleConnection(self, clientTransport):
        """
        This method may be called by ServiceReactor.
        The method stacks openened connection in a queue, another thread
        read this queue and handle connection.

        :param clientTransport: Object which describes opened connection (PlainTransport or SSLTransport)
        """
        if not self.activityMonitoring:
            self._stats["connections"] += 1
        self._threadPool.submit(self._processInThread, clientTransport)

    @property
    def wantsThrottle(self):
        """Boolean property for if the service wants requests to stop being accepted"""
        nQueued = self._threadPool._work_queue.qsize()
        return nQueued > self._cfg.getMaxWaitingPetitions()

    # Threaded process function
    def _processInThread(self, clientTransport):
        """
        This method handles a RPC, FileTransfer or Connection.
        Connection may be opened via ServiceReactor.__acceptIncomingConnection


        - Do the SSL/TLS Handshake (if dips is used) and extract credentials
        - Get the action called by the client
        - Check if the client is authorized to perform ation
          - If not, connection is closed
        - Instanciate the RequestHandler (RequestHandler contain all methods callable)

        (Following is not directly in this method but it describe what happen at
        #Execute the action)
        - Notify the client we're ready to execute the action (via _processProposal)
          and call RequestHandler._rh_executeAction()
        - Receive arguments/file/something else (depending on action) in the RequestHandler
        - Executing the action asked by the client

        :param clientTransport: Object which describe the opened connection (SSLTransport or PlainTransport)

        :return: S_OK with "closeTransport" a boolean to indicate if th connection have to be closed
                e.g. after RPC, closeTransport=True

        """
        self.__maxFD = max(self.__maxFD, clientTransport.oSocket.fileno())
        self._lockManager.lockGlobal()
        try:
            monReport = self.__startReportToMonitoring()
        except Exception:
            monReport = False
        try:
            # Handshake
            try:
                result = clientTransport.handshake()
                if not result["OK"]:
                    clientTransport.close()
                    return
            except Exception:
                return
            # Add to the transport pool
            trid = self._transportPool.add(clientTransport)
            if not trid:
                return
            # Receive and check proposal
            result = self._receiveAndCheckProposal(trid)
            if not result["OK"]:
                self._transportPool.sendAndClose(trid, result)
                return
            proposalTuple = result["Value"]
            # Instantiate handler
            result = self._instantiateHandler(trid, proposalTuple)
            if not result["OK"]:
                self._transportPool.sendAndClose(trid, result)
                return
            handlerObj = result["Value"]
            # Execute the action
            result = self._processProposal(trid, proposalTuple, handlerObj)
            # Close the connection if required
            if result["closeTransport"] or not result["OK"]:
                if not result["OK"]:
                    gLogger.error("Error processing proposal", result["Message"])
                self._transportPool.close(trid)
            return result
        finally:
            self._lockManager.unlockGlobal()
            if monReport:
                self.__endReportToMonitoring(monReport[0], monReport[1])

    @staticmethod
    def _createIdentityString(credDict, clientTransport=None):
        if "username" in credDict:
            if "group" in credDict:
                identity = "[{}:{}]".format(credDict["username"], credDict["group"])
            else:
                identity = "[%s:unknown]" % credDict["username"]
        else:
            identity = "unknown"
        if clientTransport:
            addr = clientTransport.getRemoteAddress()
            if addr:
                addr = f"{{{addr[0]}:{addr[1]}}}"
        if "DN" in credDict:
            identity += "(%s)" % credDict["DN"]
        return identity

    @staticmethod
    def _deserializeProposalTuple(serializedProposal):
        """We receive the proposalTuple as a list.
        Turn it into a tuple again
        """
        proposalTuple = tuple(tuple(x) if isinstance(x, list) else x for x in serializedProposal)
        return proposalTuple

    def _receiveAndCheckProposal(self, trid):
        clientTransport = self._transportPool.get(trid)
        # Get the peer credentials
        credDict = clientTransport.getConnectingCredentials()
        # Receive the action proposal
        retVal = clientTransport.receiveData(1024)
        if not retVal["OK"]:
            gLogger.error(
                "Invalid action proposal",
                "{} {}".format(self._createIdentityString(credDict, clientTransport), retVal["Message"]),
            )
            return S_ERROR("Invalid action proposal")
        proposalTuple = Service._deserializeProposalTuple(retVal["Value"])
        gLogger.debug("Received action from client", "/".join(list(proposalTuple[1])))
        # Check if there are extra credentials
        if proposalTuple[2]:
            clientTransport.setExtraCredentials(proposalTuple[2])
        # Check if this is the requested service
        requestedService = proposalTuple[0][0]
        if requestedService not in self._validNames:
            return S_ERROR("%s is not up in this server" % requestedService)
        # Check if the action is valid
        requestedActionType = proposalTuple[1][0]
        if requestedActionType not in Service.SVC_VALID_ACTIONS:
            return S_ERROR("%s is not a known action type" % requestedActionType)
        # Check if it's authorized
        result = self._authorizeProposal(proposalTuple[1], trid, credDict)
        if not result["OK"]:
            return result
        # Proposal is OK
        return S_OK(proposalTuple)

    def _authorizeProposal(self, actionTuple, trid, credDict):
        # Find CS path for the Auth rules
        referedAction = self._isMetaAction(actionTuple[0])
        if referedAction:
            csAuthPath = "%s/Default" % actionTuple[0]
            hardcodedMethodAuth = self._actions["auth"][actionTuple[0]]
        else:
            if actionTuple[0] == "RPC":
                csAuthPath = actionTuple[1]
            else:
                csAuthPath = "/".join(actionTuple)
            # Find if there are hardcoded auth rules in the code
            hardcodedMethodAuth = False
            if actionTuple[0] in self._actions["auth"]:
                hardcodedRulesByType = self._actions["auth"][actionTuple[0]]
                if actionTuple[0] == "FileTransfer":
                    methodName = actionTuple[1][0].lower() + actionTuple[1][1:]
                else:
                    methodName = actionTuple[1]

                if methodName in hardcodedRulesByType:
                    hardcodedMethodAuth = hardcodedRulesByType[methodName]
        # Auth time!
        if not self._authMgr.authQuery(csAuthPath, credDict, hardcodedMethodAuth):
            # Get the identity string
            identity = self._createIdentityString(credDict)
            fromHost = "unknown host"
            tr = self._transportPool.get(trid)
            if tr:
                fromHost = "/".join([str(item) for item in tr.getRemoteAddress()])
            gLogger.warn(
                "Unauthorized query",
                "to {}:{} by {} from {}".format(self._name, "/".join(actionTuple), identity, fromHost),
            )
            result = S_ERROR(ENOAUTH, "Unauthorized query")
        else:
            result = S_OK()

        # Security log
        tr = self._transportPool.get(trid)
        if not tr:
            return S_ERROR("Client disconnected")
        sourceAddress = tr.getRemoteAddress()
        identity = self._createIdentityString(credDict)
        if self.securityLogging:
            Service.SVC_SECLOG_CLIENT.addMessage(
                result["OK"],
                sourceAddress[0],
                sourceAddress[1],
                identity,
                self._cfg.getHostname(),
                self._cfg.getPort(),
                self._name,
                "/".join(actionTuple),
            )
        return result

    def _instantiateHandler(self, trid, proposalTuple=None):
        """
        Generate an instance of the handler for a given service

        :param int trid: transport ID
        :param tuple proposalTuple: tuple describing the proposed action

        :return: S_OK/S_ERROR, Value is the handler object
        """
        # Generate the client params
        clientParams = {"serviceStartTime": self._startTime}
        if proposalTuple:
            # The 4th element is the client version
            clientParams["clientVersion"] = proposalTuple[3] if len(proposalTuple) > 3 else None
            clientParams["clientSetup"] = proposalTuple[0][1]
            if len(proposalTuple[0]) < 3:
                clientParams["clientVO"] = gConfig.getValue("/DIRAC/VirtualOrganization", "unknown")
            else:
                clientParams["clientVO"] = proposalTuple[0][2]
        clientTransport = self._transportPool.get(trid)
        if clientTransport:
            clientParams["clientAddress"] = clientTransport.getRemoteAddress()
        # Generate handler dict with per client info
        handlerInitDict = dict(self._serviceInfoDict)
        for key in clientParams:
            handlerInitDict[key] = clientParams[key]
        # Instantiate and initialize
        try:
            handlerInstance = self._handler["class"](handlerInitDict, trid)
            handlerInstance.initialize()
        except Exception as e:
            gLogger.exception("Server error while loading handler: %s" % str(e))
            return S_ERROR("Server error while loading handler")
        return S_OK(handlerInstance)

    def _processProposal(self, trid, proposalTuple, handlerObj):
        # Notify the client we're ready to execute the action
        retVal = self._transportPool.send(trid, S_OK())
        if not retVal["OK"]:
            return retVal

        messageConnection = False
        if proposalTuple[1] == ("Connection", "new"):
            messageConnection = True

        if messageConnection:

            if self._msgBroker.getNumConnections() > self._cfg.getMaxMessagingConnections():
                result = S_ERROR("Maximum number of connections reached. Try later")
                result["closeTransport"] = True
                return result

            # This is a stable connection
            self._msgBroker.addTransportId(
                trid,
                self._name,
                receiveMessageCallback=self._mbReceivedMsg,
                disconnectCallback=self._mbDisconnect,
                listenToConnection=False,
            )

        result = self._executeAction(trid, proposalTuple, handlerObj)
        if result["OK"] and messageConnection:
            self._msgBroker.listenToTransport(trid)
            result = self._mbConnect(trid, handlerObj)
            if not result["OK"]:
                self._msgBroker.removeTransport(trid)

        result["closeTransport"] = not messageConnection or not result["OK"]
        return result

    def _mbConnect(self, trid, handlerObj=None):
        if not handlerObj:
            result = self._instantiateHandler(trid)
            if not result["OK"]:
                return result
            handlerObj = result["Value"]
        return handlerObj._rh_executeConnectionCallback("connected")

    def _executeAction(self, trid, proposalTuple, handlerObj):
        try:
            response = handlerObj._rh_executeAction(proposalTuple)
            if not response["OK"]:
                return response
            if self.activityMonitoring:
                self.activityMonitoringReporter.addRecord(
                    {
                        "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
                        "Host": Network.getFQDN(),
                        "ServiceName": "_".join(self._name.split("/")),
                        "Location": self._cfg.getURL(),
                        "ResponseTime": response["Value"][1],
                    }
                )
            return response["Value"][0]
        except Exception as e:
            gLogger.exception("Exception while executing handler action")
            return S_ERROR("Server error while executing action: %s" % str(e))

    def _mbReceivedMsg(self, trid, msgObj):
        result = self._authorizeProposal(
            ("Message", msgObj.getName()), trid, self._transportPool.get(trid).getConnectingCredentials()
        )
        if not result["OK"]:
            return result
        result = self._instantiateHandler(trid)
        if not result["OK"]:
            return result
        handlerObj = result["Value"]
        response = handlerObj._rh_executeMessageCallback(msgObj)
        if self.activityMonitoring and response["OK"]:
            self.activityMonitoringReporter.addRecord(
                {
                    "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
                    "Host": Network.getFQDN(),
                    "ServiceName": "_".join(self._name.split("/")),
                    "Location": self._cfg.getURL(),
                    "ResponseTime": response["Value"][1],
                }
            )
        if response["OK"]:
            return response["Value"][0]
        else:
            return response

    def _mbDisconnect(self, trid):
        result = self._instantiateHandler(trid)
        if not result["OK"]:
            return result
        handlerObj = result["Value"]
        return handlerObj._rh_executeConnectionCallback("drop")

    def __activityMonitoringReporting(self):
        """This method is called by the ThreadScheduler as a periodic task in order to commit the collected data which
        is done by the MonitoringReporter and is sent to the 'ServiceMonitoring' type.

        :return: True / False
        """
        return self.activityMonitoringReporter.commit()

    def __startReportToMonitoring(self):
        now = time.time()
        stats = os.times()
        cpuTime = stats[0] + stats[2]
        mem = None
        if now - self.__monitorLastStatsUpdate < 0:
            return (now, cpuTime, mem)
        self.__monitorLastStatsUpdate = now
        mem = psutil.Process().memory_info().rss / (1024.0 * 1024.0)
        return (now, cpuTime, mem)

    def __endReportToMonitoring(self, initialWallTime, initialCPUTime):
        wallTime = time.time() - initialWallTime
        stats = os.times()
        cpuTime = stats[0] + stats[2] - initialCPUTime
        percentage = cpuTime / wallTime * 100.0
        return percentage
