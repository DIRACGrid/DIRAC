import re
import os
import DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.Core.Utilities.ObjectLoader import loadObjects


class MessageFactory:
    def __init__(self):
        self.__definitions = {}
        self.__svcHandlers = {}

    def createMessage(self, serviceName, msgName, attrs=None):
        result = self.__loadMessagesFromService(serviceName)
        if not result["OK"]:
            return result
        if msgName not in self.__definitions[serviceName]:
            return S_ERROR(f"Could not find message definition {msgName} for service {serviceName}")
        msgObj = Message(msgName, self.__definitions[serviceName][msgName])
        if attrs is not None:
            result = msgObj.loadAttrs(attrs)
            if not result["OK"]:
                return result
        return S_OK(msgObj)

    def getMessagesForService(self, serviceName):
        result = self.__loadMessagesFromService(serviceName)
        if not result["OK"]:
            return result
        return S_OK(tuple(self.__definitions[serviceName]))

    def messageExists(self, serviceName, msgName):
        result = self.getMessagesForService(serviceName)
        if not result["OK"]:
            return False
        return msgName in result["Value"]

    def __loadHandler(self, serviceName):
        # TODO: Load handlers as the Service does (1. CS 2. SysNameSystem/Service/servNameHandler.py)
        sL = List.fromChar(serviceName, "/")
        if len(sL) != 2:
            return S_ERROR("Service name is not valid: %s" % serviceName)
        sysName = sL[0]
        svcHandlerName = "%sHandler" % sL[1]
        loadedObjs = loadObjects("%sSystem/Service" % sysName, reFilter=re.compile(r"^%s\.py$" % svcHandlerName))
        if svcHandlerName not in loadedObjs:
            return S_ERROR("Could not find %s for getting messages definition" % serviceName)
        return S_OK(loadedObjs[svcHandlerName])

    def __loadMessagesFromService(self, serviceName):
        if serviceName in self.__definitions:
            return S_OK()
        if serviceName not in self.__svcHandlers:
            result = self.__loadHandler(serviceName)
            if not result["OK"]:
                return result
            self.__svcHandlers[serviceName] = result["Value"]
        self.__definitions[serviceName] = {}
        handlerClass = self.__svcHandlers[serviceName]
        # Load message definition starting with ancestors, children override the ancestors as usual
        result = self.__loadMessagesForAncestry(handlerClass)
        if not result["OK"]:
            return result
        msgDefs = result["Value"]
        if not msgDefs:
            return S_ERROR("%s does not have messages defined" % serviceName)
        self.__definitions[serviceName] = msgDefs
        return S_OK()

    def __loadMessagesForAncestry(self, handlerClass):
        finalDefs = {}
        for ancestor in handlerClass.__bases__:
            result = self.__loadMessagesForAncestry(ancestor)
            if not result["OK"]:
                return result
            ancestorDefs = result["Value"]
            for msgName in ancestorDefs:
                finalDefs[msgName] = ancestorDefs[msgName]
        if "MSG_DEFINITIONS" not in dir(handlerClass):
            return S_OK(finalDefs)
        msgDefs = getattr(handlerClass, "MSG_DEFINITIONS")
        if not isinstance(msgDefs, dict):
            return S_ERROR("Message definitions for service %s is not a dict" % handlerClass.__name__)
        for msgName in msgDefs:
            msgDefDict = msgDefs[msgName]
            if not isinstance(msgDefDict, dict):
                return S_ERROR("Type of message definition has to be a dict")
            finalDefs[msgName] = msgDefDict
        return S_OK(finalDefs)


class Message:

    DEFAULTWAITFORACK = False

    def __init__(self, msgName, msgDefDict):
        self.__name = msgName
        self.__fDef = {}
        self.__order = []
        self.__values = {}
        self.__msgClient = None
        self.__waitForAck = Message.DEFAULTWAITFORACK
        for fName in msgDefDict:
            fType = msgDefDict[fName]
            if fType is not None and not isinstance(fType, (list, tuple)):
                self.__fDef[fName] = (fType,)
            else:
                self.__fDef[fName] = fType
            self.__order.append((fName.lower(), fName))
        self.__order = [oF[1] for oF in sorted(self.__order)]
        # "Enable" set attr
        self.__locked = True

    def setMsgClient(self, msgClient):
        self.__msgClient = msgClient

    @property
    def msgClient(self):
        return self.__msgClient

    def isOK(self):
        for k in self.__order:
            if k not in self.__values:
                return False
            if self.__fDef[k] is not None and not isinstance(self.__values[k], self.__fDef[k]):
                return False
        return True

    def setWaitForAck(self, bA):
        self.__waitForAck = bA

    def getWaitForAck(self):
        return self.__waitForAck

    def getName(self):
        return self.__name

    def getAttrNames(self):
        return self.__order

    def loadAttrsFromDict(self, dataDict):
        if not isinstance(dataDict, dict):
            return S_ERROR("Params have to be a dict")
        for k in dataDict:
            try:
                setattr(self, k, dataDict[k])
            except AttributeError as e:
                return S_ERROR(str(e))
        return S_OK()

    def dumpAttrs(self):
        try:
            return S_OK([self.__waitForAck, [self.__values[k] for k in self.__order]])
        except Exception as e:
            return S_ERROR("Could not dump message: %s doesn't have a value" % e)

    def loadAttrs(self, data):
        if not isinstance(data, (list, tuple)) and len(data) != 2:
            return S_ERROR("Data is not valid to be loaded as message")
        self.setWaitForAck(data[0])
        data = data[1]
        if not isinstance(data, (list, tuple)):
            return S_ERROR("Data is not valid to be loaded as message")
        for iP in range(len(self.__order)):
            try:
                v = data[iP]
            except IndexError:
                return S_ERROR("Data is too short!")
            k = self.__order[iP]
            try:
                setattr(self, k, v)
            except AttributeError as e:
                return S_ERROR(str(e))
        return S_OK()

    def __str__(self):
        msgStr = ["<Message %s (" % self.__name]
        for k in self.__order:
            if k in self.__values:
                v = str(self.__values[k])
                if len(v) > 5:
                    v = "%s..." % v[:5]
                msgStr.append(f"{k}->{v}")
            else:
                msgStr.append("%s" % k)
        if self.isOK():
            msgStr.append(") OK>")
        else:
            msgStr.append(") >")
        return " ".join(msgStr)

    def __repr__(self):
        return self.__str__()

    def __setattr__(self, k, v):
        # Intialization
        if "_Message__locked" not in self.__dict__:
            object.__setattr__(self, k, v)
            return
        # Normal assignment
        if k in self.__dict__:
            object.__setattr__(self, k, v)
            return
        if k not in self.__order:
            raise AttributeError(f"{k} is not valid for message {self.__name}")
        if self.__fDef[k] is not None and not isinstance(v, self.__fDef[k]):
            raise AttributeError(f"{v} is to be of type {self.__fDef[k]} for attr {k}, and is of type {type(v)}")
        self.__values[k] = v

    def __getattr__(self, k):
        if k not in self.__values:
            if k not in self.__fDef:
                raise AttributeError(f"No {k} attribute for message {self.__name}")
            raise AttributeError("%s has no value" % k)
        return self.__values[k]


#
# MessageContainer
#


class DummyMessage:
    def __init__(self, msg):
        self.msg = msg

    def getName(self):
        return self.msg["name"]

    def dumpAttrs(self):
        return S_OK(self.msg["attrs"])

    def isOK(self):
        return True

    def getWaitForAck(self):
        return self.msg["attrs"][0]
