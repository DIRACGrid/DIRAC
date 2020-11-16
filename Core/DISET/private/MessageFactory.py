from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import re
import os
import DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals


class MessageFactory(object):

  def __init__(self):
    self.__definitions = {}
    self.__svcHandlers = {}

  def createMessage(self, serviceName, msgName, attrs=None):
    result = self.__loadMessagesFromService(serviceName)
    if not result['OK']:
      return result
    if msgName not in self.__definitions[serviceName]:
      return S_ERROR("Could not find message definition %s for service %s" % (msgName, serviceName))
    msgObj = Message(msgName, self.__definitions[serviceName][msgName])
    if attrs is not None:
      result = msgObj.loadAttrs(attrs)
      if not result['OK']:
        return result
    return S_OK(msgObj)

  def getMessagesForService(self, serviceName):
    result = self.__loadMessagesFromService(serviceName)
    if not result['OK']:
      return result
    return S_OK(tuple(self.__definitions[serviceName]))

  def messageExists(self, serviceName, msgName):
    result = self.getMessagesForService(serviceName)
    if not result['OK']:
      return False
    return msgName in result['Value']

  def __loadHandler(self, serviceName):
    # TODO: Load handlers as the Service does (1. CS 2. SysNameSystem/Service/servNameHandler.py)
    sL = List.fromChar(serviceName, "/")
    if len(sL) != 2:
      return S_ERROR("Service name is not valid: %s" % serviceName)
    sysName = sL[0]
    svcHandlerName = "%sHandler" % sL[1]
    loadedObjs = loadObjects("%sSystem/Service" % sysName,
                             reFilter=re.compile(r"^%s\.py$" % svcHandlerName))
    if svcHandlerName not in loadedObjs:
      return S_ERROR("Could not find %s for getting messages definition" % serviceName)
    return S_OK(loadedObjs[svcHandlerName])

  def __loadMessagesFromService(self, serviceName):
    if serviceName in self.__definitions:
      return S_OK()
    if serviceName not in self.__svcHandlers:
      result = self.__loadHandler(serviceName)
      if not result['OK']:
        return result
      self.__svcHandlers[serviceName] = result['Value']
    self.__definitions[serviceName] = {}
    handlerClass = self.__svcHandlers[serviceName]
    # Load message definition starting with ancestors, children override the ancestors as usual
    result = self.__loadMessagesForAncestry(handlerClass)
    if not result['OK']:
      return result
    msgDefs = result['Value']
    if not msgDefs:
      return S_ERROR("%s does not have messages defined" % serviceName)
    self.__definitions[serviceName] = msgDefs
    return S_OK()

  def __loadMessagesForAncestry(self, handlerClass):
    finalDefs = {}
    for ancestor in handlerClass.__bases__:
      result = self.__loadMessagesForAncestry(ancestor)
      if not result['OK']:
        return result
      ancestorDefs = result['Value']
      for msgName in ancestorDefs:
        finalDefs[msgName] = ancestorDefs[msgName]
    if 'MSG_DEFINITIONS' not in dir(handlerClass):
      return S_OK(finalDefs)
    msgDefs = getattr(handlerClass, 'MSG_DEFINITIONS')
    if not isinstance(msgDefs, dict):
      return S_ERROR("Message definitions for service %s is not a dict" % handlerClass.__name__)
    for msgName in msgDefs:
      msgDefDict = msgDefs[msgName]
      if not isinstance(msgDefDict, dict):
        return S_ERROR("Type of message definition has to be a dict")
      finalDefs[msgName] = msgDefDict
    return S_OK(finalDefs)


class Message(object):

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
        self.__fDef[fName] = (fType, )
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
        msgStr.append("%s->%s" % (k, v))
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
    if '_Message__locked' not in self.__dict__:
      object.__setattr__(self, k, v)
      return
    # Normal assignment
    if k in self.__dict__:
      object.__setattr__(self, k, v)
      return
    if k not in self.__order:
      raise AttributeError("%s is not valid for message %s" % (k, self.__name))
    if self.__fDef[k] is not None and type(v) not in self.__fDef[k]:
      raise AttributeError("%s is to be of type %s for attr %s, and is of type %s" % (v,
                                                                                      self.__fDef[k],
                                                                                      k,
                                                                                      type(v)))
    self.__values[k] = v

  def __getattr__(self, k):
    if k not in self.__values:
      if k not in self.__fDef:
        raise AttributeError("No %s attribute for message %s" % (k, self.__name))
      raise AttributeError("%s has no value" % k)
    return self.__values[k]


#
# MessageContainer
#

class DummyMessage:

  def __init__(self, msg):
    self.msg = msg

  def getName(self):
    return self.msg['name']

  def dumpAttrs(self):
    return S_OK(self.msg['attrs'])

  def isOK(self):
    return True

  def getWaitForAck(self):
    return self.msg['attrs'][0]


#
# Object loader
#

def loadObjects(path, reFilter=None, parentClass=None):
  if not reFilter:
    reFilter = re.compile(r".*[a-z1-9]\.py$")
  pathList = List.fromChar(path, "/")

  parentModuleList = ["%sDIRAC" % ext for ext in CSGlobals.getCSExtensions()] + ['DIRAC']
  objectsToLoad = {}
  # Find which object files match
  for parentModule in parentModuleList:
    objDir = os.path.join(DIRAC.rootPath, parentModule, *pathList)
    if not os.path.isdir(objDir):
      continue
    for objFile in os.listdir(objDir):
      if reFilter.match(objFile):
        pythonClassName = objFile[:-3]
        if pythonClassName not in objectsToLoad:
          gLogger.info("Adding to message load queue %s/%s/%s" % (parentModule, path, pythonClassName))
          objectsToLoad[pythonClassName] = parentModule

  # Load them!
  loadedObjects = {}

  for pythonClassName in objectsToLoad:
    parentModule = objectsToLoad[pythonClassName]
    try:
      # Where parentModule can be DIRAC, pathList is something like [ "AccountingSystem", "Client", "Types" ]
      # And the python class name is.. well, the python class name
      objPythonPath = "%s.%s.%s" % (parentModule, ".".join(pathList), pythonClassName)
      objModule = __import__(objPythonPath,
                             globals(),
                             locals(), pythonClassName)
      objClass = getattr(objModule, pythonClassName)
    except Exception as e:
      gLogger.exception("Can't load type %s/%s: %s" % (parentModule, pythonClassName, str(e)))
      continue
    if parentClass == objClass:
      continue
    if parentClass and not issubclass(objClass, parentClass):
      gLogger.warn("%s is not a subclass of %s. Skipping" % (objClass, parentClass))
      continue
    gLogger.info("Loaded %s" % objPythonPath)
    loadedObjects[pythonClassName] = objClass

  return loadedObjects
