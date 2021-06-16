""" Base class for all services
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import six
import time
import psutil

import DIRAC

from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.Utilities import Time
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Security.Properties import CS_ADMINISTRATOR


def getServiceOption(serviceInfo, optionName, defaultValue):
  """ Get service option resolving default values from the master service
  """
  if optionName[0] == "/":
    return gConfig.getValue(optionName, defaultValue)
  for csPath in serviceInfo['csPaths']:
    result = gConfig.getOption("%s/%s" % (csPath, optionName, ), defaultValue)
    if result['OK']:
      return result['Value']
  return defaultValue


class ConnectionError(Exception):

  def __init__(self, msg):
    self.__msg = msg

  def __str__(self):
    return "ConnectionError: %s" % self.__msg


class RequestHandler(object):

  def __init__(self, handlerInitDict, trid):
    """
    Constructor

    :type handlerInitDict: dictionary
    :param handlerInitDict: Information vars for the service

    :type trid: object
    :param trid: Transport to use
    """
    # Initially serviceInfoDict is the one base to the RequestHandler
    # the one created in _rh_initializeClass
    # FSM help me for I have made a complex stuff that I will forget in 5 mins :P
    handlerInitDict.update(self.__srvInfoDict)
    self.serviceInfoDict = handlerInitDict
    self.__trid = trid

  def initialize(self):
    """Initialize this instance of the handler (to be overwritten)
    """
    pass

  @classmethod
  def _rh__initializeClass(cls, serviceInfoDict, lockManager, msgBroker, monitor):
    """
    Class initialization (not to be called by hand or overwritten!!)

    :type serviceInfoDict: dictionary
    :param serviceInfoDict: Information vars for the service
    :type msgBroker: object
    :param msgBroker: Message delivery
    :type lockManager: object
    :param lockManager: Lock manager to use
    """
    cls.__srvInfoDict = serviceInfoDict
    cls.__svcName = cls.__srvInfoDict['serviceName']
    cls.__lockManager = lockManager
    cls.__msgBroker = msgBroker
    cls.__trPool = msgBroker.getTransportPool()
    cls.__monitor = monitor
    cls.log = gLogger

  def getRemoteAddress(self):
    """
    Get the address of the remote peer.

    :return: Address of remote peer.
    """
    return self.__trPool.get(self.__trid).getRemoteAddress()

  def getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.__trPool.get(self.__trid).getConnectingCredentials()

  @classmethod
  def getCSOption(cls, optionName, defaultValue=False):
    """
    Get an option from the CS section of the services

    :return: Value for serviceSection/optionName in the CS being defaultValue the default
    """
    return cls.srv_getCSOption(optionName, defaultValue)

  def _rh_executeAction(self, proposalTuple):
    """
    Execute an action.

    :type proposalTuple: tuple
    :param proposalTuple: Type of action to execute. First position of the tuple must be the type
                        of action to execute. The second position is the action itself.
    """
    actionTuple = proposalTuple[1]
    gLogger.debug("Executing %s:%s action" % tuple(actionTuple))
    startTime = time.time()
    actionType = actionTuple[0]
    self.serviceInfoDict['actionTuple'] = actionTuple
    try:
      if actionType == "RPC":
        retVal = self.__doRPC(actionTuple[1])
      elif actionType == "FileTransfer":
        retVal = self.__doFileTransfer(actionTuple[1])
      elif actionType == "Connection":
        retVal = self.__doConnection(actionTuple[1])
      else:
        return S_ERROR("Unknown action %s" % actionType)
    except ConnectionError as excp:
      gLogger.error("ConnectionError", str(excp))
      return S_ERROR(excp)
    if not isReturnStructure(retVal):
      message = "Method %s for action %s does not return a S_OK/S_ERROR!" % (actionTuple[1], actionTuple[0])
      gLogger.error(message)
      retVal = S_ERROR(message)
    elapsedTime = time.time() - startTime
    self.__logRemoteQueryResponse(retVal, elapsedTime)
    result = self.__trPool.send(self.__trid, retVal)  # this will delete the value from the S_OK(value)
    del retVal
    return S_OK([result, elapsedTime])

#####
#
# File to/from Server Methods
#
#####

  def __doFileTransfer(self, sDirection):
    """
    Execute a file transfer action

    :type sDirection: string
    :param sDirection: Direction of the transfer
    :return: S_OK/S_ERROR
    """
    retVal = self.__trPool.receive(self.__trid)
    if not retVal['OK']:
      raise ConnectionError("Error while receiving file description %s %s" %
                            (self.srv_getFormattedRemoteCredentials(), retVal['Message']))

    # Reconvert to tuple
    fileInfo = tuple(retVal['Value'])
    sDirection = "%s%s" % (sDirection[0].lower(), sDirection[1:])
    if "transfer_%s" % sDirection not in dir(self):
      self.__trPool.send(self.__trid, S_ERROR("Service can't transfer files %s" % sDirection))
      return
    retVal = self.__trPool.send(self.__trid, S_OK("Accepted"))
    if not retVal['OK']:
      return retVal
    self.__logRemoteQuery("FileTransfer/%s" % sDirection, fileInfo)

    self.__lockManager.lock("FileTransfer/%s" % sDirection)
    try:
      try:
        fileHelper = FileHelper(self.__trPool.get(self.__trid))
        if sDirection == "fromClient":
          fileHelper.setDirection("fromClient")
          uRetVal = self.transfer_fromClient(fileInfo[0], fileInfo[1], fileInfo[2], fileHelper)
        elif sDirection == "toClient":
          fileHelper.setDirection("toClient")
          uRetVal = self.transfer_toClient(fileInfo[0], fileInfo[1], fileHelper)
        elif sDirection == "bulkFromClient":
          fileHelper.setDirection("fromClient")
          uRetVal = self.transfer_bulkFromClient(fileInfo[0], fileInfo[1], fileInfo[2], fileHelper)
        elif sDirection == "bulkToClient":
          fileHelper.setDirection("toClient")
          uRetVal = self.transfer_bulkToClient(fileInfo[0], fileInfo[1], fileHelper)
        elif sDirection == "listBulk":
          fileHelper.setDirection("toClient")
          uRetVal = self.transfer_listBulk(fileInfo[0], fileInfo[1], fileHelper)
        else:
          return S_ERROR("Direction %s does not exist!!!" % sDirection)
        if uRetVal['OK'] and not fileHelper.finishedTransmission():
          gLogger.error("You haven't finished receiving/sending the file", str(fileInfo))
          return S_ERROR("Incomplete transfer")
        del fileHelper
        return uRetVal
      finally:
        self.__lockManager.unlock("FileTransfer/%s" % sDirection)

    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception("Uncaught exception when serving Transfer", "%s" % sDirection, lException=e)
      return S_ERROR("Server error while serving %s: %s" % (sDirection, repr(e)))

  def transfer_fromClient(self, fileId, token, fileSize, fileHelper):  # pylint: disable=unused-argument
    return S_ERROR("This server does no allow receiving files")

  def transfer_toClient(self, fileId, token, fileHelper):  # pylint: disable=unused-argument
    return S_ERROR("This server does no allow sending files")

  def transfer_bulkFromClient(self, bulkId, token, bulkSize, fileHelper):  # pylint: disable=unused-argument
    return S_ERROR("This server does no allow bulk receiving")

  def transfer_bulkToClient(self, bulkId, token, fileHelper):  # pylint: disable=unused-argument
    return S_ERROR("This server does no allow bulk sending")

  def transfer_listBulk(self, bulkId, token, fileHelper):  # pylint: disable=unused-argument
    return S_ERROR("This server does no allow bulk listing")

#####
#
# RPC Methods
#
#####

  def __doRPC(self, method):
    """
    Execute an RPC action

    :type method: string
    :param method: Method to execute
    :return: S_OK/S_ERROR
    """
    retVal = self.__trPool.receive(self.__trid)
    if not retVal['OK']:
      raise ConnectionError("Error while receiving arguments %s %s" %
                            (self.srv_getFormattedRemoteCredentials(), retVal['Message']))
    args = retVal['Value']
    self.__logRemoteQuery("RPC/%s" % method, args)
    return self.__RPCCallFunction(method, args)

  def __RPCCallFunction(self, method, args):
    """
      Check the arguments then call the RPC function

      :type method: string
      :param method: arguments sended by remote client

      :return: S_OK/S_ERROR
    """
    realMethod = "export_%s" % method
    gLogger.debug("RPC to %s" % realMethod)
    try:
      # Get the method we are trying to call
      oMethod = getattr(self, realMethod)
    except BaseException:
      return S_ERROR("Unknown method %s" % method)
    # Check if the client sends correct arguments
    dRetVal = self.__checkExpectedArgumentTypes(method, args)
    if not dRetVal['OK']:
      return dRetVal
    # Lock the method with Semaphore to avoid too many calls at the same time
    self.__lockManager.lock("RPC/%s" % method)
    # 18.02.19 WARNING CHRIS
    # The line bellow adds the current transportID to the message broker
    # First of all, I do not see why it is doing so.
    # Second, this affects only one every other socket, since the
    # message broker selects on that one, and in the meantime, many sockets
    # are added and removed, without even being seen by the message broker.
    # Finally, there seem to be a double read on the socket: from the message broker
    # and from the ServiceReactor, resulting in conflict.
    # This is warned in the man page of "select".
    # it has been exhibited when testing M2Crypto.
    # I will comment it out, and try to put it in a separate commit when merging
    # self.__msgBroker.addTransportId(self.__trid,
    #                                 self.serviceInfoDict['serviceName'],
    #                                 idleRead=True)
    try:
      try:
        # Trying to execute the method
        uReturnValue = oMethod(*args)
        return uReturnValue
      finally:
        # Unlock method
        self.__lockManager.unlock("RPC/%s" % method)
        # 18.02.19 WARNING CHRIS
        # See comment above
        # self.__msgBroker.removeTransport(self.__trid, closeTransport=False)
    except Exception as e:
      gLogger.exception("Uncaught exception when serving RPC", "Function %s" % method, lException=e)
      return S_ERROR("Server error while serving %s: %s" % (method, str(e)))

  def __checkExpectedArgumentTypes(self, method, args):
    """
    Check that the arguments received match the ones expected

    :type method: string
    :param method: Method to check against
    :type args: tuple
    :param args: Arguments to check
    :return: S_OK/S_ERROR
    """
    sListName = "types_%s" % method
    try:
      oTypesList = getattr(self, sListName)
    except BaseException:
      gLogger.error("There's no types info for method", "export_%s" % method)
      return S_ERROR("Handler error for server %s while processing method %s" % (self.serviceInfoDict['serviceName'],
                                                                                 method))
    try:
      mismatch = False
      for iIndex in range(min(len(oTypesList), len(args))):
        # If None skip the parameter
        if oTypesList[iIndex] is None:
          continue
        # If parameter is a list or a tuple check types inside
        elif isinstance(oTypesList[iIndex], (tuple, list)):
          if not isinstance(args[iIndex], tuple(oTypesList[iIndex])):
            mismatch = True
        # else check the parameter
        elif not isinstance(args[iIndex], oTypesList[iIndex]):
          mismatch = True
        # Has there been a mismatch?
        if mismatch:
          sError = "Type mismatch in parameter %d (starting with param 0) Received %s, expected %s" % (
              iIndex, type(args[iIndex]), str(oTypesList[iIndex]))
          return S_ERROR(sError)
      if len(args) < len(oTypesList):
        return S_ERROR("Function %s expects at least %s arguments" % (method, len(oTypesList)))
    except Exception as v:
      sError = "Error in parameter check: %s" % str(v)
      gLogger.exception(sError)
      return S_ERROR(sError)
    return S_OK()

####
#
#  Connection methods
#
####

  __connectionCallbackTypes = {'new': [six.string_types, dict],
                               'connected': [],
                               'drop': []}

  def __doConnection(self, methodName):
    """
    Connection callbacks
    """
    retVal = self.__trPool.receive(self.__trid)
    if not retVal['OK']:
      raise ConnectionError(
          "Error while receiving arguments %s %s" % (self.srv_getFormattedRemoteCredentials(), retVal['Message']))
    args = retVal['Value']
    return self._rh_executeConnectionCallback(methodName, args)

  def _rh_executeConnectionCallback(self, methodName, args=False):
    self.__logRemoteQuery("Connection/%s" % methodName, args)
    if methodName not in RequestHandler.__connectionCallbackTypes:
      return S_ERROR("Invalid connection method %s" % methodName)
    cbTypes = RequestHandler.__connectionCallbackTypes[methodName]
    if args:
      if len(args) != len(cbTypes):
        return S_ERROR("Expected %s arguments" % len(cbTypes))
      for i in range(len(cbTypes)):
        if not isinstance(args[i], cbTypes[i]):
          return S_ERROR("Invalid type for argument %s" % i)
      self.__trPool.associateData(self.__trid, "connectData", args)

    if not args:
      args = self.__trPool.getAssociatedData(self.__trid, "connectData")

    realMethod = "conn_%s" % methodName
    gLogger.debug("Callback to %s" % realMethod)
    try:
      oMethod = getattr(self, realMethod)
    except BaseException:
      # No callback defined by handler
      return S_OK()
    try:
      if args:
        uReturnValue = oMethod(self.__trid, *args)
      else:
        uReturnValue = oMethod(self.__trid)
      return uReturnValue
    except Exception as e:
      gLogger.exception("Uncaught exception when serving Connect", "Function %s" % realMethod, lException=e)
      return S_ERROR("Server error while serving %s: %s" % (methodName, str(e)))

  def _rh_executeMessageCallback(self, msgObj):
    msgName = msgObj.getName()
    if not self.__msgBroker.getMsgFactory().messageExists(self.__svcName, msgName):
      return S_ERROR("Unknown message %s" % msgName)
    methodName = "msg_%s" % msgName
    self.__logRemoteQuery("Message/%s" % methodName, msgObj.dumpAttrs())
    startTime = time.time()
    try:
      oMethod = getattr(self, methodName)
    except BaseException:
      return S_ERROR("Handler function for message %s does not exist!" % msgName)
    self.__lockManager.lock(methodName)
    try:
      try:
        uReturnValue = oMethod(msgObj)
      except Exception as e:
        gLogger.exception("Uncaught exception when serving message", methodName, lException=e)
        return S_ERROR("Server error while serving %s: %s" % (msgName, str(e)))
    finally:
      self.__lockManager.unlock(methodName)
    if not isReturnStructure(uReturnValue):
      gLogger.error("Message does not return a S_OK/S_ERROR", msgName)
      uReturnValue = S_ERROR("Message %s does not return a S_OK/S_ERROR" % msgName)
    elapsedTime = time.time() - startTime
    self.__logRemoteQueryResponse(uReturnValue, elapsedTime)
    return S_OK([uReturnValue, elapsedTime])

####
#
#  Auth methods
#
####

  # @classmethod
  # def __authQuery( cls, method ):
  #  """
  #  Check if connecting user is allowed to perform an action
  #
  #  :type method: string
  #  :param method: Method to check
  #  :return: S_OK/S_ERROR
  #  """
  #  return cls.__srvInfoDict[ 'authManager' ].authQuery( method, cls.getRemoteCredentials() )

  def __logRemoteQuery(self, method, args):
    """
    Log the contents of a remote query

    :type method: string
    :param method: Method to log
    :type args: tuple
    :param args: Arguments of the method called
    """
    if self.srv_getCSOption("MaskRequestParams", True):
      argsString = "<masked>"
    else:
      argsString = "\n\t%s\n" % ",\n\t".join([str(arg)[:50] for arg in args])
    gLogger.notice("Executing action", "%s %s(%s)" % (self.srv_getFormattedRemoteCredentials(),
                                                      method,
                                                      argsString))

  def __logRemoteQueryResponse(self, retVal, elapsedTime):
    """
    Log the result of a query

    :type retVal: dictionary
    :param retVal: Return value of the query
    """
    if retVal['OK']:
      argsString = "OK"
    else:
      argsString = "ERROR: %s" % retVal['Message']
    gLogger.notice("Returning response", "%s (%.2f secs) %s" % (self.srv_getFormattedRemoteCredentials(),
                                                                elapsedTime, argsString))

####
#
#  Default ping method
#
####

  types_ping = []
  auth_ping = ['all']

  def export_ping(self):
    dInfo = {}
    dInfo['version'] = DIRAC.version
    dInfo['time'] = Time.dateTime()
    # Uptime
    dInfo['host uptime'] = int(time.time() - psutil.boot_time())
    startTime = self.serviceInfoDict['serviceStartTime']
    dInfo['service start time'] = self.serviceInfoDict['serviceStartTime']
    serviceUptime = Time.dateTime() - startTime
    dInfo['service uptime'] = serviceUptime.days * 3600 + serviceUptime.seconds
    # Load average
    dInfo['load'] = " ".join([str(lx) for lx in os.getloadavg()])
    dInfo['name'] = self.serviceInfoDict['serviceName']
    stTimes = os.times()
    dInfo['cpu times'] = {'user time': stTimes[0],
                          'system time': stTimes[1],
                          'children user time': stTimes[2],
                          'children system time': stTimes[3],
                          'elapsed real time': stTimes[4]
                          }

    return S_OK(dInfo)

  types_whoami = []
  auth_whoami = ['all']

  def export_whoami(self):
    """
      A simple whoami, returns all credential dictionary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      del credDict['x509Chain']
    return S_OK(credDict)

  types_echo = [six.string_types]

  @staticmethod
  def export_echo(data):
    """
    This method is used for testing performance of the service

    :param str data: data to be sent back to the caller

    :return: S_OK, Value is the input data
    """
    return S_OK(data)

  types_refreshConfiguration = [bool]
  auth_refreshConfiguration = [CS_ADMINISTRATOR]

  @staticmethod
  def export_refreshConfiguration(fromMaster):
    """
    Force refreshing the configuration data

    :param bool fromMaster: flag to refresh from the master configuration service
    """
    return gConfig.forceRefresh(fromMaster=fromMaster)

####
#
#  Utilities methods
#
####

  def srv_getRemoteAddress(self):
    """
    Get the address of the remote peer.

    :return: Address of remote peer.
    """
    return self.__trPool.get(self.__trid).getRemoteAddress()

  def srv_getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.__trPool.get(self.__trid).getConnectingCredentials()

  def srv_getFormattedRemoteCredentials(self):
    tr = self.__trPool.get(self.__trid)
    if tr:
      return tr.getFormattedCredentials()
    return "unknown"

  @classmethod
  def srv_getCSOption(cls, optionName, defaultValue=False):
    """
    Get an option from the CS section of the services

    :return: Value for serviceSection/optionName in the CS being defaultValue the default
    """
    if optionName[0] == "/":
      return gConfig.getValue(optionName, defaultValue)
    for csPath in cls.__srvInfoDict['csPaths']:
      result = gConfig.getOption("%s/%s" % (csPath, optionName, ), defaultValue)
      if result['OK']:
        return result['Value']
    return defaultValue

  def srv_getTransportID(self):
    return self.__trid

  def srv_getClientSetup(self):
    return self.serviceInfoDict['clientSetup']

  def srv_getClientVO(self):
    return self.serviceInfoDict['clientVO']

  def srv_getActionTuple(self):
    if 'actionTuple' not in self.serviceInfoDict:
      return ('Unknown yet', )
    return self.serviceInfoDict['actionTuple']

  def srv_getClientVersion(self):
    return self.serviceInfoDict.get("clientVersion")

  @classmethod
  def srv_getURL(cls):
    return cls.__srvInfoDict['URL']

  @classmethod
  def srv_getServiceName(cls):
    return cls.__srvInfoDict['serviceName']

  @classmethod
  def srv_getMonitor(cls):
    return cls.__monitor

  def srv_msgReply(self, msgObj):
    return self.__msgBroker.sendMessage(self.__trid, msgObj)

  @classmethod
  def srv_msgSend(cls, trid, msgObj):
    return cls.__msgBroker.sendMessage(trid, msgObj)

  @classmethod
  def srv_msgCreate(cls, msgName):
    return cls.__msgBroker.getMsgFactory().createMessage(cls.__svcName, msgName)

  @classmethod
  def srv_disconnectClient(cls, trid):
    return cls.__msgBroker.removeTransport(trid)

  def srv_disconnect(self, trid=None):
    if not trid:
      trid = self.srv_getTransportID()
    return self.__msgBroker.removeTransport(trid)
