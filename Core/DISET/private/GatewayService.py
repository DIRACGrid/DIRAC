""" The gateway service is used for forwarding service calls to the appropriate services.

    For this to be used, the following CS option is required::

      DIRAC
      {
        Gateways
        {
          my.site.org = dips://thisIsAn.url.org:9159/Framework/Gateway
        }
      }

    At the same time, this same gateway service should be run with option /LocalInstallation/Site
    which is different from "my.site.org" or whatever is set in the option above, to avoid initialization loops.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import sys
from six import BytesIO
import os

# TODO: Remove ThreadPool later
useThreadPoolExecutor = False
if os.getenv('DIRAC_USE_NEWTHREADPOOL', 'YES').lower() in ('yes', 'true'):
  from concurrent.futures import ThreadPoolExecutor
  useThreadPoolExecutor = True
else:
  from DIRAC.Core.Utilities.ThreadPool import ThreadPool

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.DISET.private.MessageBroker import MessageBroker, getGlobalMessageBroker
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.DISET.private.Service import Service
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.private.BaseClient import BaseClient

__RCSID__ = "$Id$"


class GatewayService(Service):
  """ Inherits from Service so it can (and should) be run as a DIRAC service,
      but replaces several of the internal methods
  """

  GATEWAY_NAME = "Framework/Gateway"

  def __init__(self):
    """ Initialize like a real service
    """
    super(GatewayService, self).__init__(
        {'modName': GatewayService.GATEWAY_NAME,
         'loadName': GatewayService.GATEWAY_NAME,
         'standalone': True,
         'moduleObj': sys.modules[DIRAC.Core.DISET.private.GatewayService.GatewayService.__module__],
         'classObj': self.__class__})
    self.__delegatedCredentials = DictCache()
    self.__transferBytesLimit = 1024 * 1024 * 100
    # to be resolved
    self._url = None
    self._handler = None
    self._threadPool = None
    self._msgBroker = None
    self._msgForwarder = None

  def initialize(self):
    """ This replaces the standard initialize from Service
    """
    # Build the URLs
    self._url = self._cfg.getURL()
    if not self._url:
      return S_ERROR("Could not build service URL for %s" % GatewayService.GATEWAY_NAME)
    gLogger.verbose("Service URL is %s" % self._url)
    # Load handler
    result = self._loadHandlerInit()
    if not result['OK']:
      return result
    self._handler = result['Value']
    # Discover Handler
    # TODO: remove later
    if useThreadPoolExecutor:
      self._threadPool = ThreadPoolExecutor(max(0, self._cfg.getMaxThreads()))
    else:
      self._threadPool = ThreadPool(1,
                                    max(0, self._cfg.getMaxThreads()),
                                    self._cfg.getMaxWaitingPetitions())
      self._threadPool.daemonize()

    self._msgBroker = MessageBroker("%sMSB" % GatewayService.GATEWAY_NAME, threadPool=self._threadPool)
    self._msgBroker.useMessageObjects(False)
    getGlobalMessageBroker().useMessageObjects(False)
    self._msgForwarder = MessageForwarder(self._msgBroker)
    return S_OK()

  def _processInThread(self, clientTransport):
    """ Threaded process function
    """
    # Handshake
    try:
      clientTransport.handshake()
    except BaseException:
      return
    # Add to the transport pool
    trid = self._transportPool.add(clientTransport)
    if not trid:
      return
    # Receive and check proposal
    result = self._receiveAndCheckProposal(trid)
    if not result['OK']:
      self._transportPool.sendAndClose(trid, result)
      return
    proposalTuple = result['Value']
    # Instantiate handler
    result = self.__getClientInitArgs(trid, proposalTuple)
    if not result['OK']:
      self._transportPool.sendAndClose(trid, result)
      return
    clientInitArgs = result['Value']
    # Execute the action
    result = self._processProposal(trid, proposalTuple, clientInitArgs)
    # Close the connection if required
    if result['closeTransport']:
      self._transportPool.close(trid)
    return result

  def _receiveAndCheckProposal(self, trid):
    clientTransport = self._transportPool.get(trid)
    # Get the peer credentials
    credDict = clientTransport.getConnectingCredentials()
    # Receive the action proposal
    retVal = clientTransport.receiveData(1024)
    if not retVal['OK']:
      gLogger.error("Invalid action proposal", "%s %s" % (self._createIdentityString(credDict,
                                                                                     clientTransport),
                                                          retVal['Message']))
      return S_ERROR("Invalid action proposal")
    proposalTuple = retVal['Value']
    gLogger.debug("Received action from client", "/".join(list(proposalTuple[1])))
    # Check if there are extra credentials
    if proposalTuple[2]:
      clientTransport.setExtraCredentials(proposalTuple[2])
    return S_OK(proposalTuple)

  def __getClientInitArgs(self, trid, proposalTuple):
    clientTransport = self._transportPool.get(trid)
    # Get the peer credentials
    credDict = clientTransport.getConnectingCredentials()
    if 'x509Chain' not in credDict:
      return S_OK()
    cKey = (credDict['DN'],
            credDict.get('group', False),
            credDict.get('extraCredentials', False),
            credDict['isLimitedProxy'])
    dP = self.__delegatedCredentials.get(cKey, 3600)
    idString = self._createIdentityString(credDict, clientTransport)
    if dP:
      gLogger.verbose("Proxy for %s is cached" % idString)
      return S_OK(dP)
    result = self.__requestDelegation(clientTransport, credDict)
    if not result['OK']:
      gLogger.warn("Could not get proxy for %s: %s" % (idString, result['Message']))
      return result
    delChain = result['Value']
    delegatedChain = delChain.dumpAllToString()['Value']
    secsLeft = delChain.getRemainingSecs()['Value'] - 1
    clientInitArgs = {BaseClient.KW_SETUP: proposalTuple[0][1],
                      BaseClient.KW_TIMEOUT: 600,
                      BaseClient.KW_IGNORE_GATEWAYS: True,
                      BaseClient.KW_USE_CERTIFICATES: False,
                      BaseClient.KW_PROXY_STRING: delegatedChain
                      }
    if BaseClient.KW_EXTRA_CREDENTIALS in credDict:
      clientInitArgs[BaseClient.KW_EXTRA_CREDENTIALS] = credDict[BaseClient.KW_EXTRA_CREDENTIALS]
    gLogger.warn("Got delegated proxy for %s: %s secs left" % (idString, secsLeft))
    self.__delegatedCredentials.add(cKey, secsLeft, clientInitArgs)
    return S_OK(clientInitArgs)

  def __requestDelegation(self, clientTransport, credDict):
    peerChain = credDict['x509Chain']
    retVal = peerChain.getCertInChain()['Value'].generateProxyRequest()
    if not retVal['OK']:
      return retVal
    delegationRequest = retVal['Value']
    retVal = delegationRequest.dumpRequest()
    if not retVal['OK']:
      retVal = S_ERROR("Server Error: Can't generate delegation request")
      clientTransport.sendData(retVal)
      return retVal
    gLogger.info("Sending delegation request for %s" % delegationRequest.getSubjectDN()['Value'])
    clientTransport.sendData(S_OK({'delegate': retVal['Value']}))
    delegatedCertChain = clientTransport.receiveData()
    delegatedChain = X509Chain(keyObj=delegationRequest.getPKey())
    retVal = delegatedChain.loadChainFromString(delegatedCertChain)
    if not retVal['OK']:
      retVal = S_ERROR("Error in receiving delegated proxy: %s" % retVal['Message'])
      clientTransport.sendData(retVal)
      return retVal
    return S_OK(delegatedChain)

  # Msg

  def _mbConnect(self, trid, handlerObj=None):
    return S_OK()

  def _mbReceivedMsg(self, cliTrid, msgObj):
    return self._msgForwarder.msgFromClient(cliTrid, msgObj)

  def _mbDisconnect(self, cliTrid):
    self._msgForwarder.cliDisconnect(cliTrid)

  # Execute action

  def _executeAction(self, trid, proposalTuple, clientInitArgs):
    clientTransport = self._transportPool.get(trid)
    credDict = clientTransport.getConnectingCredentials()
    targetService = proposalTuple[0][0]
    actionType = proposalTuple[1][0]
    actionMethod = proposalTuple[1][1]
    idString = self._createIdentityString(credDict, clientTransport)
    # OOkay! Lets do the magic!
    retVal = clientTransport.receiveData()
    if not retVal['OK']:
      gLogger.error("Error while receiving file description", retVal['Message'])
      clientTransport.sendData(S_ERROR("Error while receiving file description: %s" % retVal['Message']))
      return
    if actionType == "FileTransfer":
      gLogger.warn("Received a file transfer action from %s" % idString)
      clientTransport.sendData(S_OK("Accepted"))
      retVal = self.__forwardFileTransferCall(targetService, clientInitArgs,
                                              actionMethod, retVal['Value'], clientTransport)
    elif actionType == "RPC":
      gLogger.info("Forwarding %s/%s action to %s for %s" % (actionType, actionMethod, targetService, idString))
      retVal = self.__forwardRPCCall(targetService, clientInitArgs, actionMethod, retVal['Value'])
    elif actionType == "Connection" and actionMethod == "new":
      gLogger.info("Initiating a messaging connection to %s for %s" % (targetService, idString))
      retVal = self._msgForwarder.addClient(trid, targetService, clientInitArgs, retVal['Value'])
    else:
      gLogger.warn("Received an invalid %s/%s action from %s" % (actionType, actionMethod, idString))
      retVal = S_ERROR("Unknown type of action (%s)" % actionType)
    # TODO: Send back the data?
    if 'rpcStub' in retVal:
      retVal.pop('rpcStub')
    clientTransport.sendData(retVal)
    return retVal

  def __forwardRPCCall(self, targetService, clientInitArgs, method, params):
    if targetService == "Configuration/Server":
      if method == "getCompressedDataIfNewer":
        # Relay CS data directly
        serviceVersion = gConfigurationData.getVersion()
        retDict = {'newestVersion': serviceVersion}
        clientVersion = params[0]
        if clientVersion < serviceVersion:
          retDict['data'] = gConfigurationData.getCompressedData()
        return S_OK(retDict)
    # Default
    rpcClient = RPCClient(targetService, **clientInitArgs)
    methodObj = getattr(rpcClient, method)
    return methodObj(*params)

  def __forwardFileTransferCall(self, targetService, clientInitArgs, method,
                                params, clientTransport):
    transferRelay = TransferRelay(targetService, **clientInitArgs)
    transferRelay.setTransferLimit(self.__transferBytesLimit)
    cliFH = FileHelper(clientTransport)
    # Check file size
    if method.find("ToClient") > -1:
      cliFH.setDirection("send")
    elif method.find("FromClient") > -1:
      cliFH.setDirection("receive")
      if not self.__ftCheckMaxTransferSize(params[2]):
        cliFH.markAsTransferred()
        return S_ERROR("Transfer size is too big")
    # Forward queries
    try:
      relayMethodObject = getattr(transferRelay, 'forward%s' % method)
    except BaseException:
      return S_ERROR("Cannot forward unknown method %s" % method)
    result = relayMethodObject(cliFH, params)
    return result

  def __ftCheckMaxTransferSize(self, requestedTransferSize):
    if not self.__transferBytesLimit:
      return True
    if not requestedTransferSize:
      return True
    if requestedTransferSize <= self.__transferBytesLimit:
      return True
    return False


class TransferRelay(TransferClient):

  def setTransferLimit(self, trLimit):
    self.__transferBytesLimit = trLimit
    self.__currentMethod = ""

  def infoMsg(self, msg, dynMsg=""):
    gLogger.info("[%s] %s" % (self.__currentMethod, msg), dynMsg)

  def errMsg(self, msg, dynMsg=""):
    gLogger.error("[%s] %s" % (self.__currentMethod, msg), dynMsg)

  def getDataFromClient(self, clientFileHelper):
    sIO = BytesIO()
    self.infoMsg("About to get data from client")
    result = clientFileHelper.networkToDataSink(sIO, self.__transferBytesLimit)
    if not result['OK']:
      sIO.close()
      self.errMsg("Could not get data from client", result['Message'])
      return result
    data = sIO.getvalue()
    sIO.close()
    self.infoMsg("Got %s bytes from client" % len(data))
    return S_OK(data)

  def sendDataToClient(self, clientFileHelper, dataToSend):
    self.infoMsg("About to get send data to client")
    result = clientFileHelper.BufferToNetwork(dataToSend)
    if not result['OK']:
      self.errMsg("Could not send data to client", result['Message'])
      return result
    self.infoMsg("Sent %s bytes from client" % len(dataToSend))
    return S_OK()

  def sendDataToService(self, srvMethod, params, data):
    self.infoMsg("Sending header request to %s" % self.getDestinationService(), str(params))
    result = self._sendTransferHeader(srvMethod, params)
    if not result['OK']:
      self.errMsg("Could not send header", result['Message'])
      return result
    self.infoMsg("Starting to send data to service")
    _, srvTransport = result['Value']
    srvFileHelper = FileHelper(srvTransport)
    srvFileHelper.setDirection("send")
    result = srvFileHelper.BufferToNetwork(data)
    if not result['OK']:
      self.errMsg("Could send data to server", result['Message'])
      srvTransport.close()
      return result
    self.infoMsg("Data sent to service (%s bytes)" % len(data))
    retVal = srvTransport.receiveData()
    srvTransport.close()
    return retVal

  def getDataFromService(self, srvMethod, params):
    self.infoMsg("Sending header request to %s" % self.getDestinationService(), str(params))
    result = self._sendTransferHeader(srvMethod, params)
    if not result['OK']:
      self.errMsg("Could not send header", result['Message'])
      return result
    self.infoMsg("Starting to receive data from service")
    _, srvTransport = result['Value']
    srvFileHelper = FileHelper(srvTransport)
    srvFileHelper.setDirection("receive")
    sIO = BytesIO()
    result = srvFileHelper.networkToDataSink(sIO, self.__transferBytesLimit)
    if not result['OK']:
      self.errMsg("Could not receive data from server", result['Message'])
      srvTransport.close()
      sIO.close()
      return result
    dataReceived = sIO.getvalue()
    sIO.close()
    self.infoMsg("Received %s bytes from service" % len(dataReceived))
    retVal = srvTransport.receiveData()
    srvTransport.close()
    if not retVal['OK']:
      return retVal
    return S_OK({'data': dataReceived, 'srvResponse': retVal})

  def forwardFromClient(self, clientFileHelper, params):
    fileId, token = params[:2]
    self.__currentMethod = "FromClient"
    result = self.getDataFromClient(clientFileHelper)
    if not result['OK']:
      return result
    dataReceived = result['Value']
    receivedBytes = clientFileHelper.getTransferedBytes()
    return self.sendDataToService("FromClient", (fileId, token, receivedBytes), dataReceived)

  def forwardBulkFromClient(self, clientFileHelper, params):
    fileId, token = params[:2]
    self.__currentMethod = "BulkFromClient"
    result = self.getDataFromClient(clientFileHelper)
    if not result['OK']:
      return result
    dataReceived = result['Value']
    receivedBytes = clientFileHelper.getTransferedBytes()
    return self.sendDataToService("BulkFromClient", (fileId, token, receivedBytes), dataReceived)

  def forwardToClient(self, clientFileHelper, params):
    fileId, token = params[:2]
    self.__currentMethod = "ToClient"
    result = self.getDataFromService("ToClient", (fileId, token))
    if not result['OK']:
      return result
    dataReceived = result['Value']['data']
    srvResponse = result['Value']['srvResponse']
    result = self.sendDataToClient(clientFileHelper, dataReceived)
    if not result['OK']:
      return result
    return srvResponse

  def forwardBulkToClient(self, clientFileHelper, params):
    fileId, token = params[:2]
    self.__currentMethod = "BulkToClient"
    result = self.getDataFromService("BulkToClient", (fileId, token))
    if not result['OK']:
      return result
    dataReceived = result['Value']['data']
    srvResponse = result['Value']['srvResponse']
    result = self.sendDataToClient(clientFileHelper, dataReceived)
    if not result['OK']:
      return result
    return srvResponse

  def forwardListBulk(self, clientFileHelper, params):
    self.__currentMethod = "ListBulk"
    self.infoMsg("Sending header request to %s" % self.getDestinationService(), str(params))
    result = self._sendTransferHeader("ListBulk", params)
    if not result['OK']:
      self.errMsg("Could not send header", result['Message'])
      return result
    _, srvTransport = result['Value']
    response = srvTransport.receiveData(1048576)
    srvTransport.close()
    self.infoMsg("Sending data back to client")
    return response


class MessageForwarder(object):

  def __init__(self, msgBroker):
    self.__inOutLock = LockRing().getLock()
    self.__msgBroker = msgBroker
    self.__byClient = {}
    self.__srvToCliTrid = {}

  def addClient(self, cliTrid, destination, clientInitParams, connectParams):
    if cliTrid in self.__byClient:
      gLogger.fatal("Trid is duplicated!! this shouldn't happen")
      return
    msgClient = MessageClient(destination, **clientInitParams)
    msgClient.subscribeToDisconnect(self.__srvDisconnect)
    msgClient.subscribeToAllMessages(self.msgFromSrv)
    msgClient.setUniqueName(connectParams[0])
    result = msgClient.connect(**connectParams[1])
    if not result['OK']:
      return result
    self.__inOutLock.acquire()
    try:
      self.__byClient[cliTrid] = {'srvEnd': msgClient,
                                  'srvTrid': msgClient.getTrid(),
                                  'srvName': destination}
      self.__srvToCliTrid[msgClient.getTrid()] = cliTrid
    finally:
      self.__inOutLock.release()
    return result

  def __srvDisconnect(self, srvEndCli):
    try:
      cliTrid = self.__srvToCliTrid[srvEndCli.getTrid()]
    except IndexError:
      gLogger.exception("This shouldn't happen!")
    gLogger.info("Service %s disconnected messaging connection" % self.__byClient[cliTrid]['srvName'])
    self.__msgBroker.removeTransport(cliTrid)
    self.__removeClient(cliTrid)

  def cliDisconnect(self, cliTrid):
    if cliTrid not in self.__byClient:
      gLogger.fatal("This shouldn't happen!")
      return
    gLogger.info("Client to %s disconnected messaging connection" % self.__byClient[cliTrid]['srvName'])
    self.__byClient[cliTrid]['srvEnd'].disconnect()
    self.__removeClient(cliTrid)

  def __removeClient(self, cliTrid):
    self.__inOutLock.acquire()
    try:
      try:
        srvTrid = self.__byClient[cliTrid]['srvTrid']
        self.__byClient.pop(cliTrid)
        self.__srvToCliTrid.pop(srvTrid)
      except Exception as e:
        gLogger.exception("This shouldn't happen!", e)
    finally:
      self.__inOutLock.release()

  def msgFromClient(self, cliTrid, msgObj):
    gLogger.info("Message %s to %s service" % (msgObj.getName(), self.__byClient[cliTrid]['srvName']))
    result = self.__byClient[cliTrid]['srvEnd'].sendMessage(msgObj)
    return result

  def msgFromSrv(self, srvEndCli, msgObj):
    try:
      cliTrid = self.__srvToCliTrid[srvEndCli.getTrid()]
    except BaseException:
      gLogger.exception("This shouldn't happen")
      return S_ERROR("MsgFromSrv -> Mismatched srv2cli trid")
    gLogger.info("Message %s from %s service" % (msgObj.getName(), self.__byClient[cliTrid]['srvName']))
    return self.__msgBroker.sendMessage(cliTrid, msgObj)
