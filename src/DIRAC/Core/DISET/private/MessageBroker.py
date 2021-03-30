""" Here, we need some documentation...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import threading
import select
import time
import socket
import os

# TODO: Remove ThreadPool later
useThreadPoolExecutor = False
if os.getenv('DIRAC_USE_NEWTHREADPOOL', 'YES').lower() in ('yes', 'true'):
  from concurrent.futures import ThreadPoolExecutor
  useThreadPoolExecutor = True
else:
  from DIRAC.Core.Utilities.ThreadPool import ThreadPool
  from DIRAC.Core.Utilities.ThreadPool import getGlobalThreadPool

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.private.TransportPool import getGlobalTransportPool
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.DISET.private.MessageFactory import MessageFactory, DummyMessage


class MessageBroker(object):

  def __init__(self, name, transportPool=None, threadPool=None):
    self.__name = name
    self.__messageTransports = {}
    self.__msgCounter = 0
    self.__msgCounterLock = threading.Lock()
    self.__responseCallbacks = {}
    self.__msgInTransport = {}
    self.__listenPersistConn = False
    self.__useMessageObjects = True
    self.__callbacksLock = threading.Condition()
    self.__trInOutLock = threading.Lock()
    self.__msgFactory = MessageFactory()
    self.__log = gLogger.getSubLogger("MSGBRK")
    if not transportPool:
      transportPool = getGlobalTransportPool()
    self.__trPool = transportPool
    if not threadPool:
      if useThreadPoolExecutor:
        threadPool = ThreadPoolExecutor(100)
      else:
        threadPool = getGlobalThreadPool()
    self.__threadPool = threadPool
    self.__listeningForMessages = False
    self.__listenThread = None

  def getNumConnections(self):
    return len(self.__messageTransports)

  def getMsgFactory(self):
    return self.__msgFactory

  def useMessageObjects(self, bD):
    self.__useMessageObjects = bD

  # Message id generation

  def __generateMsgId(self):
    self.__msgCounterLock.acquire()
    try:
      msgId = "%s:%d" % (self.__name, self.__msgCounter)
      self.__msgCounter += 1
      return msgId
    finally:
      self.__msgCounterLock.release()

  def getTransportPool(self):
    return self.__trPool

  # Add and remove transport to/from broker

  def addTransport(self, transport, *args, **kwargs):
    trid = self.__trPool.add(transport)
    try:
      result = self.addTransportId(trid, *args, **kwargs)
    except Exception as e:
      gLogger.exception("Cannot add transport id", lException=e)
      result = S_ERROR("Cannot add transport id")
    if not result['OK']:
      self.__trPool.remove(trid)
      return result
    return S_OK(trid)

  def addTransportId(self, trid, svcName,
                     receiveMessageCallback=None, disconnectCallback=None,
                     idleRead=False, listenToConnection=True):
    self.__trInOutLock.acquire()
    try:
      if trid in self.__messageTransports:
        return S_OK()
      tr = self.__trPool.get(trid)
      if not tr:
        return S_ERROR("No transport with id %s registered" % trid)
      self.__messageTransports[trid] = {'transport': tr,
                                        'svcName': svcName,
                                        'cbReceiveMessage': receiveMessageCallback,
                                        'cbDisconnect': disconnectCallback,
                                        'listen': listenToConnection,
                                        'idleRead': idleRead}
      self.__startListeningThread()
      return S_OK()
    finally:
      self.__trInOutLock.release()

  def listenToTransport(self, trid, listen=True):
    self.__trInOutLock.acquire()
    try:
      if trid in self.__messageTransports:
        self.__messageTransports[trid]['listen'] = listen
      self.__startListeningThread()
    finally:
      self.__trInOutLock.release()

  # Listen to connections

  def __startListeningThread(self):
    threadDead = self.__listeningForMessages and self.__listenThread is not None and not self.__listenThread.is_alive()
    if not self.__listeningForMessages or threadDead:
      self.__listeningForMessages = True
      self.__listenThread = threading.Thread(target=self.__listenAutoReceiveConnections)
      self.__listenThread.setDaemon(True)
      self.__listenThread.start()

  def __listenAutoReceiveConnections(self):
    while self.__listeningForMessages:
      self.__trInOutLock.acquire()
      try:
        sIdList = []
        for trid in self.__messageTransports:
          mt = self.__messageTransports[trid]
          if not mt['listen']:
            continue
          sIdList.append((trid, mt['transport'].getSocket()))
        if not sIdList:
          self.__listeningForMessages = False
          return
      finally:
        self.__trInOutLock.release()
      try:
        try:
          inList, _outList, _exList = select.select([pos[1] for pos in sIdList], [], [], 1)
          if len(inList) == 0:
            continue
        except socket.error:
          time.sleep(0.001)
          continue
        except select.error:
          time.sleep(0.001)
          continue
      except Exception as e:
        gLogger.exception("Exception while selecting persistent connections", lException=e)
        continue
      for sock in inList:
        for iPos in range(len(sIdList)):
          if sock == sIdList[iPos][1]:
            trid = sIdList[iPos][0]
            if trid in self.__messageTransports:
              result = self.__receiveMsgDataAndQueue(trid)
              if not result['OK']:
                self.removeTransport(trid)
            break

  # Process received data functions

  def __receiveMsgDataAndQueue(self, trid):
    # Receive
    result = self.__trPool.receive(trid,
                                   blockAfterKeepAlive=False,
                                   idleReceive=self.__messageTransports[trid]['idleRead'])
    self.__log.debug("[trid %s] Received data: %s" % (trid, str(result)))
    # If error close transport and exit
    if not result['OK']:
      self.__log.debug("[trid %s] ERROR RCV DATA %s" % (trid, result['Message']))
      gLogger.warn("Error while receiving message", "from %s : %s" % (self.__trPool.get(trid).getFormattedCredentials(),
                                                                      result['Message']))
      return self.removeTransport(trid)
    if useThreadPoolExecutor:
      self.__threadPool.submit(self.__processIncomingData, trid, result)
    else:
      self.__threadPool.generateJobAndQueueIt(self.__processIncomingData,
                                              args=(trid, result))
    return S_OK()

  def __processIncomingData(self, trid, receivedResult):
    # If keep alive, return OK
    if 'keepAlive' in receivedResult and receivedResult['keepAlive']:
      return S_OK()
    # If idle read return
    self.__trInOutLock.acquire()
    try:
      idleRead = self.__messageTransports[trid]['idleRead']
    except KeyError:
      return S_ERROR("Transport %s unknown" % trid)
    finally:
      self.__trInOutLock.release()
    if idleRead:
      if receivedResult['Value']:
        gLogger.fatal("OOOops. Idle read has returned data!")
      return S_OK()
    if not receivedResult['Value']:
      self.__log.debug("Transport %s closed connection" % trid)
      return self.removeTransport(trid)
    # This is a message req/resp
    msg = receivedResult['Value']
    # Valid message?
    if 'request' not in msg:
      gLogger.warn("Received data does not seem to be a message !!!!")
      return self.removeTransport(trid)
    # Decide if it's a response or a request
    if msg['request']:
      # If message has Id return ACK to received
      if 'id' in msg:
        self.__sendResponse(trid, msg['id'], S_OK())
      # Process msg
      result = self.__processIncomingRequest(trid, msg)
    else:
      result = self.__processIncomingResponse(trid, msg)
    # If error close the transport
    if not result['OK']:
      gLogger.info("Closing transport because of error while processing message", result['Message'])
      return self.removeTransport(trid)
    return S_OK()

  def __processIncomingRequest(self, trid, msg):
    self.__trInOutLock.acquire()
    try:
      rcvCB = self.__messageTransports[trid]['cbReceiveMessage']
    except KeyError:
      return S_ERROR("Transport %s unknown" % trid)
    finally:
      self.__trInOutLock.release()
    if not rcvCB:
      gLogger.fatal("Transport %s does not have a callback defined and a message arrived!" % trid)
      return S_ERROR("No message was expected in for this transport")
    # Check message has id and name
    for requiredField in ['name']:
      if requiredField not in msg:
        gLogger.error("Message does not have required field", requiredField)
        return S_ERROR("Message does not have %s" % requiredField)
    # Load message
    if 'attrs' in msg:
      attrs = msg['attrs']
      if not isinstance(attrs, (tuple, list)):
        return S_ERROR("Message args has to be a tuple or a list, not %s" % type(attrs))
    else:
      attrs = None
    # Do we "unpack" or do we send the raw data to the callback?
    if self.__useMessageObjects:
      result = self.__msgFactory.createMessage(self.__messageTransports[trid]['svcName'], msg['name'], attrs)
      if not result['OK']:
        return result
      msgObj = result['Value']
    else:
      msgObj = DummyMessage(msg)
    # Is msg ok?
    if not msgObj.isOK():
      return S_ERROR("Messsage is invalid")
    try:
      # Callback it and return response
      result = rcvCB(trid, msgObj)
      if not isReturnStructure(result):
        return S_ERROR("Request function does not return a result structure")
      return result
    except Exception as e:
      # Whoops. Show exception and return
      gLogger.exception("Exception while processing message %s" % msg['name'], lException=e)
      return S_ERROR("Exception while processing message %s: %s" % (msg['name'], str(e)))

  def __processIncomingResponse(self, trid, msg):
    # This is a message response
    for requiredField in ('id', 'result'):
      if requiredField not in msg:
        gLogger.error("Message does not have required field", requiredField)
        return S_ERROR("Message does not have %s" % requiredField)
    if not isReturnStructure(msg['result']):
      return S_ERROR("Message response did not return a result structure")
    return self.__notifyCallback(msg['id'], msg['result'])

  # Sending functions

  def __sendResponse(self, trid, msgId, msgResult):
    msgResponse = {'request': False, 'id': msgId, 'result': msgResult}
    _result = self.__trPool.send(trid, S_OK(msgResponse))

  def sendMessage(self, trid, msgObj):
    if not msgObj.isOK():
      return S_ERROR("Message is not ready to be sent")
    result = self.__sendMessage(trid, msgObj)
    if not result['OK']:
      self.removeTransport(trid)
    return result

  def __sendMessage(self, trid, msgObj):
    if not self.__trPool.exists(trid):
      return S_ERROR("Not transport with id %s defined for messaging" % trid)

    msg = {'request': True, 'name': msgObj.getName()}
    attrs = msgObj.dumpAttrs()['Value']
    msg['attrs'] = attrs
    waitForAck = msgObj.getWaitForAck()

    if not waitForAck:
      return self.__trPool.send(trid, S_OK(msg))

    msgId = self.__generateMsgId()
    msg['id'] = msgId

    self.__generateMessageResponse(trid, msgId)
    result = self.__trPool.send(trid, S_OK(msg))

    # Lock and generate and wait
    self.__callbacksLock.acquire()
    try:
      if not result['OK']:
        # Release lock and exit
        self.__clearCallback(msgId)
        return result

      return self.__waitForMessageResponse(msgId)
    finally:
      self.__callbacksLock.release()

  # Callback nightmare

  # Lock need to have been aquired prior to func
  def __generateMessageResponse(self, trid, msgId):
    self.__callbacksLock.acquire()
    try:
      if msgId in self.__responseCallbacks:
        return self.__responseCallbacks[msgId]
      if trid not in self.__msgInTransport:
        self.__msgInTransport[trid] = set()
      self.__msgInTransport[trid].add(msgId)
      self.__responseCallbacks[msgId] = {'creationTime': time.time(),
                                         'trid': trid
                                         }
      return self.__responseCallbacks[msgId]
    finally:
      self.__callbacksLock.release()

  # Lock need to have been aquired prior to func
  def __waitForMessageResponse(self, msgId):
    if msgId not in self.__responseCallbacks:
      return S_ERROR("Invalid msg id")
    respCallback = self.__responseCallbacks[msgId]
    while 'result' not in respCallback and time.time() - respCallback['creationTime'] < 30:
      self.__callbacksLock.wait(30)
    self.__clearCallback(msgId)
    if 'result' in respCallback:
      return respCallback['result']
    return S_ERROR("Timeout while waiting for message ack")

  def __clearCallback(self, msgId):
    if msgId not in self.__responseCallbacks:
      return False
    trid = self.__responseCallbacks[msgId]['trid']
    self.__responseCallbacks.pop(msgId)
    try:
      self.__msgInTransport[trid].remove(msgId)
    except KeyError:
      pass
    return True

  # Lock need to have been aquired prior to func
  def __setCallbackResult(self, msgId, result=False):
    if msgId not in self.__responseCallbacks:
      return False
    self.__responseCallbacks[msgId]['result'] = result
    return True

  def __notifyCallback(self, msgId, msgResult):
    self.__callbacksLock.acquire()
    try:
      if self.__setCallbackResult(msgId, msgResult):
        self.__callbacksLock.notifyAll()
    finally:
      self.__callbacksLock.release()
    return S_OK()

  def removeTransport(self, trid, closeTransport=True):
    # Delete from the message Transports
    self.__trInOutLock.acquire()
    try:
      if trid not in self.__messageTransports:
        return S_OK()

      # Save the disconnect callback if it's there
      if self.__messageTransports[trid]['cbDisconnect']:
        cbDisconnect = self.__messageTransports[trid]['cbDisconnect']
      else:
        cbDisconnect = False

      self.__messageTransports.pop(trid)
      if closeTransport:
        self.__trPool.close(trid)
    finally:
      self.__trInOutLock.release()

    # Flush remaining messages
    self.__callbacksLock.acquire()
    try:
      msgIds = False
      if trid in self.__msgInTransport:
        msgIds = set(self.__msgInTransport[trid])
        self.__msgInTransport.pop(trid)
        for msgId in msgIds:
          self.__setCallbackResult(msgId, S_ERROR("Connection closed by peer"))
      self.__callbacksLock.notifyAll()
    finally:
      self.__callbacksLock.release()

    # Queue the disconnect CB if it's there
    if cbDisconnect:
      if useThreadPoolExecutor:
        self.__threadPool.submit(cbDisconnect, trid)
      else:
        self.__threadPool.generateJobAndQueueIt(cbDisconnect,
                                                args=(trid,))

    return S_OK()


class MessageSender(object):

  def __init__(self, serviceName, msgBroker):
    self.__serviceName = serviceName
    self.__msgBroker = msgBroker

  def getServiceName(self):
    return self.__serviceName

  def sendMessage(self, trid, msgObj):
    return self.__msgBroker.sendMessage(trid, msgObj)

  def createMessage(self, msgName):
    return self.__msgBroker.__msgFactory.createMessage(self.__serviceName, msgName)


gMessageBroker = False


def getGlobalMessageBroker():
  global gMessageBroker
  if not gMessageBroker:
    gMessageBroker = MessageBroker('GlobalMessageBroker', transportPool=getGlobalTransportPool())
  return gMessageBroker
