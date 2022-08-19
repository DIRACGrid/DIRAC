import random
import threading
import datetime
from hashlib import md5

from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.MessageBroker import getGlobalMessageBroker
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.Utilities import Network
from DIRAC.FrameworkSystem.Client.Logger import gLogger

gMsgSync = Synchronizer()


class MessageClient(BaseClient):
    class MSGException(Exception):
        pass

    def _initialize(self):
        self.__trid = False
        self.__transport = None
        self.__uniqueName = self.__generateUniqueClientName()
        self.__msgBroker = getGlobalMessageBroker()
        self.__callbacks = {}
        self.__connectExtraParams = {}
        self.__specialCallbacks = {"drop": [], "msg": []}
        self.__connectionLock = threading.Lock()

    def __generateUniqueClientName(self):
        hashStr = ":".join(
            (str(datetime.datetime.utcnow()), str(random.random()), Network.getFQDN(), gLogger.getName())
        )
        hexHash = md5(hashStr.encode()).hexdigest()
        return hexHash

    def setUniqueName(self, uniqueName):
        self.__uniqueName = uniqueName

    def __checkResult(self, result):
        if not result["OK"]:
            raise self.MSGException(result["Message"])
        return result["Value"]

    def createMessage(self, msgName):
        return self.__msgBroker.getMsgFactory().createMessage(self.getServiceName(), msgName)

    @property
    def connected(self):
        return self.__trid

    def connect(self, **extraParams):
        if extraParams:
            self.__connectExtraParams = extraParams
        # Avoid acquiring the lock if it's clear we already have a connection
        if self.__trid:
            return S_ERROR("Already connected")
        self.__connectionLock.acquire()
        try:
            if self.__trid:
                return S_ERROR("Already connected")

            trid, transport = self.__checkResult(self._connect())
            self.__checkResult(self._proposeAction(transport, ("Connection", "new")))
            self.__checkResult(transport.sendData(S_OK([self.__uniqueName, self.__connectExtraParams])))
            self.__checkResult(transport.receiveData())
            self.__checkResult(
                self.__msgBroker.addTransportId(
                    trid,
                    self._serviceName,
                    receiveMessageCallback=self.__cbRecvMsg,
                    disconnectCallback=self.__cbDisconnect,
                )
            )
            self.__trid = trid
            self.__transport = transport
        except self.MSGException as e:
            return S_ERROR(str(e))
        finally:
            self.__connectionLock.release()
        return S_OK()

    def __cbDisconnect(self, trid):
        if not self.__trid:
            return
        if self.__trid != trid:
            gLogger.error("OOps. trid's don't match. This shouldn't happen!", f"({self.__trid} vs {trid})")
            return S_ERROR("OOOPS")
        self.__trid = False
        try:
            self.__transport.close()
        except Exception:
            pass
        for cb in self.__specialCallbacks["drop"]:
            try:
                cb(self)
            except Exception:
                gLogger.exception("Exception while processing disconnect callbacks")

    def __cbRecvMsg(self, trid, msgObj):
        msgName = msgObj.getName()
        msgObj.setMsgClient(self)
        for cb in self.__specialCallbacks["msg"]:
            try:
                result = cb(self, msgObj)
                if not isReturnStructure(result):
                    gLogger.error("Callback for message does not return S_OK/S_ERROR", msgObj.getName())
                    return S_ERROR("No response")
                if not result["OK"]:
                    return result
                # If no specific callback but a generic one, return the generic one
                if msgName not in self.__callbacks:
                    return result
            except Exception:
                gLogger.exception("Exception while processing callbacks", msgObj.getName())
        if msgName not in self.__callbacks:
            return S_ERROR("Unexpected message")
        try:
            result = self.__callbacks[msgName](msgObj)
            if not isReturnStructure(result):
                gLogger.error("Callback for message does not return S_OK/S_ERROR", msgName)
                return S_ERROR("No response")
            return result
        except Exception:
            gLogger.exception("Exception while processing callbacks", msgName)
        return S_ERROR("No response")

    def getTrid(self):
        return self.__trid

    def sendMessage(self, msgObj):
        if not self.__trid:
            result = self.connect()
            if not result["OK"]:
                return result
        return self.__msgBroker.sendMessage(self.__trid, msgObj)

    def subscribeToAllMessages(self, cbFunction):
        if not callable(cbFunction):
            return S_ERROR("%s is not callable" % cbFunction)
        self.__specialCallbacks["msg"].append(cbFunction)
        return S_OK()

    def subscribeToMessage(self, msgName, cbFunction):
        if not callable(cbFunction):
            return S_ERROR("%s is not callable" % cbFunction)
        self.__callbacks[msgName] = cbFunction
        return S_OK()

    def subscribeToDisconnect(self, cbFunction):
        if not callable(cbFunction):
            return S_ERROR("%s is not callable" % cbFunction)
        self.__specialCallbacks["drop"].append(cbFunction)
        return S_OK()

    def clearSubscription(self, msgName):
        try:
            del self.__callbacks[msgName]
        except KeyError:
            return False
        return True

    def disconnect(self):
        trid = self.__trid
        self.__trid = False
        self.__msgBroker.removeTransport(trid)
