import time
import threading
from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler


class TransportPool:
    def __init__(self, logger=False):
        if logger:
            self.log = logger
        else:
            self.log = gLogger
        self.__modLock = threading.Lock()
        self.__transports = {}
        self.__listenPersistConn = False
        self.__msgCounter = 0
        result = gThreadScheduler.addPeriodicTask(5, self.__sendKeepAlives)
        if not result["OK"]:
            self.log.fatal("Cannot add task to thread scheduler", result["Message"])
        self.__keepAlivesTask = result["Value"]

    #
    # Send keep alives
    #

    def __sendKeepAlives(self, retries=5):
        if retries == 0:
            return
        tridList = []
        try:
            tridList = [trid for trid in self.__transports]
        except RuntimeError:
            self.__sendKeepAlives(retries - 1)
        for trid in tridList:
            try:
                tr = self.__transports[trid][0]
            except KeyError:
                continue
            if not tr.getKeepAliveLapse():
                continue
            try:
                tr.sendKeepAlive(now=time.time())
            except KeyError:
                continue
            except Exception:
                gLogger.exception("Cannot send keep alive")

    # exists

    def exists(self, trid):
        return trid in self.__transports

    # Add

    def add(self, transport):
        remoteAddr = transport.getRemoteAddress()
        localAddr = transport.getLocalAddress()
        self.log.debug(f"New connection -> {remoteAddr[0]}:{remoteAddr[1]}")
        trid = f"{localAddr[0]}:{localAddr[1]}->{remoteAddr[0]}:{remoteAddr[1]}"
        return self.__add(trid, transport)

    def __add(self, trid, transport):
        self.__modLock.acquire()
        try:
            if not self.exists(trid):
                self.__transports[trid] = (transport, {})
        finally:
            self.__modLock.release()
        return trid

    # Data association

    def associateData(self, trid, kw, value):
        self.__modLock.acquire()
        try:
            if trid in self.__transports:
                self.__transports[trid][1][kw] = value
        finally:
            self.__modLock.release()

    def getAssociatedData(self, trid, kw):
        try:
            return self.__transports[trid][1][kw]
        except KeyError:
            return None

    # Get transport

    def get(self, trid):
        try:
            return self.__transports[trid][0]
        except KeyError:
            return None

    # Receive
    def receive(self, trid, maxBufferSize=0, blockAfterKeepAlive=True, idleReceive=False):
        try:
            received = self.__transports[trid][0].receiveData(maxBufferSize, blockAfterKeepAlive, idleReceive)
            return received
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)

    # Send
    def send(self, trid, msg):
        try:
            transport = self.__transports[trid][0]
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)
        return transport.sendData(msg)

    # Send And Close

    def sendErrorAndClose(self, trid, msg):
        try:
            result = self.__transports[trid][0].sendData(S_ERROR(msg))
            if not result["OK"]:
                return result
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)
        finally:
            self.close(trid)

    def sendAndClose(self, trid, msg):
        try:
            result = self.__transports[trid][0].sendData(msg)
            if not result["OK"]:
                return result
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)
        finally:
            self.close(trid)

    def sendKeepAlive(self, trid, responseId=None):
        try:
            return self.__transports[trid][0].sendKeepAlive(responseId)
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)

    # Close

    def close(self, trid):
        try:
            self.__transports[trid][0].close()
        except KeyError:
            return S_ERROR("No transport with id %s defined" % trid)
        self.remove(trid)

    def remove(self, trid):
        self.__modLock.acquire()
        try:
            if trid in self.__transports:
                del self.__transports[trid]
        finally:
            self.__modLock.release()


gTransportPool = None


def getGlobalTransportPool():
    global gTransportPool
    if not gTransportPool:
        gTransportPool = TransportPool()
    return gTransportPool
