__RCSID__ = "$Id$"

import os
import time
import GSI
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.Transports.SSL.pygsi.SocketInfoFactory import gSocketInfoFactory
from DIRAC.Core.Utilities.Devloader import Devloader


GSI.SSL.set_thread_safe()


class SSLTransport(BaseTransport):

  __readWriteLock = LockRing().getLock()

  def __init__(self, *args, **kwargs):
    self.__writesDone = 0
    self.__locked = False
    BaseTransport.__init__(self, *args, **kwargs)

  def __lock(self, timeout=1000):
    while self.__locked and timeout:
      time.sleep(0.005)
      timeout -= 0.005
    if not timeout:
      return False
    SSLTransport.__readWriteLock.acquire()
    if self.__locked:
      SSLTransport.__readWriteLock.release()
      return self.__lock(timeout)
    self.__locked = True
    SSLTransport.__readWriteLock.release()
    return True

  def __unlock(self):
    self.__locked = False

  def setSocketTimeout(self, timeout):
    """
    This method is used to chenge the default timeout of the socket
    """
    gSocketInfoFactory.setSocketTimeout(timeout)

  def initAsClient(self):
    retVal = gSocketInfoFactory.getSocket(self.stServerAddress, **self.extraArgsDict)
    if not retVal['OK']:
      return retVal
    self.oSocketInfo = retVal['Value']
    self.oSocket = self.oSocketInfo.getSSLSocket()
    if not self.oSocket.session_reused():
      gLogger.debug("New session connecting to server at %s" % str(self.stServerAddress))
    self.remoteAddress = self.oSocket.getpeername()
    return S_OK()

  def initAsServer(self):
    if not self.serverMode():
      raise RuntimeError("Must be initialized as server mode")
    retVal = gSocketInfoFactory.getListeningSocket(self.stServerAddress,
                                                   self.iListenQueueSize,
                                                   self.bAllowReuseAddress,
                                                   **self.extraArgsDict)
    if not retVal['OK']:
      return retVal
    self.oSocketInfo = retVal['Value']
    self.oSocket = self.oSocketInfo.getSSLSocket()
    Devloader().addStuffToClose(self.oSocket)
    return S_OK()

  def close(self):
    gLogger.debug("Closing socket")
    try:

      # Chris 11.09.18
      # I think this will never work,
      # From the fsync man page
      # """
      # EROFS, EINVAL: fd is bound to a special file (e.g., a pipe, FIFO, or socket)
      # which does not support synchronization.
      # """"
      # For the records, it was added in 67ca305a02621cf36a558cb42896dbb599df9dc9
      # I guess it should be just removed alltogther, but well...
      # os.fsync( self.oSocket.fileno() )

      self.oSocket.close()
    except Exception as e:
      pass

  def renewServerContext(self):
    BaseTransport.renewServerContext(self)
    result = gSocketInfoFactory.renewServerContext(self.oSocketInfo)
    if not result['OK']:
      return result
    self.oSocketInfo = result['Value']
    self.oSocket = self.oSocketInfo.getSSLSocket()
    return S_OK()

  def handshake(self):
    """
      Initiate the client-server handshake and extract credentials

      :return: S_OK (with credentialDict if new session)
    """
    retVal = self.oSocketInfo.doServerHandshake()
    if not retVal['OK']:
      return retVal
    creds = retVal['Value']
    if not self.oSocket.session_reused():
      gLogger.debug("New session connecting from client at %s" % str(self.getRemoteAddress()))
    for key in creds.keys():
      self.peerCredentials[key] = creds[key]
    return S_OK()

  def setClientSocket(self, oSocket):
    if self.serverMode():
      raise RuntimeError("Must be initialized as client mode")
    self.oSocketInfo.setSSLSocket(oSocket)
    self.oSocket = oSocket
    self.remoteAddress = self.oSocket.getpeername()
    self.oSocket.settimeout(self.oSocketInfo.infoDict['timeout'])

  def acceptConnection(self):
    oClientTransport = SSLTransport(self.stServerAddress)
    oClientSocket, _stClientAddress = self.oSocket.accept()
    retVal = self.oSocketInfo.clone()
    if not retVal['OK']:
      return retVal
    oClientTransport.oSocketInfo = retVal['Value']
    oClientTransport.setClientSocket(oClientSocket)
    return S_OK(oClientTransport)

  def _read(self, bufSize=4096, skipReadyCheck=False):
    self.__lock()
    try:
      timeout = self.oSocketInfo.infoDict['timeout']
      if timeout:
        start = time.time()
      while True:
        if timeout:
          if time.time() - start > timeout:
            return S_ERROR("Socket read timeout exceeded")
        try:
          return S_OK(self.oSocket.recv(bufSize))
        except GSI.SSL.WantReadError:
          time.sleep(0.001)
        except GSI.SSL.WantWriteError:
          time.sleep(0.001)
        except GSI.SSL.ZeroReturnError:
          return S_OK("")
        except Exception as e:
          return S_ERROR("Exception while reading from peer: %s" % str(e))
    finally:
      self.__unlock()

  def isLocked(self):
    return self.__locked

  def _write(self, buf):
    self.__lock()
    try:
      # Renegotiation
      if not self.oSocketInfo.infoDict['clientMode']:
        # self.__writesDone += 1
        if self.__writesDone > 1000:

          self.__writesDone = 0
          ok = self.oSocket.renegotiate()
          if ok:
            try:
              ok = self.oSocket.do_handshake()
            except Exception as e:
              return S_ERROR("Renegotiation failed: %s" % str(e))

      sentBytes = 0
      timeout = self.oSocketInfo.infoDict['timeout']
      if timeout:
        start = time.time()
      while sentBytes < len(buf):
        try:
          if timeout:
            if time.time() - start > timeout:
              return S_ERROR("Socket write timeout exceeded")
          sent = self.oSocket.write(buf[sentBytes:])
          if sent == 0:
            return S_ERROR("Connection closed by peer")
          if sent > 0:
            sentBytes += sent
        except GSI.SSL.WantWriteError:
          time.sleep(0.001)
        except GSI.SSL.WantReadError:
          time.sleep(0.001)
        except Exception as e:
          return S_ERROR("Error while sending: %s" % str(e))
      return S_OK(sentBytes)
    finally:
      self.__unlock()
