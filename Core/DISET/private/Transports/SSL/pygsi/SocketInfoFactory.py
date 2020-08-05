""" The guy that takes case of managing sockets
"""

__RCSID__ = "$Id$"

import socket
import select
import os
import hashlib

import GSI

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Utilities import Network
from DIRAC.Core.DISET.private.Transports.SSL.pygsi.SocketInfo import SocketInfo
from DIRAC.Core.DISET.private.Transports.SSL.pygsi.SessionManager import gSessionManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger


class SocketInfoFactory(object):

  def __init__(self):
    self.__timeout = 1

  def setSocketTimeout(self, timeout):
    self.__timeout = timeout

  def getSocketTimeout(self):
    return self.__timeout

  def generateClientInfo(self, destinationHostname, kwargs):
    infoDict = {'clientMode': True,
                'hostname': destinationHostname,
                'timeout': 600,
                'enableSessions': True}
    for key in kwargs.keys():
      infoDict[key] = kwargs[key]
    try:
      return S_OK(SocketInfo(infoDict))
    except Exception as e:
      return S_ERROR("Error while creating SSL context: %s" % str(e))

  def generateServerInfo(self, kwargs):
    infoDict = {'clientMode': False, 'timeout': 30}
    for key in kwargs.keys():
      infoDict[key] = kwargs[key]
    try:
      return S_OK(SocketInfo(infoDict))
    except Exception as e:
      return S_ERROR(str(e))

  def __socketConnect(self, hostAddress, timeout, retries=2):
    addrs = socket.getaddrinfo(hostAddress[0], hostAddress[1], 0, socket.SOCK_STREAM)
    errs = []
    for addr in addrs:
      res = self.__sockConnect(addr[4], addr[0], timeout, retries)
      if res['OK']:
        return res
      else:
        errs.append(res['Message'])
    return S_ERROR(", ".join(errs))

  def __sockConnect(self, hostAddress, sockType, timeout, retries):
    try:
      osSocket = socket.socket(sockType, socket.SOCK_STREAM)
    except socket.error as e:
      gLogger.warn("Exception while creating a socket:", str(e))
      return S_ERROR("Exception while creating a socket:%s" % str(e))
    # osSocket.setblocking( 0 )
    if timeout:
      tsocket = self.getSocketTimeout()
      gLogger.debug("Connection timeout set to: ", tsocket)
      osSocket.settimeout(tsocket)  # we try to connect 3 times with 1 second timeout
    try:
      osSocket.connect(hostAddress)
    except socket.error as e:
      if e.args[0] == "timed out":
        osSocket.close()
        if retries:
          return self.__sockConnect(hostAddress, sockType, timeout, retries - 1)
        else:
          return S_ERROR("Can't connect: %s" % str(e))
      if e.args[0] not in (114, 115):
        return S_ERROR("Can't connect: %s" % str(e))
      #Connect in progress
      oL = select.select([], [osSocket], [], timeout)[1]
      if len(oL) == 0:
        osSocket.close()
        return S_ERROR("Connection timeout")
      errno = osSocket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
      if errno != 0:
        return S_ERROR("Can't connect: %s" % str((errno, os.strerror(errno))))
    return S_OK(osSocket)

  def __connect(self, socketInfo, hostAddress):
    # Connect baby!
    result = self.__socketConnect(hostAddress, socketInfo.infoDict['timeout'])
    if not result['OK']:
      return result
    osSocket = result['Value']
    # SSL MAGIC
    sslSocket = GSI.SSL.Connection(socketInfo.getSSLContext(), osSocket)
    # Generate sessionId
    sessionHash = hashlib.md5()
    sessionHash.update(str(hostAddress))
    sessionHash.update("|%s" % str(socketInfo.getLocalCredentialsLocation()))
    for key in ('proxyLocation', 'proxyString'):
      if key in socketInfo.infoDict:
        sessionHash.update("|%s" % str(socketInfo.infoDict[key]))
    if 'proxyChain' in socketInfo.infoDict:
      sessionHash.update("|%s" % socketInfo.infoDict['proxyChain'].dumpAllToString()['Value'])
    sessionId = sessionHash.hexdigest()
    socketInfo.sslContext.set_session_id(str(hash(sessionId)))
    socketInfo.setSSLSocket(sslSocket)
    if gSessionManager.isValid(sessionId):
      sslSocket.set_session(gSessionManager.get(sessionId))
    # Set the real timeout
    if socketInfo.infoDict['timeout']:
      sslSocket.settimeout(socketInfo.infoDict['timeout'])
    # Connected!
    return S_OK(sslSocket)

  def getSocket(self, hostAddress, **kwargs):
    hostName = hostAddress[0]
    retVal = self.generateClientInfo(hostName, kwargs)
    if not retVal['OK']:
      return retVal
    socketInfo = retVal['Value']
    retVal = Network.getIPsForHostName(hostName)
    if not retVal['OK']:
      return S_ERROR("Could not resolve %s: %s" % (hostName, retVal['Message']))
    ipList = retVal['Value']  # In that case the first ip always  the correct one.

    for _ in xrange(1):  # TODO: this retry can be reduced.
      connected = False
      errorsList = []
      for ip in ipList:
        ipAddress = (ip, hostAddress[1])
        retVal = self.__connect(socketInfo, ipAddress)
        if retVal['OK']:
          sslSocket = retVal['Value']
          connected = True
          break
        errorsList.append("%s: %s" % (ipAddress, retVal['Message']))
      if not connected:
        return S_ERROR("Could not connect to %s: %s" % (hostAddress, "," .join([e for e in errorsList])))
      retVal = socketInfo.doClientHandshake()
      if retVal['OK']:
        # Everything went ok. Don't need to retry
        break
    # Did the auth or the connection fail?
    if not retVal['OK']:
      return retVal
    if 'enableSessions' in kwargs and kwargs['enableSessions']:
      sessionId = hash(hostAddress)
      gSessionManager.set(sessionId, sslSocket.get_session())
    return S_OK(socketInfo)

  def getListeningSocket(self, hostAddress, listeningQueueSize=128, reuseAddress=True, **kwargs):
    try:
      osSocket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except socket.error:
      # IPv6 is probably disabled on this node, try IPv4 only instead
      osSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if reuseAddress:
      osSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    retVal = self.generateServerInfo(kwargs)
    if not retVal['OK']:
      return retVal
    socketInfo = retVal['Value']
    sslSocket = GSI.SSL.Connection(socketInfo.getSSLContext(), osSocket)
    sslSocket.bind(hostAddress)
    sslSocket.listen(listeningQueueSize)
    socketInfo.setSSLSocket(sslSocket)
    return S_OK(socketInfo)

  def renewServerContext(self, origSocketInfo):
    retVal = self.generateServerInfo(origSocketInfo.infoDict)
    if not retVal['OK']:
      return retVal
    socketInfo = retVal['Value']
    osSocket = origSocketInfo.getSSLSocket().get_socket()
    sslSocket = GSI.SSL.Connection(socketInfo.getSSLContext(), osSocket)
    socketInfo.setSSLSocket(sslSocket)
    return S_OK(socketInfo)

gSocketInfoFactory = SocketInfoFactory()
