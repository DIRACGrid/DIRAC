"""
  DIRAC class to execute services

  In the most common case, DIRAC services are executed using the dirac-service command.
  dirac-service accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Service Name]
  dirac-service then:
  - produces a instance of ServiceReactor
  - loads the required modules using the ServiceReactor.loadAgentModules method
  - starts the execution loop using the ServiceReactor.serve() method

  Service modules must be placed under the Service directory of a DIRAC System.
  DIRAC Systems are called XXXSystem where XXX is the [DIRAC System Name], and
  must inherit from the base class RequestHandler

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import select
import time
import socket
import signal
import os
import sys
import multiprocessing

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.private.Service import Service
from DIRAC.Core.DISET.private.GatewayService import GatewayService
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client import PathFinder


__RCSID__ = "$Id$"


class ServiceReactor(object):

  __transportExtraKeywords = {'SSLSessionTimeout': False,
                              'IgnoreCRLs': False,
                              'PacketTimeout': 'timeout',
                              'SocketBacklog': False,
                              }

  def __init__(self):
    self.__services = {}
    self.__alive = True
    self.__loader = ModuleLoader("Service",
                                 PathFinder.getServiceSection,
                                 RequestHandler,
                                 moduleSuffix="Handler")
    self.__maxFD = 0
    self.__listeningConnections = {}
    self.__stats = ReactorStats()
    self.__processes = []

  def initialize(self, servicesList):
    try:
      servicesList.remove(GatewayService.GATEWAY_NAME)
      self.__services[GatewayService.GATEWAY_NAME] = GatewayService()
    except ValueError:
      # No GW in the service list
      pass

    result = self.__loader.loadModules(servicesList)
    if not result['OK']:
      return result
    self.__serviceModules = self.__loader.getModules()

    for serviceName in self.__serviceModules:
      self.__services[serviceName] = Service(self.__serviceModules[serviceName])

    # Loop again to include the GW in case there is one (included in the __init__)
    for serviceName in self.__services:
      gLogger.info("Initializing %s" % serviceName)
      result = self.__services[serviceName].initialize()
      if not result['OK']:
        return result
    return S_OK()

  def closeListeningConnections(self):
    gLogger.info("Closing listening connections...")
    for svcName in self.__listeningConnections:
      if 'transport' in self.__listeningConnections[svcName]:
        try:
          self.__listeningConnections[svcName]['transport'].close()
        except Exception:
          pass
        del(self.__listeningConnections[svcName]['transport'])
    gLogger.info("Connections closed")

  def __createListeners(self):
    for serviceName in self.__services:
      svcCfg = self.__services[serviceName].getConfig()
      protocol = svcCfg.getProtocol()
      port = svcCfg.getPort()
      if not port:
        return S_ERROR("No port defined for service %s" % serviceName)
      if protocol not in gProtocolDict:
        return S_ERROR("Protocol %s is not known for service %s" % (protocol, serviceName))
      self.__listeningConnections[serviceName] = {'port': port, 'protocol': protocol}
      transportArgs = {}
      for kw in ServiceReactor.__transportExtraKeywords:
        value = svcCfg.getOption(kw)
        if value:
          ikw = ServiceReactor.__transportExtraKeywords[kw]
          if ikw:
            kw = ikw
          if kw == 'timeout':
            value = int(value)
          transportArgs[kw] = value
      gLogger.verbose("Initializing %s transport" % protocol, svcCfg.getURL())
      transport = gProtocolDict[protocol]['transport'](("", port),
                                                       bServerMode=True, **transportArgs)
      retVal = transport.initAsServer()
      if not retVal['OK']:
        return S_ERROR("Cannot start listening connection for service %s: %s" % (serviceName, retVal['Message']))
      self.__listeningConnections[serviceName]['transport'] = transport
      self.__listeningConnections[serviceName]['socket'] = transport.getSocket()
    return S_OK()

  def stopChildProcesses(self, _sig, frame):
    """
    It is used to properly stop the service when more than one process are used.
    In principle this is doing the job of runsv, becuase runsv only send a sigterm to the parent process...

    :param int _sig: the signal sent to the process
    :param object frame: execution frame which contains the child processes
    """

    handler = frame.f_locals.get('self')
    if handler and isinstance(handler, ServiceReactor):
      handler.stopAllProcess()

    for child in frame.f_locals.get('children', []):
      gLogger.info("Stopping child processes: %d" % child)
      os.kill(child, signal.SIGTERM)

    sys.exit(0)

  def serve(self):
    result = self.__createListeners()
    if not result['OK']:
      self.__closeListeningConnections()
      return result
    for svcName in self.__listeningConnections:
      gLogger.always("Listening at %s" % self.__services[svcName].getConfig().getURL())

    isMultiProcessingAllowed = False
    for svcName in self.__listeningConnections:
      if self.__services[svcName].getConfig().getCloneProcesses() > 0:
        isMultiProcessingAllowed = True
        break
    if isMultiProcessingAllowed:
      signal.signal(signal.SIGTERM, self.stopChildProcesses)
      signal.signal(signal.SIGINT, self.stopChildProcesses)
      for svcName in self.__listeningConnections:
        clones = self.__services[svcName].getConfig().getCloneProcesses()
        for i in range(1, clones):
          p = multiprocessing.Process(target=self.__startCloneProcess, args=(svcName, i))
          self.__processes.append(p)
          p.start()
          gLogger.always("Started clone process %s for %s" % (i, svcName))

    while self.__alive:
      self.__acceptIncomingConnection()

  def stopAllProcess(self):
    """
    It stops all the running processes.
    """
    for process in self.__processes:
      gLogger.info("Stopping: PID=%d, name=%s, parentPid=%d" % (process.pid, process.name, process._parent_pid))
      if process.is_alive():
        process.terminate()
        self.__processes.remove(process)

  # This function runs in a different process
  def __startCloneProcess(self, svcName, i):
    self.__services[svcName].setCloneProcessId(i)
    self.__alive = i
    while self.__alive:
      self.__acceptIncomingConnection(svcName)

  def __getListeningSocketsList(self, svcName=False):
    if svcName:
      sockets = [self.__listeningConnections[svcName]['socket']]
    else:
      sockets = []
      for svcName in self.__listeningConnections:
        sockets.append(self.__listeningConnections[svcName]['socket'])
    return sockets

  def __acceptIncomingConnection(self, svcName=False):
    """
      This method just gets the incoming connection, checks IP address
      and generates job. SSL/TLS handshake and execution of the remote call
      are made by Service._processInThread() (in another thread) so
      the service can accept other clients while another thread handling remote call

      :param str svcName=False: Name of a service if you use multiple
                                services at the same time
    """
    sockets = self.__getListeningSocketsList(svcName)
    while self.__alive:
      try:
        inList, _outList, _exList = select.select(sockets, [], [], 10)
        if len(inList) == 0:
          return
        for inSocket in inList:
          for svcName in self.__listeningConnections:
            if inSocket == self.__listeningConnections[svcName]['socket']:
              retVal = self.__listeningConnections[svcName]['transport'].acceptConnection()
              if not retVal['OK']:
                gLogger.warn("Error while accepting a connection: ", retVal['Message'])
                return
              clientTransport = retVal['Value']
      except socket.error:
        return
      self.__maxFD = max(self.__maxFD, clientTransport.oSocket.fileno())
      # Is it banned?
      clientIP = clientTransport.getRemoteAddress()[0]
      if clientIP in Registry.getBannedIPs():
        gLogger.warn("Client connected from banned ip %s" % clientIP)
        clientTransport.close()
        continue
      # Handle connection
      self.__stats.connectionStablished()
      self.__services[svcName].handleConnection(clientTransport)
      # Renew context?
      now = time.time()
      renewed = False
      for svcName in self.__listeningConnections:
        tr = self.__listeningConnections[svcName]['transport']
        if now - tr.latestServerRenewTime() > self.__services[svcName].getConfig().getContextLifeTime():
          result = tr.renewServerContext()
          if result['OK']:
            renewed = True
      if renewed:
        sockets = self.__getListeningSocketsList()

  def __closeListeningConnections(self):
    for svcName in self.__listeningConnections:
      lc = self.__listeningConnections[svcName]
      if 'transport' in lc and lc['transport']:
        lc['transport'].close()


class ReactorStats(object):

  def __init__(self):
    self.__connections = 0
    self.__established = 0
    self.__startTime = Time.dateTime()

  def connectionStablished(self):
    self.__connections += 1
