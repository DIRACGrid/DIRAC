"""
TornadoServer create a web server and load services.
It may work better with TornadoClient but as it accepts HTTPS you can create your own client
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import datetime
import os
import six

import M2Crypto

import tornado.iostream
tornado.iostream.SSLIOStream.configure(
    'tornado_m2crypto.m2iostream.M2IOStream')  # pylint: disable=wrong-import-position

from tornado.httpserver import HTTPServer
from tornado.web import Application, url
from tornado.ioloop import IOLoop
import tornado.ioloop

import DIRAC
from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import MemStat
from DIRAC.Core.Tornado.Server.HandlerManager import HandlerManager
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient

sLog = gLogger.getSubLogger(__name__)


class TornadoServer(object):
  """
    Tornado webserver

    Initialize and run an HTTPS Server for DIRAC services.
    By default it load all https services defined in the CS,
    but you can also give an explicit list.

    The listening port is either:

    * Given as parameter
    * Loaded from the CS ``/Systems/Tornado/<instance>/Port``
    * Default to 8443


    Example 1: Easy way to start tornado::

      # Initialize server and load services
      serverToLaunch = TornadoServer()

      # Start listening when ready
      serverToLaunch.startTornado()

    Example 2:We want to debug service1 and service2 only, and use another port for that ::

      services = ['component/service1:port1', 'component/service2']
      endpoints = ['component/endpoint1', 'component/endpoint2']
      serverToLaunch = TornadoServer(services=services, endpoints=endpoints, port=1234)
      serverToLaunch.startTornado()

  """

  def __init__(self, services=True, endpoints=False, port=None):
    """ C'r

        :param list services: (default True) List of service handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load services
        :param list endpoints: (default False) List of endpoint handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load endpoints
        :param int port: Port to listen to.
            If ``None``, the port is resolved following the logic described in the class documentation
    """
    # Application metadata, routes and settings mapping on the ports
    self.__appsSettings = {}
    # Default port, if enother is not discover
    if port is None:
      port = gConfig.getValue("/Systems/Tornado/%s/Port" % PathFinder.getSystemInstance('Tornado'), 8443)
    self.port = port

    # Handler manager initialization with default settings
    self.handlerManager = HandlerManager(services, endpoints)

    # Monitoring attributes
    self._monitor = MonitoringClient()
    # temp value for computation, used by the monitoring
    self.__report = None
    # Last update time stamp
    self.__monitorLastStatsUpdate = None
    self.__monitoringLoopDelay = 60  # In secs

    # If services are defined, load only these ones (useful for debug purpose or specific services)
    retVal = self.handlerManager.loadServicesHandlers()
    if not retVal['OK']:
      sLog.error(retVal['Message'])
      raise ImportError("Some services can't be loaded, check the service names and configuration.")

    retVal = self.handlerManager.loadEndpointsHandlers()
    if not retVal['OK']:
      sLog.error(retVal['Message'])
      raise ImportError("Some endpoints can't be loaded, check the endpoint names and configuration.")

  def __calculateAppSettings(self):
    """ Calculate application information mapping on the ports
    """
    # if no service list is given, load services from configuration
    handlerDict = self.handlerManager.getHandlersDict()
    for data in handlerDict.values():
      port = data.get('Port') or self.port
      for hURL in data['URLs']:
        if port not in self.__appsSettings:
          self.__appsSettings[port] = {'routes': [], 'settings': {}}
        if hURL not in self.__appsSettings[port]['routes']:
          self.__appsSettings[port]['routes'].append(hURL)
    return bool(self.__appsSettings)

  def loadServices(self, services):
    """ Load a services

        :param services: List of service handlers to load. Default value set at initialization
            If ``True``, loads all services from CS
        :type services: bool or list

        :return: S_OK()/S_ERROR()
    """
    return self.handlerManager.loadServicesHandlers(services)

  def loadEndpoints(self, endpoints):
    """ Load a endpoints

        :param endpoints: List of service handlers to load. Default value set at initialization
            If ``True``, loads all endpoints from CS
        :type endpoints: bool or list

        :return: S_OK()/S_ERROR()
    """
    return self.handlerManager.loadEndpointsHandlers(endpoints)

  def addHandlers(self, routes, settings=None, port=None):
    """ Add new routes

        :param list routes: routes
        :param dict settings: application settings
        :param int port: port
    """
    port = port or self.port
    if port not in self.__appsSettings:
      self.__appsSettings[port] = {'routes': [], 'settings': {}}
    if settings:
      self.__appsSettings[port]['settings'].update(settings)
    for route in routes:
      if route not in self.__appsSettings[port]['routes']:
        self.__appsSettings[port]['routes'].append(route)

    return S_OK()

  def startTornado(self):
    """
      Starts the tornado server when ready.
      This method never returns.
    """

    # If there is no services loaded:
    if not self.__calculateAppSettings():
      raise Exception("There is no services loaded, please check your configuration")

    sLog.debug("Starting Tornado")

    # Prepare SSL settings
    certs = Locations.getHostCertificateAndKeyLocation()
    if certs is False:
      sLog.fatal("Host certificates not found ! Can't start the Server")
      raise ImportError("Unable to load certificates")
    ca = Locations.getCAsLocation()
    ssl_options = {
        'certfile': certs[0],
        'keyfile': certs[1],
        'cert_reqs': M2Crypto.SSL.verify_peer,
        'ca_certs': ca,
        # Failed in tornado '5.1.1', 'sslDebug' not in m2netutil._SSL_CONTEXT_KEYWORDS
        # 'sslDebug': False,  # Set to true if you want to see the TLS debug messages
    }

    # Init monitoring
    self._initMonitoring()
    self.__monitorLastStatsUpdate = time.time()
    self.__report = self.__startReportToMonitoringLoop()

    # Starting monitoring, IOLoop waiting time in ms, __monitoringLoopDelay is defined in seconds
    tornado.ioloop.PeriodicCallback(self.__reportToMonitoring, self.__monitoringLoopDelay * 1000).start()

    for port, app in self.__appsSettings.items():
      sLog.debug(" - %s" % "\n - ".join(["%s = %s" % (k, ssl_options[k]) for k in ssl_options]))

      # Default server configuration
      settings = dict(compress_response=True, cookie_secret='secret')

      # Merge appllication settings
      settings.update(app['settings'])
      # Start server
      router = Application(app['routes'], **settings)
      server = HTTPServer(router, ssl_options=ssl_options, decompress_request=True)
      try:
        server.listen(int(port))
      except Exception as e:  # pylint: disable=broad-except
        sLog.exception("Exception starting HTTPServer", e)
        raise
      sLog.always("Listening on port %s" % port)
      for service in app['routes']:
        sLog.debug("Available service: %s" % service if isinstance(service, url) else service[0])

    IOLoop.current().start()

  def _initMonitoring(self):
    """
      Initialize the monitoring
    """

    self._monitor.setComponentType(MonitoringClient.COMPONENT_TORNADO)
    self._monitor.initialize()
    self._monitor.setComponentName('Tornado')

    self._monitor.registerActivity('CPU', "CPU Usage", 'Framework', "CPU,%", MonitoringClient.OP_MEAN, 600)
    self._monitor.registerActivity('MEM', "Memory Usage", 'Framework', 'Memory,MB', MonitoringClient.OP_MEAN, 600)

    self._monitor.setComponentExtraParam('DIRACVersion', DIRAC.version)
    self._monitor.setComponentExtraParam('platform', DIRAC.getPlatform())
    self._monitor.setComponentExtraParam('startTime', datetime.datetime.utcnow())

  def __reportToMonitoring(self):
    """
      Periodically report to the monitoring of the CPU and MEM
    """

    # Calculate CPU usage by comparing realtime and cpu time since last report
    self.__endReportToMonitoringLoop(*self.__report)

    # Save memory usage and save realtime/CPU time for next call
    self.__report = self.__startReportToMonitoringLoop()

  def __startReportToMonitoringLoop(self):
    """
      Snapshot of resources to be taken at the beginning
      of a monitoring cycle.
      Also sends memory snapshot to the monitoring.

      This is basically copy/paste of Service.py

      :returns: tuple (<time.time(), cpuTime )

    """
    now = time.time()  # Used to calulate a delta
    stats = os.times()
    cpuTime = stats[0] + stats[2]
    if now - self.__monitorLastStatsUpdate < 0:
      return (now, cpuTime)
    # Send CPU consumption mark
    self.__monitorLastStatsUpdate = now
    # Send Memory consumption mark
    membytes = MemStat.VmB('VmRSS:')
    if membytes:
      mem = membytes / (1024. * 1024.)
      self._monitor.addMark('MEM', mem)
    return (now, cpuTime)

  def __endReportToMonitoringLoop(self, initialWallTime, initialCPUTime):
    """
      Snapshot of resources to be taken at the end
      of a monitoring cycle.

      This is basically copy/paste of Service.py

      Determines CPU usage by comparing walltime and cputime and send it to monitor
    """
    wallTime = time.time() - initialWallTime
    stats = os.times()
    cpuTime = stats[0] + stats[2] - initialCPUTime
    percentage = cpuTime / wallTime * 100.
    if percentage > 0:
      self._monitor.addMark('CPU', percentage)
