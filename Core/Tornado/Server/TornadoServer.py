"""
TornadoServer create a web server and load services.
It may work better with TornadoClient but as it accepts HTTPS you can create your own client
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import datetime
import os

from socket import error as socketerror

import M2Crypto

import tornado.iostream
tornado.iostream.SSLIOStream.configure(
    'tornado_m2crypto.m2iostream.M2IOStream')  # pylint: disable=wrong-import-position

from tornado.httpserver import HTTPServer
from tornado.web import Application, url
from tornado.ioloop import IOLoop
import tornado.ioloop

import DIRAC
from DIRAC.Core.Tornado.Server.HandlerManager import HandlerManager
from DIRAC import gLogger, S_ERROR, S_OK, gConfig
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import MemStat


class TornadoServer(object):
  """
    Tornado webserver

    Initialize and run a HTTPS Server for DIRAC services.
    By default it load all services from configuration, but you can also give an explicit list.
    If you gave explicit list of services, only these ones are loaded

    Example 1: Easy way to start tornado::

      # Initialize server and load services
      serverToLaunch = TornadoServer()

      # Start listening when ready
      serverToLaunch.startTornado()

    Example 2:We want to debug service1 and service2 only, and use another port for that ::

      services = ['component/service1', 'component/service2']
      serverToLaunch = TornadoServer(services=services, port=1234, debugSSL=True)
      serverToLaunch.startTornado()


    **WARNING:** debug=True enable SSL debug and Tornado autoreload,
                 for extra logging use -ddd in your command line
  """

  def __init__(self, services=None, debugSSL=False, port=None):
    """
    Basic instanciation, set some variables

    :param list services: List of services you want to start, start all by default
    :param str debug: Activate debug mode of Tornado (autoreload server + more errors display) and M2Crypto
    :param int port: Used to change port, default is 443
    """

    if port is None:
      port = gConfig.getValue("/Systems/Tornado/%s/Port" % PathFinder.getSystemInstance('Tornado'), 443)

    if services and not isinstance(services, list):
      services = [services]

    # URLs for services: 1URL/Service
    self.urls = []
    # Other infos
    self.debugSSL = debugSSL  # Used by tornado and M2Crypto
    self.port = port
    self.handlerManager = HandlerManager()
    self._monitor = MonitoringClient()
    self.__monitoringLoopDelay = 60  # In secs

    # If services are defined, load only these ones (useful for debug purpose or specific services)
    if services:
      retVal = self.handlerManager.loadHandlersByServiceName(services)
      if not retVal['OK']:
        gLogger.error(retVal['Message'])
        raise ImportError("Some services can't be loaded, check the service names and configuration.")

    # if no service list is given, load services from configuration
    handlerDict = self.handlerManager.getHandlersDict()
    for item in handlerDict.items():
      # handlerDict[key].initializeService(key)
      self.urls.append(url(item[0], item[1], dict(debug=self.debugSSL)))
    # If there is no services loaded:
    if not self.urls:
      raise ImportError("There is no services loaded, please check your configuration")
    self.__report = None
    self.__monitorLastStatsUpdate = None

  def startTornado(self, multiprocess=False):
    """
      Start the tornado server when ready.
      The script is blocked in the Tornado IOLoop.
      Multiprocess option is available but may be used with caution
    """

    gLogger.debug("Starting Tornado")
    self._initMonitoring()

    if self.debugSSL:
      gLogger.warn("Server is running in debug mode")

    router = Application(self.urls, debug=self.debugSSL, compress_response=True)

    certs = Locations.getHostCertificateAndKeyLocation()
    if certs is False:
      gLogger.fatal("Host certificates not found ! Can't start the Server")
      raise ImportError("Unable to load certificates")
    ca = Locations.getCAsLocation()
    ssl_options = {
        'certfile': certs[0],
        'keyfile': certs[1],
        'cert_reqs': M2Crypto.SSL.verify_peer,
        'ca_certs': ca,
        'sslDebug': self.debugSSL
    }

    self.__monitorLastStatsUpdate = time.time()
    self.__report = self.__startReportToMonitoringLoop()

    # Starting monitoring, IOLoop waiting time in ms, __monitoringLoopDelay is defined in seconds
    tornado.ioloop.PeriodicCallback(self.__reportToMonitoring, self.__monitoringLoopDelay * 1000).start()

    # Start server
    server = HTTPServer(router, ssl_options=ssl_options, decompress_request=True)
    try:
      if multiprocess:
        server.bind(self.port)
      else:
        server.listen(self.port)
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception("Exception starting HTTPServer", e)
      return S_ERROR()
    gLogger.always("Listening on port %s" % self.port)
    for service in self.urls:
      gLogger.debug("Available service: %s" % service)

    if multiprocess:
      server.start(0)
    IOLoop.current().start()

  def _initMonitoring(self):
    """
      Init extra bits of monitoring
    """

    self._monitor.setComponentType(MonitoringClient.COMPONENT_TORNADO)
    self._monitor.initialize()
    self._monitor.setComponentName('Tornado')

    self._monitor.registerActivity('CPU', "CPU Usage", 'Framework', "CPU,%", MonitoringClient.OP_MEAN, 600)
    self._monitor.registerActivity('MEM', "Memory Usage", 'Framework', 'Memory,MB', MonitoringClient.OP_MEAN, 600)

    self._monitor.setComponentExtraParam('DIRACVersion', DIRAC.version)
    self._monitor.setComponentExtraParam('platform', DIRAC.getPlatform())
    self._monitor.setComponentExtraParam('startTime', datetime.datetime.utcnow())
    return S_OK()

  def __reportToMonitoring(self):
    """
      *Called every minute*
      Every minutes we determine CPU and Memory usage
    """

    # Calculate CPU usage by comparing realtime and cpu time since last report
    self.__endReportToMonitoringLoop(*self.__report)

    # Save memory usage and save realtime/CPU time for next call
    self.__report = self.__startReportToMonitoringLoop()

  def __startReportToMonitoringLoop(self):
    """
      Get time to prepare CPU usage monitoring and send memory usage to monitor
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
      Determine CPU usage by comparing walltime and cputime and send it to monitor
    """
    wallTime = time.time() - initialWallTime
    stats = os.times()
    cpuTime = stats[0] + stats[2] - initialCPUTime
    percentage = cpuTime / wallTime * 100.
    if percentage > 0:
      self._monitor.addMark('CPU', percentage)
