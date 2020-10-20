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

import M2Crypto

import tornado.iostream
tornado.iostream.SSLIOStream.configure(
    'tornado_m2crypto.m2iostream.M2IOStream')  # pylint: disable=wrong-import-position

from tornado.httpserver import HTTPServer
from tornado.web import Application, url
from tornado.ioloop import IOLoop
import tornado.ioloop

import DIRAC
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Security import Locations
from DIRAC.Core.Tornado.Server.HandlerManager import HandlerManager
from DIRAC.Core.Utilities import MemStat
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

      services = ['component/service1', 'component/service2']
      serverToLaunch = TornadoServer(services=services, port=1234)
      serverToLaunch.startTornado()

  """

  def __init__(self, services=None, port=None):
    """

    :param list services: (default None) List of service handlers to load. If ``None``, loads all
    :param int port: Port to listen to. If None, the port is resolved following the logic
       described in the class documentation
    """

    if port is None:
      port = gConfig.getValue("/Systems/Tornado/%s/Port" % PathFinder.getSystemInstance('Tornado'), 8443)

    if services and not isinstance(services, list):
      services = [services]

    # URLs for services.
    # Contains Tornado :py:class:`tornado.web.url` object
    self.urls = []
    # Other infos
    self.port = port
    self.handlerManager = HandlerManager()

    # Monitoring attributes

    self._monitor = MonitoringClient()
    # temp value for computation, used by the monitoring
    self.__report = None
    # Last update time stamp
    self.__monitorLastStatsUpdate = None
    self.__monitoringLoopDelay = 60  # In secs

    # If services are defined, load only these ones (useful for debug purpose or specific services)
    if services:
      retVal = self.handlerManager.loadHandlersByServiceName(services)
      if not retVal['OK']:
        sLog.error(retVal['Message'])
        raise ImportError("Some services can't be loaded, check the service names and configuration.")

    # if no service list is given, load services from configuration
    handlerDict = self.handlerManager.getHandlersDict()
    for item in handlerDict.items():
      # handlerDict[key].initializeService(key)
      self.urls.append(url(item[0], item[1]))
    # If there is no services loaded:
    if not self.urls:
      raise ImportError("There is no services loaded, please check your configuration")

  def startTornado(self):
    """
      Starts the tornado server when ready.
      This method never returns.
    """

    sLog.debug("Starting Tornado")
    self._initMonitoring()

    router = Application(self.urls,
                         debug=False,
                         compress_response=True)

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
        'sslDebug': False,  # Set to true if you want to see the TLS debug messages
    }

    self.__monitorLastStatsUpdate = time.time()
    self.__report = self.__startReportToMonitoringLoop()

    # Starting monitoring, IOLoop waiting time in ms, __monitoringLoopDelay is defined in seconds
    tornado.ioloop.PeriodicCallback(self.__reportToMonitoring, self.__monitoringLoopDelay * 1000).start()

    # Start server
    server = HTTPServer(router, ssl_options=ssl_options, decompress_request=True)
    try:
      server.listen(self.port)
    except Exception as e:  # pylint: disable=broad-except
      sLog.exception("Exception starting HTTPServer", e)
      raise
    sLog.always("Listening on port %s" % self.port)
    for service in self.urls:
      sLog.debug("Available service: %s" % service)

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
