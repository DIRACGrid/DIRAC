"""
TornadoServer create a web server and load services.
It may work better with TornadoClient but as it accepts HTTPS you can create your own client
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import time
import datetime
import tempfile
import M2Crypto
from io import open

import tornado.iostream
tornado.iostream.SSLIOStream.configure(
    'tornado_m2crypto.m2iostream.M2IOStream')  # pylint: disable=wrong-import-position

from tornado.httpserver import HTTPServer
from tornado.web import Application, url
from tornado.ioloop import IOLoop
import tornado.ioloop

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import MemStat
from DIRAC.Core.Tornado.Server.HandlerManager import HandlerManager
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Security import Locations, X509Chain, X509CRL
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient

# FROM WEB
import sys
import signal
import tornado.process
import tornado.autoreload

from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import SessionManager

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
      endpoints = ['component/endpoint1:port1', 'component/endpoint2']
      serverToLaunch = TornadoServer(services=services, endpoints=endpoints, port=1234)
      serverToLaunch.startTornado()

  """

  def __init__(self, services=True, endpoints=False, port=None, debug=False, balancer=None, processes=None):
    """ C'r

        :param list services: (default True) List of service handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load services
        :param list endpoints: (default False) List of endpoint handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load endpoints
        :param int port: Port to listen to.
            If ``None``, the port is resolved following the logic described in the class documentation
        :param bool debug: debug
        :param str balancer: if need to use balancer, e.g.:: `nginx`
        :param int processes: number of processes or if it's True just use all server CPUs
    """
    # Debug
    self.debug = debug
    # Balancer, like as nginx
    self.balancer = balancer
    # Multiprocessor mode settings
    self.processes = 1 if processes is None else 0 if processes is True else processes
    if processes:
      raise ImportError('Multiprocessor mode is not supported.')
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

  # def stopChildProcesses(self, sig, frame):
  #   """
  #   It is used to properly stop tornado when more than one process is used.
  #   In principle this is doing the job of runsv....

  #   :param int sig: the signal sent to the process
  #   :param object frame: execution frame which contains the child processes
  #   """
  #   for child in frame.f_locals.get('children', []):
  #     gLogger.info("Stopping child processes: %d" % child)
  #     os.kill(child, signal.SIGTERM)
  #   sys.exit(0)

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
      raise Exception("Unable to load certificates")
    ca = Locations.getCAsLocation()
    ssl_options = {
        'certfile': certs[0],
        'keyfile': certs[1],
        'cert_reqs': M2Crypto.SSL.verify_peer,
        'ca_certs': ca,
        'sslDebug': False,  # Set to true if you want to see the TLS debug messages
    }

    if self.balancer:
      # Create CAs for balancer
      generateRevokedCertsFile()  # it is used by nginx....
      # when NGINX is used then the Conf.HTTPS return False, it means tornado
      # does not have to be configured using 443 port
      generateCAFile()  # if we use Nginx we have to generate the cas as well...

    # ############
    # # please do no move this lines. The lines must be before the fork_processes
    # signal.signal(signal.SIGTERM, self.stopChildProcesses)
    # signal.signal(signal.SIGINT, self.stopChildProcesses)

    # # Check processes if we're under a load balancert and have only one port
    # if self.processes != 1:
    #   if not self.balancer:
    #     raise Exception("For multi processor mode, please, use balacer.")
    #   if len(self.__appsSettings) != 1:
    #     raise Exception("For multi processor mode, please, use one server port.")
    #   tornado.process.fork_processes(self.processes, max_restarts=0)
    # #############

    # Init monitoring
    self._initMonitoring()
    self.__monitorLastStatsUpdate = time.time()
    self.__report = self.__startReportToMonitoringLoop()

    # Starting monitoring, IOLoop waiting time in ms, __monitoringLoopDelay is defined in seconds
    tornado.ioloop.PeriodicCallback(self.__reportToMonitoring, self.__monitoringLoopDelay * 1000).start()

    for port, app in self.__appsSettings.items():
      sLog.debug(" - %s" % "\n - ".join(["%s = %s" % (k, ssl_options[k]) for k in ssl_options]))

      # Default server configuration
      settings = dict(debug=self.debug,
                      compress_response=True,
                      # Use gLogger instead tornado log
                      log_function=_logRequest)

      # Merge appllication settings
      settings.update(app['settings'])

      # # Don't use autoreload in debug mode for multiprocess
      # if self.processes != 1:
      #   if self.balancer:
      #     port = 8000
      #   port += tornado.process.task_id() or 0
      #   settings['debug'] = False

      # Start server
      router = Application(app['routes'], **settings)
      server = HTTPServer(router, ssl_options=ssl_options, decompress_request=True, xheaders=True)
      try:
        server.listen(port)
      except Exception as e:  # pylint: disable=broad-except
        sLog.exception("Exception starting HTTPServer", e)
        raise
      if settings['debug']:
        sLog.info("Configuring in developer mode...")
      sLog.always("Listening on 127.0.0.1:%s" % port)
      for service in app['routes']:
        sLog.debug("Available service: %s" % service if isinstance(service, url) else service[0])

    tornado.autoreload.add_reload_hook(lambda: sLog.verbose("\n == Reloading server...\n"))
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


def _logRequest(handler):
  """ This function will be called at the end of every request to log the result

      :param object handler: RequestHandler object
  """
  print('=== _logRequest')
  print('=== %s' % handler)
  print(sLog)
  print('===============')
  status = handler.get_status()
  if status < 400:
    logm = sLog.notice
  elif status < 500:
    logm = sLog.warn
  else:
    logm = sLog.error
  request_time = 1000.0 * handler.request.request_time()
  logm("%d %s %.2fms" % (status, handler._request_summary(), request_time))


def generateCAFile():
  """ Generate a single CA file with all the PEMs

      :return: str or bool
  """
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"

  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "cas.pem"),
             os.path.join(os.path.dirname(cert), "cas.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="cas.", suffix=".pem")[1]
    try:
      fd = open(fn, "w")
    except IOError:
      continue
    for caFile in os.listdir(caDir):
      caFile = os.path.join(caDir, caFile)
      chain = X509Chain.X509Chain()
      result = chain.loadChainFromFile(caFile)
      if not result['OK']:
        continue
      expired = chain.hasExpired()
      if not expired['OK'] or expired['Value']:
        continue
      fd.write(chain.dumpAllToString()['Value'].decode('utf-8'))
    fd.close()
    return fn
  return False


def generateRevokedCertsFile():
  """ Generate a single CA file with all the PEMs

      :return: str or bool
  """
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"

  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "allRevokedCerts.pem"),
             os.path.join(os.path.dirname(cert), "allRevokedCerts.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="allRevokedCerts", suffix=".pem")[1]
    try:
      fd = open(fn, "w")
    except IOError:
      continue
    for caFile in os.listdir(caDir):
      caFile = os.path.join(caDir, caFile)
      chain = X509CRL.X509CRL()
      result = chain.loadCRLFromFile(caFile)
      if not result['OK']:
        continue
      fd.write(chain.dumpAllToString()['Value'].decode('utf-8'))
    fd.close()
    return fn
  return False
