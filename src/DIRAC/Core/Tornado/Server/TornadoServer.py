"""
TornadoServer create a web server and load services.
It may work better with TornadoClient but as it accepts HTTPS you can create your own client
"""

import time
import os
import asyncio
import psutil
import secrets

import M2Crypto.SSL

import tornado.iostream

# Patch Tornado to use M2Crypto instead of standard ssl
# 1. Use M2IOStream for SSL connections
tornado.iostream.SSLIOStream.configure("tornado_m2crypto.m2iostream.M2IOStream")  # pylint: disable=no-member

# 2. Patch m2_wrap_socket in the m2iostream module and replace M2IOStream.initialize
from tornado_m2crypto import m2iostream as _m2mod
from tornado_m2crypto.m2netutil import m2_wrap_socket
from tornado_m2crypto.m2iostream import M2IOStream
from tornado.iostream import IOStream

# Patch m2_wrap_socket in both modules
import tornado_m2crypto.m2netutil as _m2netutil

_orig_m2_wrap = _m2netutil.m2_wrap_socket


def _m2_wrap_patched(sock, ssl_options, **kwargs):
    return _orig_m2_wrap(sock, ssl_options, **kwargs)


_m2netutil.m2_wrap_socket = _m2_wrap_patched
_m2mod.m2_wrap_socket = _m2_wrap_patched


# Replace M2IOStream.initialize with working version
def _m2_working_init(self, *args, **kwargs):
    self._ssl_options = kwargs.pop("ssl_options", {})

    if kwargs.pop("create_context_on_init", False):
        server_side = kwargs.pop("server_side", False)
        do_handshake_on_connect = kwargs.pop("do_handshake_on_connect", False)
        connection = args[0]
        self.socket = m2_wrap_socket(
            connection,
            self._ssl_options,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
        )
        args = (self.socket,) + args[1:]

    IOStream.__init__(self, *args, **kwargs)
    self._done_setup = False
    self._ssl_accepting = True
    self._handshake_reading = False
    self._handshake_writing = False
    self._ssl_connect_callback = None
    self._server_hostname = None

    try:
        self.socket.getpeername()
    except Exception:  # nosec: B110
        pass
    else:
        self._add_io_state(self.io_loop.WRITE)


M2IOStream.initialize = _m2_working_init

# 3. Patch M2IOStream._do_ssl_handshake to store peer cert before socket might be closed
_orig_M2IOStream_do_ssl_handshake = M2IOStream._do_ssl_handshake


def _patched_do_ssl_handshake(self):
    result = _orig_M2IOStream_do_ssl_handshake(self)
    # Store peer cert if handshake completed successfully and socket is still valid
    if self.socket is not None and not self._ssl_accepting:
        try:
            self._peer_cert = self.socket.get_peer_cert()
            self._peer_cert_chain = self.socket.get_peer_cert_chain()
        except Exception:
            self._peer_cert = None
            self._peer_cert_chain = None
    return result


M2IOStream._do_ssl_handshake = _patched_do_ssl_handshake

# 4. Patch M2IOStream.read_from_fd to return None instead of 0 when no data available
# This prevents premature stream closure after handshake completion
_orig_M2IOStream_read_from_fd = M2IOStream.read_from_fd


def _patched_read_from_fd(self, buf):
    result = _orig_M2IOStream_read_from_fd(self, buf)
    # If read_from_fd returns 0, treat it as "no data available yet"
    # instead of EOF, to prevent premature stream closure after handshake
    if result == 0:
        return None
    return result


M2IOStream.read_from_fd = _patched_read_from_fd


# 5. Patch M2IOStream.get_ssl_certificate to use stored cert
def _patched_get_ssl_cert(self, binary_form=False):
    peer_cert = getattr(self, "_peer_cert", None)
    if peer_cert is not None:
        return peer_cert.as_der() if binary_form else peer_cert
    if self.socket is not None:
        return self.socket.get_peer_cert()
    return None


def _patched_get_ssl_cert_chain(self):
    peer_chain = getattr(self, "_peer_cert_chain", None)
    if peer_chain is not None:
        return peer_chain
    if self.socket is not None:
        return self.socket.get_peer_cert_chain()
    return []


M2IOStream.get_ssl_certificate = _patched_get_ssl_cert
M2IOStream.get_ssl_certificate_chain = _patched_get_ssl_cert_chain

# 6. Patch HTTPServerRequest.get_ssl_certificate to use the stream's method
import tornado.httputil


def _m2_get_ssl_certificate(self, binary_form=False):
    """Returns the client's SSL certificate using M2Crypto."""
    if self.connection is None:
        return None
    stream = self.connection.stream
    if hasattr(stream, "get_ssl_certificate"):
        return stream.get_ssl_certificate(binary_form=binary_form)
    return None


def _m2_get_ssl_certificate_chain(self):
    """Returns the client's SSL certificate chain using M2Crypto."""
    if self.connection is None:
        return []
    stream = self.connection.stream
    if hasattr(stream, "get_ssl_certificate_chain"):
        return stream.get_ssl_certificate_chain()
    return []


tornado.httputil.HTTPServerRequest.get_ssl_certificate = _m2_get_ssl_certificate
tornado.httputil.HTTPServerRequest.get_ssl_certificate_chain = _m2_get_ssl_certificate_chain

import tornado.platform.asyncio
import tornado.ioloop
from tornado.httpserver import HTTPServer
from tornado.web import Application, RequestHandler

from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import Network, TimeUtilities
from DIRAC.Core.Tornado.Server.HandlerManager import HandlerManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

sLog = gLogger.getSubLogger(__name__)
DEBUG_M2CRYPTO = os.getenv("DIRAC_DEBUG_M2CRYPTO", "No").lower() in ("yes", "true")


class NotFoundHandler(RequestHandler):
    """Handle 404 errors"""

    def prepare(self):
        self.set_status(404)
        from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import getHTML

        self.finish(getHTML("Not found.", state=404, info="Nothing matches the given URI."))


class TornadoServer:
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
        """C'r

        :param list services: (default True) List of service handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load services
        :param list endpoints: (default False) List of endpoint handlers to load.
            If ``True``, loads all described in the CS
            If ``False``, do not load endpoints
        :param int port: Port to listen to.
            If ``None``, the port is resolved following the logic described in the class documentation
        """
        self.__startTime = time.time()
        # Application metadata, routes and settings mapping on the ports
        self.__appsSettings = {}
        # Default port, if enother is not discover
        if port is None:
            port = gConfig.getValue(f"/Systems/Tornado/Port", 8443)
        self.port = port

        # Handler manager initialization with default settings
        self.handlerManager = HandlerManager(services, endpoints)

        # temp value for computation, used by the monitoring
        self.__report = None
        # Last update time stamp
        self.__monitorLastStatsUpdate = None
        self.__monitoringLoopDelay = 60  # In secs

        self.activityMonitoring = False
        if "Monitoring" in Operations().getMonitoringBackends(monitoringType="ServiceMonitoring"):
            self.activityMonitoring = True
        # If services are defined, load only these ones (useful for debug purpose or specific services)
        retVal = self.handlerManager.loadServicesHandlers()
        if not retVal["OK"]:
            sLog.error(retVal["Message"])
            raise ImportError("Some services can't be loaded, check the service names and configuration.")
        # Response time to load services
        self.__elapsedTime = time.time() - self.__startTime
        retVal = self.handlerManager.loadEndpointsHandlers()
        if not retVal["OK"]:
            sLog.error(retVal["Message"])
            raise ImportError("Some endpoints can't be loaded, check the endpoint names and configuration.")

    def __calculateAppSettings(self):
        """Calculate application information mapping on the ports"""
        # if no service list is given, load services from configuration
        handlerDict = self.handlerManager.getHandlersDict()
        for data in handlerDict.values():
            port = data.get("Port") or self.port
            for hURL in data["URLs"]:
                if port not in self.__appsSettings:
                    self.__appsSettings[port] = {"routes": [], "settings": {}}
                if hURL not in self.__appsSettings[port]["routes"]:
                    self.__appsSettings[port]["routes"].append(hURL)
        return bool(self.__appsSettings)

    def loadServices(self, services):
        """Load a services

        :param services: List of service handlers to load. Default value set at initialization
            If ``True``, loads all services from CS
        :type services: bool or list

        :return: S_OK()/S_ERROR()
        """
        return self.handlerManager.loadServicesHandlers(services)

    def loadEndpoints(self, endpoints):
        """Load a endpoints

        :param endpoints: List of service handlers to load. Default value set at initialization
            If ``True``, loads all endpoints from CS
        :type endpoints: bool or list

        :return: S_OK()/S_ERROR()
        """
        return self.handlerManager.loadEndpointsHandlers(endpoints)

    def addHandlers(self, routes, settings=None, port=None):
        """Add new routes

        :param list routes: routes
        :param dict settings: application settings
        :param int port: port
        """
        port = port or self.port
        if port not in self.__appsSettings:
            self.__appsSettings[port] = {"routes": [], "settings": {}}
        if settings:
            self.__appsSettings[port]["settings"].update(settings)
        for route in routes:
            if route not in self.__appsSettings[port]["routes"]:
                self.__appsSettings[port]["routes"].append(route)

        return S_OK()

    def startTornado(self):
        """
        Starts the tornado server when ready.
        This method never returns.
        """

        # If we are running with python3, Tornado will use asyncio,
        # and we have to convince it to let us run in a different thread
        # This statement must be placed before setting PeriodicCallback
        asyncio.set_event_loop_policy(tornado.platform.asyncio.AnyThreadEventLoopPolicy())

        # If there is no services list:
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
            "certfile": certs[0],
            "keyfile": certs[1],
            "cert_reqs": M2Crypto.SSL.verify_peer,
            "ca_certs": ca,
            "sslDebug": DEBUG_M2CRYPTO,  # Set to true if you want to see the TLS debug messages
        }

        # Init monitoring
        if self.activityMonitoring:
            from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

            self.activityMonitoringReporter = MonitoringReporter(monitoringType="ServiceMonitoring")
            self.__monitorLastStatsUpdate = time.time()
            self.__report = self.__startReportToMonitoringLoop()
            # Response time
            # Starting monitoring, IOLoop waiting time in ms, __monitoringLoopDelay is defined in seconds
            tornado.ioloop.PeriodicCallback(self.__reportToMonitoring, self.__monitoringLoopDelay * 1000).start()

        for port, app in self.__appsSettings.items():
            sLog.debug(" - %s" % "\n - ".join([f"{k} = {ssl_options[k]}" for k in ssl_options]))

            # Default server configuration
            settings = dict(compress_response=True, cookie_secret=secrets.token_hex(32))

            # Merge appllication settings
            settings.update(app["settings"])
            # Start server
            router = Application(app["routes"], default_handler_class=NotFoundHandler, **settings)
            server = HTTPServer(router, ssl_options=ssl_options, decompress_request=True)
            try:
                server.listen(int(port))
            except Exception as e:  # pylint: disable=broad-except
                sLog.exception("Exception starting HTTPServer", e)
                raise
            sLog.always(f"Listening on port {port}")

        tornado.ioloop.IOLoop.current().start()

    def __reportToMonitoring(self):
        """
        Periodically reports to Monitoring
        """

        # Calculate CPU usage by comparing realtime and cpu time since last report
        percentage = self.__endReportToMonitoringLoop(self.__report[0], self.__report[1])
        # Send record to Monitoring
        self.activityMonitoringReporter.addRecord(
            {
                "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
                "Host": Network.getFQDN(),
                "ServiceName": "Tornado",
                "MemoryUsage": self.__report[2],
                "CpuPercentage": percentage,
            }
        )
        self.activityMonitoringReporter.commit()
        # Send memory usage and save realtime/CPU time for next call
        self.__report = self.__startReportToMonitoringLoop()

        # For each handler,
        for urlSpec in self.handlerManager.getHandlersDict().values():
            # If there is more than one URL, it's
            # most likely something that inherit from TornadoREST
            # so don't even try to monitor...
            if len(urlSpec["URLs"]) > 1:
                continue
            handler = urlSpec["URLs"][0].handler_class
            # If there is a Monitoring reporter, call commit on it
            if getattr(handler, "activityMonitoringReporter", None):
                handler.activityMonitoringReporter.commit()

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
        mem = psutil.Process().memory_info().rss / (1024.0 * 1024.0)
        return (now, cpuTime, mem)

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
        percentage = cpuTime / wallTime * 100.0
        return percentage
