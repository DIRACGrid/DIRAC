"""
  TornadoService represent one service services, your handler must inherith form this class
  TornadoService may be used only by TornadoServer.

  To create you must write this "minimal" code::

    from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
    class yourServiceHandler(TornadoService):

      @classmethod
      def initializeHandler(cls, infosDict):
        ## Called 1 time, at first request.
        ## You don't need to use super or to call any parents method, it's managed by the server

      def initializeRequest(self):
        ## Called at each request

      auth_someMethod = ['all']
      def export_someMethod(self):
        #Insert your method here, don't forgot the return


  Then you must configure service like any other service

"""

import os
import time
from datetime import datetime
from tornado.web import RequestHandler
from tornado import gen
import tornado.ioloop
from tornado.ioloop import IOLoop


import DIRAC
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import ENOAUTH
from DIRAC import gConfig
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.Tornado.Utilities import HTTPErrorCodes


class TornadoService(RequestHandler):  # pylint: disable=abstract-method
  """
    TornadoService main class, manage all tornado services
    Instanciated at each request
  """

  # Because we initialize at first request, we use a flag to know if it's already done
  __FLAG_INIT_DONE = False

  # MonitoringClient, we don't use gMonitor which is not thread-safe
  # We also need to add specific attributes for each service
  _monitor = None

  @classmethod
  def _initMonitoring(cls, serviceName, fullUrl):
    """
      Init monitoring specific to service
    """

    # Init extra bits of monitoring

    cls._monitor = MonitoringClient()
    cls._monitor.setComponentType(MonitoringClient.COMPONENT_WEB)

    cls._monitor.initialize()

    if tornado.process.task_id() is None:  # Single process mode
      cls._monitor.setComponentName('Tornado/%s' % serviceName)
    else:
      cls._monitor.setComponentName('Tornado/CPU%d/%s' % (tornado.process.task_id(), serviceName))

    cls._monitor.setComponentLocation(fullUrl)

    cls._monitor.registerActivity("Queries", "Queries served", "Framework", "queries", MonitoringClient.OP_RATE)

    cls._monitor.setComponentExtraParam('DIRACVersion', DIRAC.version)
    cls._monitor.setComponentExtraParam('platform', DIRAC.getPlatform())
    cls._monitor.setComponentExtraParam('startTime', datetime.utcnow())

    cls._stats = {'requests': 0, 'monitorLastStatsUpdate': time.time()}

    return S_OK()

  @classmethod
  def __initializeService(cls, relativeUrl, absoluteUrl, debug):
    """
      Initialize a service, called at first request

      :param relativeUrl: the url, something like "/component/service"
      :param absoluteUrl: the url, something like "https://dirac.cern.ch:1234/component/service"
      :param debug: boolean which indicate if server running in debug mode
    """
    # Url starts with a "/", we just remove it
    serviceName = relativeUrl[1:]

    cls.debug = debug
    cls.log = gLogger
    cls._startTime = datetime.utcnow()
    cls.log.info("First use of %s, initializing service..." % relativeUrl)
    cls._authManager = AuthManager("%s/Authorization" % PathFinder.getServiceSection(serviceName))

    cls._initMonitoring(serviceName, absoluteUrl)

    cls._serviceName = serviceName
    cls._validNames = [serviceName]
    serviceInfo = {'serviceName': serviceName,
                   'serviceSectionPath': PathFinder.getServiceSection(serviceName),
                   'csPaths': [PathFinder.getServiceSection(serviceName)],
                   'URL': absoluteUrl
                   }
    cls._serviceInfoDict = serviceInfo

    cls.__monitorLastStatsUpdate = time.time()

    try:
      cls.initializeHandler(serviceInfo)
    # If anything happen during initialization, we return the error
    # broad-except is necessary because we can't really control the exception in the handlers
    except Exception as e:  # pylint: disable=broad-except
      gLogger.error(e)
      return S_ERROR('Error while initializing')

    cls.__FLAG_INIT_DONE = True
    return S_OK()

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """
      This may be overwrited when you write a DIRAC service handler
      And it must be a class method. This method is called only one time,
      at the first request

      :param dict ServiceInfoDict: infos about services, it contains
                                    'serviceName', 'serviceSectionPath',
                                    'csPaths'and 'URL'
    """
    pass

  def initializeRequest(self):
    """
      Called at every request, may be overwrited in your handler.
    """
    pass

  # This function is designed to be overwrited as we want in Tornado
  # It's why we should disable pylint for this one
  def initialize(self, debug):  # pylint: disable=arguments-differ
    """
      initialize, called at every request
      ..warning:: DO NOT REWRITE THIS FUNCTION IN YOUR HANDLER
          ==> initialize in DISET became initializeRequest in HTTPS !
    """
    self.debug = debug
    self.authorized = False
    self.method = None
    self.requestStartTime = time.time()
    self.credDict = None
    self.authorized = False
    self.method = None

    # On internet you can find "HTTP Error Code" or "HTTP Status Code" for that.
    # In fact code>=400 is an error (like "404 Not Found"), code<400 is a status (like "200 OK")
    self._httpError = HTTPErrorCodes.HTTP_OK
    if not self.__FLAG_INIT_DONE:
      init = self.__initializeService(self.srv_getURL(), self.request.full_url(), debug)
      if not init['OK']:
        self._httpError = HTTPErrorCodes.HTTP_INTERNAL_SERVER_ERROR
        gLogger.error("Error during initalization on %s" % self.request.full_url())
        gLogger.debug(init)
        return False

    self._stats['requests'] += 1
    self._monitor.setComponentExtraParam('queries', self._stats['requests'])
    self._monitor.addMark("Queries")
    return True

  def prepare(self):
    """
      prepare the request, it read certificates and check authorizations.
    """
    self.method = self.get_argument("method")
    self.log.notice("Incoming request on /%s: %s" % (self._serviceName, self.method))

    # Init of service must be checked here, because if it have crashed we are
    # not able to end request at initialization (can't write on client)
    if not self.__FLAG_INIT_DONE:
      error = encode("Service can't be initialized ! Check logs on the server for more informations.")
      self.__write_return(error)
      self.finish()

    try:
      self.credDict = self._gatherPeerCredentials()
    except Exception:  # pylint: disable=broad-except
      # If an error occur when reading certificates we close connection
      # It can be strange but the RFC, for HTTP, say's that when error happend
      # before authenfication we return 401 UNAUTHORIZED instead of 403 FORBIDDEN
      self.reportUnauthorizedAccess(HTTPErrorCodes.HTTP_UNAUTHORIZED)

    try:
      hardcodedAuth = getattr(self, 'auth_' + self.method)
    except AttributeError:
      hardcodedAuth = None

    self.authorized = self._authManager.authQuery(self.method, self.credDict, hardcodedAuth)
    if not self.authorized:
      self.reportUnauthorizedAccess()

  @gen.coroutine
  def post(self):  # pylint: disable=arguments-differ
    """
    HTTP POST, used for RPC
      Call the remote method, client may send his method via "method" argument
      and list of arguments in JSON in "args" argument
    """

    # Execute the method
    # First argument is "None", it's because we let Tornado manage the executor
    retVal = yield IOLoop.current().run_in_executor(None, self.__executeMethod)

    # Tornado recommend to write in main thread
    self.__write_return(retVal.result())
    self.finish()

  @gen.coroutine
  def __executeMethod(self):
    """
      Execute the method called, this method is executed in an executor
      We have several try except to catch the different problem who can occurs

      - First, the method does not exist => Attribute error, return an error to client
      - second, anything happend during execution => General Exception, send error to client
    """

    # getting method
    try:
      method = getattr(self, 'export_%s' % self.method)
    except AttributeError as e:
      self._httpError = HTTPErrorCodes.HTTP_NOT_IMPLEMENTED
      return S_ERROR("Unknown method %s" % self.method)

    # Decode args
    args_encoded = self.get_body_argument('args', default=encode([]))

    args = decode(args_encoded)[0]
    # Execute
    try:
      self.initializeRequest()
      retVal = method(*args)
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception("Exception serving request", "%s:%s" % (str(e), repr(e)))
      retVal = S_ERROR(repr(e))
      self._httpError = HTTPErrorCodes.HTTP_INTERNAL_SERVER_ERROR

    return retVal

  def __write_return(self, dictionnary):
    """
      Write to client what we wan't to return to client
      It must be a dictionnary
    """

    # In case of error in server side we hide server CallStack to client
    if 'CallStack' in dictionnary:
      del dictionnary['CallStack']

    # Write status code before writing, by default error code is "200 OK"
    self.set_status(self._httpError)
    self.write(encode(dictionnary))

  def reportUnauthorizedAccess(self, errorCode=401):
    """
      This method stop the current request and return an error to client


      :param int errorCode: Error code, 403 is "Forbidden" and 401 is "Unauthorized"
    """
    error = S_ERROR(ENOAUTH, "Unauthorized query")
    gLogger.error(
        "Unauthorized access to %s: %s(%s) from %s" %
        (self.request.path,
         self.credDict['DN'],
         self.request.remote_ip))

    self._httpError = errorCode
    self.__write_return(error)
    self.finish()

  def on_finish(self):
    """
      Called after the end of HTTP request
    """
    requestDuration = time.time() - self.requestStartTime
    gLogger.notice("Ending request to %s after %fs" % (self.srv_getURL(), requestDuration))

  def _gatherPeerCredentials(self):
    """
      Load client certchain in DIRAC and extract informations.

      The dictionnary returned is designed to work with the AuthManager,
      already written for DISET and re-used for HTTPS.
    """

    # This line get certificates, it must be change when M2Crypto will be fully integrated in tornado
    chainAsText = self.request.get_ssl_certificate().as_pem()
    peerChain = X509Chain()

    # Here we read all certificate chain
    cert_chain = self.request.get_ssl_certificate_chain()
    for cert in cert_chain:
      chainAsText += cert.as_pem()

    # And we let some utilities do the job...
    # Following lines just get the right info, at the right place
    peerChain.loadChainFromString(chainAsText)

    # Retrieve the credentials
    res = peerChain.getCredentials(withRegistryInfo=False)
    if not res['OK']:
      raise Exception(res['Message'])

    credDict = res['Value']

    # We check if client sends extra credentials...
    if "extraCredentials" in self.request.arguments:
      extraCred = self.get_argument("extraCredentials")
      if extraCred:
        credDict['extraCredentials'] = decode(extraCred)[0]
    return credDict


####
#
#   Default method
#
####

  auth_ping = ['all']

  def export_ping(self):
    """
      Default ping method, returns some info about server.

      It returns the exact same information as DISET, for transparency purpose.
    """
    # COPY FROM DIRAC.Core.DISET.RequestHandler
    dInfo = {}
    dInfo['version'] = DIRAC.version
    dInfo['time'] = datetime.utcnow()
    # Uptime
    try:
      with open("/proc/uptime") as oFD:
        iUptime = long(float(oFD.readline().split()[0].strip()))
      dInfo['host uptime'] = iUptime
    except BaseException:
      pass
    startTime = self._startTime
    dInfo['service start time'] = self._startTime
    serviceUptime = datetime.utcnow() - startTime
    dInfo['service uptime'] = serviceUptime.days * 3600 + serviceUptime.seconds
    # Load average
    try:
      with open("/proc/loadavg") as oFD:
        sLine = oFD.readline()
      dInfo['load'] = " ".join(sLine.split()[:3])
    except BaseException:
      pass
    dInfo['name'] = self._serviceInfoDict['serviceName']
    stTimes = os.times()
    dInfo['cpu times'] = {'user time': stTimes[0],
                          'system time': stTimes[1],
                          'children user time': stTimes[2],
                          'children system time': stTimes[3],
                          'elapsed real time': stTimes[4]
                          }

    return S_OK(dInfo)

  auth_echo = ['all']

  @staticmethod
  def export_echo(data):
    """
    This method used for testing the performance of a service
    """
    return S_OK(data)

  auth_whoami = ['authenticated']

  def export_whoami(self):
    """
      A simple whoami, returns all credential dictionnary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      # Not serializable
      del credDict['x509Chain']
    return S_OK(credDict)

####
#
#  Utilities methods, some getters.
#  From DIRAC.Core.DISET.requestHandler to get same interface in the handlers.
#  Adapted for Tornado.
#  These method are copied from DISET RequestHandler, they are not all used when i'm writing
#  these lines. I rewrite them for Tornado to get them ready when a new HTTPS service need them
#
####

  @classmethod
  def srv_getCSOption(cls, optionName, defaultValue=False):
    """
    Get an option from the CS section of the services

    :return: Value for serviceSection/optionName in the CS being defaultValue the default
    """
    if optionName[0] == "/":
      return gConfig.getValue(optionName, defaultValue)
    for csPath in cls._serviceInfoDict['csPaths']:
      result = gConfig.getOption("%s/%s" % (csPath, optionName, ), defaultValue)
      if result['OK']:
        return result['Value']
    return defaultValue

  def getCSOption(self, optionName, defaultValue=False):
    """
      Just for keeping same public interface
    """
    return self.srv_getCSOption(optionName, defaultValue)

  def srv_getRemoteAddress(self):
    """
    Get the address of the remote peer.

    :return: Address of remote peer.
    """
    return self.request.remote_ip

  def getRemoteAddress(self):
    """
      Just for keeping same public interface
    """
    return self.srv_getRemoteAddress()

  def srv_getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.credDict

  def getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.credDict

  def srv_getFormattedRemoteCredentials(self):
    """
      Return the DN of user
    """
    try:
      return self.credDict['DN']
    except KeyError:  # Called before reading certificate chain
      return "unknown"

  def srv_getServiceName(self):
    """
      Return the service name
    """
    return self._serviceInfoDict['serviceName']

  def srv_getURL(self):
    """
      Return the URL
    """
    return self.request.path
