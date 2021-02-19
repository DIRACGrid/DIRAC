""" BaseRequestHandler is the base class for tornados services and etc handlers.
    It directly inherits from :py:class:`tornado.web.RequestHandler`
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open

import os
import time
import threading
from datetime import datetime
from six.moves import http_client
from six.moves.urllib.parse import unquote

import tornado
from tornado import gen
from tornado.web import RequestHandler, HTTPError
from tornado.ioloop import IOLoop
from tornado.httpclient import HTTPResponse
from tornado.concurrent import Future

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import ResourceProtector

sLog = gLogger.getSubLogger(__name__)


class BaseRequestHandler(RequestHandler):
  # Because we initialize at first request, we use a flag to know if it's already done
  __init_done = False
  # Lock to make sure that two threads are not initializing at the same time
  __init_lock = threading.RLock()
  
  # MonitoringClient, we don't use gMonitor which is not thread-safe
  # We also need to add specific attributes for each service
  _monitor = None

  # Auth requirements
  AUTH_PROPS = None
  # Type of component
  MONITORING_COMPONENT = MonitoringClient.COMPONENT_WEB

  # Prefix of methods names
  METHOD_PREFIX = "export_"

  # Authentication types: SSL, JWT, VISITOR
  AUTHZ_GRANTS = ['SSL', 'JWT']
  
  @classmethod
  def _initMonitoring(cls, serviceName, fullUrl):
    """
      Initialize the monitoring specific to this handler
      This has to be called only by :py:meth:`.__initializeService`
      to ensure thread safety and unicity of the call.

      :param serviceName: relative URL ``/<System>/<Component>``
      :param fullUrl: full URl like ``https://<host>:<port>/<System>/<Component>``
    """

    # Init extra bits of monitoring

    cls._monitor = MonitoringClient()
    cls._monitor.setComponentType(cls.MONITORING_COMPONENT)

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
  def _getServiceName(cls, request):
    """ Search service name in request.

        :param object request: tornado Request

        :return: str
    """
    raise NotImplementedError()
  
  @classmethod
  def _getServiceAuthSection(cls, serviceName):
    """ Search service auth section.

        :param str serviceName: service name

        :return: str
    """
    return "%s/Authorization" % PathFinder.getServiceSection(serviceName)
  
  @classmethod
  def _getServiceInfo(cls, serviceName, request):
    """ Fill service information.

        :param str serviceName: service name
        :param object request: tornado Request

        :return: dict
    """
    return {}

  @classmethod
  def __initializeService(cls, request):
    """
      Initialize a service.
      The work is only perform once at the first request.

      :param object request: tornado Request

      :returns: S_OK
    """
    # If the initialization was already done successfuly,
    # we can just return
    if cls.__init_done:
      return S_OK()

    # Otherwise, do the work but with a lock
    with cls.__init_lock:

      # Check again that the initialization was not done by another thread
      # while we were waiting for the lock
      if cls.__init_done:
        return S_OK()

      # absoluteUrl: full URL e.g. ``https://<host>:<port>/<System>/<Component>``
      absoluteUrl = request.path
      serviceName = cls._getServiceName(request)

      cls._startTime = datetime.utcnow()
      sLog.info("First use of %s, initializing service..." % serviceName)
      cls._authManager = AuthManager(cls._getServiceAuthSection(serviceName))

      cls._initMonitoring(serviceName, absoluteUrl)

      cls._serviceName = serviceName
      cls._validNames = [serviceName]
      serviceInfo = cls._getServiceInfo(serviceName, request)

      cls._serviceInfoDict = serviceInfo

      cls.__monitorLastStatsUpdate = time.time()

      cls.initializeHandler(serviceInfo)

      cls.__init_done = True

      return S_OK()
  
  @classmethod
  def initializeHandler(cls, serviceInfo):
    """
      This may be overwritten when you write a DIRAC service handler
      And it must be a class method. This method is called only one time,
      at the first request

      :param dict ServiceInfoDict: infos about services, it contains
                                    'serviceName', 'serviceSectionPath',
                                    'csPaths' and 'URL'
    """
    pass

  def initializeRequest(self):
    """
      Called at every request, may be overwritten in your handler.
    """
    pass
  
  # This is a Tornado magic method
  def initialize(self):  # pylint: disable=arguments-differ
    """
      Initialize the handler, called at every request.

      It just calls :py:meth:`.__initializeService`

      If anything goes wrong, the client will get ``Connection aborted``
      error. See details inside the method.

      ..warning::
        DO NOT REWRITE THIS FUNCTION IN YOUR HANDLER
        ==> initialize in DISET became initializeRequest in HTTPS !
    """
    # Only initialized once
    if not self.__init_done:
      # Ideally, if something goes wrong, we would like to return a Server Error 500
      # but this method cannot write back to the client as per the
      # `tornado doc <https://www.tornadoweb.org/en/stable/guide/structure.html#overriding-requesthandler-methods>`_.
      # So the client will get a ``Connection aborted```
      try:
        res = self.__initializeService(self.request)
        if not res['OK']:
          raise Exception(res['Message'])
      except Exception as e:
        sLog.error("Error in initialization", repr(e))
        raise

  def _monitorRequest(self):
    """ Monitor action for each request
    """
    self._stats['requests'] += 1
    self._monitor.setComponentExtraParam('queries', self._stats['requests'])
    self._monitor.addMark("Queries")

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    raise NotImplementedError()

  def _getMethodArgs(self, args):
    """ Decode args.

        :return: list
    """
    return args

  def _getMethodAuthProps(self):
    """ Resolves the hard coded authorization requirements for method.

        :return: object
    """
    try:
      return getattr(self, 'auth_' + self.method)
    except AttributeError:
      if not isinstance(self.AUTH_PROPS, (list, tuple)):
        self.AUTH_PROPS = [p.strip() for p in self.AUTH_PROPS.split(",") if p.strip()]
      return self.AUTH_PROPS

  def _getMethod(self):
    """ Get method object.

        :return: object
    """
    try:
      return getattr(self, '%s%s' % (self.METHOD_PREFIX, self.method))
    except AttributeError as e:
      sLog.error("Invalid method", self.method)
      raise HTTPError(status_code=http_client.NOT_IMPLEMENTED)

  def prepare(self):
    """
      Prepare the request. It reads certificates and check authorizations.
      We make the assumption that there is always going to be a ``method`` argument
      regardless of the HTTP method used

    """

    # "method" argument of the POST call.
    # This resolves into the ``export_<method>`` method
    # on the handler side
    # If the argument is not available, the method exists
    # and an error 400 ``Bad Request`` is returned to the client
    self.method = self._getMethodName()

    self._monitorRequest()

    try:
      self.credDict = self._gatherPeerCredentials()
    except Exception as e:  # pylint: disable=broad-except
      # If an error occur when reading certificates we close connection
      # It can be strange but the RFC, for HTTP, say's that when error happend
      # before authentication we return 401 UNAUTHORIZED instead of 403 FORBIDDEN
      sLog.debug(str(e))
      sLog.error(
          "Error gathering credentials ", "%s; path %s" %
          (self.getRemoteAddress(), self.request.path))
      raise HTTPError(status_code=http_client.UNAUTHORIZED)

    # Check whether we are authorized to perform the query
    # Note that performing the authQuery modifies the credDict...
    authorized = self._authManager.authQuery(self.method, self.credDict,
                                             self._getMethodAuthProps())
    if not authorized:
      extraInfo = ''
      if self.credDict.get('DN'):
        extraInfo += 'DN: %s' % self.credDict['DN']
      if self.credDict.get('ID'):
        extraInfo += 'ID: %s' % self.credDict['ID']
      sLog.error(
          "Unauthorized access", "Identity %s; path %s; %s" %
          (self.srv_getFormattedRemoteCredentials(),
           self.request.path, extraInfo))
      raise HTTPError(status_code=http_client.UNAUTHORIZED)

  # Make post a coroutine.
  # See https://www.tornadoweb.org/en/branch5.1/guide/coroutines.html#coroutines
  # for details
  @gen.coroutine
  def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """
      Method to handle incoming ``POST`` requests.
      Note that all the arguments are already prepared in the :py:meth:`.prepare`
      method.

      The ``POST`` arguments expected are:

      * ``method``: name of the method to call
      * ``args``: JSON encoded arguments for the method
      * ``extraCredentials``: (optional) Extra informations to authenticate client
      * ``rawContent``: (optionnal, default False) If set to True, return the raw output
        of the method called.

      If ``rawContent`` was requested by the client, the ``Content-Type``
      is ``application/octet-stream``, otherwise we set it to ``application/json``
      and JEncode retVal.

      If ``retVal`` is a dictionary that contains a ``Callstack`` item,
      it is removed, not to leak internal information.


      Example of call using ``requests``::

        In [20]: url = 'https://server:8443/DataManagement/TornadoFileCatalog'
          ...: cert = '/tmp/x509up_u1000'
          ...: kwargs = {'method':'whoami'}
          ...: caPath = '/home/dirac/ClientInstallDIR/etc/grid-security/certificates/'
          ...: with requests.post(url, data=kwargs, cert=cert, verify=caPath) as r:
          ...:     print r.json()
          ...:
        {u'OK': True,
            u'Value': {u'DN': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'group': u'dirac_user',
            u'identity': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'isLimitedProxy': False,
            u'isProxy': True,
            u'issuer': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'properties': [u'NormalUser'],
            u'secondsLeft': 85441,
            u'subject': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=2409820262',
            u'username': u'adminusername',
            u'validDN': False,
            u'validGroup': False}}
    """
    # Execute the method in an executor (basically a separate thread)
    # Because of that, we cannot calls certain methods like `self.write`
    # in _executeMethod. This is because these methods are not threadsafe
    # https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
    # However, we can still rely on instance attributes to store what should
    # be sent back (reminder: there is an instance
    # of this class created for each request)
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)

    # retVal is :py:class:`tornado.concurrent.Future`
    self._finishFuture(retVal)

  @gen.coroutine
  def _executeMethod(self, args):
    """
      Execute the method called, this method is ran in an executor
      We have several try except to catch the different problem which can occur

      - First, the method does not exist => Attribute error, return an error to client
      - second, anything happend during execution => General Exception, send error to client

      .. warning::
        This method is called in an executor, and so cannot use methods like self.write
        See https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
    """

    sLog.notice(
        "Incoming request %s /%s: %s" %
        (self.srv_getFormattedRemoteCredentials(),
         self._serviceName,
         self.method))

    # getting method
    method = self._getMethod()
    methodArgs = self._getMethodArgs(args)

    # Execute
    try:
      self.initializeRequest()
      retVal = method(*args)
    except Exception as e:  # pylint: disable=broad-except
      sLog.exception("Exception serving request", "%s:%s" % (str(e), repr(e)))
      raise HTTPError(http_client.INTERNAL_SERVER_ERROR)

    return retVal

  def _finishFuture(self, retVal):
    """ Handler Future result

        :param object retVal: tornado.concurrent.Future
    """
    
    # Wait result only if it's a Future object
    self.result = retVal.result() if isinstance(retVal, Future) else retVal

    # Here it is safe to write back to the client, because we are not
    # in a thread anymore

    # Parse HTTPResponse
    if isinstance(self.result, HTTPResponse):
      self.set_status(self.result.code)
      for key in self.result.headers:
        self.set_header(key, self.result.headers[key])
      if self.result.body:
        self.write(self.result.body)

    # If set to true, do not JEncode the return of the RPC call
    # This is basically only used for file download through
    # the 'streamToClient' method.
    elif self.get_argument('rawContent', default=False):
      # See 4.5.1 http://www.rfc-editor.org/rfc/rfc2046.txt
      self.set_header("Content-Type", "application/octet-stream")
      self.write(self.result)
    
    # Return simple text or html
    elif isinstance(self.result, str):
      self.write(self.result)
    
    # DIRAC JSON
    else:
      self.set_header("Content-Type", "application/json")
      self.write(encode(self.result))

    self.finish()

  def on_finish(self):
    """
      Called after the end of HTTP request.
      Log the request duration
    """
    elapsedTime = 1000.0 * self.request.request_time()

    argsString = "OK"
    try:
      if not self.result['OK']:
        argsString = "ERROR: %s" % self.result['Message']
    except (AttributeError, KeyError, TypeError):  # In case it is not a DIRAC structure
      if self._reason != 'OK':
        argsString = 'ERROR %s' % self._reason

    sLog.notice("Returning response", "%s %s (%.2f ms) %s" % (self.srv_getFormattedRemoteCredentials(),
                                                              self._serviceName,
                                                              elapsedTime, argsString))

  def _gatherPeerCredentials(self):
    """ Returne a dictionary designed to work with the AuthManager,
        already written for DISET and re-used for HTTPS.

        :returns: a dict containing the return of :py:meth:`DIRAC.Core.Security.X509Chain.X509Chain.getCredentials`
                  (not a DIRAC structure !)
    """
    err = []
    result = None
    for a in self.AUTHZ_GRANTS or ['VISITOR']:  # AUTHZ_GRANTS must contain something
      if a.upper() == 'SSL':
        result = self.__authzCertificate()
      elif a.upper() == 'JWT':
        result = self.__authzToken()
      elif a.upper() == 'VISITOR':
        result = S_OK({})
      else:
        raise Exception('%s authentication type is not supported.' % a)
      if result['OK']:
        break
      err.append('%s authentication: %s' % (a.upper(), result['Message']))
    
    # Report on failed authentication attempts
    if err:
      if result['OK']:
        for e in err:
          print(e)
      else:
        raise Exception('; '.join(err))
    
    return result['Value']

  def __authzCertificate(self):
    """ Load client certchain in DIRAC and extract informations.

        :return: S_OK(dict)/S_ERROR()
    """
    peerChain = X509Chain()
    derCert = self.request.get_ssl_certificate()

    # Get client certificate pem
    if derCert:
      chainAsText = derCert.as_pem()
      # Here we read all certificate chain
      cert_chain = self.request.get_ssl_certificate_chain()
      for cert in cert_chain:
        chainAsText += cert.as_pem()
    elif self.request.headers.get('X-Ssl_client_verify') == 'SUCCESS':
      chainAsTextEncoded = self.request.headers.get('X-SSL-CERT')
      chainAsText = unquote(chainAsTextEncoded)
    else:
      return S_ERROR('Not found a valide client certificate.')

    peerChain.loadChainFromString(chainAsText)

    # Retrieve the credentials
    res = peerChain.getCredentials(withRegistryInfo=False)
    if not res['OK']:
      return res

    credDict = res['Value']

    # We check if client sends extra credentials...
    if "extraCredentials" in self.request.arguments:
      extraCred = self.get_argument("extraCredentials")
      if extraCred:
        credDict['extraCredentials'] = decode(extraCred)[0]
    return S_OK(credDict)
  
  def __authzToken(self):
    """ Load token claims in DIRAC and extract informations.

        :return: S_OK(dict)/S_ERROR()
    """
    try:
      token = ResourceProtector().acquire_token(self.request)
    except Exception as e:
      return S_ERROR(str(e))
    return {'ID': token.sub, 'issuer': token.issuer, 'group': token.groups[0]}

  @property
  def log(self):
    return sLog

  def getDN(self):
    return self.credDict.get('DN', '')

  def getID(self):
    return self.credDict.get('ID', '')

  def getUserName(self):
    return self.credDict.get('username', '')

  def getUserGroup(self):
    return self.credDict.get('group', '')

  def getProperties(self):
    return self.credDict.get('properties', [])

  def isRegisteredUser(self):
    return self.credDict.get('username', 'anonymous') != 'anonymous' and self.credDict.get('group')

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
      with open("/proc/uptime", 'rt') as oFD:
        iUptime = int(float(oFD.readline().split()[0].strip()))
      dInfo['host uptime'] = iUptime
    except Exception:  # pylint: disable=broad-except
      pass
    startTime = self._startTime
    dInfo['service start time'] = self._startTime
    serviceUptime = datetime.utcnow() - startTime
    dInfo['service uptime'] = serviceUptime.days * 3600 + serviceUptime.seconds
    # Load average
    try:
      with open("/proc/loadavg", 'rt') as oFD:
        dInfo['load'] = " ".join(oFD.read().split()[:3])
    except Exception:  # pylint: disable=broad-except
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
      A simple whoami, returns all credential dictionary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      # Not serializable
      del credDict['x509Chain']
    return S_OK(credDict)

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

    remote_ip = self.request.remote_ip
    # Although it would be trivial to add this attribute in _HTTPRequestContext,
    # Tornado won't release anymore 5.1 series, so go the hacky way
    try:
      remote_port = self.request.connection.stream.socket.getpeername()[1]
    except Exception:  # pylint: disable=broad-except
      remote_port = 0

    return (remote_ip, remote_port)

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

      Mostly copy paste from
      :py:meth:`DIRAC.Core.DISET.private.Transports.BaseTransport.BaseTransport.getFormattedCredentials`

      Note that the information will be complete only once the AuthManager was called
    """
    address = self.getRemoteAddress()
    peerId = ""
    # Depending on where this is call, it may be that credDict is not yet filled.
    # (reminder: AuthQuery fills part of it..)
    try:
      peerId = "[%s:%s]" % (self.credDict['group'], self.credDict['username'])
    except AttributeError:
      pass

    if address[0].find(":") > -1:
      return "([%s]:%s)%s" % (address[0], address[1], peerId)
    return "(%s:%s)%s" % (address[0], address[1], peerId)

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
