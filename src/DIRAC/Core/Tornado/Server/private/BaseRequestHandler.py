""" BaseRequestHandler is the base class for tornados services and etc handlers.
    It directly inherits from :py:class:`tornado.web.RequestHandler`
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open

import os
import jwt
import time
import threading
from datetime import datetime
from six import string_types
from six.moves import http_client
from six.moves.urllib.parse import unquote
from functools import partial

import tornado
from tornado import gen
from tornado.web import RequestHandler, HTTPError
from tornado.ioloop import IOLoop
from tornado.concurrent import Future

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.Resources.IdProvider.Utilities import getProvidersForInstance

sLog = gLogger.getSubLogger(__name__.split('.')[-1])


class BaseRequestHandler(RequestHandler):
  """ Base class for all the Handlers.
      It directly inherits from :py:class:`tornado.web.RequestHandler`

      Each HTTP request is served by a new instance of this class.

      For the sequence of method called, please refer to
      the `tornado documentation <https://www.tornadoweb.org/en/stable/guide/structure.html>`_.

      This class is basic for :py:class:`DIRAC.Core.Tornado.Server.TornadoService.TornadoService`
      and :py:class:`DIRAC.Core.Tornado.Server.TornadoREST.TornadoREST`.

      In order to create a class that inherits from `BaseRequestHandler`, it has to
      follow a certain skeleton::

        class TornadoInstance(BaseRequestHandler):

          # Prefix of methods names
          METHOD_PREFIX = "export_"

          @classmethod
          def _getServiceName(cls, request):
            ''' Search service name in request
            '''
            return request.path[1:]

          @classmethod
          def _getServiceInfo(cls, serviceName, request):
            ''' Fill service information.
            '''
            return {'serviceName': serviceName,
                    'serviceSectionPath': PathFinder.getServiceSection(serviceName),
                    'csPaths': [PathFinder.getServiceSection(serviceName)],
                    'URL': request.full_url()}

          @classmethod
          def _getServiceAuthSection(cls, serviceName):
            ''' Search service "Authorization" configuration section.
            '''
            return "%s/Authorization" % PathFinder.getServiceSection(serviceName)

          def _getMethodName(self):
            ''' Parse method name.
            '''
            return self.get_argument("method")

          def _getMethodArgs(self, args):
            ''' Decode args.
            '''
            args_encoded = self.get_body_argument('args', default=encode([]))
            return decode(args_encoded)[0]

          # Make post a coroutine.
          # See https://www.tornadoweb.org/en/branch5.1/guide/coroutines.html#coroutines
          # for details
          @gen.coroutine
          def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
            ''' Describe HTTP method to use
            '''
            # Execute the method in an executor (basically a separate thread)
            # Because of that, we cannot calls certain methods like `self.write`
            # in __executeMethod. This is because these methods are not threadsafe
            # https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
            # However, we can still rely on instance attributes to store what should
            # be sent back (reminder: there is an instance of this class created for each request)
            retVal = yield IOLoop.current().run_in_executor(*self._prepareExecutor(args))
            # retVal is :py:class:`tornado.concurrent.Future`
            self._finishFuture(retVal)

      For compatibility with the existing :py:class:`DIRAC.Core.DISET.TransferClient.TransferClient`,
      the handler can define a method ``export_streamToClient``. This is the method that will be called
      whenever ``TransferClient.receiveFile`` is called. It is the equivalent of the DISET
      ``transfer_toClient``.
      Note that this is here only for compatibility, and we discourage using it for new purposes, as it is
      bound to disappear.
  """
  # Because we initialize at first request, we use a flag to know if it's already done
  __init_done = False
  # Lock to make sure that two threads are not initializing at the same time
  __init_lock = threading.RLock()

  # MonitoringClient, we don't use gMonitor which is not thread-safe
  # We also need to add specific attributes for each service
  _monitor = None

  # System name with which this component is associated
  SYSTEM = None

  # Auth requirements
  AUTH_PROPS = None

  # Type of component
  MONITORING_COMPONENT = MonitoringClient.COMPONENT_WEB

  # Prefix of methods names
  METHOD_PREFIX = "export_"

  # Which grant type to use
  USE_AUTHZ_GRANTS = ['SSL', 'JWT']

  # Definition of identity providers
  _idps = IdProviderFactory()
  _idp = {}

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
    raise NotImplementedError('Please, create the _getServiceName class method')

  @classmethod
  def _getServiceAuthSection(cls, serviceName):
    """ Search service auth section.

        :param str serviceName: service name

        :return: str
    """
    raise NotImplementedError('Please, create the _getServiceAuthSection class method')

  @classmethod
  def _getServiceInfo(cls, serviceName, request):
    """ Fill service information.

        :param str serviceName: service name
        :param object request: tornado Request

        :return: dict
    """
    gLogger.warn('Service information will not be collected because the _getServiceInfo method is not defined.')
    return {}

  @classmethod
  def __loadIdPs(cls):
    """ Load identity providers that will be used to verify tokens
    """
    gLogger.info('Load identity providers..')
    # Research Identity Providers
    result = getProvidersForInstance('Id')
    if result['OK']:
      for providerName in result['Value']:
        result = cls._idps.getIdProvider(providerName)
        if result['OK']:
          cls._idp[result['Value'].issuer.strip('/')] = result['Value']
        else:
          gLogger.error(result['Message'])

  @classmethod
  def __initializeService(cls, request):
    """
      Initialize a service.
      The work is only performed once at the first request.

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

      # Load all registred identity providers
      cls.__loadIdPs()

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

      # Some pre-initialization
      cls._initializeHandler()

      cls.initializeHandler(serviceInfo)

      cls.__init_done = True

      return S_OK()

  @classmethod
  def _initializeHandler(cls):
    """
      If you are writing your own framework that follows this class
      and you need to add something before initializing the service,
      such as initializing the OAuth client, then you need to change this method.
    """
    pass

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
    raise NotImplementedError('Please, create the _getMethodName method')

  def _getMethodArgs(self, args):
    """ Decode args.

        :return: list
    """
    return args

  def _getMethodAuthProps(self):
    """ Resolves the hard coded authorization requirements for method.

        :return: list
    """
    if self.AUTH_PROPS and not isinstance(self.AUTH_PROPS, (list, tuple)):
      self.AUTH_PROPS = [p.strip() for p in self.AUTH_PROPS.split(",") if p.strip()]
    return getattr(self, 'auth_' + self.method, self.AUTH_PROPS)

  def _getMethod(self):
    """ Get method function to call.

        :return: function
    """
    method = getattr(self, '%s%s' % (self.METHOD_PREFIX, self.method), None)
    if not callable(method):
      sLog.error("Invalid method", self.method)
      raise HTTPError(status_code=http_client.NOT_IMPLEMENTED)
    return method

  def prepare(self):
    """
      Tornados prepare method that called before request
    """

    # "method" argument of the POST call.
    # This resolves into the ``export_<method>`` method
    # on the handler side
    # If the argument is not available, the method exists
    # and an error 400 ``Bad Request`` is returned to the client
    self.method = self._getMethodName()

    self._monitorRequest()

    self._prepare()

  def _prepare(self):
    """
      Prepare the request. It reads certificates and check authorizations.
      We make the assumption that there is always going to be a ``method`` argument
      regardless of the HTTP method used

    """
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
      raise HTTPError(http_client.UNAUTHORIZED, str(e))

    # Check whether we are authorized to perform the query
    # Note that performing the authQuery modifies the credDict...
    authorized = self._authManager.authQuery(self.method, self.credDict,
                                             self._getMethodAuthProps())
    if not authorized:
      extraInfo = ''
      if self.credDict.get('ID'):
        extraInfo += 'ID: %s' % self.credDict['ID']
      elif self.credDict.get('DN'):
        extraInfo += 'DN: %s' % self.credDict['DN']
      sLog.error(
          "Unauthorized access", "Identity %s; path %s; %s" %
          (self.srv_getFormattedRemoteCredentials(),
           self.request.path, extraInfo))
      raise HTTPError(http_client.UNAUTHORIZED)

  def __executeMethod(self, targetMethod, args):
    """
      Execute the method called, this method is ran in an executor
      We have several try except to catch the different problem which can occur

      - First, the method does not exist => Attribute error, return an error to client
      - second, anything happend during execution => General Exception, send error to client

      .. warning::
        This method is called in an executor, and so cannot use methods like self.write
        See https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes

      :param str targetMethod: name of the method to call
      :param list args: target method arguments

      :return: Future
    """

    sLog.notice(
        "Incoming request %s /%s: %s" %
        (self.srv_getFormattedRemoteCredentials(),
         self._serviceName,
         self.method))

    # Execute
    try:
      self.initializeRequest()
      return targetMethod(*args)
    except Exception as e:  # pylint: disable=broad-except
      sLog.exception("Exception serving request", "%s:%s" % (str(e), repr(e)))
      raise e if isinstance(e, HTTPError) else HTTPError(http_client.INTERNAL_SERVER_ERROR, str(e))

  def _prepareExecutor(self, args):
    """ Preparation of necessary arguments for the `__executeMethod` method

        :param list args: arguments passed to the `post`, `get`, etc. tornado methods

        :return: executor, target method with arguments
    """
    return None, partial(self.__executeMethod, self._getMethod(), self._getMethodArgs(args))

  def _finishFuture(self, retVal):
    """ Handler Future result

        :param object retVal: tornado.concurrent.Future
    """
    self.result = retVal

    # Here it is safe to write back to the client, because we are not in a thread anymore

    # If you need to end the method using tornado methods, outside the thread,
    # you need to define the finish_<methodName> method.
    # This method will be started after __executeMethod is completed.
    finishFunc = getattr(self, 'finish_%s' % self.method, None)
    if callable(finishFunc):
      finishFunc()

    # In case nothing is returned
    elif retVal is None:
      self.finish()

    # If set to true, do not JEncode the return of the RPC call
    # This is basically only used for file download through
    # the 'streamToClient' method.
    elif self.get_argument('rawContent', default=False):
      # See 4.5.1 http://www.rfc-editor.org/rfc/rfc2046.txt
      self.set_header("Content-Type", "application/octet-stream")
      self.finish(retVal)

    # Return simple text or html
    elif isinstance(retVal, string_types):
      self.finish(retVal)

    # JSON
    else:
      self.set_header("Content-Type", "application/json")
      self.finish(encode(retVal))

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

  def _gatherPeerCredentials(self, grants=None):
    """ Returne a dictionary designed to work with the AuthManager,
        already written for DISET and re-used for HTTPS.

        :param list grants: grants to use

        :returns: a dict containing user credentials
    """
    err = []

    # At least some authorization method must be defined, if nothing is defined,
    # the authorization will go through the `_authzVISITOR` method and
    # everyone will have access as anonymous@visitor
    for grant in (grants or self.USE_AUTHZ_GRANTS or 'VISITOR'):
      grant = grant.upper()
      grantFunc = getattr(self, '_authz%s' % grant, None)
      result = grantFunc() if callable(grantFunc) else S_ERROR('%s authentication type is not supported.' % grant)
      if result['OK']:
        for e in err:
          sLog.debug(e)
        sLog.debug('%s authentication success.' % grant)
        return result['Value']
      err.append('%s authentication: %s' % (grant, result['Message']))

    # Report on failed authentication attempts
    raise Exception('; '.join(err))

  def _authzSSL(self):
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
        chainAsText = cert.as_pem()
    elif self.request.headers.get('X-Ssl_client_verify') == 'SUCCESS':
      chainAsTextEncoded = self.request.headers.get('X-SSL-CERT')
      chainAsText = unquote(chainAsTextEncoded)
    else:
      return S_ERROR(DErrno.ECERTFIND, 'Valid certificate not found.')

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

  def _authzJWT(self, accessToken=None):
    """ Load token claims in DIRAC and extract informations.

        :param str accessToken: access_token

        :return: S_OK(dict)/S_ERROR()
    """
    if not accessToken:
      # Export token from headers
      token = self.request.headers.get('Authorization')
      if not token or len(token.split()) != 2:
        return S_ERROR(DErrno.EATOKENFIND, 'Not found a bearer access token.')
      tokenType, accessToken = token.split()
      if tokenType.lower() != 'bearer':
        return S_ERROR(DErrno.ETOKENTYPE, 'Found a not bearer access token.')

    # Read token without verification to get issuer
    self.log.debug('Read issuer from access token', accessToken)
    issuer = jwt.decode(accessToken, leeway=300, options=dict(verify_signature=False,
                                                              verify_aud=False))['iss'].strip('/')
    # Verify token
    self.log.debug('Verify access token')
    result = self._idp[issuer].verifyToken(accessToken)
    self.log.debug('Search user group')
    return self._idp[issuer].researchGroup(result['Value'], accessToken) if result['OK'] else result

  def _authzVISITOR(self):
    """ Visitor access

        :return: S_OK(dict)
    """
    return S_OK({})

  @property
  def log(self):
    return sLog

  def getDN(self):
    return self.credDict.get('DN', '')

  def getUserName(self):
    return self.credDict.get('username', '')

  def getUserGroup(self):
    return self.credDict.get('group', '')

  def getProperties(self):
    return self.credDict.get('properties', [])

  def isRegisteredUser(self):
    return self.credDict.get('username', 'anonymous') != 'anonymous' and self.credDict.get('group')

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
      peerId = "[%s:%s]" % (self.credDict.get('group', 'visitor'), self.credDict.get('username', 'anonymous'))
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
