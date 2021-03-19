""" Main module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import ssl
import json
import functools
import traceback

from concurrent.futures import ThreadPoolExecutor

from authlib.jose import jwt
from authlib.common.security import generate_token

import tornado.web
import tornado.websocket
from tornado import gen
from tornado.web import HTTPError
from tornado.ioloop import IOLoop

from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from DIRAC.Core.Security import Properties
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Utilities.JEncode import encode
from DIRAC.Core.Tornado.Web import Conf
from DIRAC.Core.Tornado.Web.SessionData import SessionData
from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
from DIRAC.Core.Tornado.Server.BaseRequestHandler import BaseRequestHandler
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import ResourceProtector
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


global gThreadPool
gThreadPool = ThreadPoolExecutor(100)
sLog = gLogger.getSubLogger(__name__)


class WErr(HTTPError):

  def __init__(self, code, msg="", **kwargs):
    super(WErr, self).__init__(code, str(msg) or None)
    for k in kwargs:
      setattr(self, k, kwargs[k])
    self.ok = False
    self.msg = msg
    self.kwargs = kwargs

  def __str__(self):
    return super(WErr, self).__str__()

  @classmethod
  def fromSERROR(cls, result):
    """ Prevent major problem with % in the message """
    return cls(500, result['Message'].replace("%", ""))


class WOK(object):

  def __init__(self, data=False, **kwargs):
    for k in kwargs:
      setattr(self, k, kwargs[k])
    self.ok = True
    self.data = data


def asyncWithCallback(method):
  return tornado.web.asynchronous(method)


def asyncGen(method):
  return gen.coroutine(method)


class WebHandler(TornadoREST):
  __disetConfig = ThreadConfig()

  # Auth requirements
  AUTH_PROPS = None
  # Location of the handler in the URL
  LOCATION = ""
  # URL Schema with holders to generate handler urls
  URLSCHEMA = ""
  # RE to extract group and setup
  PATH_RE = None
  # Prefix of methods names
  METHOD_PREFIX = "web_"
  # Change JWT authz method
  AUTHZ_JWT_METHOD = cls.__authzToken

  def threadTask(self, method, *args, **kwargs):
    def threadJob(*targs, **tkwargs):
      args = targs[0]
      disetConf = targs[1]
      self.__disetConfig.reset()
      self.__disetConfig.load(disetConf)
      return method(*args, **tkwargs)

    targs = (args, self.__disetDump)
    return IOLoop.current().run_in_executor(gThreadPool, functools.partial(threadJob, *targs, **kwargs))

  def __disetBlockDecor(self, func):
    def wrapper(*args, **kwargs):
      raise RuntimeError("All DISET calls must be made from inside a Threaded Task!")
    return wrapper

  @classmethod
  def _getServiceName(cls, request):
    """ Search service name in request

        :param object request: tornado Request

        :return: str
    """
    match = cls.PATH_RE.match(request.path)
    groups = match.groups()
    route = groups[2]
    return route if route[-1] == "/" else route[:route.rfind("/")]

  @classmethod
  def _getServiceAuthSection(cls, serviceName):
    """ Search service auth section. Developers MUST
        implement it in subclass.

        :param str serviceName: service name

        :return: str
    """
    return Conf.getAuthSectionForHandler(serviceName)

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    match = self.PATH_RE.match(self.request.path)
    groups = match.groups()
    route = groups[2]
    return "index" if route[-1] == "/" else route[route.rfind("/") + 1:]

  def prepare(self):
    """
      Prepare the request. It reads certificates and check authorizations.
      We make the assumption that there is always going to be a ``method`` argument
      regardless of the HTTP method used

    """
    self.__session = None
    self.__parseURI()
    self.__disetConfig.reset()
    self.__disetConfig.setDecorator(self.__disetBlockDecor)
    self.__disetDump = self.__disetConfig.dump()

    super(WebHandler, self).prepare()

    # TODO:
    # if self.credDict.get('DN') and self.isTrustedHost(self.credDict['DN']):
    #   self.log.info("Request is coming from Trusted host")
    #   authorized = True

    if self.getDN():
      self.__disetConfig.setDN(self.getDN())
    if self.getID():
      self.__disetConfig.setID(self.getID())
    # pylint: disable=no-value-for-parameter
    if self.getUserGroup():  # pylint: disable=no-value-for-parameter
      self.__disetConfig.setGroup(self.getUserGroup())  # pylint: disable=no-value-for-parameter
    self.__disetConfig.setSetup(self.__setup)
    self.__disetDump = self.__disetConfig.dump()

    self.__sessionData = SessionData(self.credDict, self.__setup)
    self.__forceRefreshCS()

  def __parseURI(self):
    match = self.PATH_RE.match(self.request.path)
    groups = match.groups()
    self.__setup = groups[0] or Conf.setup()
    self.__group = groups[1]
    self.__route = groups[2]
    self.__args = groups[3:]

  def __forceRefreshCS(self):
    """ Force refresh configuration from master configuration server
    """
    if self.request.headers.get('X-RefreshConfiguration') == 'True':
      self.log.debug('Initialize force refresh..')
      if not AuthManager('').authQuery("", dict(self.credDict), "CSAdministrator"):
        raise WErr(401, 'Cannot initialize force refresh, request not authenticated')
      result = gConfig.forceRefresh()
      if not result['OK']:
        raise WErr(501, result['Message'])

  def _gatherPeerCredentials(self):
    """
      Load client certchain in DIRAC and extract informations.

      The dictionary returned is designed to work with the AuthManager,
      already written for DISET and re-used for HTTPS.

      :returns: a dict containing the return of :py:meth:`DIRAC.Core.Security.X509Chain.X509Chain.getCredentials`
                (not a DIRAC structure !)
    """
    credDict = {}

    # Authorization type
    self.__authGrant = self.get_cookie('authGrant', 'Certificate')

    if self.request.protocol == "https" and self.__authGrant.lower() != 'visitor':
      if self.__authGrant == 'Session':
        # read session
        credDict = self.__readSession(self.get_secure_cookie('session_id'))

      else:
        # Read token and certificate
        credDict = super(WebHandler, self)._gatherPeerCredentials()

      # Add a group if it present in the request path
      if self.__group:
        credDict['validGroup'] = False
        credDict['group'] = self.__group

    return credDict

  # def _request_summary(self):
  #   """ Return a string returning the summary of the request

  #       :return: str
  #   """
  #   summ = super(WebHandler, self)._request_summary()
  #   cl = []
  #   if self.credDict.get('validDN', False):
  #     cl.append(self.credDict['username'])
  #     if self.credDict.get('validGroup', False):
  #       cl.append("@%s" % self.credDict['group'])
  #     cl.append(" (%s)" % self.credDict['DN'])
  #   summ = "%s %s" % (summ, "".join(cl))
  #   return summ

  def __readSession(self, sessionID):
    """ Fill credentionals from session

        :param str sessionID: session id

        :return: dict
    """
    if not sessionID:
      return {}

    session = self.application.getSession(sessionID)
    if not session or not session.token:
      self.clear_cookie('session_id')
      raise Exception('%s session expired.' % sessionID)

    if self.request.headers.get("Authorization"):
      token = ResourceProtector().acquire_token(self.request, 'changeGroup')

      # Is session active?
      if session.token.access_token != token.access_token:
        raise Exception('%s session invalid, token is not match.' % sessionID)
    token = ResourceProtector().validator(session.token.refresh_token, 'changeGroup', None, 'OR')

    # Update session expired time
    self.application.updateSession(session)
    return {'ID': token.sub, 'issuer': token.issuer, 'group': self.__group, 'validGroup': False}

  def __authzToken(self):
    """ Load token claims in DIRAC and extract informations.

        :return: S_OK(dict)/S_ERROR()
    """
    # TODO: provide scope validation
    try:
      token = ResourceProtector().acquire_token(self.request)
    except Exception as e:
      return S_ERROR(str(e))
    return S_OK({'ID': token.sub, 'issuer': token.issuer, 'group': token.groups[0]})

  # def _readToken(self, scope=None):
  #   """ Fill credentionals from session

  #       :param str scope: scope

  #       :return: dict
  #   """
  #   scope = self.__group and ('g:%s' % self.__group)
  #   return TornadoREST._readToken(self, scope)

  @property
  def log(self):
    return sLog

  # @classmethod
  # def getLog(cls):
  #   return cls.__log

  def getCurrentSession(self):
    return self.__session

  def getUserSetup(self):
    return self.__setup

  def getSessionData(self):
    return self.__sessionData.getData()

  def getAppSettings(self, app=None):
    return Conf.getAppSettings(app or self.__class__.__name__.replace('Handler', '')).get('Value') or {}

  def actionURL(self, action=""):
    """ Given an action name for the handler, return the URL

        :param str action: action

        :return: str
    """
    if action == "index":
      action = ""
    group = self.getUserGroup()
    if group:
      group = "/g:%s" % group
    setup = self.getUserSetup()
    if setup:
      setup = "/s:%s" % setup
    location = self.LOCATION
    if location:
      location = "/%s" % location
    ats = dict(action=action, group=group, setup=setup, location=location)
    return self.URLSCHEMA % ats

  # def isTrustedHost(self, dn):
  #   """ Check if the request coming from a TrustedHost

  #       :param str dn: certificate DN

  #       :return: bool if the host is Trusrted it return true otherwise false
  #   """
  #   retVal = Registry.getHostnameForDN(dn)
  #   if retVal['OK']:
  #     hostname = retVal['Value']
  #     if Properties.TRUSTED_HOST in Registry.getPropertiesForHost(hostname, []):
  #       return True
  #   return False

  def get(self, setup, group, route, *pathArgs):
    method = self._getMethod()
    return method(*pathArgs)

  def post(self, *args, **kwargs):
    return self.get(*args, **kwargs)

  def delete(self, *args, **kwargs):
    return self.get(*args, **kwargs)

  def write_error(self, status_code, **kwargs):
    self.set_status(status_code)
    cType = "text/plain"
    data = self._reason
    if 'exc_info' in kwargs:
      ex = kwargs['exc_info'][1]
      trace = traceback.format_exception(*kwargs["exc_info"])
      if not isinstance(ex, WErr):
        data += "\n".join(trace)
      else:
        if self.settings.get("debug"):
          self.log.error("Request ended in error:\n  %s" % "\n  ".join(trace))
        data = ex.msg
        if isinstance(data, dict):
          cType = "application/json"
          data = json.dumps(data)
    self.set_header('Content-Type', cType)
    self.finish(data)

  def finishJEncode(self, o):
    """ Encode data before finish
    """
    self.finish(encode(o))


class WebSocketHandler(tornado.websocket.WebSocketHandler, WebHandler):

  def __init__(self, *args, **kwargs):
    WebHandler.__init__(self, *args, **kwargs)
    tornado.websocket.WebSocketHandler.__init__(self, *args, **kwargs)

  def open(self, setup, group, route):
    return self.on_open()

  def on_open(self):
    pass
