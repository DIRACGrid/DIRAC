""" Main module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ssl
import json
import functools
import traceback

from concurrent.futures import ThreadPoolExecutor

import tornado.web
import tornado.gen
import tornado.ioloop
import tornado.websocket
import tornado.stack_context

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Web import Conf
from DIRAC.Core.Web.SessionData import SessionData
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Utilities.JEncode import encode
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.AuthManagerData import gAuthManagerData


global gThreadPool
gThreadPool = ThreadPoolExecutor(100)


class WErr(tornado.web.HTTPError):

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
  return tornado.gen.coroutine(method)


class WebHandler(tornado.web.RequestHandler):
  __disetConfig = ThreadConfig()
  __log = False

  # Auth requirements
  AUTH_PROPS = None
  # Location of the handler in the URL
  LOCATION = ""
  # URL Schema with holders to generate handler urls
  URLSCHEMA = ""
  # RE to extract group and setup
  PATH_RE = None
  # If need to use request path for declare some value/option
  OVERPATH = False

  def threadTask(self, method, *args, **kwargs):
    def threadJob(*targs, **tkwargs):
      args = targs[0]
      disetConf = targs[1]
      self.__disetConfig.reset()
      self.__disetConfig.load(disetConf)
      return method(*args, **tkwargs)

    targs = (args, self.__disetDump)
    return tornado.ioloop.IOLoop.current().run_in_executor(gThreadPool,
                                                           functools.partial(threadJob, *targs, **kwargs))

  def __disetBlockDecor(self, func):
    def wrapper(*args, **kwargs):
      raise RuntimeError("All DISET calls must be made from inside a Threaded Task!")

    return wrapper

  def __init__(self, *args, **kwargs):
    """ Initialize the handler
    """
    super(WebHandler, self).__init__(*args, **kwargs)
    if not WebHandler.__log:
      WebHandler.__log = gLogger.getSubLogger(self.__class__.__name__)
    # Look idetity provider and session
    self.__idp = self.get_cookie("TypeAuth") or "Certificate"
    self.__session = self.get_cookie(self.__idp) or None
    # Fill credentials
    self.__credDict = {}
    self.__setup = Conf.setup()
    result = self.__processCredentials()
    if not result['OK']:
      self.__idp = "Visitor"
      self.log.error(result['Message'], 'Continue as Visitor.')
    self.log.verbose("%s authentication" % self.__idp,
                     'with %s session' % self.__session if self.__session else '')
    # Restore identity provider
    self.set_cookie("TypeAuth", self.__idp)
    # Setup diset
    self.__disetConfig.reset()
    self.__disetConfig.setDecorator(self.__disetBlockDecor)
    self.__disetDump = self.__disetConfig.dump()
    match = self.PATH_RE.match(self.request.path)
    pathItems = match.groups()
    self._pathResult = self.__checkPath(*pathItems[:3])
    self.overpath = pathItems[3:] and pathItems[3] or ''
    self.__sessionData = SessionData(self.__credDict, self.__setup)
    self.__forceRefreshCS()

  def __forceRefreshCS(self):
    """ Force refresh configuration from master configuration server
    """
    if self.request.headers.get('X-RefreshConfiguration') == 'True':
      self.log.debug('Initialize force refresh..')
      if not AuthManager('').authQuery("", dict(self.__credDict), "CSAdministrator"):
        raise WErr(401, 'Cannot initialize force refresh, request not authenticated')
      result = gConfig.forceRefresh()
      if not result['OK']:
        raise WErr(501, result['Message'])

  def __processCredentials(self):
    """ Extract the user credentials based on the certificate or what comes from the balancer
    """
    # Unsecure protocol only for visitors
    if self.request.protocol != "https" or self.__idp == "Visitor":
      return S_OK()

    # For certificate
    if self.__idp == 'Certificate':
      return self.__readCertificate()

    # Look enabled authentication types in CS
    result = Conf.getCSSections("TypeAuths")
    if not result['OK']:
      self.log.warn('To enable idenyity provider need to use "TypeAuths" section, but %s' % result['Message'])
    if self.__idp not in (result.get('Value') or []):
      return S_ERROR("%s is absent in configuration." % self.__idp)

    if not self.__session:
      return S_ERROR('No found session in cookies.')

    result = gAuthManagerData.getIDForSession(self.__session)
    if not result['OK']:
      self.set_cookie(self.__idp, '')
    else:
      self.__credDict['ID'] = result['Value']
    return result

  def _request_summary(self):
    """ Return a string returning the summary of the request

        :return: str
    """
    summ = super(WebHandler, self)._request_summary()
    cl = []
    if self.__credDict.get('validDN', False):
      cl.append(self.__credDict['username'])
      if self.__credDict.get('validGroup', False):
        cl.append("@%s" % self.__credDict['group'])
      cl.append(" (%s)" % self.__credDict['DN'])
    summ = "%s %s" % (summ, "".join(cl))
    return summ

  def __readCertificate(self):
    """ Fill credentional from certificate and check is registred

        :return: S_OK()/S_ERROR()
    """
    if Conf.balancer() == "nginx":
      # NGINX
      headers = self.request.headers
      if not headers:
        return S_ERROR('No headers found.')
      if headers.get('X-Scheme') == "https" and headers.get('X-Ssl_client_verify') == 'SUCCESS':
        DN = headers['X-Ssl_client_s_dn']
        if not DN.startswith('/'):
          items = DN.split(',')
          items.reverse()
          DN = '/' + '/'.join(items)
        self.__credDict['DN'] = DN
        self.__credDict['issuer'] = headers['X-Ssl_client_i_dn']
      else:
        return S_ERROR('No certificate upload to browser.')

    else:
      # TORNADO
      derCert = self.request.get_ssl_certificate(binary_form=True)
      if not derCert:
        return S_ERROR('No certificate found.')
      pemCert = ssl.DER_cert_to_PEM_cert(derCert)
      chain = X509Chain()
      chain.loadChainFromString(pemCert)
      result = chain.getCredentials()
      if not result['OK']:
        return S_ERROR("Could not get client credentials %s" % result['Message'])
      self.__credDict = result['Value']
      # Hack. Data coming from OSSL directly and DISET difer in DN/subject
      try:
        self.__credDict['DN'] = self.__credDict['subject']
      except KeyError:
        pass

    result = Registry.getUsernameForDN(self.__credDict['DN'])
    if not result['OK']:
      return result
    self.__credDict['username'] = result['Value']
    return S_OK()

  @property
  def log(self):
    return self.__log

  @classmethod
  def getLog(cls):
    return cls.__log

  def getDN(self):
    return self.__credDict.get('DN', '')

  def getID(self):
    return self.__credDict.get('ID', '')

  def getIdP(self):
    return self.__idp

  def getSession(self):
    return self.__session

  def getUserName(self):
    return self.__credDict.get('username', '')

  def getUserGroup(self):
    return self.__credDict.get('group', '')

  def getUserSetup(self):
    return self.__setup

  def getProperties(self):
    return self.__credDict.get('properties', [])

  def isRegisteredUser(self):
    return self.__credDict.get('username', 'anonymous') != 'anonymous' and self.__credDict.get('group')

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

  def __auth(self, handlerRoute, group, method):
    """ Authenticate request

        :param str handlerRoute: the name of the handler
        :param str group: DIRAC group
        :param str method: the name of the method

        :return: bool
    """
    if not isinstance(self.AUTH_PROPS, (list, tuple)):
      self.AUTH_PROPS = [p.strip() for p in self.AUTH_PROPS.split(",") if p.strip()]

    self.__credDict['validGroup'] = False
    self.__credDict['group'] = group
    auth = AuthManager(Conf.getAuthSectionForHandler(handlerRoute))
    ok = auth.authQuery(method, self.__credDict, self.AUTH_PROPS)
    if ok:
      self.__credDict['validGroup'] = True
      # WARN: __credDict['properties'] already defined in AuthManager in the last version of DIRAC
      self.__credDict['properties'] = Registry.getPropertiesForGroup(self.__credDict['group'], [])
      msg = ' - '
      if self.__credDict.get('DN'):
        msg = '%s' % self.__credDict['DN']
      elif self.__credDict.get('ID'):
        result = gAuthManagerData.getIdPsForID(self.__credDict['ID'])  # pylint: disable=no-member
        if not result['OK']:
          self.log.error(result['Message'])
          return False
        msg = 'IdP: %s, ID: %s' % (result['Value'], self.__credDict['ID'])
      self.log.info("AUTH OK: %s by %s@%s (%s)" % (handlerRoute, self.__credDict['username'],
                                                   self.__credDict['group'], msg))
    else:
      self.log.info("AUTH KO: %s by %s@%s" % (handlerRoute, self.__credDict['username'], self.__credDict['group']))

    if self.isTrustedHost(self.__credDict.get('DN')):
      self.log.info("Request is coming from Trusted host")
      return True

    return ok

  def isTrustedHost(self, dn):
    """ Check if the request coming from a TrustedHost
        :param str dn: certificate DN

        :return: bool if the host is Trusrted it return true otherwise false
    """
    retVal = Registry.getHostnameForDN(dn)
    if retVal['OK']:
      hostname = retVal['Value']
      if Properties.TRUSTED_HOST in Registry.getPropertiesForHost(hostname, []):
        return True
    return False

  def __checkPath(self, setup, group, route):
    """ Check the request, auth, credentials and DISET config

        :param str setup: setup name
        :param str group: group name
        :param str route: route

        :return: WOK()/WErr()
    """
    if route[-1] == "/":
      methodName = "index"
      handlerRoute = route
    else:
      iP = route.rfind("/")
      methodName = route[iP + 1:]
      handlerRoute = route[:iP]
    if setup:
      self.__setup = setup
    if not self.__auth(handlerRoute, group, methodName):
      return WErr(401, "Unauthorized. %s" % methodName)

    DN = self.getDN()
    if DN:
      self.__disetConfig.setDN(DN)
    ID = self.getID()
    if ID:
      self.__disetConfig.setID(ID)

    # pylint: disable=no-value-for-parameter
    if self.getUserGroup():  # pylint: disable=no-value-for-parameter
      self.__disetConfig.setGroup(self.getUserGroup())  # pylint: disable=no-value-for-parameter
    self.__disetConfig.setSetup(setup)
    self.__disetDump = self.__disetConfig.dump()

    return WOK(methodName)

  def get(self, setup, group, route, overpath=None):
    if not self._pathResult.ok:
      raise self._pathResult
    methodName = "web_%s" % self._pathResult.data
    try:
      mObj = getattr(self, methodName)
    except AttributeError as e:
      self.log.fatal("This should not happen!! %s" % e)
      raise tornado.web.HTTPError(404)
    return mObj()

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
    if not self._pathResult.ok:
      raise self._pathResult
    return self.on_open()

  def on_open(self):
    pass
