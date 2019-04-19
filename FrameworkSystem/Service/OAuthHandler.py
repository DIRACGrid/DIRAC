""" The OAuth service provides a toolkit to authoticate throught oAuth2 session.
"""
import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.FrameworkSystem.DB.OAuthDB import OAuthDB
from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"

gOAuthDB = None


def initializeOAuthHandler(serviceInfo):
    """ Handler initialization
    """
    global gOAuthDB
    gOAuthDB = OAuthDB()
    result = gThreadScheduler.addPeriodicTask(3600, gOAuthDB.cleanZombie)
    return S_OK()


class OAuthHandler(RequestHandler):

  @classmethod
  def initializeOAuthHandler(cls, serviceInfo):
    """ Handler initialization
    """
    return S_OK()

  def initialize(self):
    """ Response initialization
    """

  types_checkToken = [basestring]

  def export_checkToken(self, token):
    """ Check status of tokens, refresh and back dict.
    """
    gLogger.notice("Check token %s." % token)
    result = gOAuthDB.fetch_token(token)
    gLogger.notice(result)
    return S_OK(result)

  types_getProxyDNExpTimeFromOAuthProxyProvider = [basestring]

  def export_getProxyDNExpTimeFromOAuthProxyProvider(self, proxyProvider, userName=None, proxyLiveTime=None, DN=None):
    """ Get proxy, user DN, exired sec from DB
    """
    if not userName and not DN:
      return S_ERROR('No user name or DN set')
    if not proxyProvider:
      return S_ERROR('No proxy provider set')
    return gOAuthDB.get_proxy_dn_exptime(proxyProvider, proxylivetime=proxyLiveTime, username=userName, DN=DN)

  types_getProxy = [basestring]

  def export_getProxy(self, username, group, voms=None, proxylivetime=None):
    """ Create proxy
        result: proxy pem type(str) or S_ERROR
    """
    result = Registry.getDNForUsername(username)
    if not result['OK']:
      return S_ERROR('Cannot get proxy')
    if not Registry.getGroupsForUser(username)['OK']:
      return S_ERROR('Cannot get proxy')
    for DN in result['Value']:
      if group in Registry.getGroupsFromDNProperties(DN):
        if voms:
          voms = Registry.getVOForGroup(group)
          result = gProxyManager.downloadVOMSProxy(DN, group, requiredVOMSAttribute=voms,
                                                   requiredTimeLeft=proxylivetime)
        else:
          result = gProxyManager.downloadProxy(DN, group, requiredTimeLeft=int(proxylivetime))
        if result['OK']:
          gLogger.info(result)
          chain = result['Value']
          return chain.dumpAllToString()
    return S_ERROR('Somethink going wrone')

  types_getUsrnameForState = [basestring]

  def export_getUsrnameForState(self, state):
    """ Listen DB to get username by state """
    result = gOAuthDB.get_by_state(state, ['UserName', 'State'])
    if result['OK']:
      return S_OK({'username': result['Value']['UserName'], 'state': result['Value']['State']})
    return result

  types_killState = [basestring]

  def export_killState(self, state):
    """ Kill session """
    return gOAuthDB.kill_state(state)

  types_get_link_by_state = [basestring]

  def export_get_link_by_state(self, state):
    return gOAuthDB.get_link_by_state(state)

  types_waitStateResponse = [basestring]

  def export_waitStateResponse(self, state, group=None, needProxy=False,
                               voms=None, proxyLifeTime=43200, time_out=20, sleeptime=5):
    """ Listen DB to get status of auth """
    gLogger.notice("Read auth status for '%s' state." % state)
    start = time.time()
    runtime = 0
    result = S_ERROR()
    for _i in range(int(int(time_out) // int(sleeptime))):
      time.sleep(sleeptime)
      runtime = time.time() - start
      if runtime > time_out:
        gOAuthDB.kill_state(state)
        return S_ERROR('Timeout')
      result = gOAuthDB.get_by_state(state)
      gLogger.notice("result: '%s' of state." % state)
      gLogger.notice(result)
      if result['OK']:
        status = result['Value']['Status']
        if not status:
          return S_ERROR('We lost your request.')
        elif status == 'prepared':
          continue
        elif status == 'visitor':
          return S_OK({'Status': status, 'Message': result['Value']['Comment']})
        elif status == 'failed':
          return S_ERROR(result['Value']['Comment'])
        elif status == 'authed':
          resD = result['Value']
          if needProxy:
            if not group:
              result = Registry.findDefaultGroupForUser(resD['UserName'])
              if not result['OK']:
                return result
              group = result['Value']
            elif group not in Registry.getGroupsForUser(resD['UserName'])['Value']:
              return S_ERROR('%s group is not found for %s user.' % (group, resD['UserName']))
            result = self.export_getProxy(resD['UserName'], group, voms=voms, proxylivetime=proxyLifeTime)
            if not result['OK']:
              gLogger.notice('Proxy was not created.')
              return result
            gLogger.notice('Proxy was created.')
            return S_OK({'Status': status, 'proxy': result['Value']})
        return result
    return result

  types_create_auth_request_uri = [basestring]

  def export_create_auth_request_uri(self, idp):
    """ Create request uri to IdP authority end-point and store it
    """
    gLogger.notice("Creating authority request uri for '%s' IdP." % idp)
    result = gOAuthDB.get_auth_request_uri(idp)
    if not result['OK']:
      return S_ERROR('Cannot create authority request uri.')
    return result

  types_parse_auth_response = [basestring]

  def export_parse_auth_response(self, code, state):
    """ Make request to IdP with responsed authority code to get token and store it
    """
    gLogger.notice("Making request(code: %s, state: %s) to get token." % (code, state))
    return gOAuthDB.parse_auth_response(code, state)

  @staticmethod
  def __cleanOAuthDBZombie():
    """ Check OAuthDB for zombie sessions """
    gLogger.notice("Killing zombie sessions")
    result = gOAuthDB.cleanZombie()
    if not result['OK']:
      gLogger.error(result['Message'])
      return result
    gLogger.notice("Cleaning is done!")
    return S_OK('Cleaning is done!')
