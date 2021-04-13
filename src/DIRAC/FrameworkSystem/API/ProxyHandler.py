""" Handler to serve the DIRAC proxy data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.ConfigurationSystem.Client.Utilities import isDownloadablePersonalProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsernameInGroup

__RCSID__ = "$Id$"


class ProxyHandler(TornadoREST):
  USE_AUTHZ_GRANTS = ['JWT']
  RAISE_DIRAC_ERROR = True
  SYSTEM = 'Framework'
  AUTH_PROPS = "authenticated"
  LOCATION = "/DIRAC"

  def initializeRequest(self):
    """ Request initialization """
    self.proxyCli = ProxyManagerClient(delegatedGroup=self.getUserGroup(),
                                       delegatedID=self.getID(), delegatedDN=self.getDN())

  path_proxy = [r'([a-z]*)[\/]?([a-z]*)']

  def web_proxy(self, user=None, group=None):
    """ RESTful endpoints to user proxy management to retrieve personal proxy.

        GET LOCATION/proxy

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | Authorization  | header | Provide access token                      | Bearer jkagfbfd3r4ubf887gqduyqwogasd8 |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | lifetime       | query  | requested proxy live time in seconds      | 3600                                  |
        |                |        | by default 6 hours (optional)             |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | voms           | query  | to get user proxy with VOMS extension,    | true                                  |
        |                |        | by defaul false (optional)                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+

        Request example::

          GET LOCATION/proxy
          Authorization: Bearer <access_token>

        Response::

          HTTP/1.1 200 OK

          <proxy as string>
    """
    voms = self.get_argument('voms', None)
    try:
      proxyLifeTime = int(self.get_argument('lifetime', 3600 * 6))
    except Exception:
      return S_ERROR('Cannot read "lifetime" argument.')

    # GET
    if self.request.method == 'GET':
      # # Return content of Proxy DB
      # if 'metadata' in optns:
      #   pass

      # Return personal proxy
      if not user and not group:
        return self.__getProxy(self.getUserName(), self.getUserGroup(), voms, proxyLifeTime)

      elif user and group:
        return self.__getProxy(user, group, voms, proxyLifeTime)

      else:
        return S_ERROR("Wrone request.")

  def __getProxy(self, user, group, voms, lifetime):
    """ Get proxy

        :param str user: user name
        :param str group: group name
        :param bool voms: add voms ext
        :param int lifetime: proxy lifetime

        :return: S_OK(str)/S_ERROR()
    """
    lifetime = min(lifetime, 3600 * 6)

    # Allowe to take only personal proxy
    if self.getUserName() != user or self.getUserGroup() != group:
      return S_ERROR('Sorry, only personal proxy is allowed to download')

    if not isDownloadablePersonalProxy():
      return S_ERROR("You can't get proxy, configuration settings(downloadablePersonalProxy) not allow to do that.")

    if voms:
      result = self.proxyCli.downloadVOMSProxy(user, group, requiredTimeLeft=lifetime)
    else:
      result = self.proxyCli.downloadProxy(user, group, requiredTimeLeft=lifetime)
    if result['OK']:
      self.log.notice('Proxy was created.')
      return result['Value'].dumpAllToString()
    return result
