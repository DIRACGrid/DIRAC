""" Handler to serve the DIRAC proxy data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsernameInGroup

__RCSID__ = "$Id$"


class ProxyHandler(TornadoREST):
  AUTH_PROPS = "authenticated"
  LOCATION = "/DIRAC"

  def initializeRequest(self):
    """ Request initialization """
    self.proxyCli = ProxyManagerClient(delegatedGroup=self.getUserGroup(),
                                       delegatedID=self.getID(), delegatedDN=self.getDN())

  path_proxy = [r'([a-z]*)[\/]?([a-z]*)']

  def web_proxy(self, user=None, group=None):
    """ REST endpoints to user proxy management

        **GET** /proxy?<options> -- retrieve personal proxy

          Options:
            * *voms* -- to get user proxy with VOMS extension(optional)
            * *lifetime* -- requested proxy live time(optional)

          Response is a proxy certificate as text

        **GET** /proxy/<user>/<group>?<options> -- retrieve proxy
          * *user* -- user name
          * *group* -- group name

          Options:
            * *voms* -- to get user proxy with VOMS extension(optional)
            * *lifetime* -- requested proxy live time(optional)

          Response is a proxy certificate as text

        **GET** /proxy/metadata?<options> -- retrieve proxy metadata..
    """
    voms = self.get_argument('voms', None)
    try:
      proxyLifeTime = int(self.get_argument('lifetime', 3600 * 12))
    except Exception:
      raise S_ERROR('Cannot read "lifetime" argument.')

    # GET
    if self.request.method == 'GET':
      # # Return content of Proxy DB
      # if 'metadata' in optns:
      #   pass

      # Return personal proxy
      if not user and not group:
        result = self.proxyCli.downloadPersonalProxy(self.getUserName(), self.getUserGroup(),
                                                     requiredTimeLeft=proxyLifeTime, voms=voms)
        if result['OK']:
          self.log.notice('Proxy was created.')
          result = result['Value'].dumpAllToString()
        return self._raiseDIRACError(result)

      # Return proxy
      elif user and group:

        # Get proxy to string
        result = getDNForUsernameInGroup(user, group)
        if not result['OK'] or not result.get('Value'):
          raise '%s@%s has no registred DN: %s' % (user, group, result.get('Message') or "")

        if voms:
          result = self.proxyCli.downloadVOMSProxy(user, group, requiredTimeLeft=proxyLifeTime)
        else:
          result = self.proxyCli.downloadProxy(user, group, requiredTimeLeft=proxyLifeTime)
        if result['OK']:
          self.log.notice('Proxy was created.')
          result = result['Value'].dumpAllToString()
        return self._raiseDIRACError(result)

      else:
        raise "Wrone request."
