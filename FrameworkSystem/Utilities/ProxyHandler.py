""" Handler to serve the DIRAC proxy data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import time
import base64

from tornado import web, gen
from tornado.template import Template

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Web.WebHandler import WebHandler, asyncGen, WErr
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsernameInGroup

__RCSID__ = "$Id$"


class ProxyHandler(WebHandler):
  OVERPATH = True
  AUTH_PROPS = "authenticated"
  LOCATION = "/"

  def initialize(self):
    super(ProxyHandler, self).initialize()
    self.args = {}
    for arg in self.request.arguments:
      if len(self.request.arguments[arg]) > 1:
        self.args[arg] = self.request.arguments[arg]
      else:
        self.args[arg] = self.request.arguments[arg][0] or ''
    return S_OK()

  @asyncGen
  def web_proxy(self):
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
    voms = self.args.get('voms')
    proxyLifeTime = 3600 * 12
    if re.match('[0-9]+', self.args.get('lifetime') or ''):
      proxyLifeTime = int(self.args.get('lifetime'))
    optns = self.overpath.strip('/').split('/')

    credGroup = self.getUserGroup()
    proxyCli = ProxyManagerClient(delegatedGroup=credGroup, delegatedID=self.getID(), delegatedDN=self.getDN())

    # GET
    if self.request.method == 'GET':
      # Return content of Proxy DB
      if 'metadata' in optns:
        pass

      # Return personal proxy
      elif not self.overpath:
        result = yield self.threadTask(proxyCli.downloadPersonalProxy, self.getUserName(),
                                       credGroup, requiredTimeLeft=proxyLifeTime, voms=voms)
        if not result['OK']:
          raise WErr(500, result['Message'])
        self.log.notice('Proxy was created.')
        result = result['Value'].dumpAllToString()
        if not result['OK']:
          raise WErr(500, result['Message'])
        self.finishJEncode(result['Value'])

      # Return proxy
      elif len(optns) == 2:
        user = optns[0]
        group = optns[1]

        # Get proxy to string
        result = getDNForUsernameInGroup(user, group)
        if not result['OK'] or not result.get('Value'):
          raise WErr(500, '%s@%s has no registred DN: %s' % (user, group, result.get('Message') or ""))

        if voms:
          result = yield self.threadTask(proxyCli.downloadVOMSProxy, user, group, requiredTimeLeft=proxyLifeTime)
        else:
          result = yield self.threadTask(proxyCli.downloadProxy, user, group, requiredTimeLeft=proxyLifeTime)
        if not result['OK']:
          raise WErr(500, result['Message'])
        self.log.notice('Proxy was created.')
        result = result['Value'].dumpAllToString()
        if not result['OK']:
          raise WErr(500, result['Message'])
        self.finishJEncode(result['Value'])

      else:
        raise WErr(404, "Wrone way")
