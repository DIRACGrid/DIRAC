""" HTTP API of the DIRAC configuration data, rewrite from the RESTDIRAC project
"""
import re
import json

from tornado import web, gen
from tornado.template import Template

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Resources, Registry
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

from DIRAC.Core.Web.WebHandler import WebHandler, asyncGen, WErr

__RCSID__ = "$Id$"


class ConfigurationHandler(WebHandler):
  OVERPATH = True
  AUTH_PROPS = "all"
  LOCATION = "/"

  def initialize(self):
    super(ConfigurationHandler, self).initialize()
    self.args = {}
    for arg in self.request.arguments:
      if len(self.request.arguments[arg]) > 1:
        self.args[arg] = self.request.arguments[arg]
      else:
        self.args[arg] = self.request.arguments[arg][0] or ''
    return S_OK()

  @asyncGen
  def web_conf(self):
    """ REST endpoint for configuration system:

        **GET** /conf/<key>?<options> -- get configuration information

          Options:
            * *path* -- path in the configuration structure, by default it's "/". 
            * *version* -- the configuration version of the requester, if *version* is newer
                           than the one present on the server, an empty result will be returned 
        
          Response:
            +-----------+---------------------------------------+------------------------+
            | *key*     | Description                           | Type                   |
            +-----------+---------------------------------------+------------------------+
            | dump      | Current CFG()                         | encoded in json format |
            +-----------+---------------------------------------+------------------------+
            | option    | Option value                          | text                   |
            +-----------+---------------------------------------+------------------------+
            | options   | Options list in a section             | encoded in json format |
            +-----------+---------------------------------------+------------------------+
            | dict      | Options with values in a section      | encoded in json format |
            +-----------+---------------------------------------+------------------------+
            | sections  | Sections list in a section            | text                   |
            +-----------+---------------------------------------+------------------------+
    """
    self.log.notice('Request configuration information')
    optns = self.overpath.strip('/').split('/')
    path = self.args.get('path', '/')
    if not optns or len(optns) > 1:
      raise WErr(404, "You forgot to set attribute.")

    result = S_ERROR('%s request unsuported' % optns[0])
    if 'version' in self.args and (self.args.get('version') or '0') >= gConfigurationData.getVersion():
      self.finish()
    if optns[0] == 'dump':
      remoteCFG = yield self.threadTask(gConfigurationData.getRemoteCFG)
      result['Value'] = str(remoteCFG)
    elif optns[0] == 'option':
      result = yield self.threadTask(gConfig.getOption, path)
    elif optns[0] == 'dict':
      result = yield self.threadTask(gConfig.getOptionsDict, path)
    elif optns[0] == 'options':
      result = yield self.threadTask(gConfig.getOptions, path)
    elif optns[0] == 'sections':
      result = yield self.threadTask(gConfig.getSections, path)
    elif optns[0] == 'getGroupsStatusByUsername':
      result = yield self.threadTask(gProxyManager.getGroupsStatusByUsername, **self.args)
    elif any([optns[0] == m and re.match('^[a-z][A-z]+', m) for m in dir(Registry)]) and self.isRegisteredUser():
      result = yield self.threadTask(getattr(Registry, optns[0]), **self.args)
    else:
      raise WErr(500, '%s request unsuported' % optns[0])
      # result = yield self.threadTask(getattr(Registry, optns[0]), **self.args)

    if not result['OK']:
      raise WErr(404, result['Message'])
    self.finishJEncode(result['Value'])

  @asyncGen
  def post(self):
    """ Post method
    """
    pass
