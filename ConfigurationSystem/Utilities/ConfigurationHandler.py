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
    """ Configuration endpoint, used to:
          GET /conf/get?<options> -- get configuration information, with arguments
            * options:
              * fullCFG - to get dump of configuration
              * option - option path to get option value
              * options - section path to get list of options
              * section - section path to get dict of all options/values there
              * sections - section path to get list of sections there
              * version - version of configuration that request information(optional)

          GET /conf/<helper method>?<arguments> -- get some information by using helpers methods
            * helper method - helper method of configuration service
            * arguments - arguments specifecly for every helper method

        :return: json with requested data
    """
    self.log.notice('Request configuration information')
    optns = self.overpath.strip('/').split('/')
    if not optns or len(optns) > 1:
      raise WErr(404, "Wrone way")

    result = S_ERROR('%s request unsuported' % optns[0])
    if optns[0] == 'get':
      if 'version' in self.args and (self.args.get('version') or '0') >= gConfigurationData.getVersion():
        self.finish()

      result = {}
      if 'fullCFG' in self.args:
        remoteCFG = yield self.threadTask(gConfigurationData.getRemoteCFG)
        result['Value'] = str(remoteCFG)
      elif 'option' in self.args:
        result = yield self.threadTask(gConfig.getOption, self.args['option'])
      elif 'section' in self.args:
        result = yield self.threadTask(gConfig.getOptionsDict, self.args['section'])
      elif 'options' in self.args:
        result = yield self.threadTask(gConfig.getOptions, self.args['options'])
      elif 'sections' in self.args:
        result = yield self.threadTask(gConfig.getSections, self.args['sections'])
      else:
        raise WErr(500, 'Invalid argument')
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
