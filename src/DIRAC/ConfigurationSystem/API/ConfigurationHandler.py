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

from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST

__RCSID__ = "$Id$"


class ConfigurationHandler(TornadoREST):
  AUTH_PROPS = "all"
  LOCATION = "/DIRAC"
  METHOD_PREFIX = 'web_'

  path_conf = ['([a-z]+)']
  def web_conf(self, key):
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

    path = self.get_argument('path', '/')

    version = self.get_argument('version', None)
    if version and (version or '0') >= gConfigurationData.getVersion():
      return ''
    if key == 'dump':
      return str(gConfigurationData.getRemoteCFG())
    elif key == 'option':
      return gConfig.getOption(path)
    elif key == 'dict':
      return gConfig.getOptionsDict(path)
    elif key == 'options':
      return gConfig.getOptions(path)
    elif key == 'sections':
      return gConfig.getSections(path)
    elif key == 'getGroupsStatusByUsername':
      return gProxyManager.getgetGroupsStatusByUsername(**dict(self.request.arguments))
    elif any([key == m and re.match('^[a-z][A-z]+', m) for m in dir(Registry)]) and self.isRegisteredUser():
      method = getattr(Registry, key)
      return method(**dict(self.request.arguments))
    else:
      return S_ERROR('%s request unsuported' % key)
