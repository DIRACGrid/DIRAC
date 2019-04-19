""" HTTP API of the DIRAC configuration data
"""

__RCSID__ = "$Id$"

import json
import time
import tornado

from tornado import web, gen
from tornado.template import Template

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.HTTP.Lib.WebHandler import WebHandler, asyncGen


class ConfAPIHandler(WebHandler):
  OFF = False
  AUTH_PROPS = "all"
  LOCATION = "/configuration"

  @asyncGen
  def web_get(self):
    """ Method to getting some configuration information
    :return: requested data
    """
    gLogger.debug('Get CS request:\n %s' % self.request)
    args = self.request.arguments
    if args.get('option'):
      path = args['option'][0]
      result = yield self.threadTask(gConfig.getOption, path)
      if not result['OK']:
        raise tornado.web.HTTPError(404, result['Message'])
      self.finish(json.dumps(result['Value']))
    elif args.get('section'):
      path = args['section'][0]
      result = yield self.threadTask(gConfig.getOptionsDict, path)
      if not result['OK']:
        raise tornado.web.HTTPError(404, result['Message'])
      self.finish(json.dumps(result['Value']))
    elif args.get('options'):
      path = args['options'][0]
      result = yield self.threadTask(gConfig.getOptions, path)
      if not result['OK']:
        raise tornado.web.HTTPError(404, result['Message'])
      self.finish(json.dumps(result['Value']))
    elif args.get('sections'):
      path = args['sections'][0]
      result = yield self.threadTask(gConfig.getSections, path)
      if not result['OK']:
        raise tornado.web.HTTPError(404, result['Message'])
      self.finish(json.dumps(result['Value']))
    else:
      raise tornado.web.HTTPError(500, 'Invalid argument')

  @asyncGen
  def post(self):
    """ Post method """
    pass
    