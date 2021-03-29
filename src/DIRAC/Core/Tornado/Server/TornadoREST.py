"""
TornadoService is the base class for your handlers.
It directly inherits from :py:class:`tornado.web.RequestHandler`
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import tornado.ioloop
from tornado import gen
from tornado.web import HTTPError
from tornado.ioloop import IOLoop
from six.moves import http_client

import DIRAC

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Tornado.Server.BaseRequestHandler import BaseRequestHandler

sLog = gLogger.getSubLogger(__name__)


class TornadoREST(BaseRequestHandler):  # pylint: disable=abstract-method
  USE_AUTHZ_GRANTS = ['SSL', 'JWT', 'VISITOR']
  METHOD_PREFIX = 'web_'
  LOCATION = '/'

  @classmethod
  def _getServiceName(cls, request):
    """ Define endpoint full name

        :param object request: tornado Request

        :return: str
    """
    if not cls.SYSTEM:
      raise Exception("System name of '%s' REST API must be defined." % cls.__name__)
    return "/".join([cls.SYSTEM, cls.__name__])

  @classmethod
  def _getServiceAuthSection(cls, endpointName):
    """ Search endpoint auth section.

        :param str endpointName: endpoint name

        :return: str
    """
    return "%s/Authorization" % PathFinder.getAPISection(endpointName)

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    method = self.request.path.replace(self.LOCATION, '').strip('/').split('/')[0]:
    if not method or not hasattr(cls, ''.join([cls.METHOD_PREFIX, method]):
      return 'index'

  @gen.coroutine
  def get(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """ Method to handle incoming ``GET`` requests.
        Logic copied from :py:func:`~DIRAC.Core.Tornado.Server.BaseRequestHandler.post`.
    """
    # Execute the method in an executor (basically a separate thread)
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)

    # retVal is :py:class:`tornado.concurrent.Future`
    self._finishFuture(retVal)

  def _raiseDIRACError(self, result):
    """ Parse DIRAC result to raise S_ERROR or return S_OK value

        :param object result: DIRAC result

        :return: Value if result is S_OK
    """
    # DIRAC errors convert to HTTP server error
    if not self.result['OK']:
      sLog.error(self.result['Message'])
      raise HTTPError(http_client.INTERNAL_SERVER_ERROR, self.result['Message'])
    return result['Value']
