"""
TornadoREST is the base class for your RESTful API handlers.
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
      raise Exception("System name must be defined.")
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
    print(self.request.path)
    method = self.request.path.replace(self.LOCATION, '', 1).strip('/').split('/')[0]
    print(method)
    if method and hasattr(self, ''.join([self.METHOD_PREFIX, method])):
      return method
    elif hasattr(self, '%sindex' % self.METHOD_PREFIX):
      gLogger.warn('%s method not implemented. Use the index method to handle this.' % method)
      return 'index'
    else:
      raise NotImplementedError('%s method not implemented. \
                                You can use the index method to handle this.' % method)

  @gen.coroutine
  def get(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """ Method to handle incoming ``GET`` requests.
        Logic copied from :py:func:`~DIRAC.Core.Tornado.Server.BaseRequestHandler.post`.
    """
    # Execute the method in an executor (basically a separate thread)
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)

    # retVal is :py:class:`tornado.concurrent.Future`
    self._finishFuture(retVal)
