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
from DIRAC.Core.Tornado.Web import Conf
from DIRAC.Core.Tornado.Server.BaseRequestHandler import BaseRequestHandler
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import ResourceProtector

sLog = gLogger.getSubLogger(__name__)


class TornadoREST(BaseRequestHandler):  # pylint: disable=abstract-method
  METHOD_PREFIX = 'web_'

  @classmethod
  def _getServiceName(cls, request):
    """ Search service name in request.

        :param object request: tornado Request

        :return: str
    """
    try:
      return cls.LOCATION.split('/')[-1].strip('/')
    except expression as identifier:
      return cls.__name__
  
  @classmethod
  def _getServiceAuthSection(cls, serviceName):
    """ Search service auth section.

        :param str serviceName: service name

        :return: str
    """
    return Conf.getAuthSectionForHandler(serviceName)

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    try:
      return self.request.path.split(self.LOCATION)[1].split('?')[0].strip('/').split('/')[0].strip('/')
    except Exception:
      return 'index'

  @gen.coroutine
  def get(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """
    """
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)

    # retVal is :py:class:`tornado.concurrent.Future`
    self._finishFuture(retVal)

  def _finishFuture(self, retVal):
    """ Handler Future result

        :param object retVal: tornado.concurrent.Future
    """
    result = retVal.result()
    try:
      if not result['OK']:
        raise HTTPError(http_client.INTERNAL_SERVER_ERROR)
      result = result['Value']
    except (AttributeError, KeyError, TypeError):
      pass
    super(TornadoREST, self)._finishFuture(result)
