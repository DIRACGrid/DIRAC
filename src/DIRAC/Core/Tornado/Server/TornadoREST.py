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

from DIRAC import gLogger, S_OK
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Tornado.Server.BaseRequestHandler import BaseRequestHandler

sLog = gLogger.getSubLogger(__name__)


class TornadoREST(BaseRequestHandler):  # pylint: disable=abstract-method
  """ Base class for all the endpoints handlers.
      It directly inherits from :py:class:`DIRAC.Core.Tornado.Server.BaseRequestHandler.BaseRequestHandler`

      Each HTTP request is served by a new instance of this class.

      In order to create a handler for your service, it has to
      follow a certain skeleton::

        from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
        class yourEndpointHandler(TornadoREST):

          @classmethod
          def initializeHandler(cls, infosDict):
            ''' Called only once when the first request for this handler arrives Useful for initializing DB or so.
            '''
            pass

          def initializeRequest(self):
            ''' Called at the beginning of each request
            '''
            pass

          # Specify the path arguments
          path_someMethod = ['([A-z0-9-_]*)']

          # Specify the default permission for the method
          # See :py:class:`DIRAC.Core.DISET.AuthManager.AuthManager`
          auth_someMethod = ['authenticated']

          def web_someMethod(self, provider=None):
            ''' Your method
            '''
            return S_OK(provider)

      Note that because we inherit from :py:class:`tornado.web.RequestHandler`
      and we are running using executors, the methods you export cannot write
      back directly to the client. Please see inline comments for more details.

      In order to pass information around and keep some states, we use instance attributes.
      These are initialized in the :py:meth:`.initialize` method.

      The handler define the ``post`` and ``get`` verbs. Please refer to :py:meth:`.post` for the details.
  """

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
    method = self.request.path.replace(self.LOCATION, '', 1).strip('/').split('/')[0]
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
        Note that all the arguments are already prepared in the :py:meth:`.prepare` method.
    """
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)
    self._finishFuture(retVal)

  @gen.coroutine
  def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """ Method to handle incoming ``POST`` requests.
        Note that all the arguments are already prepared in the :py:meth:`.prepare` method.
    """
    retVal = yield IOLoop.current().run_in_executor(None, self._executeMethod, args)
    self._finishFuture(retVal)

  auth_echo = ['all']

  @staticmethod
  def web_echo(data):
    """
    This method used for testing the performance of a service
    """
    return S_OK(data)

  auth_whoami = ['authenticated']

  def web_whoami(self):
    """
      A simple whoami, returns all credential dictionary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      # Not serializable
      del credDict['x509Chain']
    return S_OK(credDict)
