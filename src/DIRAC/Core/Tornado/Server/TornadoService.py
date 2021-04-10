"""
TornadoService is the base class for your handlers.
It directly inherits from :py:class:`tornado.web.RequestHandler`
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open

import os
import time
import threading
from datetime import datetime
from six.moves import http_client

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Tornado.Server.BaseRequestHandler import BaseRequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient

sLog = gLogger.getSubLogger(__name__)


class TornadoService(BaseRequestHandler):  # pylint: disable=abstract-method
  """
    Base class for all the Handlers.
    It directly inherits from :py:class:`tornado.web.RequestHandler`

    Each HTTP request is served by a new instance of this class.

    For the sequence of method called, please refer to
    the `tornado documentation <https://www.tornadoweb.org/en/stable/guide/structure.html>`_.

    For compatibility with the existing :py:class:`DIRAC.Core.DISET.TransferClient.TransferClient`,
    the handler can define a method ``export_streamToClient``. This is the method that will be called
    whenever ``TransferClient.receiveFile`` is called. It is the equivalent of the DISET
    ``transfer_toClient``.
    Note that this is here only for compatibility, and we discourage using it for new purposes, as it is
    bound to disappear.

    The handler only define the ``post`` verb. Please refer to :py:meth:`.post` for the details.

    In order to create a handler for your service, it has to
    follow a certain skeleton::

      from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
      class yourServiceHandler(TornadoService):

        # Called only once when the first
        # request for this handler arrives
        # Useful for initializing DB or so.
        # You don't need to use super or to call any parents method, it's managed by the server
        @classmethod
        def initializeHandler(cls, infosDict):
          '''Called only once when the first
             request for this handler arrives
             Useful for initializing DB or so.
             You don't need to use super or to call any parents method, it's managed by the server
          '''
          pass


        def initializeRequest(self):
          '''
             Called at the beginning of each request
          '''
          pass

        # Specify the default permission for the method
        # See :py:class:`DIRAC.Core.DISET.AuthManager.AuthManager`
        auth_someMethod = ['authenticated']


        def export_someMethod(self):
          '''The method you want to export.
           It must start with ``export_``
           and it must return an S_OK/S_ERROR structure
          '''
          return S_ERROR()


        def export_streamToClient(self, myDataToSend, token):
          ''' Automatically called when ``Transfer.receiveFile`` is called.
              Contrary to the other ``export_`` methods, it does not need
              to return a DIRAC structure.
          '''

          # Do whatever with the token

          with open(myFileToSend, 'r') as fd:
            return fd.read()


    Note that because we inherit from :py:class:`tornado.web.RequestHandler`
    and we are running using executors, the methods you export cannot write
    back directly to the client. Please see inline comments for more details.

    In order to pass information around and keep some states, we use instance attributes.
    These are initialized in the :py:meth:`.initialize` method.

  """
  # Prefix of methods names
  METHOD_PREFIX = "export_"

  @classmethod
  def _getServiceName(cls, request):
    """ Search service name in request.

        :param object request: tornado Request

        :return: str
    """
    # Expected path: ``/<System>/<Component>``
    return request.path[1:]

  @classmethod
  def _getServiceInfo(cls, serviceName, request):
    """ Fill service information.

        :param str serviceName: service name
        :param object request: tornado Request

        :return: dict
    """
    return {'serviceName': serviceName,
            'serviceSectionPath': PathFinder.getServiceSection(serviceName),
            'csPaths': [PathFinder.getServiceSection(serviceName)],
            'URL': request.full_url()}

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    return self.get_argument("method")

  def _getMethodArgs(self, args):
    """ Decode args.

        :return: list
    """
    args_encoded = self.get_body_argument('args', default=encode([]))
    return decode(args_encoded)[0]
