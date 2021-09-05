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
from datetime import datetime

import tornado.ioloop
from tornado import gen
from tornado.ioloop import IOLoop

import DIRAC

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import BaseRequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder

sLog = gLogger.getSubLogger(__name__)


class TornadoService(BaseRequestHandler):  # pylint: disable=abstract-method
  """
    Base class for all the sevices handlers.
    It directly inherits from :py:class:`DIRAC.Core.Tornado.Server.BaseRequestHandler.BaseRequestHandler`

    Each HTTP request is served by a new instance of this class.

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

    The handler only define the ``post`` verb. Please refer to :py:meth:`.post` for the details.

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

  @classmethod
  def _getServiceAuthSection(cls, serviceName):
    """ Search service auth section.

        :param str serviceName: service name

        :return: str
    """
    return "%s/Authorization" % PathFinder.getServiceSection(serviceName)

  def _getMethodName(self):
    """ Parse method name.

        :return: str
    """
    return self.get_argument("method")

  def _getMethodArgs(self, args):
    """ Decode args.

        :return: tuple
    """
    args_encoded = self.get_body_argument('args', default=encode([]))
    return (decode(args_encoded)[0], {})

  # Make post a coroutine.
  # See https://www.tornadoweb.org/en/branch5.1/guide/coroutines.html#coroutines
  # for details
  @gen.coroutine
  def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
    """
      Method to handle incoming ``POST`` requests.
      Note that all the arguments are already prepared in the :py:meth:`.prepare`
      method.

      The ``POST`` arguments expected are:

      * ``method``: name of the method to call
      * ``args``: JSON encoded arguments for the method
      * ``extraCredentials``: (optional) Extra informations to authenticate client
      * ``rawContent``: (optionnal, default False) If set to True, return the raw output
        of the method called.

      If ``rawContent`` was requested by the client, the ``Content-Type``
      is ``application/octet-stream``, otherwise we set it to ``application/json``
      and JEncode retVal.

      If ``retVal`` is a dictionary that contains a ``Callstack`` item,
      it is removed, not to leak internal information.


      Example of call using ``requests``::

        In [20]: url = 'https://server:8443/DataManagement/TornadoFileCatalog'
          ...: cert = '/tmp/x509up_u1000'
          ...: kwargs = {'method':'whoami'}
          ...: caPath = '/home/dirac/ClientInstallDIR/etc/grid-security/certificates/'
          ...: with requests.post(url, data=kwargs, cert=cert, verify=caPath) as r:
          ...:     print r.json()
          ...:
        {u'OK': True,
            u'Value': {u'DN': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'group': u'dirac_user',
            u'identity': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'isLimitedProxy': False,
            u'isProxy': True,
            u'issuer': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch',
            u'properties': [u'NormalUser'],
            u'secondsLeft': 85441,
            u'subject': u'/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch/CN=2409820262',
            u'username': u'adminusername',
            u'validDN': False,
            u'validGroup': False}}
    """
    # Execute the method in an executor (basically a separate thread)
    # Because of that, we cannot calls certain methods like `self.write`
    # in _executeMethod. This is because these methods are not threadsafe
    # https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
    # However, we can still rely on instance attributes to store what should
    # be sent back (reminder: there is an instance
    # of this class created for each request)
    retVal = yield IOLoop.current().run_in_executor(*self._prepareExecutor(args))

    # retVal is :py:class:`tornado.concurrent.Future`
    self._finishFuture(retVal)

  auth_ping = ['all']

  def export_ping(self):
    """
      Default ping method, returns some info about server.

      It returns the exact same information as DISET, for transparency purpose.
    """
    # COPY FROM DIRAC.Core.DISET.RequestHandler
    dInfo = {}
    dInfo['version'] = DIRAC.version
    dInfo['time'] = datetime.utcnow()
    # Uptime
    try:
      with open("/proc/uptime", 'rt') as oFD:
        iUptime = int(float(oFD.readline().split()[0].strip()))
      dInfo['host uptime'] = iUptime
    except Exception:  # pylint: disable=broad-except
      pass
    startTime = self._startTime
    dInfo['service start time'] = self._startTime
    serviceUptime = datetime.utcnow() - startTime
    dInfo['service uptime'] = serviceUptime.days * 3600 + serviceUptime.seconds
    # Load average
    try:
      with open("/proc/loadavg", 'rt') as oFD:
        dInfo['load'] = " ".join(oFD.read().split()[:3])
    except Exception:  # pylint: disable=broad-except
      pass
    dInfo['name'] = self._serviceInfoDict['serviceName']
    stTimes = os.times()
    dInfo['cpu times'] = {'user time': stTimes[0],
                          'system time': stTimes[1],
                          'children user time': stTimes[2],
                          'children system time': stTimes[3],
                          'elapsed real time': stTimes[4]
                          }

    return S_OK(dInfo)

  auth_echo = ['all']

  @staticmethod
  def export_echo(data):
    """
    This method used for testing the performance of a service
    """
    return S_OK(data)

  auth_whoami = ['authenticated']

  def export_whoami(self):
    """
      A simple whoami, returns all credential dictionary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      # Not serializable
      del credDict['x509Chain']
    return S_OK(credDict)
