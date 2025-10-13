"""
TornadoService is the base class for your handlers.
It directly inherits from :py:class:`tornado.web.RequestHandler`
"""


import os
from datetime import datetime
from tornado.web import url as TornadoURL

import DIRAC

from DIRAC import gLogger, S_OK
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import BaseRequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder


class TornadoService(BaseRequestHandler):  # pylint: disable=abstract-method
    """
    Base class for all the sevices handlers.

    For compatibility with the existing :py:class:`DIRAC.Core.DISET.TransferClient.TransferClient`,
    the handler can define a method ``export_streamToClient``. This is the method that will be called
    whenever ``TransferClient.receiveFile`` is called. It is the equivalent of the DISET ``transfer_toClient``.
    Note that this is here only for compatibility, and we discourage using it for new purposes, as it is
    bound to disappear.

    In order to create a handler for your service, it has to follow a certain skeleton.

    .. code-block:: python


        from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
        class yourServiceHandler(TornadoService):

            @classmethod
            def initializeHandler(cls, infosDict):
                '''Called only once when the first request for this handler arrives.
                Useful for initializing DB or so.
                You don't need to use super or to call any parents method, it's managed by the server
                '''
                pass

            def initializeRequest(self):
                '''Called at the beginning of each request
                '''
                pass

            # Specify the default permission for the method
            # See :py:class:`DIRAC.Core.DISET.AuthManager.AuthManager`
            auth_someMethod = ['authenticated']

            def export_someMethod(self):
                '''The method you want to export. It must start with ``export_``
                and it must return an S_OK/S_ERROR structure
                '''
                return S_ERROR()

            def export_streamToClient(self, myDataToSend, token):
                ''' Automatically called when ``Transfer.receiveFile`` is called.
                Contrary to the other ``export_`` methods, it does not need to return a DIRAC structure.
                '''

                # Do whatever with the token

                with open(myFileToSend, 'r') as fd:
                    return fd.read()


    Note that because we inherit from :py:class:`tornado.web.RequestHandler`
    and we are running using executors, the methods you export cannot write
    back directly to the client. Please see inline comments in
    :py:class:`BaseRequestHandler <DIRAC.Core.Tornado.Server.private.BaseRequestHandler.BaseRequestHandler>` for more details.

    In order to pass information around and keep some states, we use instance attributes.
    These are initialized in the ``initialize`` methods.

    The handler only define the ``post`` verb. Please refer to :py:meth:`.post` for the details.

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

    # DIRAC services use RPC calls only with POST http method.
    SUPPORTED_METHODS = ("POST",)

    @classmethod
    def _pre_initialize(cls) -> list:
        """This method is run by the Tornado server to prepare the handler for launch.
        Preinitialization is called only once!

        :returns: a list of URL (not the string with "https://..." but the tornado object)
                  see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
        """
        # Expected path: ``/<System>/<Component>``
        cls._serviceName = cls._fullComponentName
        cls.log.verbose(f" - Route /{cls._serviceName.strip('/')} ->  {cls.__name__}")
        return [TornadoURL(f"/{cls._serviceName.strip('/')}", cls)]

    @classmethod
    def _getComponentInfoDict(cls, serviceName, fullURL):
        """Fill service information.

        :param str serviceName: service name, see :py:meth:`_getFullComponentName`
        :param str fullURL: incoming request path
        :return: dict
        """
        path = PathFinder.getServiceSection(serviceName)
        cls._serviceInfoDict = {
            "serviceName": serviceName,
            "serviceSectionPath": path,
            "csPaths": [path],
            "URL": fullURL,
        }
        return cls._serviceInfoDict

    @classmethod
    def _getCSAuthorizationSection(cls, serviceName):
        """Search service auth section.

        :param str serviceName: service name, see :py:meth:`_getFullComponentName`

        :return: str
        """
        return f"{PathFinder.getServiceSection(serviceName)}/Authorization"

    def _getMethod(self) -> str:
        """Get target function name"""
        # Get method object using prefix and method name from request
        return f"{self.METHOD_PREFIX}{self.get_argument('method')}"

    def _getMethodArgs(self, args: tuple, kwargs: dict) -> tuple:
        """Decode target function arguments."""
        args_encoded = self.get_body_argument("args", default=self.encode([]))
        return (self.decode(args_encoded)[0], {})

    auth_ping = ["all"]

    def export_ping(self):
        """
        Default ping method, returns some info about server.

        It returns the exact same information as DISET, for transparency purpose.
        """
        # COPY FROM DIRAC.Core.DISET.RequestHandler
        dInfo = {}
        dInfo["version"] = DIRAC.version
        dInfo["time"] = datetime.utcnow()
        # Uptime
        try:
            with open("/proc/uptime") as oFD:
                iUptime = int(float(oFD.readline().split()[0].strip()))
            dInfo["host uptime"] = iUptime
        except Exception:  # pylint: disable=broad-except
            pass
        startTime = self._startTime
        dInfo["service start time"] = self._startTime
        serviceUptime = datetime.utcnow() - startTime
        dInfo["service uptime"] = int(serviceUptime.total_seconds())
        # Load average
        try:
            with open("/proc/loadavg") as oFD:
                dInfo["load"] = " ".join(oFD.read().split()[:3])
        except Exception:  # pylint: disable=broad-except
            pass
        dInfo["name"] = self._serviceInfoDict["serviceName"]
        stTimes = os.times()
        dInfo["cpu times"] = {
            "user time": stTimes[0],
            "system time": stTimes[1],
            "children user time": stTimes[2],
            "children system time": stTimes[3],
            "elapsed real time": stTimes[4],
        }

        return S_OK(dInfo)

    auth_echo = ["all"]

    @staticmethod
    def export_echo(data):
        """
        This method used for testing the performance of a service
        """
        return S_OK(data)

    auth_whoami = ["authenticated"]

    def export_whoami(self):
        """
        A simple whoami, returns all credential dictionary, except certificate chain object.
        """
        credDict = self.srv_getRemoteCredentials()
        if "x509Chain" in credDict:
            # Not serializable
            del credDict["x509Chain"]
        return S_OK(credDict)
