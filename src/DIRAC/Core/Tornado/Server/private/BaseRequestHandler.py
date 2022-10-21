"""DIRAC server has various passive components listening to incoming client requests
   and reacting accordingly by serving requested information,
   such as **services** or **APIs**.

   This module is basic for each of these components and describes the basic concept of access to them.
"""
import time
import inspect
import threading
from datetime import datetime

from http import HTTPStatus
from urllib.parse import unquote
from functools import partial

import jwt
import tornado
from tornado.web import RequestHandler, HTTPError
from tornado.ioloop import IOLoop

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.IdProvider.Utilities import getProvidersForInstance
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory


def set_attribute(attr, val):
    """Decorator to determine target method settings. Set method attribute.

    Usage::

        @set_attribute('my attribure', 'value')
        def export_myMethod(self):
            pass
    """

    def Inner(func):
        setattr(func, attr, val)
        return func

    return Inner


authentication = partial(set_attribute, "authentication")
authentication.__doc__ = """
Decorator to determine authentication types

Usage::

    @authentication(["SSL", "VISITOR"])
    def export_myMethod(self):
        pass
"""

authorization = partial(set_attribute, "authorization")
authorization.__doc__ = """
Decorator to determine authorization requirements

Usage::

    @authorization(["authenticated"])
    def export_myMethod(self):
        pass
"""


class TornadoResponse:
    """:py:class:`~BaseRequestHandler` uses multithreading to process requests, so the logic you describe in the target method
    in the handler that inherit ``BaseRequestHandler`` will be called in a non-main thread.

    Tornado warns that "methods on RequestHandler and elsewhere in Tornado are not thread-safe"
    https://www.tornadoweb.org/en/stable/web.html#thread-safety-notes.

    This class registers tornado methods with arguments in the same order they are called
    from ``TornadoResponse`` instance to call them later in the main thread and can be useful
    if you are afraid to use Tornado methods in a non-main thread due to a warning from Tornado.

    This is used in exceptional cases, in most cases it is not required, just use ``return S_OK(data)`` instead.

    Usage example::

        class MyHandler(BaseRequestHandlerChildHandler):

            def export_myTargetMethod(self):
                # Here we want to use the tornado method, but we want to do it in the main thread.
                # Let's create an TornadoResponse instance and
                # call the tornado methods we need in the order in which we want them to run in the main thread.
                resp = TornadoResponse('data')
                resp.set_header("Content-Type", "application/x-tar")
                # And finally, for example, redirect to another place
                return resp.redirect('https://my_another_server/redirect_endpoint')

    """

    # Let's see what methods RequestHandler has
    __attrs = inspect.getmembers(RequestHandler)

    def __init__(self, payload=None, status_code=None):
        """C'or

        :param payload: response body
        :param int status_code: response status code
        """
        self.payload = payload
        self.status_code = status_code
        self.actions = []  # a list of registered actions to perform in the main thread
        for mName, mObj in self.__attrs:
            # Let's make sure that this is the usual RequestHandler method
            if inspect.isroutine(mObj) and not mName.startswith("_") and not mName.startswith("get"):
                setattr(self, mName, partial(self.__setAction, mName))

    def __setAction(self, methodName, *args, **kwargs):
        """Register new action

        :param str methodName: ``RequestHandler`` method name

        :return: ``TornadoResponse`` instance
        """
        self.actions.append((methodName, args, kwargs))
        # Let's return the instance of the class so that it can be returned immediately. For example:
        # resp = TornadoResponse('data')
        # return resp.redirect('https://server')
        return self

    def _runActions(self, reqObj):
        """This method is executed after returning to the main thread.
        Look the :py:meth:`__execute` method.

        :param reqObj: ``RequestHandler`` instance
        """
        # Assign a status code if it has been transmitted.
        if self.status_code:
            reqObj.set_status(self.status_code)
        for mName, args, kwargs in self.actions:
            getattr(reqObj, mName)(*args, **kwargs)
        # Will we check if the finish method has already been called.
        if not reqObj._finished:
            # if not what are we waiting for?
            reqObj.finish(self.payload)


class BaseRequestHandler(RequestHandler):
    """This class primarily describes the process of processing an incoming request
    and the methods of authentication and authorization.

    Each HTTP request is served by a new instance of this class.

    For the sequence of method called, please refer to
    the `tornado documentation <https://www.tornadoweb.org/en/stable/guide/structure.html>`_.

    In order to pass information around and keep some states, we use instance attributes.
    These are initialized in the :py:meth:`.initialize` method.

    This class is basic for :py:class:`TornadoService <DIRAC.Core.Tornado.Server.TornadoService.TornadoService>`
    and :py:class:`TornadoREST <DIRAC.Core.Tornado.Server.TornadoREST.TornadoREST>`.
    Check them out, this is a good example of writing a new child class if needed.

    .. digraph:: structure
        :align: center

        node [shape=plaintext]
        RequestHandler [label="tornado.web.RequestHandler"];

        {TornadoService, TornadoREST} -> BaseRequestHandler;
        BaseRequestHandler -> RequestHandler [label="  inherit", fontsize=8];

    In order to create a class that inherits from ``BaseRequestHandler``,
    first you need to determine what HTTP methods need to be supported.
    Override the class variable ``SUPPORTED_METHODS`` by writing down the necessary methods there.
    Note that by default all HTTP methods are supported.

    It is important to understand that the handler belongs to the system.
    The class variable ``SYSTEM_NAME`` displays the system name. By default it is taken from the module name.
    This value is used to generate the full component name, see :py:meth:`_getFullComponentName` method

    This class also defines some variables for writing your handler's methods:

        - ``DEFAULT_AUTHORIZATION`` describes the general authorization rules for the entire handler
        - ``auth_<method name>`` describes authorization rules for a single method and has higher priority than ``DEFAULT_AUTHORIZATION``
        - ``METHOD_PREFIX`` helps in finding the target method, see the :py:meth:`_getMethod` methods, where described how exactly.

    It is worth noting that DIRAC supports several ways to authorize
    the request and they are all descriptive in ``DEFAULT_AUTHENTICATION``.
    Authorization schema is associated with ``_authz<SHEMA SHORT NAME>``
    method and will be applied alternately as they are defined in the variable until one of them is successfully executed.
    If no authorization method completes successfully, access will be denied.
    The following authorization schemas are supported by default:

        - ``SSL`` (:py:meth:`_authzSSL`) - reads the X509 certificate sent with the request
        - ``JWT`` (:py:meth:`_authzJWT`) - reads the Bearer Access Token sent with the request
        - ``VISITOR`` (:py:meth:`_authzVISITOR`) - authentication as visitor

    Also, if necessary, you can create a new type of authorization by simply creating the appropriate method::

        def _authzMYAUTH(self):
            '''Another authorization algoritm.'''
            # Do somthing
            return S_OK(credentials)  # return user credentials as a dictionary

    The name of the component to monitor the developer can specify in the ``MONITORING_COMPONENT`` class variable,
    see :py:class:`MonitoringClient <DIRAC.FrameworkSystem.Client.MonitoringClient.MonitoringClient>` class for more details.

    Review the class variables, explanatory comments. You are free to overwrite class variables to suit your needs.

    The class contains methods that require implementation:

        - :py:meth:`_pre_initialize`
        - :py:meth:`_getCSAuthorizarionSection`
        - :py:meth:`_getMethod`
        - :py:meth:`_getMethodArgs`

    Some methods have basic behavior, but developers can rewrite them:

        - :py:meth:`_getFullComponentName`
        - :py:meth:`_getComponentInfoDict`
        - :py:meth:`_monitorRequest`

    Designed for overwriting in the final handler if necessary:

        - :py:meth:`initializeHandler`
        - :py:meth:`initializeRequest`

    .. warning:: Do not change methods derived from ``tornado.web.RequestHandler``, e.g.: initialize, prepare, get, post, etc.


    Let's analyze the incoming request processing algorithm.

    .. image:: /_static/Systems/Core/BaseRequestHandler.png
        :alt: https://dirac.readthedocs.io/en/integration/_images/BaseRequestHandler.png (source https://github.com/TaykYoku/DIRACIMGS/raw/main/BaseRequestHandler.svg)

    But before the handler can accept requests, you need to start :py:mod:`TornadoServer <DIRAC.Core.Tornado.Server.TornadoServer>`.
    At startup, :py:class:`HandlerManager <DIRAC.Core.Tornado.Server.HandlerManager.HandlerManager>` call :py:meth:`__pre_initialize`
    handler method that inspects the handler and its methods to generate tornados URLs of access to it:

        - specifies the full name of the component, including the name of the system to which it belongs, see :py:meth:`_getFullComponentName`.
        - initialization of the main authorization class, see :py:class:`AuthManager <DIRAC.Core.DISET.AuthManager.AuthManager>` for more details.
        - call :py:meth:`__pre_initialize` that should explore the handler, prepare all the necessary attributes and most importantly - return the list of URL tornadoes

    The first request starts the process of initializing the handler, see the :py:meth:`initialize` method:

        - load all registered identity providers for authentication with access token, see :py:meth:`__loadIdPs`.
        - create a ``cls.log`` logger that should be used in the children classes instead of directly ``gLogger`` (this allows to carry the ``tornadoComponent`` information, crutial for centralized logging)
        - initialization of the monitoring specific to this handler, see :py:meth:`__initMonitoring`.
        - initialization of the target handler that inherit this one, see :py:meth:`initializeHandler`.

    Next, first of all the tornados prepare method is called which does the following:

        - determines determines the name of the target method and checks its presence, see :py:meth:`_getMethod`.
        - request monitoring, see :py:meth:`_monitorRequest`.
        - authentication request using one of the available algorithms called ``DEFAULT_AUTHENTICATION``, see :py:meth:`_gatherPeerCredentials` for more details.
        - and finally authorizing the request to access the component, see :py:meth:`authQuery <DIRAC.Core.DISET.AuthManager.AuthManager.authQuery>` for more details.

    If all goes well, then a method is executed,
    the name of which coincides with the name of the request method (e.g.: :py:meth:`get`) which does:

        - execute the target method in an executor a separate thread.
        - defines the arguments of the target method, see :py:meth:`_getMethodArgs`.
        - initialization of the each request, see :py:meth:`initializeRequest`.
        - the result of the target method is processed in the main thread and returned to the client, see :py:meth:`__execute`.

    """

    # Because we initialize at first request, we use a flag to know if it's already done
    __init_done = False
    # Lock to make sure that two threads are not initializing at the same time
    __init_lock = threading.RLock()

    # Definition of identity providers, used to authorize requests with access tokens
    _idps = IdProviderFactory()
    _idp = {}

    # The variable that will contain the result of the request, see __execute method
    __result = None

    # Below are variables that the developer can OVERWRITE as needed

    # System name with which this component is associated.
    # Developer can overwrite this
    # if your handler is outside the DIRAC system package (src/DIRAC/XXXSystem/<path to your handler>)
    SYSTEM_NAME = None
    COMPONENT_NAME = None

    # Base system URL. If defined, it is added as a prefix to the handler generated.
    BASE_URL = None

    # Base handler URL
    DEFAULT_LOCATION = "/"

    # Prefix of the target methods names if need to use a special prefix. By default its "export_".
    METHOD_PREFIX = "export_"

    # What authorization type to use.
    # This definition refers to the type of authentication,
    # ie which algorithm will be used to verify the incoming request and obtain user credentials.
    # These algorithms will be applied in the same order as in the list.
    #  SSL - add to list to enable certificate reading
    #  JWT - add to list to enable reading Bearer token
    #  VISITOR - add to list to enable authentication as visitor, that is, without verification
    DEFAULT_AUTHENTICATION = ["SSL", "JWT"]

    # Authorization requirements, properties that applied by default to all handler methods, if defined.
    # Note that `auth_methodName` will have a higher priority.
    DEFAULT_AUTHORIZATION = None

    # This will be overridden in __initialize to be handler specific
    log = gLogger.getSubLogger(__name__.split(".")[-1])

    # This defines a static method to encode and decode results
    # By default JEncode is used, but encode/decode can be overriden
    encode = staticmethod(encode)
    decode = staticmethod(decode)

    @classmethod
    def __pre_initialize(cls) -> list:
        """This method is run by the Tornado server to prepare the handler for launch,
        see :py:class:`HandlerManager <DIRAC.Core.Tornado.Server.HandlerManager.HandlerManager>`

        :returns: a list of URL (not the string with "https://..." but the tornado object)
                  see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
        """
        # Set full component name, e.g.: <System>/<Component>
        cls._fullComponentName = cls._getFullComponentName()

        # Define base request path
        if not cls.DEFAULT_LOCATION:
            # By default use full component name as location
            cls.DEFAULT_LOCATION = cls._fullComponentName

        # SUPPORTED_METHODS should be a list type
        if isinstance(cls.SUPPORTED_METHODS, str):
            cls.SUPPORTED_METHODS = (cls.SUPPORTED_METHODS,)

        # authorization manager initialization
        cls._authManager = AuthManager(cls._getCSAuthorizarionSection(cls._fullComponentName))

        if not (urls := cls._pre_initialize()):
            cls.log.warn("no target method found", f"{cls.__name__}")
        return urls

    @classmethod
    def _pre_initialize(cls) -> list:
        """This method is run by the Tornado server to prepare the handler for launch,
        see :py:meth:`__pre_initialize`.

        In this method you have to analyze the handler,
        its methods and return the URL list for the Tornado server.

        Preinitialization is called only once!

        Usage:

            class MyBaseClass(BaseRequestHandler):

                @classmethod
                def _pre_initialize(cls) -> list:
                    # Expected URL path: ``/<System>/<Component>``
                    return [TornadoURL(f"/{cls._fullComponentName.strip('/')}", cls)]

        :returns: a list of URL (not the string with "https://..." but the tornado object)
                  see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
        """
        raise NotImplementedError("Please, create the _pre_initialize class method")

    @classmethod
    def __initMonitoring(cls, fullComponentName: str, fullUrl: str) -> dict:
        """
        Initialize the monitoring specific to this handler
        This has to be called only by :py:meth:`.__initialize`
        to ensure thread safety and unicity of the call.

        :param componentName: relative URL ``/<System>/<Component>``
        :param fullUrl: full URl like ``https://<host>:<port>/<System>/<Component>``
        """
        cls._stats = {"requests": 0, "monitorLastStatsUpdate": time.time()}

        return S_OK()

    @classmethod
    def _getFullComponentName(cls) -> str:
        """Search the full name of the component, including the name of the system to which it belongs.
        CAN be implemented by developer.
        """
        if cls.SYSTEM_NAME is None:
            # If the system name is not specified, it is taken from the module.
            cls.SYSTEM_NAME = ([m[:-6] for m in cls.__module__.split(".") if m.endswith("System")] or [None]).pop()
        if cls.COMPONENT_NAME is None:
            # If the service name is not specified, it is taken from the handler.
            cls.COMPONENT_NAME = cls.__name__[: -len("Handler")]
        return f"{cls.SYSTEM_NAME}/{cls.COMPONENT_NAME}" if cls.SYSTEM_NAME else cls.COMPONENT_NAME

    @classmethod
    def __loadIdPs(cls) -> None:
        """Load identity providers that will be used to verify tokens"""
        cls.log.debug("Load identity providers..")
        # Research Identity Providers
        result = getProvidersForInstance("Id")
        if result["OK"]:
            for providerName in result["Value"]:
                result = cls._idps.getIdProvider(providerName)
                if result["OK"]:
                    cls._idp[result["Value"].issuer.strip("/")] = result["Value"]
                else:
                    cls.log.error("Error getting IDP", "{}: {}".format(providerName, result["Message"]))

    @classmethod
    def _getCSAuthorizarionSection(cls, fullComponentName: str) -> str:
        """Search component authorization section in CS.
        SHOULD be implemented by developer.

        :param fullComponentName: full component name, see :py:meth:`_getFullComponentName`
        """
        raise NotImplementedError("Please, create the _getCSAuthorizarionSection class method")

    @classmethod
    def _getComponentInfoDict(cls, fullComponentName: str, fullURL: str) -> dict:
        """Fills the dictionary with information about the current component,
        e.g.: 'serviceName', 'serviceSectionPath', 'csPaths'.
        SHOULD be implemented by developer.

        :param fullComponentName: full component name, see :py:meth:`_getFullComponentName`
        :param fullURL: incoming request path
        """
        raise NotImplementedError("Please, create the _getComponentInfoDict class method")

    @classmethod
    def __initialize(cls, request):
        """
        Initialize a component.
        The work is only performed once at the first request.

        :param object request: incoming request, :py:class:`tornado.httputil.HTTPServerRequest`

        :returns: S_OK
        """
        # If the initialization was already done successfuly,
        # we can just return
        if cls.__init_done:
            return S_OK()

        # Otherwise, do the work but with a lock
        with cls.__init_lock:

            # Check again that the initialization was not done by another thread
            # while we were waiting for the lock
            if cls.__init_done:
                return S_OK()

            cls.log = gLogger.getSubLogger(cls.__name__)
            cls.log._setOption("tornadoComponent", cls._fullComponentName)

            # Load all registered identity providers
            cls.__loadIdPs()

            # absoluteUrl: full URL e.g. ``https://<host>:<port>/<System>/<Component>``
            absoluteUrl = request.full_url()

            # The time at which the handler was initialized
            cls._startTime = datetime.utcnow()
            cls.log.info("Initializing method for first use", f"{cls._fullComponentName}, initializing..")

            # component monitoring initialization
            cls.__initMonitoring(cls._fullComponentName, absoluteUrl)

            cls._componentInfoDict = cls._getComponentInfoDict(cls._fullComponentName, absoluteUrl)

            cls.initializeHandler(cls._componentInfoDict)

            cls.__init_done = True

            return S_OK()

    @classmethod
    def initializeHandler(cls, componentInfo: dict):
        """This method for handler initializaion. This method is called only one time,
        at the first request. CAN be implemented by developer.

        :param componentInfo: infos about component, see :py:meth:`_getComponentInfoDict`.
        """
        pass

    def initializeRequest(self):
        """Called at every request, may be overwritten in your handler.
        CAN be implemented by developer.
        """
        pass

    # This is a Tornado magic method
    def initialize(self, **kwargs):  # pylint: disable=arguments-differ
        """
        Initialize the handler, called at every request.

        It just calls :py:meth:`.__initialize`

        If anything goes wrong, the client will get ``Connection aborted``
        error. See details inside the method.

        ..warning::
          DO NOT REWRITE THIS FUNCTION IN YOUR HANDLER
          ==> initialize in DISET became initializeRequest in HTTPS !
        """

        self._init_kwargs = kwargs
        # Only initialized once
        if not self.__init_done:
            # Ideally, if something goes wrong, we would like to return a Server Error 500
            # but this method cannot write back to the client as per the
            # `tornado doc <https://www.tornadoweb.org/en/stable/guide/structure.html#overriding-requesthandler-methods>`_.
            # So the client will get a ``Connection aborted```
            try:
                res = self.__initialize(self.request)
                if not res["OK"]:
                    raise Exception(res["Message"])
            except Exception as e:
                self.log.error("Error in initialization", repr(e))
                raise

    def _monitorRequest(self) -> None:
        """Monitor action for each request.
        CAN be implemented by developer.
        """
        self._stats["requests"] += 1

    def _getMethod(self):
        """Parse method name from incoming request.
        Based on this name, the target method to run will be determined.
        SHOULD be implemented by developer.
        """
        raise NotImplementedError("Please, create the _getMethod method")

    def _getMethodArgs(self, args: tuple, kwargs: dict):
        """Decode target method arguments from incoming request.
        SHOULD be implemented by developer.

        :param args: arguments comming to :py:meth:`get` and other HTTP methods.

        :return: (list, dict) -- tuple contain args and kwargs
        """
        raise NotImplementedError("Please, create the _getMethodArgs method")

    def __getMethodAuthProps(self) -> list:
        """Resolves the hard coded authorization requirements for the method.
        CAN be implemented by developer.

        There are two ways to define authorization requirements for the target method:
        Use auth_< method name > class value or use `AUTH` decorator:

            @authorization(['authenticated'])
            def export_myMethod(self):
                # Do something

        If this is not explicitly specified for the target method, the default value will be taken
        from `DEFAULT_AUTHORIZATION` class value.

        List of required :mod:`Properties <DIRAC.Core.Security.Properties>`.
        """
        # Convert default authorization requirements to list
        if self.DEFAULT_AUTHORIZATION and not isinstance(self.DEFAULT_AUTHORIZATION, (list, tuple)):
            self.DEFAULT_AUTHORIZATION = [p.strip() for p in self.DEFAULT_AUTHORIZATION.split(",") if p.strip()]

        # Define target method authorization requirements
        return getattr(
            self, "auth_" + self.__methodName, getattr(self.methodObj, "authorization", self.DEFAULT_AUTHORIZATION)
        )

    async def prepare(self):
        """Tornados prepare method that called before request"""
        ioloop = IOLoop.current()
        # Register activities "Fire and forget"
        ioloop.run_in_executor(None, self._monitorRequest)
        await ioloop.run_in_executor(None, self.__prepare)

    def __prepare(self):
        """Prepare the request. It reads certificates or tokens and check authorizations.
        We make the assumption that there is always going to be a ``method`` argument
        regardless of the HTTP method used
        """
        # Define the target method
        if not (method := self._getMethod()):
            self.log.error("The appropriate method could not be found.")
            raise HTTPError(status_code=HTTPStatus.BAD_REQUEST)

        if isinstance(method, str):
            methodName = method
            self.methodObj = getattr(self, method, None)
        else:
            self.methodObj = method
            methodName = method.__name__

        if not callable(self.methodObj):
            self.log.error("Invalid method", methodName)
            raise HTTPError(status_code=HTTPStatus.NOT_IMPLEMENTED)

        # Get target method core name
        self.__methodName = methodName[methodName.find("_") + 1 :]

        try:
            self.credDict = self._gatherPeerCredentials()
        except Exception as e:  # pylint: disable=broad-except
            # If an error occur when reading certificates we close connection
            # It can be strange but the RFC, for HTTP, say's that when error happend
            # before authentication we return 401 UNAUTHORIZED instead of 403 FORBIDDEN
            self.log.exception(e)
            self.log.error("Error gathering credentials ", f"{self.getRemoteAddress()}; path {self.request.path}")
            raise HTTPError(HTTPStatus.UNAUTHORIZED, str(e))

        # Check whether we are authorized to perform the query
        # Note that performing the authQuery modifies the credDict...
        authorized = self._authManager.authQuery(self.__methodName, self.credDict, self.__getMethodAuthProps())
        if not authorized:
            extraInfo = ""
            if self.credDict.get("ID"):
                extraInfo += "ID: %s" % self.credDict["ID"]
            elif self.credDict.get("DN"):
                extraInfo += "DN: %s" % self.credDict["DN"]
            self.log.error(
                "Unauthorized access",
                f"Identity {self.srv_getFormattedRemoteCredentials()}; path {self.request.path}; {extraInfo}",
            )
            raise HTTPError(HTTPStatus.UNAUTHORIZED)

    def _executeMethod(self, args: list, kwargs: dict):
        """Execute the requested method.

        This method is guaranteed to be called in a dedicated thread so thread
        locals can be safely used.

        We have several try except to catch the different problem which can occur

        - First, the method does not exist => Attribute error, return an error to client
        - second, anything happened during execution => General Exception, send error to client

        .. warning:: This method is called in an executor, and so cannot use methods like self.write, see :py:class:`TornadoResponse`.

        :param args: target method arguments
        :param kwargs: target method keyword arguments
        """
        args, kwargs = self._getMethodArgs(args, kwargs)

        credentials = self.srv_getFormattedRemoteCredentials()
        self.log.notice("Incoming request", f"{credentials} {self._fullComponentName}: {self.__methodName}")
        try:
            self.initializeRequest()
            return self.methodObj(*args, **kwargs)
        except Exception as e:  # pylint: disable=broad-except
            self.log.exception("Exception serving request", f"{e}:{e!r}")
            if isinstance(e, HTTPError):
                raise
            raise HTTPError(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))

    def on_finish(self):
        """
        Called after the end of HTTP request.
        Log the request duration
        """
        elapsedTime = 1000.0 * self.request.request_time()
        credentials = self.srv_getFormattedRemoteCredentials()

        argsString = f"OK {self._status_code}"
        # Finish with DIRAC result
        if isReturnStructure(self.__result):
            if self.__result["OK"]:
                argsString = "OK"
            else:
                argsString = f"ERROR: {self.__result['Message']}"
                if callStack := self.__result.pop("CallStack", None):
                    argsString += "\n" + "".join(callStack)
        # If bad HTTP status code
        if self._status_code >= 400:
            argsString = f"ERROR {self._status_code}: {self._reason}"

        self.log.notice(
            "Returning response", f"{credentials} {self._fullComponentName} ({elapsedTime:.2f} ms) {argsString}"
        )

    def _gatherPeerCredentials(self, grants: list = None) -> dict:
        """Return a dictionary designed to work with the :py:class:`AuthManager <DIRAC.Core.DISET.AuthManager.AuthManager>`,
        already written for DISET and re-used for HTTPS.

        This method attempts to authenticate the request by using the authentication types defined in ``DEFAULT_AUTHENTICATION``.

        The following types of authentication are currently available:

          - certificate reading, see :py:meth:`_authzSSL`.
          - reading Bearer token, see :py:meth`_authzJWT`.
          - authentication as visitor, that is, without verification, see :py:meth`_authzVISITOR`.

        To add your own authentication type, create a `_authzYourGrantType` method that should return ``S_OK(dict)``
        in case of successful authorization.

        :param grants: grants to use

        :returns: a dict containing user credentials
        """
        err = []

        # At least some authorization method must be defined, if nothing is defined,
        # the authorization will go through the `_authzVISITOR` method and
        # everyone will have access as anonymous@visitor
        for grant in grants or self.DEFAULT_AUTHENTICATION or "VISITOR":
            grant = grant.upper()
            grantFunc = getattr(self, "_authz%s" % grant, None)
            # pylint: disable=not-callable
            result = grantFunc() if callable(grantFunc) else S_ERROR("%s authentication type is not supported." % grant)
            if result["OK"]:
                for e in err:
                    self.log.debug(e)
                self.log.debug("%s authentication success." % grant)
                return result["Value"]
            err.append("{} authentication: {}".format(grant, result["Message"]))

        # Report on failed authentication attempts
        raise Exception("; ".join(err))

    def _authzSSL(self):
        """Load client certchain in DIRAC and extract information.

        :return: S_OK(dict)/S_ERROR()
        """
        try:
            derCert = self.request.get_ssl_certificate()
        except Exception:
            # If 'IOStream' object has no attribute 'get_ssl_certificate'
            derCert = None

        # Boolean whether we are behind a balancer and can trust headers
        balancer = gConfig.getValue("/WebApp/Balancer", "none") != "none"

        # Get client certificate as pem
        if derCert:
            chainAsText = derCert.as_pem().decode("ascii")
            # Read all certificate chain
            chainAsText += "".join([cert.as_pem().decode("ascii") for cert in self.request.get_ssl_certificate_chain()])
        elif balancer:
            if self.request.headers.get("X-Ssl_client_verify") == "SUCCESS" and self.request.headers.get("X-SSL-CERT"):
                chainAsText = unquote(self.request.headers.get("X-SSL-CERT"))
        else:
            return S_ERROR(DErrno.ECERTFIND, "Valid certificate not found.")

        # Load full certificate chain
        peerChain = X509Chain()
        peerChain.loadChainFromString(chainAsText)

        # Retrieve the credentials
        res = peerChain.getCredentials(withRegistryInfo=False)
        if not res["OK"]:
            return res

        credDict = res["Value"]

        credDict["x509Chain"] = peerChain
        res = peerChain.isProxy()
        if not res["OK"]:
            return res
        credDict["isProxy"] = res["Value"]

        if credDict["isProxy"]:
            credDict["DN"] = credDict["identity"]
        else:
            credDict["DN"] = credDict["subject"]

        res = peerChain.isLimitedProxy()
        if not res["OK"]:
            return res
        credDict["isLimitedProxy"] = res["Value"]

        # We check if client sends extra credentials...
        if "extraCredentials" in self.request.arguments:
            extraCred = self.get_argument("extraCredentials")
            if extraCred:
                credDict["extraCredentials"] = self.decode(extraCred)[0]
        return S_OK(credDict)

    def _authzJWT(self, accessToken=None):
        """Load token claims in DIRAC and extract information.

        :param str accessToken: access_token

        :return: S_OK(dict)/S_ERROR()
        """
        if not accessToken:
            # Export token from headers
            token = self.request.headers.get("Authorization")
            if not token or len(token.split()) != 2:
                return S_ERROR(DErrno.EATOKENFIND, "Not found a bearer access token.")
            tokenType, accessToken = token.split()
            if tokenType.lower() != "bearer":
                return S_ERROR(DErrno.ETOKENTYPE, "Found a not bearer access token.")

        # Read token without verification to get issuer
        self.log.debug("Read issuer from access token", accessToken)
        issuer = jwt.decode(accessToken, leeway=300, options=dict(verify_signature=False, verify_aud=False))[
            "iss"
        ].strip("/")
        # Verify token
        self.log.debug("Verify access token")
        result = self._idp[issuer].verifyToken(accessToken)
        self.log.debug("Search user group")
        return self._idp[issuer].researchGroup(result["Value"], accessToken) if result["OK"] else result

    def _authzVISITOR(self):
        """Visitor access

        :return: S_OK(dict)
        """
        return S_OK({})

    def getUserDN(self):
        return self.credDict.get("DN", "")

    def getUserName(self):
        return self.credDict.get("username", "")

    def getUserGroup(self):
        return self.credDict.get("group", "")

    def getProperties(self):
        return self.credDict.get("properties", [])

    def isRegisteredUser(self):
        return self.credDict.get("username", "anonymous") != "anonymous" and self.credDict.get("group")

    @classmethod
    def srv_getCSOption(cls, optionName, defaultValue=False):
        """
        Get an option from the CS section of the services

        :return: Value for serviceSection/optionName in the CS being defaultValue the default
        """
        if optionName[0] == "/":
            return gConfig.getValue(optionName, defaultValue)
        for csPath in cls._componentInfoDict["csPaths"]:
            result = gConfig.getOption(
                "%s/%s"
                % (
                    csPath,
                    optionName,
                ),
                defaultValue,
            )
            if result["OK"]:
                return result["Value"]
        return defaultValue

    def getCSOption(self, optionName, defaultValue=False):
        """
        Just for keeping same public interface
        """
        return self.srv_getCSOption(optionName, defaultValue)

    def srv_getRemoteAddress(self):
        """
        Get the address of the remote peer.

        :return: Address of remote peer.
        """

        remote_ip = self.request.remote_ip
        # Although it would be trivial to add this attribute in _HTTPRequestContext,
        # Tornado won't release anymore 5.1 series, so go the hacky way
        try:
            remote_port = self.request.connection.stream.socket.getpeername()[1]
        except Exception:  # pylint: disable=broad-except
            remote_port = 0

        return (remote_ip, remote_port)

    def getRemoteAddress(self):
        """
        Just for keeping same public interface
        """
        return self.srv_getRemoteAddress()

    def srv_getRemoteCredentials(self):
        """
        Get the credentials of the remote peer.

        :return: Credentials dictionary of remote peer.
        """
        return self.credDict

    def getRemoteCredentials(self):
        """
        Get the credentials of the remote peer.

        :return: Credentials dictionary of remote peer.
        """
        return self.credDict

    def srv_getFormattedRemoteCredentials(self):
        """
        Return the DN of user

        Mostly copy paste from
        :py:meth:`DIRAC.Core.DISET.private.Transports.BaseTransport.BaseTransport.getFormattedCredentials`

        Note that the information will be complete only once the AuthManager was called
        """
        address = self.getRemoteAddress()
        peerId = ""
        # Depending on where this is call, it may be that credDict is not yet filled.
        # (reminder: AuthQuery fills part of it..)
        try:
            peerId = "[{}:{}]".format(self.credDict.get("group", "visitor"), self.credDict.get("username", "anonymous"))
        except (AttributeError, KeyError):
            pass

        if address[0].find(":") > -1:
            return f"([{address[0]}]:{address[1]}){peerId}"
        return f"({address[0]}:{address[1]}){peerId}"

    # Here we define all HTTP methods, but ONLY those defined in SUPPORTED_METHODS class variable will be used!!!
    async def __execute(self, *args, **kwargs):  # pylint: disable=arguments-differ
        # Execute the method in an executor (basically a separate thread)
        # Because of that, we cannot calls certain methods like `self.write`
        # in _executeMethod. This is because these methods are not threadsafe
        # https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
        # However, we can still rely on instance attributes to store what should
        # be sent back (reminder: there is an instance of this class created for each request)
        self.__result = await IOLoop.current().run_in_executor(None, partial(self._executeMethod, args, kwargs))

        # Strip the exception/callstack info from S_ERROR responses
        if isinstance(self.__result, dict):
            # ExecInfo comes from the exception
            if "ExecInfo" in self.__result:
                del self.__result["ExecInfo"]
            # CallStack comes from the S_ERROR construction
            if "CallStack" in self.__result:
                del self.__result["CallStack"]

        # Here it is safe to write back to the client, because we are not in a thread anymore
        if isinstance(self.__result, TornadoResponse):
            self.__result._runActions(self)

        # If you need to end the method using tornado methods, outside the thread,
        # you need to define the finish_<methodName> method.
        # This method will be started after _executeMethod is completed.
        elif callable(finishFunc := getattr(self, f"finish_{self.__methodName}", None)):
            finishFunc()

        # In case nothing is returned
        elif self.__result is None:
            self.finish()

        # If set to true, do not JEncode the return of the RPC call
        # This is basically only used for file download through
        # the 'streamToClient' method.
        elif self.get_argument("rawContent", default=False):
            # See 4.5.1 http://www.rfc-editor.org/rfc/rfc2046.txt
            self.set_header("Content-Type", "application/octet-stream")
            self.finish(self.__result)

        # Return simple text or html
        elif isinstance(self.__result, (str, bytes)):
            self.finish(self.__result)

        # JSON
        else:
            self.set_header("Content-Type", "application/json")
            self.finish(self.encode(self.__result))

    # Make a coroutine, see https://www.tornadoweb.org/en/branch5.1/guide/coroutines.html#coroutines for details
    async def get(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``GET`` requests.
        .. note:: all the arguments are already prepared in the :py:meth:`.prepare` method.
        """
        await self.__execute(*args, **kwargs)

    async def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``POST`` requests."""
        await self.__execute(*args, **kwargs)

    async def head(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``HEAD`` requests."""
        await self.__execute(*args, **kwargs)

    async def delete(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``DELETE`` requests."""
        await self.__execute(*args, **kwargs)

    async def patch(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``PATCH`` requests."""
        await self.__execute(*args, **kwargs)

    async def put(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``PUT`` requests."""
        await self.__execute(*args, **kwargs)

    async def options(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``OPTIONS`` requests."""
        await self.__execute(*args, **kwargs)
