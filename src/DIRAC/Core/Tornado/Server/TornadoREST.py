"""
TornadoREST is the base class for your RESTful API handlers.
It directly inherits from :py:class:`tornado.web.RequestHandler`
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import inspect
from tornado import gen
from tornado.ioloop import IOLoop
from urllib.parse import unquote

from DIRAC import gLogger, S_OK
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import BaseRequestHandler

sLog = gLogger.getSubLogger(__name__)


class TornadoREST(BaseRequestHandler):  # pylint: disable=abstract-method
    """Base class for all the endpoints handlers.
    It directly inherits from :py:class:`DIRAC.Core.Tornado.Server.BaseRequestHandler.BaseRequestHandler`

    Each HTTP request is served by a new instance of this class.

    ### Example
    In order to create a handler for your service, it has to follow a certain skeleton:

    .. code-block:: python

        from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
        class yourEndpointHandler(TornadoREST):

            SYSTEM = "Configuration"
            LOCATION = "/registry"

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
            auth_users = ['authenticated']

            def web_users(self, count: int = 0):
                ''' Your method. It will be available for queries such as https://domain/registry/users?count=1

                TornadoREST will try to convert the received argument `count` to int.
                '''
                return Registry.getAllUsers()[:count]

    Note that because we inherit from :py:class:`tornado.web.RequestHandler`
    and we are running using executors, the methods you export cannot write
    back directly to the client. Please see inline comments for more details.

    In order to pass information around and keep some states, we use instance attributes.
    These are initialized in the :py:meth:`.initialize` method.

    The handler define the ``post`` and ``get`` verbs. Please refer to :py:meth:`.post` for the details.
    """

    USE_AUTHZ_GRANTS = ["SSL", "JWT", "VISITOR"]
    METHOD_PREFIX = "web_"
    LOCATION = "/"

    @classmethod
    def _getServiceName(cls, request):
        """Define endpoint full name

        :param object request: tornado Request

        :return: str
        """
        if not cls.SYSTEM:
            raise Exception("System name must be defined.")
        return "/".join([cls.SYSTEM, cls.__name__])

    @classmethod
    def _getServiceAuthSection(cls, endpointName):
        """Search endpoint auth section.

        :param str endpointName: endpoint name

        :return: str
        """
        return "%s/Authorization" % PathFinder.getAPISection(endpointName)

    def _getMethodName(self):
        """Parse method name. By default we read the first section in the path
        following the coincidence with the value of `LOCATION`.
        If such a method is not defined, then try to use the `index` method.

        :return: str
        """
        method = self.request.path.replace(self.LOCATION, "", 1).strip("/").split("/")[0]
        if method and hasattr(self, "".join([self.METHOD_PREFIX, method])):
            return method
        elif hasattr(self, "%sindex" % self.METHOD_PREFIX):
            gLogger.warn("%s method not implemented. Use the index method to handle this." % method)
            return "index"
        else:
            raise NotImplementedError(
                "%s method not implemented. You can use the index method to handle this." % method
            )

    def _getMethodArgs(self, args):
        """Search method arguments.

        By default, the arguments are taken from the description of the method itself.
        Then the arguments received in the request are assigned by the name of the method arguments.

        .. warning:: this means that the target methods cannot be wrapped in the decorator,
                     or if so the decorator must duplicate the arguments and annotation of the target method

        :param tuple args: positional arguments, they are determined by a variable path_< method name >, e.g.:
                           `path_methodName = ['([A-z0-9-_]*)']`. In most cases, this is simply not used.

        :return: tuple -- contain args and kwargs
        """
        # Read signature of a target function
        # https://docs.python.org/3/library/inspect.html#inspect.Signature
        signature = inspect.signature(self.methodObj)

        # Collect all values of the arguments transferred in a request
        args = [unquote(a) for a in args]  # positional arguments
        kwargs = {a: self.get_argument(a) for a in self.request.arguments}  # keyword arguments

        # Create a mapping from request arguments to parameters
        bound = signature.bind(*args, **kwargs)
        # Set default values for missing arguments.
        bound.apply_defaults()

        keywordArguments = {}
        positionalArguments = []
        # Now let's check whether the value of the argument corresponds to the type specified in the objective function or type of the default value.
        for name in signature.parameters:
            value = bound.arguments[name]
            kind = signature.parameters[name].kind
            default = signature.parameters[name].default

            # Determine what type of the target function argument is expected. By Default it's str.
            annotation = (
                signature.parameters[name].annotation
                # Select the type specified in the target function, if any.
                # E.g.: def export_f(self, number:int): return S_OK(number)
                if signature.parameters[name].annotation is not inspect.Parameter.empty
                else str
                # If there is no argument annotation, take the default value type, if any
                # E.g.: def export_f(self, number=0): return S_OK(number)
                if default is inspect.Parameter.empty
                else type(default)
            )

            # If the type of the argument value does not match the expectation, we convert it to the appropriate type
            if value != default:
                # Get list of the arguments
                if annotation is list:
                    value = self.get_arguments(name) if name in self.request.arguments else [value]
                # Get integer argument
                elif annotation is int:
                    value = int(value)

            # Collect positional parameters separately
            if kind == inspect.Parameter.POSITIONAL_ONLY:
                positionalArguments.append(value)
            elif kind == inspect.Parameter.VAR_POSITIONAL:
                positionalArguments.extend(value)
            elif kind == inspect.Parameter.VAR_KEYWORD:
                keywordArguments.update(value)
            else:
                keywordArguments[name] = value

        return (positionalArguments, keywordArguments)

    @gen.coroutine
    def get(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``GET`` requests.
        Note that all the arguments are already prepared in the :py:meth:`.prepare` method.
        """
        retVal = yield IOLoop.current().run_in_executor(*self._prepareExecutor(args))
        self._finishFuture(retVal)

    @gen.coroutine
    def post(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Method to handle incoming ``POST`` requests.
        Note that all the arguments are already prepared in the :py:meth:`.prepare` method.
        """
        retVal = yield IOLoop.current().run_in_executor(*self._prepareExecutor(args))
        self._finishFuture(retVal)

    auth_echo = ["all"]

    @staticmethod
    def web_echo(data):
        """
        This method used for testing the performance of a service
        """
        return S_OK(data)

    auth_whoami = ["authenticated"]

    def web_whoami(self):
        """A simple whoami, returns all credential dictionary, except certificate chain object."""
        credDict = self.srv_getRemoteCredentials()
        if "x509Chain" in credDict:
            # Not serializable
            del credDict["x509Chain"]
        return S_OK(credDict)
