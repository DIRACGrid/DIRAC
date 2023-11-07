""" Module that expose the base class for DIRAC Clients.

    This class exposes possible RPC calls, given a url of a service.
"""
import ast
import importlib_resources
from functools import partial, partialmethod

from DIRAC.Core.Tornado.Client.ClientSelector import RPCClientSelector
from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.DISET import DEFAULT_RPC_TIMEOUT


class partialmethodWithDoc(partialmethod):
    """Extension of meth:`functools.partialmethod` that preserves docstrings"""

    def __get__(self, instance, owner):
        func = super().__get__(instance, owner)
        func.__doc__ = self.__doc__
        return func


class Client:
    """Simple class to redirect unknown actions directly to the server. Arguments
    to the constructor are passed to the RPCClient constructor as they are.
    Some of them can however be overwritten at each call (url and timeout).

    - The self.serverURL member should be set by the inheriting class
    """

    # Default https (RPC)Client
    httpsClient = TornadoClient

    def __init__(self, **kwargs):
        """C'tor.

        :param kwargs: just stored as an attribute and passed when creating
                      the RPCClient
        """
        self.serverURL = kwargs.pop("url", None)
        self.__kwargs = kwargs
        self.timeout = DEFAULT_RPC_TIMEOUT

    def __getattr__(self, name):
        """Store the attribute asked and call executeRPC.
        This means that Client should not be shared between threads !
        """
        # This allows the dir() method to work as well as tab completion in ipython
        if name == "__dir__":
            return super().__getattr__()  # pylint: disable=no-member
        return partial(self.executeRPC, call=name)

    def setServer(self, url):
        """Set the server URL used by default

        :param url: url of the service
        """
        self.serverURL = url

    def getClientKWArgs(self):
        """Returns a copy of the connection arguments"""
        return dict(self.__kwargs)

    def getServer(self):
        """Getter for the server url. Useful ?"""
        return self.serverURL

    @property
    @deprecated("To be removed once we're sure self.call has been removed")
    def call(self):
        raise NotImplementedError("This should be unreachable")

    def executeRPC(self, *parms, **kws):
        """This method extracts some parameters from kwargs that
        are used as parameter of the constructor or RPCClient.
        Unfortunately, only a few of all the available
        parameters of BaseClient are exposed.

        :param rpc: if an RPC client is passed, use that one
        :param timeout: we can change the timeout on a per call bases. Default is self.timeout
        :param url: We can specify which url to use
        """
        toExecute = kws.pop("call")
        # Check whether 'rpc' keyword is specified
        rpc = kws.pop("rpc", False)
        # Check whether the 'timeout' keyword is specified
        timeout = kws.pop("timeout", self.timeout)
        # Check whether the 'url' keyword is specified
        url = kws.pop("url", "")
        # Create the RPCClient
        rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
        # Execute the method
        return getattr(rpcClient, toExecute)(*parms)

    def _getRPC(self, rpc=None, url="", timeout=None):
        """Return an RPCClient object constructed following the attributes.

        :param rpc: if set, returns this object
        :param url: url of the service. If not set, use self.serverURL
        :param timeout: timeout of the call. If not given, self.timeout will be used
        """
        if not rpc:
            if not url:
                url = self.serverURL

            if not timeout:
                timeout = self.timeout

            self.__kwargs["timeout"] = timeout

            rpc = RPCClientSelector(
                url, httpsClient=self.httpsClient, diracxClient=getattr(self, "diracxClient", None), **self.__kwargs
            )
        return rpc


def createClient(serviceName):
    """Decorator to expose the service functions automatically in the Client.

    :param str serviceName: system/service. e.g. WorkloadManagement/JobMonitoring
    """
    systemName, handlerName = serviceName.split("/")
    handlerModuleName = handlerName + "Handler"
    # by convention they are the same
    handlerClassName = handlerModuleName
    handlerClassPath = f"{systemName}System.Service.{handlerModuleName}.{handlerClassName}"

    def genFunc(funcName, arguments, handlerClassPath, doc):
        """Create a function with *funcName* taking *arguments*."""
        doc = "" if doc is None else doc
        funcDocString = f"{funcName}({', '.join(arguments)}, **kwargs)\n"
        # do not describe self or cls in the parameter description
        if arguments and arguments[0] in ("self", "cls"):
            arguments = arguments[1:]

        # Create the actual functions, with or without arguments, **kwargs can be: rpc, timeout, url
        func = partialmethodWithDoc(Client.executeRPC, call=funcName)
        func.__doc__ = funcDocString + doc
        func.__doc__ += "\n\nAutomatically created for the service function "
        func.__doc__ += f":func:`~{handlerClassPath}.export_{funcName}`"
        # add description for parameters, if that is not already done for the docstring of function in the service
        if arguments and ":param " not in doc:
            func.__doc__ += "\n\n"
            func.__doc__ += "\n".join(f":param {par}: {par}" for par in arguments)
        return func

    def addFunctions(clientCls):
        """Add the functions to the decorated class."""
        for extension in extensionsByPriority():
            moduleBase = f"{extension}.{systemName}System.Service"
            fullHandlerClassPath = f"{extension}.{handlerClassPath}"
            try:
                path = importlib_resources.files(moduleBase) / f"{handlerModuleName}.py"
                handlerAst = ast.parse(path.read_text(), f"{moduleBase}.{handlerModuleName}")
            except (ImportError, OSError):
                continue

            # loop over all the nodes (classes, functions, imports) in the handlerModule
            for node in ast.iter_child_nodes(handlerAst):
                # find only a class that starts with the name of the handlerClass
                if not (isinstance(node, ast.ClassDef) and node.name.startswith(handlerClassName)):
                    continue
                for member in ast.iter_child_nodes(node):
                    # only look at functions
                    if not isinstance(member, ast.FunctionDef):
                        continue
                    if not member.name.startswith("export_"):
                        continue
                    funcName = member.name[len("export_") :]
                    if hasattr(clientCls, funcName):
                        continue
                    arguments = [a.arg for a in member.args.args]
                    func = genFunc(funcName, arguments, fullHandlerClassPath, ast.get_docstring(member))
                    # add the implementation of the function to the class attributes
                    setattr(clientCls, funcName, func)

        return clientCls

    return addFunctions


def executeRPCStub(rpcStub):
    """
    Playback a stub with the correct client (https or dips)
    """
    baseStub, methName, args = rpcStub
    url, callParams = baseStub
    # Make a copy to update it
    stub = dict(callParams)
    stub["url"] = url
    # Generate a RPCClient with the same parameters
    client = Client(**stub)
    # Get a functor to execute the RPC call
    rpcFunc = getattr(client, methName)
    # Reproduce the call
    return rpcFunc(*args)
