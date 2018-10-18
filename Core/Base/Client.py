""" Module that expose the base class for DIRAC Clients.

    This class exposes possible RPC calls, given a url of a service.
"""

__RCSID__ = "$Id$"

import ast
import os
from itertools import izip_longest

from DIRAC.Core.DISET.RPCClient import RPCClient


class Client(object):
  """ Simple class to redirect unknown actions directly to the server. Arguments
      to the constructor are passed to the RPCClient constructor as they are.
      Some of them can however be overwritten at each call (url and timeout).
      This class is not thread safe !

      - The self.serverURL member should be set by the inheriting class
  """

  def __init__(self, **kwargs):
    """ C'tor.

        :param kwargs: just stored as an attribute and passed when creating
                      the RPCClient
    """
    self.serverURL = None
    self.call = None  # I suppose it is initialized here to make pylint happy
    self.__kwargs = kwargs

  def __getattr__(self, name):
    """ Store the attribute asked and call executeRPC.
        This means that Client should not be shared between threads !
    """
    # This allows the dir() method to work as well as tab completion in ipython
    if name == '__dir__':
      return super(Client, self).__getattr__()  # pylint: disable=no-member
    self.call = name
    return self.executeRPC

  def setServer(self, url):
    """ Set the server URL used by default

        :param url: url of the service
    """
    self.serverURL = url

  def setTimeout(self, timeout):
    """ Specify the timeout of the call. Forwarded to RPCClient

        :param int timeout: timeout for the RPC calls
    """
    self.__kwargs['timeout'] = timeout

  def getServer(self):
    """ Getter for the server url. Useful ?
    """
    return self.serverURL

  def executeRPC(self, *parms, **kws):
    """ This method extracts some parameters from kwargs that
        are used as parameter of the constructor or RPCClient.
        Unfortunately, only a few of all the available
        parameters of BaseClient are exposed.

        :param rpc: if an RPC client is passed, use that one
        :param timeout: we can change the timeout on a per call bases. Default 120 s
        :param url: We can specify which url to use
    """
    toExecute = self.call
    # Check whether 'rpc' keyword is specified
    rpc = kws.pop('rpc', False)
    # Check whether the 'timeout' keyword is specified
    timeout = kws.pop('timeout', 120)
    # Check whether the 'url' keyword is specified
    url = kws.pop('url', '')
    # Create the RPCClient
    rpcClient = self._getRPC(rpc, url, timeout)
    # Execute the method
    return getattr(rpcClient, toExecute)(*parms)

  def _getRPC(self, rpc=None, url='', timeout=600):
    """ Return an RPCClient object constructed following the attributes.

        :param rpc: if set, returns this object
        :param url: url of the service. If not set, use self.serverURL
        :param timeout: timeout of the call
    """
    if not rpc:
      if not url:
        url = self.serverURL
      self.__kwargs.setdefault('timeout', timeout)
      rpc = RPCClient(url, **self.__kwargs)
    return rpc


def createClient(name, handlerModulePath, handlerClassName):
  """Decorator to expose the service functions automatically in the Client.

  :param str name: name of the client class
  :param str handlerModulePath: path to the service handler moduler relatative to the $DIRAC variable
  :param str handlerClassName: name of the service handler class
  """
  def addFunctions(clientCls):
    """Add the functions to the decorated class."""
    attrDict = dict(clientCls.__dict__)
    bases = (Client,)
    fullPath = os.path.join(os.environ.get('DIRAC', './'), handlerModulePath)
    if not os.path.exists(fullPath):
      return type(name, bases, attrDict)

    def genFunc(funcName, arguments, argTypes, doc):
      """Create a function with *funcName* taking *arguments*."""
      doc = '' if doc is None else doc
      if arguments and arguments[0] in ('self', 'cls'):
        arguments = arguments[1:]
      if arguments:
        def func(self, *args, **kwargs):  # pylint: disable=missing-docstring
          self.call = funcName
          return self.executeRPC(*args, **kwargs)
      else:
        def func(self, **kwargs):  # pylint: disable=missing-docstring
          self.call = funcName
          return self.executeRPC(**kwargs)
      func.__doc__ = doc + "\n\nAutomatically created for the service function :func:`~%s.%s.export_%s`" % \
          (handlerModulePath, handlerClassName, funcName)
      parameterDoc = ''
      if arguments and ":param " not in doc:
        parameterDoc = "\n".join(":param %(par)s: %(par)s" % dict(par=par)
                                 for par in arguments)
      if argTypes and ":param " not in doc:
        parameterDoc += "\n" + "\n".join(":type %(par)s: %(type)s" % dict(par=par, type=argType)
                                         for par, argType in izip_longest(arguments, argTypes))
      if parameterDoc and ":param " not in doc:
        func.__doc__ += "\n\n" + parameterDoc
      return func

    with open(fullPath) as moduleFile:
      handlerAst = ast.parse(moduleFile.read(), fullPath)

    for node in ast.iter_child_nodes(handlerAst):
      if not (isinstance(node, ast.ClassDef) and node.name == handlerClassName):
        continue
      for member in ast.iter_child_nodes(node):
        if not isinstance(member, ast.FunctionDef):
          continue
        arguments = [a.id for a in member.args.args]
        function = member.name
        if not function.startswith('export_'):
          continue
        funcName = function[len('export_'):]
        argTypes = None  # give up on those for now
        if funcName in attrDict:
          continue
        attrDict[funcName] = genFunc(funcName, arguments, argTypes, ast.get_docstring(member))

    return type(name, bases, attrDict)

  return addFunctions
