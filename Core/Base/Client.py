""" Module that expose the base class for DIRAC Clients.

    This class exposes possible RPC calls, given a url of a service.
"""

__RCSID__ = "$Id$"

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

        :param timeout: guess...
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
    rpc = False
    if 'rpc' in kws:
      rpc = kws['rpc']
      del kws['rpc']
    # Check whether the 'timeout' keyword is specified
    timeout = 120
    if 'timeout' in kws:
      timeout = kws['timeout']
      del kws['timeout']
    # Check whether the 'url' keyword is specified
    url = ''
    if 'url' in kws:
      url = kws['url']
      del kws['url']
    # Create the RPCClient
    rpcClient = self._getRPC(rpc, url, timeout)
    # Execute the method
    return getattr(rpcClient, toExecute)(*parms)
    # evalString = "rpcClient.%s(*parms,**kws)" % toExecute
    # return eval( evalString )

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
