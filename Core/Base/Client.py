""" Base class for DIRAC Client """

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                     import RPCClient

class Client:
  """ Simple class to redirect unknown actions directly to the server. Arguments
      to the constructor are passed to the RPCClient constructor as they are.
      
      - The self.serverURL member should be set by the inheriting class
  """
  def __init__( self, **kwargs ):
    self.serverURL = None
    self.call = None    
    self.__kwargs = kwargs

  def setServer( self, url ):
    self.serverURL = url

  def setTimeout( self, timeout ):
    if type( timeout ) == int:
      timeout = timeout
      self.__kwargs['timeout'] = timeout
    elif type( timeout ) == str:
      try:
        timeout = int( timeout )
        self.__kwargs['timeout'] = timeout
      except ValueError:
        pass

  def getServer( self ):
    return self.serverURL

  def __getattr__( self, name ):
    self.call = name
    return self.executeRPC

  def executeRPC( self, *parms, **kws ):
    toExecute = self.call
    # Check whether 'rpc' keyword is specified
    rpc = False
    if kws.has_key( 'rpc' ):
      rpc = kws['rpc']
      del kws['rpc']
    # Check whether the 'timeout' keyword is specified
    timeout = 120
    if kws.has_key( 'timeout' ):
      timeout = kws['timeout']
      del kws['timeout']
    # Check whether the 'url' keyword is specified
    url = ''
    if kws.has_key( 'url' ):
      url = kws['url']
      del kws['url']
    # Create the RPCClient
    rpcClient = self._getRPC( rpc, url, timeout )
    # Execute the method
    return getattr( rpcClient, toExecute )( *parms )
    # evalString = "rpcClient.%s(*parms,**kws)" % toExecute
    # return eval( evalString )

  def _getRPC( self, rpc = False, url = '', timeout = None ):
    """ Return RPCClient object to url
    """
    if not rpc:
      if not url:
        url = self.serverURL
      if timeout:
        self.setTimeout( timeout )
      rpc = RPCClient( url, **self.__kwargs )
    return rpc
