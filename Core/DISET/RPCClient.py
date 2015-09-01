""" RPCClient object is used to create RPC connection to services
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.private.InnerRPCClient import InnerRPCClient

class _MagicMethod( object ):

  def __init__( self, doRPCFunc, remoteFuncName ):
    self.__doRPCFunc = doRPCFunc
    self.__remoteFuncName = remoteFuncName

  def __getattr__( self, remoteFuncName ):
    return _MagicMethod( self.__doRPCFunc, "%s.%s" % ( self.__remoteFuncName, remoteFuncName ) )

  def __call__(self, *args ):
    return self.__doRPCFunc( self.__remoteFuncName, args )

  def __str__( self ):
    return "<RPCClient method %s>" % self.__remoteFuncName

class RPCClient( object ):

  def __init__( self, *args, **kwargs ):
    """
    Constructor
    """
    self.__innerRPCClient = InnerRPCClient( *args, **kwargs )

  def __doRPC( self, sFunctionName, args ):
    """
    Execute the RPC action
    """
    retVal = self.__innerRPCClient.executeRPC( sFunctionName, args )
    return retVal

  def __getattr__( self, attrName ):
    """
    Function for emulating the existance of functions
    """
    if attrName in dir( self.__innerRPCClient ):
      return getattr( self.__innerRPCClient, attrName )
    return _MagicMethod( self.__doRPC, attrName )

def executeRPCStub( rpcStub ):
  """
  Playback a stub
  """
  #Generate a RPCClient with the same parameters
  rpcClient = RPCClient( rpcStub[0][0], **rpcStub[0][1] )
  #Get a functor to execute the RPC call
  rpcFunc = getattr( rpcClient, rpcStub[1] )
  #Reproduce the call
  return rpcFunc( *rpcStub[2] )
