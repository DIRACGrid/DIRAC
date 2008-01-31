# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RPCClient.py,v 1.4 2008/01/31 15:55:57 acasajus Exp $
__RCSID__ = "$Id: RPCClient.py,v 1.4 2008/01/31 15:55:57 acasajus Exp $"

from DIRAC.Core.DISET.private.InnerRPCClient import InnerRPCClient

class _MagicMethod:

  def __init__( self, doRPCFunc, remoteFuncName ):
    self.doRPCFunc = doRPCFunc
    self.remoteFuncName = remoteFuncName

  def __getattr__( self, remoteFuncName ):
    return _MagicMethod( self.doRPCFunc, "%s.%s" % ( self.remoteFuncName, remoteFuncName ) )

  def __call__(self, *args ):
      return self.doRPCFunc( self.remoteFuncName, args )

class RPCClient:

  def __init__( self, *args, **kwargs ):
    """
    Constructor
    """
    self.innerRPCClient = InnerRPCClient( *args, **kwargs )

  def __doRPC( self, sFunctionName, args ):
    """
    Execute the RPC action
    """
    retVal = self.innerRPCClient.executeRPC( sFunctionName, args )
    return retVal

  def __getattr__( self, attrName ):
    """
    Function for emulating the existance of functions
    """
    if attrName in dir( self.innerRPCClient ):
      return getattr( self.innerRPCClient, attrName )
    return _MagicMethod( self.__doRPC, attrName )

def executeRPCStub( rpcStub ):
  """
  Playback a stub
  """
  #Generate a RPCClient with the same parameters
  rpcClient = RPCClient( rpcStub[0], **rpcStub[1] )
  #Get a functor to execute the RPC call
  rpcFunc = getattr( rpcClient, rpcStub[2] )
  #Reproduce the call
  return rpcFunc( *rpcStub[3] )