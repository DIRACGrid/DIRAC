# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RPCClient.py,v 1.2 2007/06/13 19:29:38 acasajus Exp $
__RCSID__ = "$Id: RPCClient.py,v 1.2 2007/06/13 19:29:38 acasajus Exp $"

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
    self.innerRPCClient = InnerRPCClient( *args, **kwargs )

  def __doRPC( self, sFunctionName, args ):
    retVal = self.innerRPCClient.executeRPC( sFunctionName, args )
    return retVal

  def __getattr__( self, attrName ):
    if attrName in dir( self.innerRPCClient ):
      return getattr( self.innerRPCClient, attrName )
    return _MagicMethod( self.__doRPC, attrName )
