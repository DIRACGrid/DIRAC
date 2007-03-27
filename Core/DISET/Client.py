# $Header$
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.private.BaseClient import BaseClient

class _MagicMethod:

  def __init__( self, oSendFunction, sName ):
    self.oSendFunction = oSendFunction
    self.sName = sName
    
  def __getattr__( self, sName ):
    return _MagicMethod( self.oSendFunction, "%s.%s" % ( self.sName, sName ) )
  
  def __call__(self, *args):
    return self.oSendFunction( self.sName, args )
      
class Client( BaseClient ):
  
  def __serverRPC( self, sFunctionName, stArgs ):
    self._connect()
    dRetVal = self._proposeAction( "RPC" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    stFunction = ( sFunctionName, stArgs )
    self.oServerTransport.sendData( stFunction )
    return self.oServerTransport.receiveData()
    
  def __getattr__( self, sName ):
    return _MagicMethod( self.__serverRPC, sName )
  
if __name__=="__main__":
  oCS = Client( "CS" )
  print oCS.pack()
  print oCS.ping()
