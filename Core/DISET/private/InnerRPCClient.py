# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/InnerRPCClient.py,v 1.1 2007/05/03 18:59:48 acasajus Exp $
__RCSID__ = "$Id: InnerRPCClient.py,v 1.1 2007/05/03 18:59:48 acasajus Exp $"

from DIRAC.Core.Utilities import Subprocess
from DIRAC.Core.DISET.private.BaseClient import BaseClient


class InnerRPCClient( BaseClient ):

  def executeRPC( self, functionName, args, kwargs ):
    if self.timeout:
      retVal = Subprocess.pythonCall( self.timeout, self.__serverRPC, functionName, args, kwargs)
      if retVal[ 'OK' ]:
        return retVal[ 'Value' ]
      return retVal
    else:
      return self.__serverRPC( functionName, args, kwargs )

  def __serverRPC( self, functionName, args, kwargs ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self._proposeAction( "RPC" )
    if not retVal[ 'OK' ]:
      return retVal
    functionTuple = ( functionName, args, kwargs )
    self.transport.sendData( functionTuple )
    return self.transport.receiveData()

