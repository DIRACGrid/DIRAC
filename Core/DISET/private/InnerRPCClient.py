# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/InnerRPCClient.py,v 1.8 2008/08/01 13:04:41 acasajus Exp $
__RCSID__ = "$Id: InnerRPCClient.py,v 1.8 2008/08/01 13:04:41 acasajus Exp $"

import types
from DIRAC.Core.Utilities import Subprocess
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


class InnerRPCClient( BaseClient ):

  def executeRPC( self, functionName, args ):
    if self.timeout:
      retVal = Subprocess.pythonCall( self.timeout, self.__serverRPC, functionName, args )
      if retVal[ 'OK' ]:
        return retVal[ 'Value' ]
      return retVal
    else:
      return self.__serverRPC( functionName, args )

  def __serverRPC( self, functionName, args ):
    stub = ( self._getBaseStub(), functionName, args )
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    transport = retVal[ 'Value' ]
    retVal = self._proposeAction( transport, ( "RPC", functionName ) )
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    retVal = transport.sendData( S_OK( args ) )
    if not retVal[ 'OK' ]:
      return retVal
    receivedData = transport.receiveData()
    transport.close()
    if type( receivedData ) == types.DictType:
      receivedData[ 'rpcStub' ] = stub
    return receivedData

