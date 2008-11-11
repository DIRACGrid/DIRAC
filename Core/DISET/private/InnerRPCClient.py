# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/InnerRPCClient.py,v 1.9 2008/11/11 17:36:42 acasajus Exp $
__RCSID__ = "$Id: InnerRPCClient.py,v 1.9 2008/11/11 17:36:42 acasajus Exp $"

import types
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


class InnerRPCClient( BaseClient ):

  def executeRPC( self, functionName, args ):
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

