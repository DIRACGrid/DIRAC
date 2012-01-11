# $HeadURL$
__RCSID__ = "$Id$"

import types
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


class InnerRPCClient( BaseClient ):

  def executeRPC( self, functionName, args ):
    stub = ( self._getBaseStub(), functionName, args )
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    trid, transport = retVal[ 'Value' ]
    try:
      retVal = self._proposeAction( transport, ( "RPC", functionName ) )
      if not retVal[ 'OK' ]:
        retVal[ 'rpcStub' ] = stub
        return retVal
      retVal = transport.sendData( S_OK( args ) )
      if not retVal[ 'OK' ]:
        return retVal
      receivedData = transport.receiveData()
      if type( receivedData ) == types.DictType:
        receivedData[ 'rpcStub' ] = stub
      return receivedData
    finally:
      self._disconnect( trid )

