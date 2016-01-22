# $HeadURL$
__RCSID__ = "$Id$"

import types
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


class InnerRPCClient( BaseClient ):

  __retry = 0
  
  def executeRPC( self, functionName, args ):
    stub = ( self._getBaseStub(), functionName, args )
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    trid, transport = retVal[ 'Value' ]
    try:
      retVal = self._proposeAction( transport, ( "RPC", functionName ) )
      if not retVal['OK']:
        if retVal['Message'] == "Unauthorized query":  # TODO: DErno will help!:
          retVal[ 'rpcStub' ] = stub
          return retVal
        else:  # we have network problem or the service is not responding  
          if self.__retry < 3:
            self.__retry += 1
            return self.executeRPC( functionName, args )
          else:
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

