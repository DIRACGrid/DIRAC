""" This module hosts the logic for executing an RPC call.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK
from DIRAC.Core.Utilities.DErrno import cmpError, ENOAUTH

class InnerRPCClient( BaseClient ):

  __retry = 0

  def executeRPC( self, functionName, args ):
    retVal = self._connect()
    stub = ( self._getBaseStub(), functionName, args )
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    trid, transport = retVal[ 'Value' ]
    try:
      retVal = self._proposeAction( transport, ( "RPC", functionName ) )
      if not retVal['OK']:
        if cmpError( retVal, ENOAUTH ):  # This query is unauthorized
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
      if isinstance( receivedData, dict ):
        receivedData[ 'rpcStub' ] = stub
      return receivedData
    finally:
      self._disconnect( trid )
