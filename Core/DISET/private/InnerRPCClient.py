""" This module hosts the logic for executing an RPC call.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.Utilities.ReturnValues import S_OK
from DIRAC.Core.Utilities.DErrno import cmpError, ENOAUTH

class InnerRPCClient( BaseClient ):
  """ This class instruments the BaseClient to perform RPC calls.
      At every RPC call, this class:
	* connects
	* proposes the action
	* sends the method parameters
	* retrieve the result
	* disconnect
  """

  # Number of times we retry the call.
  # The connection retry is handled by BaseClient
  __retry = 0

  def executeRPC( self, functionName, args ):
    """ Perform the RPC call, connect before and disconnect after.

	:param functionName: name of the function
	:param args: arguments to the function

	:return: in case of success, the return of the server call. In any case
		we add the connection stub to it.


    """
    retVal = self._connect()

    # Generate the stub which contains all the connection and call options
    stub = ( self._getBaseStub(), functionName, args )
    if not retVal[ 'OK' ]:
      retVal[ 'rpcStub' ] = stub
      return retVal
    # Get the transport connection ID as well as the Transport object
    trid, transport = retVal[ 'Value' ]
    try:
      # Handshake to perform the RPC call for functionName
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

      # Send the arguments to the function
      retVal = transport.sendData( S_OK( args ) )
      if not retVal[ 'OK' ]:
        return retVal

      # Get the result of the call and append the stub to it
      receivedData = transport.receiveData()
      if isinstance( receivedData, dict ):
        receivedData[ 'rpcStub' ] = stub
      return receivedData
    finally:
      self._disconnect( trid )
