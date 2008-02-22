
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.private.BaseClient import BaseClient

class GatewayHandler(RequestHandler):

  def initialize( self ):
    credDict = self.getRemoteCredentials()
    if 'DN' not in credDict:
      self.userDescription = "anonymous@noGroup"
    else:
      self.userDescription = "%s@%s" % ( credDict[ 'DN' ], credDict[ 'group' ] )
    self.userDescription += ":%s" % self.getRemoteAddress()[0]
    self.rpcInitArgs = {
                   BaseClient.KW_SETUP : self.serviceInfoDict[ 'clientSetup' ],
                   BaseClient.KW_DELEGATED_DN : credDict[ 'DN' ],
                   BaseClient.KW_DELEGATED_GROUP : credDict[ 'group' ],
                   BaseClient.KW_TIMEOUT : 60,
                   BaseClient.KW_IGNORE_GATEWAYS : True
                   }

  def executeAction( self, actionTuple ):
    retVal = self._clientTransport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving file description", retVal[ 'Message' ] )
      self._clientTransport.sendData(  S_ERROR( "Error while receiving file description: %s" % retVal[ 'Message' ] ) )
      return
    actionType = actionTuple[0]
    if actionType == "FileTransfer":
      gLogger.warn( "Received a file transfer action from %s" % self.userDescription )
      retVal =  S_ERROR( "File transfer can't be forwarded" )
    elif actionType == "RPC":
      gLogger.info( "Forwarding %s action from %s" % ( actionType, self.userDescription ) )
      retVal = self.forwardRPCCall( actionTuple[1], retVal[ 'Value' ] )
    else:
      gLogger.warn( "Received an unknown %s action from %s" % ( actionType, self.userDescription ) )
      retVal =  S_ERROR( "Unknown type of action (%s)" % actionType )
    self._clientTransport.sendData( retVal )

  def forwardRPCCall( self, method, params ):
    targetRPCService = self.serviceInfoDict[ 'serviceName' ]
    gLogger.info( "Executing RPC method %s to %s for %s" % ( method, targetRPCService, self.userDescription ) )
    rpcClient = RPCClient( targetRPCService, **self.rpcInitArgs )
    method = getattr( rpcClient, method )
    return method( *params )
