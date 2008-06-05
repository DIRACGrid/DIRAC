# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/GatewayDispatcher.py,v 1.3 2008/06/05 10:20:16 acasajus Exp $
__RCSID__ = "$Id: GatewayDispatcher.py,v 1.3 2008/06/05 10:20:16 acasajus Exp $"

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.Core import Security

from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.private.Dispatcher import Dispatcher
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.private.BaseClient import BaseClient

class GatewayDispatcher( Dispatcher ):

  gatewayServiceName = "Framework/Gateway"

  def __init__( self, serviceCfgList ):
    Dispatcher.__init__( self, serviceCfgList )

  def loadHandlers( self ):
    """
    No handler to load for the gateway
    """
    return S_OK()

  def initializeHandlers( self ):
    """
    No static initialization
    """
    return S_OK()

  def _lock( self, svcName ):
    pass

  def _unlock( self, svcName ):
    pass

  def _authorizeClientProposal( self, service, actionTuple, clientTransport ):
    """
    Authorize the action being proposed by the client
    """
    if actionTuple[0] == 'RPC':
      action = actionTuple[1]
    else:
      action = "%s/%s" % actionTuple
    credDict = clientTransport.getConnectingCredentials()
    retVal = self._authorizeAction( service, action, credDict )
    if not retVal[ 'OK' ]:
      clientTransport.sendData( retVal )
      return False
    return True

  def _authorizeAction( self, serviceName, action, credDict ):
    try:
      authManager = AuthManager( "%s/Authorization" % getServiceSection( serviceName ) )
    except:
      return S_ERROR( "Service %s is unknown" % serviceName )
    gLogger.debug( "Trying credentials %s" % credDict )
    if not authManager.authQuery( action, credDict ):
      if 'username' in credDict.keys():
        username = credDict[ 'username' ]
      else:
        username = 'unauthenticated'
      gLogger.info( "Unauthorized query", "to %s" % action )
      return S_ERROR( "Unauthorized query to %s:%s" % ( serviceName, action ) )
    return S_OK()

  def _executeAction( self, proposalTuple, clientTransport ):
    """
    Execute an action
    """
    credDict = clientTransport.getConnectingCredentials()
    retVal = self.__checkDelegation( clientTransport )
    if not retVal[ 'OK' ]:
      return retVal
    delegatedChain = retVal[ 'Value' ]
    #Generate basic init args
    targetService = proposalTuple[0][0]
    clientInitArgs = {
                        BaseClient.KW_SETUP : proposalTuple[0][1],
                        BaseClient.KW_TIMEOUT : 600,
                        BaseClient.KW_IGNORE_GATEWAYS : True,
                        BaseClient.KW_USE_CERTIFICATES : False,
                        BaseClient.KW_PROXY_STRING : delegatedChain
                        }
    #OOkay! Lets do the magic!
    retVal = clientTransport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving file description", retVal[ 'Message' ] )
      self._clientTransport.sendData(  S_ERROR( "Error while receiving file description: %s" % retVal[ 'Message' ] ) )
      return
    actionType = proposalTuple[1][0]
    actionMethod = proposalTuple[1][1]
    userDesc = self.__getUserDescription( credDict )
    #Filetransfer not here yet :P
    if actionType == "FileTransfer":
      gLogger.warn( "Received a file transfer action from %s" % userDesc )
      retVal =  S_ERROR( "File transfer can't be forwarded" )
    elif actionType == "RPC":
      gLogger.info( "Forwarding %s action from %s" % ( actionType, userDesc ) )
      retVal = self.__forwardRPCCall( targetService, clientInitArgs, actionMethod, retVal[ 'Value' ] )
    else:
      gLogger.warn( "Received an unknown %s action from %s" % ( actionType, userDesc ) )
      retVal =  S_ERROR( "Unknown type of action (%s)" % actionType )
    clientTransport.sendData( retVal )

  def __checkDelegation( self, clientTransport ):
    """
    Check the delegation
    """
    #Ask for delegation
    credDict = clientTransport.getConnectingCredentials()
    #If it's not secure don't ask for delegation
    if 'x509Chain' not in credDict:
      return S_OK()
    peerChain = credDict[ 'x509Chain' ]
    retVal = peerChain.getCertInChain()[ 'Value' ].generateProxyRequest()
    if not retVal[ 'OK' ]:
      return retVal
    delegationRequest = retVal[ 'Value' ]
    retVal = delegationRequest.dumpRequest()
    if not retVal[ 'OK' ]:
      retVal = S_ERROR( "Server Error: Can't generate delegation request" )
      clientTransport.sendData( retVal )
      return retVal
    gLogger.info( "Sending delegation request for %s" % delegationRequest.getSubjectDN()[ 'Value' ] )
    clientTransport.sendData( S_OK( { 'delegate' : retVal[ 'Value' ] } ) )
    delegatedCertChain = clientTransport.receiveData()
    delegatedChain = Security.X509Chain( keyObj = delegationRequest.getPKey() )
    retVal = delegatedChain.loadChainFromString( delegatedCertChain )
    if not retVal[ 'OK' ]:
      retVal = S_ERROR( "Error in receiving delegated proxy: %s" % retVal[ 'Message' ] )
      clientTransport.sendData( retVal )
      return retVal
    clientTransport.sendData( S_OK() )
    return delegatedChain.dumpAllToString()

  def __getUserDescription( self, credDict ):
    if 'DN' not in credDict:
      return "anonymous@noGroup"
    else:
      return "%s@%s" % ( credDict[ 'DN' ], credDict[ 'group' ] )

  def __forwardRPCCall( self, targetService, clientInitArgs, method, params ):
    rpcClient = RPCClient( targetService, **clientInitArgs )
    methodObj = getattr( rpcClient, method )
    return methodObj( *params )
