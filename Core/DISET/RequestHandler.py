# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RequestHandler.py,v 1.34 2008/05/07 16:28:20 acasajus Exp $
__RCSID__ = "$Id: RequestHandler.py,v 1.34 2008/05/07 16:28:20 acasajus Exp $"

import os
import types
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities import Time

class RequestHandler:

  def __init__( self, serviceInfoDict,
                transport,
                lockManager ):
    """
    Constructor

    @type serviceInfoDict: dictionary
    @param serviceInfoDict: Information vars for the service
    @type transport: object
    @param transport: Transport to use
    @type lockManager: object
    @param lockManager: Lock manager to use
    """
    self.serviceInfoDict = serviceInfoDict
    self._clientTransport = transport
    self._lockManager = lockManager

  def initialize( self ):
    """
    Dummy function to be inherited by real handlers. This function will be called when initializing
    the server.
    """
    pass

  def getRemoteAddress(self):
    """
    Get the address of the remote peer.

    @return : Address of remote peer.
    """
    return self._clientTransport.getRemoteAddress()

  def getRemoteCredentials( self ):
    """
    Get the credentials of the remote peer.

    @return : Credentials dictionary of remote peer.
    """
    return self._clientTransport.getConnectingCredentials()

  def executeAction( self, actionTuple ):
    """
    Execute an action.

    @type actionTuple: tuple
    @param actionTuple: Type of action to execute. First position of the tuple must be the type
                        of action to execute. The second position is the action itself.
    """
    gLogger.debug( "Executing %s:%s action" % actionTuple )
    actionType = actionTuple[0]
    if actionType == "RPC":
      retVal = self.__doRPC( actionTuple[1] )
    elif actionType == "FileTransfer":
      retVal = self.__doFileTransfer( actionTuple[1] )
    else:
      raise Exception( "Unknown action (%s)" % action )
    if not retVal:
      message = "Method %s for action %s does not have a return value!" % ( actionTuple[1], actionTuple[0] )
      gLogger.error( message )
      retVal = S_ERROR( message )
    self.__logRemoteQueryResponse(  retVal )
    self._clientTransport.sendData( retVal )

#####
#
# File to/from Server Methods
#
#####

  def __doFileTransfer( self, sDirection ):
    """
    Execute a file transfer action

    @type sDirection: string
    @param sDirection: Direction of the transfer
    @return: S_OK/S_ERROR
    """
    retVal = self._clientTransport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving file description", retVal[ 'Message' ] )
      return S_ERROR( "Error while receiving file description: %s" % retVal[ 'Message' ] )
    fileInfo = retVal[ 'Value' ]
    sDirection = "%s%s" % ( sDirection[0].lower(), sDirection[1:] )
    if "transfer_%s" % sDirection not in dir( self ):
      self._clientTransport.sendData( S_ERROR( "Service can't transfer files %s" % sDirection ) )
      return
    self._clientTransport.sendData( S_OK( "Accepted" ) )
    self.__logRemoteQuery( "FileTransfer/%s" % sDirection, fileInfo )
    fileHelper = FileHelper( self._clientTransport )
    if sDirection == "fromClient":
      uRetVal = self.transfer_fromClient( fileInfo[0], fileInfo[1], fileInfo[2], fileHelper )
    elif sDirection == "toClient" :
      uRetVal = self.transfer_toClient( fileInfo[0], fileInfo[1], fileHelper )
    elif sDirection == "bulkFromClient" :
      uRetVal = self.transfer_bulkFromClient( fileInfo[0], fileInfo[1], fileInfo[2], fileHelper )
    elif sDirection == "bulkToClient" :
      uRetVal = self.transfer_bulkToClient( fileInfo[0], fileInfo[1], fileHelper )
    elif sDirection == "listBulk" :
      uRetVal = self.transfer_listBulk( fileInfo[0], fileInfo[1], fileHelper )
    else:
      return S_ERROR( "Direction %s does not exist!!!" % sDirection )
    if uRetVal[ 'OK' ] and not fileHelper.finishedTransmission():
      gLogger.error( "You haven't finished receiving/sending the file", str( fileInfo ) )
      return S_ERROR( "Incomplete transfer" )
    return uRetVal

  def transfer_fromClient( self, fileId, token, fileSize, fileHelper ):
    return S_ERROR( "This server does no allow receiving files" )

  def transfer_toClient( self, fileId, token, fileHelper ):
    return S_ERROR( "This server does no allow sending files" )

  def transfer_bulkFromClient( self, bulkId, token, bulkSize, fileHelper ):
    return S_ERROR( "This server does no allow bulk receiving" )

  def transfer_bulkToClient( self, bulkId, token, fileHelper ):
    return S_ERROR( "This server does no allow bulk sending" )

  def transfer_listBulk( self, bulkId, token, fileHelper ):
    return S_ERROR( "This server does no allow bulk listing" )

#####
#
# RPC Methods
#
#####

  def __doRPC( self, method ):
    """
    Execute an RPC action

    @type method: string
    @param method: Method to execute
    @return: S_OK/S_ERROR
    """
    retVal = self._clientTransport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving function arguments", retVal[ 'Message' ] )
      return S_ERROR( "Error while receiving function arguments: %s" % retVal[ 'Message' ] )
    args = retVal[ 'Value' ]
    self.__logRemoteQuery( "RPC/%s" % method, args )
    return self.__RPCCallFunction( method, args )

  def __RPCCallFunction( self, method, args ):
    realMethod = "export_%s" % method
    gLogger.debug( "RPC to %s" % realMethod )
    try:
      oMethod = getattr( self, realMethod )
    except:
      return S_ERROR( "Unknown method %s" % method )
    dRetVal = self.__RPCCheckExpectedArgumentTypes( method, args )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self._lockManager.lock( method )
    try:
      try:
        uReturnValue = oMethod( *args )
        return uReturnValue
      finally:
        self._lockManager.unlock( method )
    except Exception, v:
      gLogger.exception( "Uncaught exception when serving RPC", "Function %s" % method )
      return S_ERROR( "Server error while serving %s: %s" % ( method, str( v ) ) )

  def __RPCCheckExpectedArgumentTypes( self, method, args ):
    """
    Check that the arguments received match the ones expected

    @type method: string
    @param method: Method to check against
    @type args: tuple
    @params args: Arguments to check
    @return: S_OK/S_ERROR
    """
    sListName = "types_%s" % method
    try:
      oTypesList = getattr( self, sListName )
    except:
      gLogger.error( "There's no types info for method export_%s" % method )
      return S_ERROR( "Handler error for server %s while processing method %s" % (
                                                                                  self.serviceInfoDict[ 'serviceName' ],
                                                                                  method ) )
    try:
      mismatch = False
      for iIndex in range( min( len( oTypesList ), len( args ) ) ):
        #If none skip a parameter
        if oTypesList[ iIndex ] == None:
          continue
        #If parameter is a list or a tuple check types inside
        elif type( oTypesList[ iIndex ] ) in ( types.TupleType, types.ListType ):
          if not type( args[ iIndex ] ) in oTypesList[ iIndex ]:
            mismatch = True
        #else check the parameter
        elif not type( args[ iIndex ] ) == oTypesList[ iIndex ]:
          mismatch = True
        #Has there been a mismatch?
        if mismatch:
          sError = "Type mismatch in parameter %d" % iIndex
          return S_ERROR( sError )
      if len( args ) < len( oTypesList ):
        return S_ERROR( "Function %s expects at least %s arguments" % ( method, len( oTypesList ) ) )
    except Exception, v:
      sError = "Error in parameter check: %s" % str(v)
      gLogger.exception( sError )
      return S_ERROR( sError )
    return S_OK()

####
#
#  Auth methods
#
####

  def __authQuery( self, method ):
    """
    Check if connecting user is allowed to perform an action

    @type method: string
    @param method: Method to check
    @return: S_OK/S_ERROR
    """
    return self.serviceInfoDict[ 'authManager' ].authQuery( method, self.getRemoteCredentials() )

  def __logRemoteQuery( self, method, args ):
    """
    Log the contents of a remote query

    @type method: string
    @param method: Method to log
    @type args: tuple
    @param args: Arguments of the method called
    """
    peerCreds = self.getRemoteCredentials()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s:%s]" % ( peerCreds[ 'group' ], peerCreds[ 'username' ] )
    else:
      peerId = ""
    if False:
      argsString = ", ".join( [ str( arg )[:20] for arg in args ] )
    else:
      argsString = "<masked>"
    gLogger.info( "Executing action", "(%s:%s)%s %s( %s )" % ( self.serviceInfoDict[ 'clientAddress' ][0],
                                                        self.serviceInfoDict[ 'clientAddress' ][1],
                                                        peerId,
                                                        method,
                                                        argsString ) )

  def __logRemoteQueryResponse( self, retVal ):
    """
    Log the result of a query

    @type retVal: dictionary
    @param retVal: Return value of the query
    """
    peerCreds = self.getRemoteCredentials()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s:%s]" % ( peerCreds[ 'group' ], peerCreds[ 'username' ] )
    else:
      peerId = ""
    argsSring = str( retVal )[:100]
    gLogger.info( "Returning response", "(%s:%s)%s %s" % ( self.serviceInfoDict[ 'clientAddress' ][0],
                                                           self.serviceInfoDict[ 'clientAddress' ][1],
                                                           peerId,
                                                           argsSring ) )

####
#
#  Default ping method
#
####

  types_ping = []
  def export_ping( self ):
    dInfo = {}
    dInfo[ 'time' ] = Time.dateTime()
    #Uptime
    try:
      oFD = file( "/proc/uptime" )
      iUptime = long( float( oFD.readline().split()[0].strip() ) )
      oFD.close()
      dInfo[ 'host uptime' ] = iUptime
    except:
      pass
    startTime = self.serviceInfoDict[ 'serviceStartTime' ]
    dInfo[ 'service start time' ] = self.serviceInfoDict[ 'serviceStartTime' ]
    serviceUptime = Time.dateTime() - startTime
    dInfo[ 'service uptime' ] = serviceUptime.days * 3600 + serviceUptime.seconds
    #Load average
    try:
      oFD = file( "/proc/loadavg" )
      sLine = oFD.readline()
      oFD.close()
      dInfo[ 'load' ] = " ".join( sLine.split()[:3] )
    except:
      pass
    dInfo[ 'name' ] = self.serviceInfoDict[ 'serviceName' ]
    stTimes = os.times()
    dInfo[ 'cpu times' ] = { 'user time' : stTimes[0],
                             'system time' : stTimes[1],
                             'children user time' : stTimes[2],
                             'children system time' : stTimes[3],
                             'elapsed real time' : stTimes[4]
                           }

    return S_OK( dInfo )

####
#
#  Default get Credentials method
#
####

  types_getCredentials = []
  def export_getCredentials( self ):
    return S_OK( self.getRemoteCredentials() )