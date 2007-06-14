# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RequestHandler.py,v 1.17 2007/06/14 09:16:48 acasajus Exp $
__RCSID__ = "$Id: RequestHandler.py,v 1.17 2007/06/14 09:16:48 acasajus Exp $"

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
    self.serviceInfoDict = serviceInfoDict
    self.transport = transport
    self.lockManager = lockManager

  def initialize( self ):
    pass

  def getRemoteCredentials( self ):
    return self.transport.getConnectingCredentials()

  def executeAction( self, actionTuple ):
    gLogger.verbose( "Executing %s:%s action" % actionTuple )
    actionType = actionTuple[0]
    if actionType == "RPC":
      retVal = self.__doRPC( actionTuple[1] )
    elif actionType == "FileTransfer":
      retVal = self.__doFileTransfer( actionTuple[1] )
    else:
      raise RuntimeException( "Unknown action (%s)" % action )
    if not retVal:
      message = "Method %s for action %s does not have a return value!" % ( actionTuple[1], actionTuple[0] )
      gLogger.error( message )
      retVal = S_ERROR( message )
    self.transport.sendData( retVal )

#####
#
# File to/from Server Methods
#
#####

  def __doFileTransfer( self, sDirection ):
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving file description", retVal[ 'Message' ] )
      return S_ERROR( "Error while receiving file description: %s" % retVal[ 'Message' ] )
    fileInfo = retVal[ 'Value' ]
    sDirection = "%s%s" % ( sDirection[0].lower(), sDirection[1:] )
    if "transfer_%s" % sDirection not in dir( self ):
      self.transport.sendData( S_ERROR( "Service can't transfer files %s" % sDirection ) )
      return
    self.transport.sendData( S_OK() )
    fileHelper = FileHelper( self.transport )
    if sDirection == "fromClient":
      uRetVal = self.transfer_fromClient( fileInfo[0], fileInfo[1], fileHelper )
    elif sDirection == "toClient" :
      uRetVal = self.transfer_toClient( fileInfo[0], fileHelper )
    else:
      return S_ERROR( "Direction %s does not exist!!!" % sDirection )
    if not fileHelper.finishedTransmission():
      gLogger.error( "You haven't finished receiving the file", str( fileInfo ) )
      return S_ERROR( "Incomplete transfer" )
    return uRetVal

  def fileFromClientCallback( self, fileId, fileSize, fileHelper ):
    return S_ERROR( "This server does no allow receiving files" )

  def fileToClientCallback( self, fileId, fileHelper ):
    return S_ERROR( "This server does no allow sending files" )

#####
#
# RPC Methods
#
#####

  def __doRPC( self, method ):
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while receiving function arguments", retVal[ 'Message' ] )
      return S_ERROR( "Error while receiving function arguments: %s" % retVal[ 'Message' ] )
    args = retVal[ 'Value' ]
    self.__logRPCQuery( method, args )
    return self.__RPCCallFunction( method, args )

  def __logRPCQuery( self, method, args ):
    peerCreds = self.getRemoteCredentials()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s]" % peerCreds[ 'username' ]
    else:
      peerId = ""
    argsSring = [ str( arg )[:20] for arg in args ]
    gLogger.info( "Executing RPC", "%s %s( %s )" % ( peerId, method, argsSring ) )

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
    self.lockManager.lock( method )
    try:
      try:
        uReturnValue = oMethod( *args )
        return uReturnValue
      finally:
        self.lockManager.unlock( method )
    except Exception, v:
      gLogger.exception( "Uncaught exception when serving RPC", "Function %s" % method )
      return S_ERROR( "Server error while serving %s: %s" % ( method, str( v ) ) )

  def __RPCCheckExpectedArgumentTypes( self, method, args ):
    sListName = "types_%s" % method
    try:
      oTypesList = getattr( self, sListName )
    except:
      gLogger.error( "There's no types info for method export_%s" % method )
      return S_ERROR( "Handler error for server %s while processing method %s" % (
                                                                                  self.serviceInfoDict[ 'serviceName' ],
                                                                                  method ) )
    try:
      for iIndex in range( min( len( oTypesList ), len( args ) ) ):
        if not type( args[ iIndex ] ) == oTypesList[ iIndex ]:
          sError = "Type mismatch in parameter %d" % iIndex
          return S_ERROR( sError )
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
    return self.serviceInfoDict[ 'authManager' ].authQuery( method, self.getRemoteCredentials() )

####
#
#  Default ping method
#
####

  types_ping = []
  def export_ping( self ):
    dInfo = {}
    dInfo[ 'time' ] = Time.toString()
    #Uptime
    oFD = file( "/proc/uptime" )
    iUptime = long( float( oFD.readline().split()[0].strip() ) )
    oFD.close()
    iDays = iUptime / ( 86400 )
    iHours = iUptime / 3600  - iDays * 24
    iMinutes = iUptime / 60 - iHours * 60 - iDays * 1440
    iSeconds = iUptime - iMinutes * 60- iHours * 3600 - iDays * 86400
    dInfo[ 'uptime' ] = "%dd %02d:%02d:%02d" % ( iDays, iHours, iMinutes, iSeconds)
    #Load average
    oFD = file( "/proc/loadavg" )
    sLine = oFD.readline()
    oFD.close()
    dInfo[ 'load' ] = " ".join( sLine.split()[:3] )
    dInfo[ 'name' ] = self.serviceInfoDict[ 'serviceName' ]

    return S_OK( dInfo )

####
#
#  Default get Credentials method
#
####

  types_getCredentials = []
  def export_getCredentials( self ):
    return S_OK( self.getRemoteCredentials() )