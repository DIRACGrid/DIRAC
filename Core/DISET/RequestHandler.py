# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RequestHandler.py,v 1.15 2007/05/29 16:54:16 acasajus Exp $
__RCSID__ = "$Id: RequestHandler.py,v 1.15 2007/05/29 16:54:16 acasajus Exp $"

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

  def executeAction( self, action ):
    if action == "RPC":
      gLogger.verbose( "Executing RPC action" )
      self.__doRPC()
    elif action == "FFC":
      gLogger.verbose( "Executing FFC action" )
      self.__doFileTransfer( "FromClient" )
    elif action == "FTC":
      gLogger.verbose( "Executing FTC action" )
      self.__doFileTransfer( "ToClient" )
    else:
      raise RuntimeException( "Unknown action (%s)" % action )

#####
#
# File to/from Server Methods
#
#####

  def __doFileTransfer( self, sDirection ):
    fileInfo = self.transport.receiveData()
    if not self.__authQuery( "FileTransfer/%s" % sDirection ):
      self.transport.sendData( S_ERROR( "Unauthorized query" ) )
      return
    if "file%sCallback" % sDirection not in dir( self ):
      self.transport.sendData( S_ERROR( "Service can't transfer files in that direction" ) )
      return
    self.transport.sendData( S_OK() )
    fileHelper = FileHelper( self.transport )
    if sDirection == "FromClient":
      uRetVal = self.transfer_fromClient( fileInfo[0], fileInfo[1], fileHelper )
    elif sDirection == "ToClient" :
      uRetVal = self.transfer_toClient( fileInfo[0], fileHelper )
    else:
      S_ERROR( "Direction does not exist!!!" )
    if not fileHelper.finishedTransmission():
      gLogger.error( "You haven't finished receiving the file", str( fileInfo ) )
    self.transport.sendData( uRetVal )

  def fileFromClientCallback( self, fileId, fileSize, fileHelper ):
    return S_ERROR( "This server does no allow receiving files" )

  def fileToClientCallback( self, fileId, fileHelper ):
    return S_ERROR( "This server does no allow sending files" )

#####
#
# RPC Methods
#
#####

  def __doRPC( self ):
    stRPCQuery = self.transport.receiveData()
    if not self.__authQuery( stRPCQuery[0] ):
      credDict = self.getRemoteCredentials()
      if 'username' in credDict.keys():
        username = credDict[ 'username' ]
      else:
        username = 'unauthenticated'
      gLogger.verbose( "Unauthorized query", "%s by %s" % ( stRPCQuery[0], username ) )
      self.transport.sendData( S_ERROR( "Unauthorized query" ) )
    else:
      self.__logRPCQuery( stRPCQuery )
      uReturnValue = self.__RPCCallFunction( stRPCQuery )
      self.transport.sendData( uReturnValue )

  def __logRPCQuery( self, stRPCQuery ):
    peerCreds = self.getRemoteCredentials()
    if peerCreds.has_key( 'username' ):
      peerId = "[%s]" % peerCreds[ 'username' ]
    else:
      peerId = ""
    args = [ str( arg )[:20] for arg in stRPCQuery[1] ]
    kwargs = [ "%s = %s" % ( str( key ), str( stRPCQuery[2][key] ) ) for key in stRPCQuery[2].keys() ]
    finalArgs = ", ".join( args + kwargs )
    gLogger.info( "Executing RPC", "%s %s( %s )" % ( peerId, stRPCQuery[0], finalArgs ) )

  def __RPCCallFunction( self, stRPCQuery ):
    sRealMethodName = "export_%s" % stRPCQuery[0]
    gLogger.debug( "RPC to %s" % sRealMethodName )
    try:
      sRealMethodName = "export_%s" % stRPCQuery[0]
      oMethod = getattr( self, sRealMethodName )
    except:
      return S_ERROR( "Unknown method %s" % stRPCQuery[0] )
    dRetVal = self.__RPCCheckExpectedArgumentTypes( stRPCQuery )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.lockManager.lock( stRPCQuery[0] )
    try:
      try:
        uReturnValue = oMethod( *stRPCQuery[1], **stRPCQuery[2] )
        return uReturnValue
      finally:
        self.lockManager.unlock( stRPCQuery[0] )
    except Exception, v:
      gLogger.exception( "Uncaught exception when serving RPC", "Function %s" % stRPCQuery[0] )
      return S_ERROR( "Error while serving %s: %s" % ( stRPCQuery[0], str( v ) ) )

  def __RPCCheckExpectedArgumentTypes( self, stRPCQuery ):
    sListName = "types_%s" % stRPCQuery[0]
    try:
      oTypesList = getattr( self, sListName )
    except:
      gLogger.error( "There's no types info for method export_%s" % stRPCQuery[0] )
      return S_ERROR( "Handler error for server %s while processing method %s" % (
                                                                                  "/".join( self.serviceInfoTuple ),
                                                                                  stRPCQuery[0] ) )
    try:
      for iIndex in range( min( len( oTypesList ), len( stRPCQuery[1] ) ) ):
        if not type( stRPCQuery[1][ iIndex ] ) == oTypesList[ iIndex ]:
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
    dInfo[ 'name' ] = str( self.serviceInfoTuple )

    return S_OK( dInfo )

####
#
#  Default get Credentials method
#
####

  types_getCredentials = []
  def export_getCredentials( self ):
    return S_OK( self.getRemoteCredentials() )