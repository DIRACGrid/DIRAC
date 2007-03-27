# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/RequestHandler.py,v 1.2 2007/03/27 10:56:46 acasajus Exp $
__RCSID__ = "$Id: RequestHandler.py,v 1.2 2007/03/27 10:56:46 acasajus Exp $"

from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import Time

class RequestHandler:
  
  def __init__( self, stServiceInfo, 
                stServerAddress, 
                oClientTransport,
                oLockManager,
                oServiceConfiguration ):
    self.stServiceInfo = stServiceInfo
    self.stServerAddress = stServerAddress
    self.oClientTransport = oClientTransport
    self.oLockManager = oLockManager
    self.oServiceConfiguration = oServiceConfiguration
    
  def initialize( self ):
    pass

  def getServiceConfig( self ):
    return self.oServiceConfiguration
    
  def executeAction( self, sAction ):
    if sAction == "RPC":
      gLogger.verbose( "Executing RPC action" )
      self.__doRPC()
    elif sAction == "FFC":
      gLogger.verbose( "Executing FFC action" )
      self.__doFileTransfer( "FromClient" )
    elif sAction == "FTC":
      gLogger.verbose( "Executing FTC action" )
      self.__doFileTransfer( "ToClient" )
    else:
      raise RuntimeException( "Unknown action (%s)" % sAction )
    
#####
#
# File to/from Server Methods
#
#####
    
  def __doFileTransfer( self, sDirection ):
    sFileId = self.oClientTransport.receiveData()
    if not self.__authorizeTransferQuery( sDirection, sFileId ):
      self.oClientTransport.sendData( S_ERROR( "Unauthorized query" ) )
      return
    if "File%sCallback" % sDirection not in dir( self ):
      self.oClientTransport.sendData( S_ERROR( "Service can't transfer files in that direction" ) )
    self.oClientTransport.sendData( S_OK() )
    oFH = FileHelper( self.oClientTransport )
    if sDirection == "FromClient":
      uRetVal = self.FileFromClientCallback( sFileId, oFH )
    elif sDirection == "ToClient" :
      uRetVal = self.FileToClientCallback( sFileId, oFH )
    else:
      S_ERROR( "Direction does not exist!!!" )
    print uRetVal
    if not oFH.finishedTransmission():
      gLogger.error( "You haven't finished receiving the file", sFileId )
    self.oClientTransport.sendData( uRetVal )
    
  def __authorizeTransferQuery( self, sDirection, sFileId ):
    #TODO
    return True
 
    
#####
#
# RPC Methods
#
#####
    
  def __doRPC( self ):
    stRPCQuery = self.oClientTransport.receiveData()
    if not self.__RPCAuthorizeQuery( stRPCQuery ):
      self.oClientTransport.sendData( S_ERROR( "Unauthorized query" ) )
    else:
      uReturnValue = self.__RPCCallFunction( stRPCQuery )
      self.oClientTransport.sendData( uReturnValue )
  
  def __RPCAuthorizeQuery( self, stRPCQuery ):
    #TODO: Authorize correctly
    return True
    
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
    self.oLockManager.lock( stRPCQuery[0] )
    try:
      try:
        print stRPCQuery[1]
        uReturnValue = oMethod( *stRPCQuery[1] )
        return uReturnValue
      finally:
        self.oLockManager.unlock( stRPCQuery[0] )
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
                                                                                  "/".join( self.stServiceInfo ), 
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
    dInfo[ 'name' ] = str( self.stServiceInfo )
    
    return S_OK( dInfo )
