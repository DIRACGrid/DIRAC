# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Dispatcher.py,v 1.1 2007/03/09 15:27:47 rgracian Exp $
__RCSID__ = "$Id: Dispatcher.py,v 1.1 2007/03/09 15:27:47 rgracian Exp $"

from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class Dispatcher:
  
  def __init__( self, oServiceConf ):
    self.oServiceConf = oServiceConf
  
  def loadHandler( self ):
    dRetVal = self.__initializeRequestHandlers()
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.__initializeLocks()
    return S_OK()

  def __initializeLocks( self ):
    iMaxWaitingPetitions = self.oServiceConf.getMaxWaitingPetitions()
    self.oLockManager = LockManager( iMaxWaitingPetitions )
    oLockManager = self.oLockManager.createNewLockManager( "%s" % self.sServiceName )
    oRequestHandler = self.dHandlerInfo[ "handlerClass" ]
    self.dHandlerInfo[ "lockManager" ] = oLockManager
    for sMethod in dir( oRequestHandler ):
      if sMethod.find( "export_" ) == 0:
        sRealMethodName = sMethod.replace( "export_", "" )
        iThreadLimit = self.oServiceConf.getMaxThreadsPerFunction( sRealMethodName )
        oLockManager.createNewLock( sRealMethodName, iThreadLimit )    
            
  def __initializeRequestHandlers( self ):
    self.sServiceName = self.oServiceConf.getName()
    sHandlerFileLocation = self.oServiceConf.getHandlerLocation()
    if not sHandlerFileLocation:
      return S_ERROR( "handlerLocation is not defined in %s" % self.oServiceConf.getServiceName() )
    gLogger.debug( "Found a handler", sHandlerFileLocation )
    if sHandlerFileLocation.find( "Handler.py" ) != len( sHandlerFileLocation ) - 10:
      return S_ERROR( "File %s does not have a valid handler name" % sHandlerFileLocation )
    sHandlerFileLocation = sHandlerFileLocation.replace( ".py", "" )
    lServicePath = List.fromChar( sHandlerFileLocation, "/" )
    sHandlerName = lServicePath[-1]
    try:
      oHandlerModule = __import__( ".".join( lServicePath ), 
                                   globals(), 
                                   locals(), sHandlerName )
      oHandlerClass  = getattr( oHandlerModule, sHandlerName )
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't import handler: %s" % str( e ) )
    try:
      oHandlerInitMethod = getattr( oHandlerModule, "initialize%s" % sHandlerName )
      gLogger.debug( "Found initialization function for service" )
    except:
      oHandlerInitMethod = False
      gLogger.debug( "Not found initialization function for service" )
    self.dHandlerInfo = {}
    self.dHandlerInfo[ "handlerName" ] = sHandlerName
    self.dHandlerInfo[ "handlerModule" ] = oHandlerModule
    self.dHandlerInfo[ "handlerClass" ] = oHandlerClass
    self.dHandlerInfo[ "handlerInitialization" ] = oHandlerInitMethod
    return S_OK()
      
  def getHandlerForService( self, sServiceName ):
    gLogger.debug( "Dispatching action", "%s vs %s" % ( self.sServiceName, sServiceName  ) )
    if self.sServiceName == sServiceName:
      return self.dHandlerInfo
    else:
      return False
    
  def getHandlerInfo( self ):
    return self.dHandlerInfo
  
  def getServiceName( self ):
    return self.sServiceName
    
  def lock( self ):
    self.oLockManager.lockGlobal()
    
  def unlock( self ):
    self.oLockManager.unlockGlobal()
