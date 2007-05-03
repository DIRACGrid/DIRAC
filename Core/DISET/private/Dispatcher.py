# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Dispatcher.py,v 1.2 2007/05/03 18:59:48 acasajus Exp $
__RCSID__ = "$Id: Dispatcher.py,v 1.2 2007/05/03 18:59:48 acasajus Exp $"

from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class Dispatcher:

  def __init__( self, serviceCfg ):
    self.serviceCfg = serviceCfg
    self.serviceName = self.serviceCfg.getName()

  def loadHandler( self ):
    retVal = self.__initializeRequestHandlers()
    if not retVal[ 'OK' ]:
      return retVal
    self.__initializeLocks()
    return S_OK()

  def __initializeLocks( self ):
    maxWaitingRequests = self.serviceCfg.getMaxWaitingPetitions()
    self.lockManager = LockManager( maxWaitingRequests )
    funcLockManager = self.lockManager.createNewLockManager( self.serviceName )
    requestHandler = self.handlerDict[ "handlerClass" ]
    self.handlerDict[ "lockManager" ] = funcLockManager
    for methodName in dir( requestHandler ):
      if methodName.find( "export_" ) == 0:
        exportedMethodName = methodName.replace( "export_", "" )
        threadLimit = self.serviceCfg.getMaxThreadsPerFunction( exportedMethodName )
        funcLockManager.createNewLock( exportedMethodName, threadLimit )

  def __initializeRequestHandlers( self ):
    handlerLocation = self.serviceCfg.getHandlerLocation()
    if not handlerLocation:
      return S_ERROR( "handlerLocation is not defined in %s" % self.serviceCfg.getSectionPath() )
    gLogger.debug( "Found a handler", handlerLocation )
    if handlerLocation.find( "Handler.py" ) != len( handlerLocation ) - 10:
      return S_ERROR( "File %s does not have a valid handler name" % handlerLocation )
    handlerLocation = handlerLocation.replace( ".py", "" )
    lServicePath = List.fromChar( handlerLocation, "/" )
    handlerName = lServicePath[-1]
    try:
      handlerModule = __import__( ".".join( lServicePath ),
                                   globals(),
                                   locals(), handlerName )
      handlerClass  = getattr( handlerModule, handlerName )
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't import handler: %s" % str( e ) )
    try:
      handlerInitMethod = getattr( handlerModule, "initialize%s" % handlerName )
      gLogger.debug( "Found initialization function for service" )
    except:
      handlerInitMethod = False
      gLogger.debug( "Not found initialization function for service" )
    self.handlerDict = {}
    self.handlerDict[ "handlerName" ] = handlerName
    self.handlerDict[ "handlerModule" ] = handlerModule
    self.handlerDict[ "handlerClass" ] = handlerClass
    self.handlerDict[ "handlerInitialization" ] = handlerInitMethod
    return S_OK()

  def getHandlerForService( self, serviceName ):
    gLogger.debug( "Dispatching action", "%s vs %s" % ( self.serviceName, serviceName  ) )
    if self.serviceName == serviceName:
      return self.handlerDict
    else:
      return False

  def getHandlerInfo( self ):
    return self.handlerDict

  def lock( self ):
    self.lockManager.lockGlobal()

  def unlock( self ):
    self.lockManager.unlockGlobal()
