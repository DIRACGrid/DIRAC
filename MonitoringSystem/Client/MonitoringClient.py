# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Client/MonitoringClient.py,v 1.9 2007/12/21 16:04:44 acasajus Exp $
__RCSID__ = "$Id: MonitoringClient.py,v 1.9 2007/12/21 16:04:44 acasajus Exp $"

import threading
import time
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities import Time, ExitCallback, Network
from DIRAC.MonitoringSystem.private.ServiceInterface import gServiceInterface
from DIRAC.Core.DISET.RPCClient import RPCClient

class MonitoringClient:

  #Different types of operations
  OP_MEAN = "mean"
  OP_ACUM = "acum"
  OP_SUM  = "sum"
  OP_RATE = "rate"

  #Predefined components that can be registered
  COMPONENT_SERVICE = "service"
  COMPONENT_AGENT   = "agent"
  COMPONENT_WEB     = "web"
  COMPONENT_SCRIPT  = "script"

  def __init__( self ):
    self.sourceId = 0
    self.sourceDict = {}
    self.sourceDict[ 'componentType' ] = "unknown"
    self.sourceDict[ 'componentName' ] = "unknown"
    self.sourceDict[ 'componentLocation' ] = "unknown"
    self.newActivitiesDict = {}
    self.activitiesDefinitions = {}
    self.activitiesMarks = {}
    self.failedTransmissions = []
    self.activitiesLock = threading.Lock()
    self.flushingLock = threading.Lock()
    self.timeStep = 60
    self.enabled = True

  def initialize( self ):
    self.logger = gLogger.getSubLogger( "Monitoring" )
    self.logger.info( "Initializing Service Monitor")
    self.sourceDict[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup" )
    self.sourceDict[ 'site' ] = gConfig.getValue( "/DIRAC/Site", "" )
    if self.sourceDict[ 'componentType' ] == self.COMPONENT_SERVICE:
      self.cfgSection = PathFinder.getSystemSection( self.sourceDict[ 'componentName' ] )
    elif self.sourceDict[ 'componentType' ] == self.COMPONENT_AGENT:
      self.cfgSection = PathFinder.getAgentSection( self.sourceDict[ 'componentName' ] )
      self.setComponentLocation( Network.getFQDN() )
    elif self.sourceDict[ 'componentType' ] == self.COMPONENT_WEB:
      self.cfgSection = "/Website"
      self.setComponentLocation( 'http://%s' % Network.getFQDN() )
      self.setComponentName( 'Web' )
    elif self.sourceDict[ 'componentType' ] == self.COMPONENT_SCRIPT:
      self.cfgSection = "/Script"
    else:
      raise Exception( "Component type has not been defined" )
    self.__initializeSendMode()
    ExitCallback.registerExitCallback( self.forceFlush )

  def __initializeSendMode( self ):
    """
    Initialize sending mode
    Reads configuration options:
      SendMode:
        - periodic : Data will be sent periodically
        - manual : flush() method has to be called manually
      SendPeriod:
        - <number> : Seconds between periodic updates. Minimum value is 300
    """
    self.sendingMode = gConfig.getValue( "%s/SendMode" % self.cfgSection, "periodic" )
    if self.sendingMode == "periodic":
      self.sendingPeriod = max( 60, gConfig.getValue( "%s/SendPeriod" % self.cfgSection, 60 ) )
      self.sendingThread = threading.Thread( target = self.__periodicFlush )
      self.sendingThread.setDaemon( 1 )
      self.sendingThread.start()

  def __periodicFlush( self ):
    while self.sendingMode == "periodic":
      self.logger.verbose( "Waiting %s seconds to send data" % self.sendingPeriod )
      time.sleep( self.sendingPeriod )
      self.flush()

  def setComponentLocation( self, componentLocation = False ):
    """
    Set the location of the component reporting.

    @type  componentLocation: string
    @param componentLocation: Location of the component reporting
    """
    if not componentLocation:
      self.sourceDict[ 'componentLocation' ] = gConfig.getValue( "/Site" )
    else:
      self.sourceDict[ 'componentLocation' ] = componentLocation

  def setComponentName( self, componentName ):
    """
    Set the name of the component reporting.

    @type  componentName: string
    @param componentName: Name of the component reporting
    """
    self.sourceDict[ 'componentName' ] = componentName

  def setComponentType( self, componentType ):
    """
    Define the type of component reporting data.

    @type  componentType: string
    @param componentType: Defines the grouping of the host by type. All the possibilities
                            are defined in the Constants.py file
    """
    self.sourceDict[ 'componentType' ] = componentType

  def registerActivity( self, name, description, category, unit, operation, ):
    """
    Register new activity. Before reporting information to the server, the activity
    must be registered.

    @type  name: string
    @param name: Id of the activity to report
    @type description: string
    @param description: Description of the activity
    @type  category: string
    @param category: Grouping of the activity
    @type  unit: string
    @param unit: String representing the unit that will be printed in the plots
    @type  operation: string
    @param operation: Type of data operation to represent data. All the possibilities
                        are defined in the Constants.py file
    """
    self.activitiesLock.acquire()
    try:
      self.logger.verbose( "Registering activity %s" % name )
      if name not in self.activitiesDefinitions:
        self.activitiesDefinitions[ name ] = { "category" : category,
                                               "description" : description,
                                               "unit" : unit,
                                               "type" : operation
                                              }
        self.activitiesMarks[ name ] = {}
        self.newActivitiesDict[ name ] = self.activitiesDefinitions[ name ]
    finally:
      self.activitiesLock.release()

  def __UTCStepTime(self):
    return int( time.mktime( time.gmtime() ) / self.timeStep ) * self.timeStep

  def addMark( self, name, value = 1 ):
    """
    Add a new mark to the specified activity

    @type  name: string
    @param name: Name of the activity to report
    @type  value: number
    @param value: Weight of the mark. By default it's one.
    """
    if name not in self.activitiesDefinitions:
      raise Exception( "You must register activity %s before adding marks to it" % name)
    self.activitiesLock.acquire()
    try:
      self.logger.verbose( "Adding mark to %s" % name )
      markTime = self.__UTCStepTime()
      if markTime in self.activitiesMarks[ name ]:
        self.activitiesMarks[ name ][ markTime ].append( value )
      else:
        self.activitiesMarks[ name ][ markTime ] = [ value ]
    finally:
      self.activitiesLock.release()

  def __consolidateMarks( self, allData ):
    """
      Copies all marks except last step ones
      and consolidates them
    """
    if allData:
      lastStepToSend = int( time.mktime( time.gmtime() ) )
    else:
      lastStepToSend = self.__UTCStepTime() - self.timeStep
    consolidatedMarks = {}
    remainderMarks = {}
    for key in self.activitiesMarks:
      consolidatedMarks[ key ] = {}
      remainderMarks [ key ] = {}
      for markTime in self.activitiesMarks[ key ]:
        markValue = self.activitiesMarks[ key ][ markTime ]
        if markTime > lastStepToSend:
          remainderMarks[ key ][ markTime ] = markValue
        else:
          consolidatedMarks[ key ][ markTime ] = markValue
          #Consolidate the copied ones
          totalValue = 0
          for mark in consolidatedMarks[ key ][ markTime ]:
            totalValue += mark
          if self.activitiesDefinitions[ key ][ 'type' ] == self.OP_MEAN:
            totalValue /= len( consolidatedMarks[ key ][ markTime ] )
          elif self.activitiesDefinitions[ key ][ 'type' ] == self.OP_RATE:
            totalValue /= 60
          consolidatedMarks[ key ][ markTime ] = totalValue
      if len( consolidatedMarks[ key ] ) == 0:
        del( consolidatedMarks[ key ] )
    self.activitiesMarks = remainderMarks
    return consolidatedMarks

  def flush( self, allData = False ):
    self.flushingLock.acquire()
    self.logger.verbose( "Sending information to server" )
    try:
      self.activitiesLock.acquire()
      try:
        self.logger.verbose( "Consolidating data...")
        activitiesToRegister = {}
        if len( self.newActivitiesDict ) > 0:
          activitiesToRegister = self.newActivitiesDict
          self.newActivitiesDict = {}
        marksDict = self.__consolidateMarks( allData )
      finally:
        self.activitiesLock.release()
      #Commit new activities
      if gConfig.getValue( "%s/DisableMonitoring" % self.cfgSection, "false" ).lower() in \
            ( "yes", "y", "true", "1" ):
        self.logger.verbose( "Sending data has been disabled" )
        return
      if len( activitiesToRegister ) or len( marksDict ):
        if allData:
          timeout = False
        else:
          timeout = 10
        self.__sendData( activitiesToRegister, marksDict, timeout )
    finally:
      self.flushingLock.release()

  def __sendData( self, acRegister, acMarks, secsTimeout = 30 ):
    if not self.enabled:
      print "OUT"
      return
    if gServiceInterface.serviceRunning():
      rpcClient = gServiceInterface
    else:
      rpcClient = RPCClient( "Monitoring/Server", timeout = secsTimeout )
    if not self.__sendFailed( rpcClient ):
      return
    if len( acRegister ):
      if not self.__sendRegistration( rpcClient, acRegister ):
        self.failedTransmissions.append( ( self.__sendMarks, acMarks ) )
        return
    if len( acMarks ):
      self.__sendMarks( rpcClient, acMarks )

  def __sendFailed( self, rpcClient ):
    while len( self.failedTransmissions ) > 100:
      self.failedTransmissions.pop(0)
    while len( self.failedTransmissions ) > 0:
      transTuple = self.failedTransmissions[0]
      if not transTuple[0]( rpcClient, transTuple[1] ):
        return False
      self.failedTransmissions.pop(0)
    return True

  def __sendRegistration( self, rpcClient, acRegister ):
    self.logger.verbose( "Registering activities" )
    retDict = rpcClient.registerActivities( self.sourceDict, acRegister )
    if not retDict[ 'OK' ]:
      self.logger.error( "Can't register activities", retDict[ 'Message' ] )
      self.failedTransmissions.append( ( self.__sendRegistration, acRegister ) )
      return False
    self.sourceId = retDict[ 'Value' ]
    return True

  def __sendMarks( self, rpcClient, acMarks ):
    assert self.sourceId
    self.logger.verbose( "Sending marks" )
    retDict = rpcClient.commitMarks( self.sourceId, acMarks )
    if not retDict[ 'OK' ]:
      self.logger.error( "Can't send activities marks", retDict[ 'Message' ] )
      self.failedTransmissions.append( ( self.__sendMarks, acMarks ) )
      return False
    if len ( retDict[ 'Value' ] ) > 0:
      gLogger.verbose( "There are activities unregistered" )
      acRegister = {}
      acMissedMarks = {}
      for acName in retDict[ 'Value' ]:
        if acName in self.activitiesDefinitions:
          acRegister[ acName ] = self.activitiesDefinitions[ acName ]
          acMissedMarks[ acName ] = acMarks[ acName ]
        else:
          gLogger.verbose( "Server reported unregistered activity that does not exist" )
      gLogger.verbose( "Reregistering activities %s" % ", ".join( acRegister.keys() ) )
      return self.__sendRegistration( rpcClient, acRegister ) and rpcClient.commitMarks( self.sourceId, acMissedMarks )[ 'OK' ]
    return True


  def forceFlush( self, exitCode ):
    self.sendingMode = "none"
    self.flush( allData = True )

gMonitor = MonitoringClient()