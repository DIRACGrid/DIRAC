# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Client/MonitoringClient.py,v 1.3 2007/11/15 16:03:54 acasajus Exp $
__RCSID__ = "$Id: MonitoringClient.py,v 1.3 2007/11/15 16:03:54 acasajus Exp $"

import threading
import time
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities import Time, ExitCallback
from DIRAC.MonitoringSystem.private.ServiceInterface import gServiceInterface
from DIRAC.Core.DISET.RPCClient import RPCClient

class MonitoringClient:

  #Different types of operations
  OP_MEAN = "mean"
  OP_SUM  = "sum"
  OP_RATE = "rate"

  #Predefined components that can be registered
  COMPONENT_SERVICE = "service"
  COMPONENT_AGENT   = "agent"

  def __init__( self ):
    self.sourceId = 0
    self.sourceDict = {}
    self.sourceDict[ 'componentType' ] = "unknown"
    self.sourceDict[ 'componentName' ] = "unknown"
    self.sourceDict[ 'componentLocation' ] = "unknown"
    self.newActivitiesDict = {}
    self.activitiesDefinitions = {}
    self.activitiesMarks = {}
    self.activitiesLock = threading.Lock()
    self.flushingLock = threading.Lock()
    self.timeStep = 60

  def initialize( self ):
    self.logger = gLogger.getSubLogger( "Monitoring" )
    self.logger.info( "Initializing Service Monitor")
    self.sourceDict[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup" )
    self.sourceDict[ 'site' ] = gConfig.getValue( "/DIRAC/Site", "" )
    if self.sourceDict[ 'componentType' ] == self.COMPONENT_SERVICE:
      self.cfgSection = PathFinder.getSystemSection( self.sourceDict[ 'componentName' ] )
    elif self.sourceDict[ 'componentType' ] == self.COMPONENT_AGENT:
      self.cfgSection = PathFinder.getAgentSection( self.sourceDict[ 'componentName' ] )
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
      self.sendingPeriod = max( 2, gConfig.getValue( "%s/SendPeriod" % self.cfgSection, 2 ) )
      self.sendingThread = threading.Thread( target = self.__periodicFlush )
      self.sendingThread.start()

  def __periodicFlush( self ):
    while self.sendingMode == "periodic":
      self.logger.info( "Waiting %s seconds to send data" % self.sendingPeriod )
      time.sleep( self.sendingPeriod )
      self.flush()

  def setComponentLocation( self, componentLocation ):
    """
    Set the location of the component reporting.

    @type  componentLocation: string
    @param componentLocation: Location of the component reporting
    """
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
      self.logger.info( "Registering activity %s" % name )
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

  def __consolidateMarks(self):
    """
      Copies all marks except last step ones
      and consolidates them
    """
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
          consolidatedMarks[ key ][ markTime ] = totalValue
      if len( consolidatedMarks[ key ] ) == 0:
        del( consolidatedMarks[ key ] )
    self.activitiesMarks = remainderMarks
    return consolidatedMarks

  def flush( self ):
    self.flushingLock.acquire()
    self.logger.info( "Sending information to server" )
    try:
      self.activitiesLock.acquire()
      try:
        self.logger.verbose( "Consolidating data...")
        activitiesToRegister = {}
        if len( self.newActivitiesDict ) > 0:
          activitiesToRegister = self.newActivitiesDict
          self.newActivitiesDict = {}
        marksDict = self.__consolidateMarks()
      finally:
        self.activitiesLock.release()
      #Commit new activities
      if gConfig.getValue( "%s/DisableMonitoring" % self.cfgSection, "true" ).lower() in \
            ( "yes", "y", "true", "1" ):
        self.logger.info( "Sending data has been disabled" )
        return
      if len( activitiesToRegister ) or len( marksDict ):
        self.__sendData( activitiesToRegister, marksDict )
    finally:
      self.flushingLock.release()

  def __sendData( self, acRegister, acMarks ):
    if gServiceInterface.serviceRunning():
      rpcClient = gServiceInterface
    else:
      rpcClient = RPCClient( "Monitoring/Server", timeout = 30 )
    if len( acRegister ):
      self.logger.verbose( "Registering activities" )
      retDict = rpcClient.registerActivities( self.sourceDict, acRegister )
      if retDict[ 'OK' ]:
        self.sourceId = retDict[ 'Value' ]
      else:
        self.logger.error( "Can't register activities", retDict[ 'Message' ] )
    if len( acMarks ):
      assert self.sourceId
      self.logger.verbose( "Sending marks" )
      retDict = rpcClient.commitMarks( self.sourceId, acMarks )
      if not retDict[ 'OK' ]:
        self.logger.error( "Can't send activities marks", retDict[ 'Message' ] )

  def forceFlush( self, exitCode ):
    self.flush()

gMonitor = MonitoringClient()