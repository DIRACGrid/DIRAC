# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Client/MonitoringClient.py,v 1.27 2008/05/05 12:33:39 acasajus Exp $
__RCSID__ = "$Id: MonitoringClient.py,v 1.27 2008/05/05 12:33:39 acasajus Exp $"

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
    self.activitiesDefinitions = {}
    self.activitiesMarks = {}
    self.definitionsToSend = {}
    self.marksToSend = {}
    self.activitiesLock = threading.Lock()
    self.flushingLock = threading.Lock()
    self.timeStep = 60

  def initialize( self ):
    self.logger = gLogger.getSubLogger( "Monitoring" )
    self.logger.debug( "Initializing Monitoring Client")
    self.sourceDict[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup" )
    self.sourceDict[ 'site' ] = gConfig.getValue( "/LocalSite/Site", "Not specified" )
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
    #ExitCallback.registerExitCallback( self.forceFlush )

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
      self.sendingPeriod = max( 60, gConfig.getValue( "%s/SendPeriod" % self.cfgSection, 300 ) )
      self.sendingThread = threading.Thread( target = self.__periodicFlush )
      self.sendingThread.setDaemon( 1 )
      self.sendingThread.start()
      #HACK: Avoid exiting while the thread is starting
      time.sleep( 0.1 )

  def __periodicFlush( self ):
    while self.sendingMode == "periodic":
      self.logger.debug( "Waiting %s seconds to send data (%d threads running)" % ( self.sendingPeriod, threading.activeCount() ) )
      time.sleep( self.sendingPeriod )
      try:
        self.flush()
      except Exception, e:
        gLogger.error( "Error in commiting data to monitoring", str( e ) )

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

  def registerActivity( self, name, description, category, unit, operation, bucketLength = 60 ):
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
    @type  bucketLength: int
    @param bucketLength: Bucket length in seconds
    """
    self.activitiesLock.acquire()
    try:
      self.logger.debug( "Registering activity %s" % name )
      if name not in self.activitiesDefinitions:
        self.activitiesDefinitions[ name ] = { "category" : category,
                                               "description" : description,
                                               "unit" : unit,
                                               "type" : operation,
                                               "bucketLength" : bucketLength
                                              }
        self.activitiesMarks[ name ] = {}
        self.definitionsToSend[ name ] = dict( self.activitiesDefinitions[ name ] )
    finally:
      self.activitiesLock.release()

  def __UTCStepTime( self, acName ):
    stepLength = self.activitiesDefinitions[ acName ][ 'bucketLength' ]
    nowEpoch = int( time.mktime( time.gmtime() ) )
    return nowEpoch - nowEpoch % stepLength

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
      self.logger.debug( "Adding mark to %s" % name )
      markTime = self.__UTCStepTime( name )
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
    consolidatedMarks = {}
    remainderMarks = {}
    for key in self.activitiesMarks:
      if allData:
        lastStepToSend = int( time.mktime( time.gmtime() ) )
      else:
        lastStepToSend = self.__UTCStepTime( key )
      consolidatedMarks[ key ] = {}
      remainderMarks [ key ] = {}
      for markTime in self.activitiesMarks[ key ]:
        markValue = self.activitiesMarks[ key ][ markTime ]
        if markTime >= lastStepToSend:
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

  def flush( self, allData = False ):
    self.flushingLock.acquire()
    self.logger.debug( "Sending information to server" )
    try:
      self.activitiesLock.acquire()
      try:
        self.logger.debug( "Consolidating data...")
        self.__appendMarksToSend( self.__consolidateMarks( allData ) )
      finally:
        self.activitiesLock.release()
      #Commit new activities
      if self.__dataToSend():
        if not self.__disabled():
          if allData:
            timeout = False
          else:
            timeout = 10
          self.__sendData( timeout )
      self.__pruneMarksData()
    finally:
      self.flushingLock.release()

  def __disabled( self ):
    return gConfig.getValue( "%s/DisableMonitoring" % self.cfgSection, "false" ).lower() in \
        ( "yes", "y", "true", "1" )

  def __dataToSend( self ):
    return len( self.definitionsToSend ) or len( self.marksToSend )

  def __appendMarksToSend( self, acMarks ):
    if self.__disabled():
      return
    for acName in acMarks:
      if acName in self.marksToSend:
        for timeMark in acMarks[ acName ]:
          self.marksToSend[ acName ][ timeMark ] = acMarks[ acName ][ timeMark ]
      else:
        self.marksToSend[ acName ] = acMarks[ acName ]

  def __sendData( self, secsTimeout = 60 ):
    if gServiceInterface.serviceRunning():
      self.logger.debug( "Using internal interface to send data")
      rpcClient = gServiceInterface
    else:
      self.logger.debug( "Creating RPC client" )
      rpcClient = RPCClient( "Monitoring/Server", timeout = secsTimeout )
    #Send registrations
    if not self.__sendRegistration( rpcClient ):
      return False
    #Send marks
    maxIteration = 5
    if self.__sendMarks( rpcClient ) and maxIteration:
      maxIteration -= 1
      if not self.__sendRegistration( rpcClient ):
        return False

  def __pruneMarksData(self):
    for acName in self.marksToSend:
      maxBuckets = 86400 / self.activitiesDefinitions[ acName ][ 'bucketLength' ]
      if len( self.marksToSend[ acName ] ) > maxBuckets:
        timeSlots = self.marksToSend[ acName ].keys()
        timeSlots.sort()
        while len( self.marksToSend[ acName ] ) > maxBuckets:
          del( self.marksToSend[ acName ][ timeSlots.pop(0) ] )

  def __sendRegistration( self, rpcClient ):
    if not len( self.definitionsToSend ):
      return True
    self.logger.debug( "Registering activities" )
    retDict = rpcClient.registerActivities( self.sourceDict, self.definitionsToSend )
    if not retDict[ 'OK' ]:
      self.logger.error( "Can't register activities", retDict[ 'Message' ] )
      return False
    self.sourceId = retDict[ 'Value' ]
    self.definitionsToSend = {}
    return True

  def __sendMarks( self, rpcClient ):
    """
    Return true if activities to declare
    """
    assert self.sourceId
    self.logger.debug( "Sending marks" )
    retDict = rpcClient.commitMarks( self.sourceId, self.marksToSend )
    if not retDict[ 'OK' ]:
      self.logger.error( "Can't send activities marks", retDict[ 'Message' ] )
      return False
    acMissedMarks = {}
    if len ( retDict[ 'Value' ] ) > 0:
      self.logger.debug( "There are activities unregistered" )
      for acName in retDict[ 'Value' ]:
        if acName in self.activitiesDefinitions:
          self.definitionsToSend[ acName ] = dict( self.activitiesDefinitions[ acName ] )
          acMissedMarks[ acName ] = self.marksToSend[ acName ]
        else:
          self.logger.debug( "Server reported unregistered activity that does not exist" )
    self.marksToSend = acMissedMarks
    return len( self.definitionsToSend )


  def forceFlush( self, exitCode ):
    self.sendingMode = "none"
    self.flush( allData = True )

gMonitor = MonitoringClient()