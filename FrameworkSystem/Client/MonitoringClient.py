# $HeadURL$
__RCSID__ = "b7db10b (2013-03-06 01:10:41 +0100) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

#import threading
import time
import types
import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities import Time, Network, ThreadScheduler
from DIRAC.Core.DISET.RPCClient import RPCClient

class MonitoringClientActivityNotDefined( Exception ):
  def __init__( self, message ):
    self.message = str(message)
  def __str__( self ):
    return self.message

class MonitoringClientActivityValueTypeError( Exception ):
  def __init__( self, message ):
    self.message = message 
  def __str__( self ):
    return self.message

class MonitoringClientUnknownParameter( Exception ):
  def __init__( self, message ):
    self.message = message 
  def __str__( self ):
    return self.message
    
class MonitoringFlusher:
  """
  This class flushes all monitoring clients registered
  """
  def __init__( self ):
    self.__mcList = []
    ThreadScheduler.gThreadScheduler.addPeriodicTask( 300, self.flush )
    #HACK: Avoid exiting while the thread is starting
    time.sleep( 0.1 )

  def flush( self, allData = False ):
    for mc in self.__mcList:
      mc.flush( allData )

  def registerMonitoringClient( self, mc ):
    if mc not in self.__mcList:
      self.__mcList.append( mc )

gMonitoringFlusher = MonitoringFlusher()

class MonitoringClient(object):

  #Different types of operations
  OP_MEAN = "mean"
  OP_ACUM = "acum"
  OP_SUM = "sum"
  OP_RATE = "rate"

  #Predefined components that can be registered
  COMPONENT_SERVICE = "service"
  COMPONENT_AGENT = "agent"
  COMPONENT_WEB = "web"
  COMPONENT_SCRIPT = "script"

  __validMonitoringValues = ( types.IntType, types.LongType, types.FloatType )

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
    self.__compRegistrationExtraDict = {}
    self.__compCommitExtraDict = {}
    self.__activitiesLock = None #threading.Lock()
    self.__flushingLock = None #threading.Lock()
    self.timeStep = 60
    self.__initialized = False
    self.__enabled = True

  @property
  def activitiesLock( self ):
    if not self.__activitiesLock:
      self.__activitiesLock = LockRing().getLock( "activityLock" )
    return self.__activitiesLock

  @property
  def flushingLock( self ):
    if not self.__flushingLock:
      self.__flushingLock = LockRing().getLock( "flushingLock" )
    return self.__flushingLock
  
  def disable( self ):
    self.__enabled = False

  def enable( self ):
    self.__enabled = True

  def setComponentExtraParam( self, name, value ):
    if name in ( 'version', 'DIRACVersion', 'description', 'startTime', 'platform' ):
      self.__compRegistrationExtraDict[ name ] = str( value )
    elif name in ( 'cycles', 'queries' ):
      self.__compCommitExtraDict[ name ] = str( value )
    else:
      raise MonitoringClientUnknownParameter( "Unknown parameter %s" % name )
      #raise Exception( "Unknown parameter %s" % name )

  def initialize( self ):
    self.logger = gLogger.getSubLogger( "Monitoring" )
    self.logger.debug( "Initializing Monitoring Client" )
    self.sourceDict[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup" )
    self.sourceDict[ 'site' ] = DIRAC.siteName()
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
    gMonitoringFlusher.registerMonitoringClient( self )
    #ExitCallback.registerExitCallback( self.forceFlush )
    self.__initialized = True

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
    if not self.__initialized:
      return
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
    nowEpoch = int( Time.toEpoch() )
    return nowEpoch - nowEpoch % stepLength

  def addMark( self, name, value = 1 ):
    """
    Add a new mark to the specified activity

    @type  name: string
    @param name: Name of the activity to report
    @type  value: number
    @param value: Weight of the mark. By default it's one.
    """
    if not self.__initialized:
      return
    if not self.__enabled:
      return
    if name not in self.activitiesDefinitions:
      raise MonitoringClientActivityNotDefined( "You must register activity %s before adding marks to it" % name )
      #raise Exception( "You must register activity %s before adding marks to it" % name )
    if type( value ) not in self.__validMonitoringValues:
      raise MonitoringClientActivityValueTypeError( "Activity '%s' value's type (%s) is not valid" % ( name, type(value) ) )
      #raise Exception( "Value's type %s is not valid" % value )
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
        lastStepToSend = int( Time.toEpoch() )
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
    if not self.__enabled or not self.__initialized:
      return
    self.flushingLock.acquire()
    self.logger.debug( "Sending information to server" )
    try:
      self.activitiesLock.acquire()
      try:
        self.logger.debug( "Consolidating data..." )
        self.__appendMarksToSend( self.__consolidateMarks( allData ) )
      finally:
        self.activitiesLock.release()
      #Commit new activities
      if self.__dataToSend():
        if not self.__disabled():
          self.__sendData()
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

  def __sendData( self, secsTimeout = False ):
    from DIRAC.FrameworkSystem.private.monitoring.ServiceInterface import gServiceInterface
    if gServiceInterface.serviceRunning():
      self.logger.debug( "Using internal interface to send data" )
      rpcClient = gServiceInterface
    else:
      self.logger.debug( "Creating RPC client" )
      rpcClient = RPCClient( "Framework/Monitoring", timeout = secsTimeout )
    #Send registrations
    if not self.__sendRegistration( rpcClient ):
      return False
    #Send marks
    maxIteration = 5
    if self.__sendMarks( rpcClient ) and maxIteration:
      maxIteration -= 1
      if not self.__sendRegistration( rpcClient ):
        return False

  def __pruneMarksData( self ):
    for acName in self.marksToSend:
      maxBuckets = 86400 / self.activitiesDefinitions[ acName ][ 'bucketLength' ]
      if len( self.marksToSend[ acName ] ) > maxBuckets:
        timeSlots = self.marksToSend[ acName ].keys()
        timeSlots.sort()
        while len( self.marksToSend[ acName ] ) > maxBuckets:
          del( self.marksToSend[ acName ][ timeSlots.pop( 0 ) ] )

  def __sendRegistration( self, rpcClient ):
    if not len( self.definitionsToSend ):
      return True
    self.logger.debug( "Registering activities" )
    retDict = rpcClient.registerActivities( self.sourceDict,
                                            self.definitionsToSend,
                                            self.__compRegistrationExtraDict )
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
    retDict = rpcClient.commitMarks( self.sourceId,
                                     self.marksToSend,
                                     self.__compCommitExtraDict )
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

  def getComponentsStatus( self, condDict ):
    rpcClient = RPCClient( "Framework/Monitoring", timeout = 100 )
    return rpcClient.getComponentsStatus( condDict )

  def __filterComponent( self, component, condDict ):
    for key in condDict:
      if key not in component:
        return False
      condVal = condDict[ key ]
      componentVal = component[ key ]
      if type( condVal ) in ( types.ListType, types.TupleType ):
        if componentVal not in condVal:
          return False
      else:
        if componentVal != condVal:
          return False
    return True

  def getComponentsStatusWebFormatted( self, condDict = {}, sortingList = [], startItem = 0, maxItems = 0 ):
    result = self.getComponentsStatus( condDict )
    if not result[ 'OK' ]:
      return result
    compDict, fields = result[ 'Value' ]
    tabledData = []
    for setup in compDict:
      for type in compDict[ setup ]:
        for name in compDict[ setup ][ type ]:
          for component in compDict[ setup ][ type ][ name ]:
            #How here we are. Now we need to filter the components
            if not self.__filterComponent( component, condDict ):
              continue
            #Add to tabledData!
            row = []
            for field in fields:
              if field not in component:
                row.append( "" )
              else:
                row.append( component[ field ] )
            tabledData.append( row )
    #We've got the data in table form
    #Now it's time to sort it
    if sortingList:
      sortingData = []
      sortField = sortingList[0][0]
      if sortField not in fields:
        return S_ERROR( "Sorting field %s does not exist" % sortField )
      sortDirection = sortingList[0][1]
      fieldIndex = 0
      for i in range( len( fields ) ):
        if fields[i] == sortField:
          fieldIndex = i
          break
      for row in tabledData:
        sortingData.append( ( row[ fieldIndex ], row ) )
      sortingData.sort()
      if sortDirection == "DESC":
        sortingData.reverse()
      tabledData = [ row[1] for row in sortingData ]
    #Now need to limit
    numRows = len( tabledData )
    tabledData = tabledData[ startItem: ]
    if maxItems:
      tabledData = tabledData[ :maxItems ]
    returnData = { 'ParameterNames' : fields,
                   'Records' : tabledData,
                   'TotalRecords' : numRows,
                   }
    return S_OK( returnData )


gMonitor = MonitoringClient()
