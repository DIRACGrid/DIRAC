########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/DB/ComponentMonitoringDB.py,v 1.6 2009/02/24 16:50:27 acasajus Exp $
########################################################################
""" ComponentMonitoring class is a front-end to the Component monitoring Database
"""

__RCSID__ = "$Id: ComponentMonitoringDB.py,v 1.6 2009/02/24 16:50:27 acasajus Exp $"

import time
import random
import md5
import types
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time

class ComponentMonitoringDB(DB):

  def __init__(self, requireVoms = False,
               useMyProxy = False,
               maxQueueSize = 10 ):
    DB.__init__(self,'ComponentMonitoringDB','Monitoring/ComponentMonitoringDB',maxQueueSize)
    random.seed()
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])
    self.__optionalFields = ( 'startTime', 'cycles', 'version', 'queries',
                              'DIRACVersion', 'description', 'platform' )

  def getOptionalFields(self):
    return self.__optionalFields

  def __getTableName( self, name ):
    return "compmon_%s" % name

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    tN = self.__getTableName( "Components" )
    if tN not in tablesInDB:
      tablesD[ tN ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                     'ComponentName' : 'VARCHAR(255) NOT NULL',
                                     'Setup' : 'VARCHAR(255) NOT NULL',
                                     'Type' : 'ENUM ( "service", "agent" ) NOT NULL',
                                     'Host' : 'VARCHAR(255) NOT NULL',
                                     'Port' : 'INTEGER DEFAULT 0',
                                     'LastHeartbeat' : 'DATETIME NOT NULL',
                                     'StartTime' : 'DATETIME NOT NULL',
                                     'LoggingState' : 'VARCHAR(64) DEFAULT "unknown"',
                                     'Cycles' : 'INTEGER',
                                     'Queries' : 'INTEGER'
                                   },
                                   'PrimaryKey' : 'Id',
                                   'Indexes' : { 'ComponentIndex' : [ 'ComponentName', 'Setup', 'Host', 'Port' ],
                                                 'TypeIndex' : [ 'Type' ],
                                               }
                      }

    tN = self.__getTableName( "VersionHistory" )
    if tN not in tablesInDB:
      tablesD[ tN ] = { 'Fields' : { 'CompId' : 'INTEGER NOT NULL',
                                     'Timestamp' : 'DATETIME NOT NULL',
                                     'Version' : 'VARCHAR(255)',
                                     'DIRACVersion' : 'VARCHAR(255) NOT NULL',
                                     'Platform' : 'VARCHAR(255) NOT NULL',
                                     'Description' : 'BLOB',
                                   },
                                  'Indexes' : { 'Component' : [ 'CompId' ] }
                      }

    return self._createTables( tablesD )

  def __datetime2str( self, dt ):
    if type( dt ) == types.StringType:
      return dt
    return "%s-%s-%s %s:%s:%s" % ( dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second )

  def __registerIfNotThere( self, compDict ):
    """
    Register the component if it's not there
    """
    sqlCond = []
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName( "Components" )
    for field in ( 'componentName', 'setup', 'type', 'host', 'port' ):
      if field not in compDict:
        if field == 'port':
          continue
        return S_ERROR( "Missing %s field in the component dict" % field )
      value = compDict[ field ]
      field = field.capitalize()
      sqlInsertFields.append( field )
      sqlInsertValues.append( "'%s'" % value )
      sqlCond.append( "%s = '%s'" % ( field, value ) )
    compLogName = ":".join( sqlInsertValues ).replace("'","")
    self.log.info( "Trying to register %s" % compLogName )
    result = self._query( "SELECT id FROM `%s` WHERE %s" % ( tableName, " AND ".join( sqlCond ) ) )
    if not result[ 'OK' ]:
      self.log.error( "Cannot register %s: %s" % ( compLogName, result[ 'Message' ] ) )
      return result
    if len( result[ 'Value' ] ):
      compId = result[ 'Value' ][0][0]
      self.log.info( "%s has compId %s" % ( compLogName, compId ) )
      return S_OK( compId )
    #It's not there, we just need to insert it
    sqlInsertFields.append( "LastHeartbeat" )
    sqlInsertValues.append( "UTC_TIMESTAMP()" )
    if 'startTime' in compDict:
      sqlInsertFields.append( "StartTime" )
      val = compDict[ 'startTime' ]
      if type( val ) in Time._allDateTypes:
        val = self.__datetime2str( val )
      sqlInsertValues.append( "'%s'" % val )
    for field in ( 'cycles', 'queries' ):
      if field not in compDict:
        compDict[ field ] = 0
      value = compDict[ field ]
      field = field.capitalize()
      sqlInsertFields.append( field )
      sqlInsertValues.append( str( value ) )
    self.log.info( "Registering component %s" % compLogName )
    result = self._update( "INSERT INTO `%s` ( %s ) VALUES ( %s )" % ( tableName,
                                                                       ", ".join( sqlInsertFields ),
                                                                       ", ".join( sqlInsertValues ) ) )
    if not result[ 'OK' ]:
      return result
    compId = result[ 'lastRowId' ]
    self.log.info( "%s has compId %s" % ( compLogName, compId ) )
    return S_OK( compId )

  def __updateVersionHistoryIfNeeded( self, compId, compDict ):
    """
    Register the component if it's not there
    """
    sqlCond = [ "CompId=%s" % compId ]
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName( "VersionHistory" )
    for field in ( 'version', 'DIRACVersion', 'platform' ):
      if field not in compDict:
        return S_ERROR( "Missing %s field in the component dict" % field )
      value = compDict[ field ]
      field = field.capitalize()
      sqlInsertFields.append( field )
      sqlInsertValues.append( "'%s'" % value )
      sqlCond.append( "%s = '%s'" % ( field, value ) )
    result = self._query( "SELECT CompId FROM `%s` WHERE %s" % ( tableName, " AND ".join( sqlCond ) ) )
    if not result[ 'OK' ]:
      return result
    if len( result[ 'Value' ] ):
      return S_OK( compId )
    #It's not there, we just need to insert it
    sqlInsertFields.append( 'CompId' )
    sqlInsertValues.append( str( compId ) )
    sqlInsertFields.append( 'Timestamp' )
    sqlInsertValues.append( 'UTC_TIMESTAMP()' )
    if 'description' in compDict:
      sqlInsertFields.append( "Description" )
      result = self._escapeString( compDict[ 'description' ] )
      if not result[ 'OK' ]:
        return result
      sqlInsertValues.append( result[ 'Value' ] )
    result = self._update( "INSERT INTO `%s` ( %s ) VALUES ( %s )" % ( tableName,
                                                                       ", ".join( sqlInsertFields ),
                                                                       ", ".join( sqlInsertValues ) ) )
    if not result[ 'OK' ]:
      return result
    return S_OK( compId )

  def registerComponent( self, compDict, shallow = False ):
    """
    Register a new component in the DB. If it's already registered return id
    """
    result = self.__registerIfNotThere( compDict )
    if not result[ 'OK' ]:
      return result
    compId = result[ 'Value' ]
    if shallow:
      return S_OK( compId )
    #Check if something has changed in the version history
    result = self.__updateVersionHistoryIfNeeded( compId, compDict )
    if not result[ 'OK' ]:
      return result
    return S_OK( compId )

  def heartbeat( self, compDict ):
    """
    Update heartbeat
    """
    if 'compId' not in compDict:
      result = self.__registerIfNotThere( compDict )
      if not result[ 'OK' ]:
        return result
      compId = result[ 'Value' ]
      compDict[ 'compId' ] = compId
    sqlUpdateFields = [ 'LastHeartbeat=UTC_TIMESTAMP()' ]
    for field in ( 'cycles', 'queries' ):
      value = 0
      if field in compDict:
        value = compDict[ field ]
      sqlUpdateFields.append( "%s=%s" % ( field.capitalize(), value ) )
    if 'startTime' in compDict:
      sqlUpdateFields.append( "StartTime='%s'" % compDict[ 'startTime' ] )
    return self._update( "UPDATE `%s` SET %s WHERE Id=%s" % ( self.__getTableName( "Components" ),
                                                                       ", ".join( sqlUpdateFields ),
                                                                       compDict[ 'compId' ] ) )

  def __getComponents( self, condDict ):
    compTable = self.__getTableName( "Components" )
    fields = ( "Setup", "Type", "ComponentName", "Host", "Port", "StartTime", "LastHeartbeat", 'cycles', 'queries' )
    sqlWhere = []
    for field in condDict:
      val = condDict[ field ]
      if type( val ) == types.StringType:
        sqlWhere.append( "%s='%s'" % ( field, val ) )
      elif type( val ) in ( types.IntType, types.LongType, types.FloatType ):
        sqlWhere.append( "%s='%s'" % ( field, val ) )
      else:
        sqlWhere.append( "( %s )" % " OR ".join( [ "%s='%s'" % ( field, v ) for v in val ] ) )
    result = self._query( "SELECT %s FROM `%s` WHERE %s" % ( ", ".join( fields, ), compTable, " AND ".join( sqlWhere ) ) )
    if not result[ 'OK' ]:
      return result
    records = []
    for record in result[ 'Value' ]:
      rD = {}
      for i in range( len( fields ) ):
        rD[ fields[i] ] = record[i]
      records.append( rD )
    return S_OK( StatusSet( records ) )

  def getComponentsStatus( self, setup ):
    result = self.__getComponents( { 'Setup' : setup } )
    if not result[ 'OK' ]:
      return result
    statusSet = result[ 'Value' ]
    for type in ( 'agent' , 'service' ):
      #Iterate through systems
      result = gConfig.getOptionsDict( "/DIRAC/Setups/%s" % setup )
      if not result[ 'OK' ]:
        return result
      systems = result[ 'Value' ]
      for system in systems:
        instance = systems[ system ]
        #Get entries for the instance of a system
        result = gConfig.getSections( "/Systems/%s/%s/%s" % ( system, instance, "%ss" % type.capitalize() ) )
        if not result[ 'OK' ]:
          self.log.warn( "Opps, sytem seems to be defined wrong\n", "System %s at %s: %s" % ( system, instance, result[ 'Message' ] ) )
          continue
        components = result[ 'Value' ]
        for component in components:
          componentName = "%s/%s" % ( system, component )
          compDict = { 'ComponentName' : componentName,
                       'Type' : type,
                       'Setup' : setup
                      }
          if type == 'service':
            result = gConfig.getOption( "/Systems/%s/%s/%s/%s/Port" % ( system, instance,
                                                                        "%ss" % type.capitalize(), component ) )
            if not result[ 'OK' ]:
              self.log.error( "Component is not well defined", result[ 'Message' ] )
              continue
            try:
              compDict[ 'Port' ] = int( result[ 'Value' ] )
            except:
              self.log.error( "Port for component doesn't seem to be a number", "%s for setup %s" % ( componentName, setup ) )
          result = statusSet.setComponentAsRequired( compDict )
          if not result[ 'OK' ]:
            self.log.error( "Error while setting component as required", result[ 'Message' ] )
    return S_OK( statusSet )

class StatusSet:

  def __init__( self, dbRecordsList = [] ):
    self.__requiredSet = {}
    self.__requiredFields = ( 'Setup', 'Type', 'ComponentName' )
    self.__maxSecsSinceHeartbeat = 600
    self.setDBRecords( dbRecordsList )

  def setDBRecords( self, recordsList ):
    self.__dbSet = {}
    for record in recordsList:
      cD = self.__dbSet
      for field in self.__requiredFields:
        fVal = record[ field ]
        if fVal not in cD:
          if field == self.__requiredFields[-1]:
            cD[ fVal ] = []
          else:
            cD[ fVal ] = {}
        cD = cD[ fVal ]
      cD.append( record )
    return S_OK()

  def setComponentAsRequired( self, compDict ):
    for field in self.__requiredFields:
      if field not in compDict:
        return S_ERROR( "Missing %s field in component description" % field )
    cD = self.__requiredSet
    for field in self.__requiredFields:
      val = compDict[ field ]
      if val not in cD:
        if field == self.__requiredFields[-1]:
          cD[ val ] = []
        else:
          cD[ val ] = {}
      cD = cD[ val ]

    dbD = self.__dbSet
    for field in self.__requiredFields:
      val = compDict[ field ]
      if val not in dbD:
        self.__addMissingRequiredComponent( compDict )
        return S_OK()
      dbD = dbD[ val ]
    self.__addFoundRequiredComponent( compDict )
    return S_OK()

  def __addMissingRequiredComponent( self, compDict ):
    cD = self.__requiredSet
    for field in self.__requiredFields:
      val = compDict[ field ]
      cD = cD[ val ]
    compDict[ 'Status' ] = 'Error'
    compDict[ 'Message' ] = "Component is not up or hasn't connected to register yet"
    cD.append( compDict )

  def __addFoundRequiredComponent( self, compDict ):
    cD = self.__requiredSet
    dbD = self.__dbSet
    now = Time.dateTime()
    for field in self.__requiredFields:
      val = compDict[ field ]
      cD = cD[ val ]
      dbD = dbD[ val ]
    for component in dbD:
      component[ 'Status' ] = 'OK'
      if compDict[ 'Type' ] == "service":
        if 'Port' not in component:
          component[ 'Status' ] = "Error"
          component[ 'Message' ] = "Port is not defined"
        elif str(component[ 'Port' ]) != str(compDict[ 'Port' ]):
          component[ 'Status' ] = "Error"
          component[ 'Message' ] = "Port (%s) is different that specified in the CS (%s)" % ( component[ 'Port' ], compDict[ 'Port' ] )
      elapsed = now - component[ 'LastHeartbeat' ]
      elapsed = elapsed.days * 86400 + elapsed.seconds
      if elapsed > self.__maxSecsSinceHeartbeat:
        component[ 'Status' ] = "Error"
        component[ 'Message' ] = "Last heartbeat was received at %s (%s secs ago)" % ( component[ 'LastHeartbeat' ], elapsed)
      cD.append( component )

  def getRequiredComponents( self ):
    return self.__requiredSet
