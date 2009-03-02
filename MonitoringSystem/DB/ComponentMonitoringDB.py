########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/DB/ComponentMonitoringDB.py,v 1.10 2009/03/02 17:02:12 acasajus Exp $
########################################################################
""" ComponentMonitoring class is a front-end to the Component monitoring Database
"""

__RCSID__ = "$Id: ComponentMonitoringDB.py,v 1.10 2009/03/02 17:02:12 acasajus Exp $"

import time
import random
import md5
import types
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, List, Network

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
    self.__mainFields = ( "Id", "Setup", "Type", "ComponentName", "Host", "Port",
                          "StartTime", "LastHeartbeat", "cycles", "queries", "LoggingState" )
    self.__versionFields = ( 'VersionTimestamp', 'Version', 'DIRACVersion', 'Platform', 'Description' )

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
                                     'VersionTimestamp' : 'DATETIME NOT NULL',
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
    sqlInsertFields.append( 'VersionTimestamp' )
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
      sqlUpdateFields.append( "StartTime='%s'" % self.__datetime2str( compDict[ 'startTime' ] ) )
    return self._update( "UPDATE `%s` SET %s WHERE Id=%s" % ( self.__getTableName( "Components" ),
                                                                       ", ".join( sqlUpdateFields ),
                                                                       compDict[ 'compId' ] ) )

  def __getComponents( self, condDict ):
    """
    Load the components in the DB
    """
    compTable = self.__getTableName( "Components" )
    mainFields = ", ".join( self.__mainFields )
    versionTable = self.__getTableName( "VersionHistory" )
    versionFields = ", ".join( self.__versionFields )
    sqlWhere = []
    for field in condDict:
      val = condDict[ field ]
      if type( val ) == types.StringType:
        sqlWhere.append( "%s='%s'" % ( field, val ) )
      elif type( val ) in ( types.IntType, types.LongType, types.FloatType ):
        sqlWhere.append( "%s='%s'" % ( field, val ) )
      else:
        sqlWhere.append( "( %s )" % " OR ".join( [ "%s='%s'" % ( field, v ) for v in val ] ) )
    if sqlWhere:
      sqlWhere = "WHERE %s" % " AND ".join( sqlWhere )
    else:
      sqlWhere = ""
    result = self._query( "SELECT %s FROM `%s` %s" % ( mainFields, compTable, sqlWhere ) )
    if not result[ 'OK' ]:
      return result
    records = []
    dbData = result[ 'Value' ]
    for record in dbData:
      rD = {}
      for i in range( len( self.__mainFields ) ):
        rD[ self.__mainFields[i] ] = record[i]
      result = self._query( "SELECT %s FROM `%s` WHERE CompId=%s ORDER BY VersionTimestamp DESC LIMIT 1" % ( versionFields,
                                                                                                             versionTable,
                                                                                                             rD[ 'Id' ] ) )
      if not result[ 'OK' ]:
        return result
      if len( result[ 'Value' ] ) > 0:
        versionRec = result[ 'Value' ][0]
        for i in range( len( self.__versionFields ) ):
          rD[ self.__versionFields[i] ] = versionRec[i]
      del( rD[ 'Id' ] )
      records.append( rD )
    return S_OK( StatusSet( records ) )

  def __checkCondition( self, condDict, field, value ):
    if field not in condDict:
      return True
    condVal = condDict[ field ]
    if type( condVal ) in ( types.ListType, types.TupleType ):
      return value in condVal
    return value == condVal

  def getComponentsStatus( self, conditionDict = {} ):
    """
    Get the status of the defined components in the CS compared to the ones that are known in the DB
    """
    result = self.__getComponents( conditionDict )
    if not result[ 'OK' ]:
      return result
    statusSet = result[ 'Value' ]
    requiredComponents = {}
    result = gConfig.getSections( "/DIRAC/Setups" )
    if not result[ 'OK' ]:
      return result
    for setup in result[ 'Value' ]:
      if not self.__checkCondition( conditionDict, "Setup", setup ):
        continue
      #Iterate through systems
      result = gConfig.getOptionsDict( "/DIRAC/Setups/%s" % setup )
      if not result[ 'OK' ]:
        return result
      systems = result[ 'Value' ]
      for system in systems:
        instance = systems[ system ]
        #Walk the URLs
        result = gConfig.getOptionsDict( "/Systems/%s/%s/URLs" % ( system, instance ) )
        if not result[ 'OK' ]:
          self.log.warn ( "There doesn't to be defined the URLs section for %s in %s instance" % ( system, instance ) )
        else:
          serviceURLs = result[ 'Value' ]
          for service in serviceURLs:
            for url in List.fromChar( serviceURLs[ service ] ):
              loc = url[ url.find( "://" ) + 3: ]
              iS = loc.find( "/" )
              componentName = loc[ iS+1: ]
              loc = loc[ :iS ]
              hostname, port = loc.split( ":" )
              compDict = { 'ComponentName' : componentName,
                           'Type' : 'service',
                           'Setup' : setup,
                           'Host' : hostname,
                           'Port' : int(port)
                          }
              allowed = True
              for key in compDict:
                if not self.__checkCondition( conditionDict, key, compDict[key] ):
                  allowed = False
                  break
              if not allowed:
                break

              rC = statusSet.walkSet( requiredComponents, compDict )
              if compDict not in rC:
                rC.append( compDict )
        #Check defined agents and serviecs
        for type in ( 'agent' , 'service' ):
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
            allowed = True
            for key in compDict:
              if not self.__checkCondition( conditionDict, key, compDict[key] ):
                allowed = False
                break
            if not allowed:
              break
            rC = statusSet.walkSet( requiredComponents, compDict )
            if compDict not in rC:
              rC.append( compDict )
    #WALK THE DICT
    statusSet.setComponentsAsRequired( requiredComponents )
    return S_OK( ( statusSet.getRequiredComponents(),
                   self.__mainFields[1:] + self.__versionFields + ( 'Status', 'Message' ) ) )

class StatusSet:

  def __init__( self, dbRecordsList = [] ):
    self.__requiredSet = {}
    self.__requiredFields = ( 'Setup', 'Type', 'ComponentName' )
    self.__maxSecsSinceHeartbeat = 600
    self.setDBRecords( dbRecordsList )

  def setDBRecords( self, recordsList ):
    self.__dbSet = {}
    for record in recordsList:
      cD = self.walkSet( self.__dbSet, record )
      cD.append( record )
    return S_OK()

  def walkSet( self, setDict, compDict, createMissing = True ):
    sD = setDict
    for field in self.__requiredFields:
      val = compDict[ field ]
      if val not in sD:
        if not createMissing:
          return None
        if field == self.__requiredFields[-1]:
          sD[ val ] = []
        else:
          sD[ val ] = {}
      sD = sD[ val ]
    return sD

  def setComponentsAsRequired( self, requiredSet ):
    for setup in requiredSet:
      for type in requiredSet[ setup ]:
        for name in requiredSet[ setup ][ type ]:
          #Need to narrow down required
          cDL = requiredSet[ setup ][ type ][ name ]
          filtered = []
          for i in range( len( cDL ) ):
            alreadyContained = False
            cD = cDL[i]
            for j in range( len( cDL ) ):
              if i==j:
                continue
              pc = cDL[j]
              match = True
              for key in cD:
                if key not in pc:
                  match = False
                  break
                if key == 'Host':
                  result = Network.checkHostsMatch( cD[key], pc[key] )
                  print "NETWORK CHECK! %s vs %s" % ( cD[key], pc[key] )
                  print result
                  if not result[ 'OK' ] or not result[ 'Value' ]:
                    match = False
                    break
                else:
                  if cD[key] != pc[key]:
                    match = False
                    break
              if match:
                alreadyContained = True
            if not alreadyContained:
              filtered.append( cD )
          self.__setComponentListAsRequired( filtered )


  def __setComponentListAsRequired( self, compDictList ):
    dbD = self.walkSet( self.__dbSet, compDictList[0], createMissing = False )
    if not dbD:
      self.__addMissingDefinedComponents( compDictList )
      return S_OK()
    self.__addFoundDefinedComponent( compDictList )
    return S_OK()

  def __addMissingDefinedComponents( self, compDictList ):
    cD = self.walkSet( self.__requiredSet, compDictList[0] )
    for compDict in compDictList:
      compDict[ 'Status' ] = 'Error'
      compDict[ 'Message' ] = "Component is not up or hasn't connected to register yet"
      cD.append( compDict )

  def __addFoundDefinedComponent( self, compDictList ):
    cD = self.walkSet( self.__requiredSet, compDictList[0] )
    dbD = self.walkSet( self.__dbSet, compDictList[0]  )
    now = Time.dateTime()
    unmatched = compDictList
    for component in dbD:
      component[ 'Status' ] = 'OK'
      if component[ 'Type' ] == "service":
        if 'Port' not in component:
          component[ 'Status' ] = "Error"
          component[ 'Message' ] = "Port is not defined"
        elif component[ 'Port' ] not in [ compDict[ 'Port' ] for compDict in compDictList ]:
          component[ 'Status' ] = "Error"
          component[ 'Message' ] = "Port (%s) is different that specified in the CS (%s)" % ( component[ 'Port' ], compDict[ 'Port' ] )
      elapsed = now - component[ 'LastHeartbeat' ]
      elapsed = elapsed.days * 86400 + elapsed.seconds
      if elapsed > self.__maxSecsSinceHeartbeat:
        component[ 'Status' ] = "Error"
        component[ 'Message' ] = "Last heartbeat was received at %s (%s secs ago)" % ( component[ 'LastHeartbeat' ], elapsed)
      cD.append( component )
      #See if we have a perfect match
      newUnmatched = []
      for compDict in unmatched:
        perfectMatch = True
        for field in compDict:
          if field not in component:
            perfectMatch = False
          if field == 'Host':
            result = Network.checkHostsMatch( compDict[ field ], component[ field ] )
            print "NETWORK CHECK! %s vs %s" % ( compDict[ field ], component[ field ] )
            print result
            if not result[ 'OK' ] or not result[ 'Value' ]:
              perfectMatch = False
          else:
            if compDict[ field ] != component[ field ]:
              perfectMatch = False
        if not perfectMatch:
          newUnmatched.append( compDict )
      unmatched = newUnmatched
    for compDict in unmatched:
      compDict[ 'Status' ] = "Error"
      compDict[ 'Message' ] = "There is no component up with this properties"
      cD.append( compDict )


  def getRequiredComponents( self ):
    return self.__requiredSet
