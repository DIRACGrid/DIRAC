########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/DB/ComponentMonitoringDB.py,v 1.3 2009/02/20 12:16:47 acasajus Exp $
########################################################################
""" ComponentMonitoring class is a front-end to the Component monitoring Database
"""

__RCSID__ = "$Id: ComponentMonitoringDB.py,v 1.3 2009/02/20 12:16:47 acasajus Exp $"

import time
import random
import md5
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

class ComponentMonitoringDB(DB):

  def __init__(self, requireVoms = False,
               useMyProxy = False,
               maxQueueSize = 10 ):
    DB.__init__(self,'ComponentMonitoringDB','Monitoring/ComponentMonitoringDB',maxQueueSize)
    random.seed()
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

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
                                     'Location' : 'VARCHAR(255) NOT NULL',
                                     'LastHeartbeat' : 'DATETIME NOT NULL',
                                     'StartTime' : 'DATETIME NOT NULL',
                                     'Cycles' : 'INTEGER'
                                   },
                                   'PrimaryKey' : 'Id',
                                   'Indexes' : { 'ComponentIndex' : [ 'ComponentName', 'Setup', 'Location' ],
                                                 'TypeIndex' : [ 'Type' ],
                                               }
                      }

    tN = self.__getTableName( "VersionHistory" )
    if tN not in tablesInDB:
      tablesD[ tN ] = { 'Fields' : { 'CompId' : 'INTEGER NOT NULL',
                                     'Timestamp' : 'DATETIME NOT NULL',
                                     'Version' : 'VARCHAR(255)',
                                     'DIRACVersion' : 'VARCHAR(255) NOT NULL',
                                     'Description' : 'BLOB',
                                   },
                                  'Indexes' : { 'Component' : [ 'CompId' ] }
                      }

    return self._createTables( tablesD )

  def __registerIfNotThere( self, compDict ):
    """
    Register the component if it's not there
    """
    sqlCond = []
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName( "Components" )
    for field in ( 'componentName', 'setup', 'type', 'location' ):
      if field not in compDict:
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
      return result
    if len( result[ 'Value' ] ):
      compId = result[ 'Value' ][0][0]
      self.log.info( "%s has compId %s" % ( compLogName, compId ) )
      return S_OK( compId )
    #It's not there, we just need to insert it
    sqlInsertFields.append( "LastHeartbeat" )
    sqlInsertValues.append( "UTC_TIMESTAMP()" )
    sqlInsertFields.append( "StartTime" )
    if 'startTime' in compDict:
      sqlInsertValues.append( compDict[ 'startTime' ] )
    else:
      sqlInsertValues.append( "UTC_TIMESTAMP()" )
    if 'cycles' not in compDict:
      compDict[ 'cycles' ] = 0
    for field in ( 'cycles', ):
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
    sqlCond = [ "ComponentId=%s" % compId ]
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName( "VersionHistory" )
    for field in ( 'version', 'DIRACVersion' ):
      if field not in compDict:
        return S_ERROR( "Missing %s field in the component dict" % field )
      value = compDict[ field ]
      field = field.capitalize()
      sqlInsertFields.append( field )
      sqlInsertValues.append( "'%s'" % value )
      sqlCond.append( "%s = '%s'" % ( field, value ) )
    result = self._query( "SELECT ComponentId FROM `%s` WHERE %s" % ( tableName, " AND ".join( sqlCond ) ) )
    if not result[ 'OK' ]:
      return result
    if len( result[ 'Value' ] ):
      return S_OK()
    #It's not there, we just need to insert it
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
    return S_OK()

  def registerComponent( self, compDict, shallow = False ):
    """
    Register a new component in the DB. If it's already registered return id
    """
    print "Comp"
    result = self.__registerIfNotThere( compDict )
    if not result[ 'OK' ]:
      return result
    compId = result[ 'Value' ]
    if shallow:
      return S_OK( compId )
    #Check if something has changed in the version history
    return self.__updateVersionHistoryIfNeeded( compId, compDict )

  def heartbeat( self, compDict ):
    """
    Update heartbeat
    """
    if 'compId' not in compDict:
      result = self.__registerIfNotThere( compDict )
      if not result[ 'OK' ]:
        return result
      compId = result[ 'Value' ][0][0]
      compDict[ 'compId' ] = compId
    sqlUpdateFields = [ 'LastHeartbeat=UTC_TIMESTAMP()' ]
    cycles = 0
    if 'cycles' in compDict:
      cycles = compDict[ 'cycles' ]
    sqlUpdateFields.append( "Cycles=%s" % cycles )
    if 'startTime' in compDict:
      sqlUpdateFields.append( "StartTime=%s" % compDict[ 'startTime' ] )
    return self._update( "UPDATE `%s` SET %s WHERE Id=%s" % ( self.__getTableName( "Components" ),
                                                                       ", ".join( sqlUpdateFields ),
                                                                       compDict[ 'compId' ] ) )