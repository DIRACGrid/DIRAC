########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/TaskQueueDB.py,v 1.5 2008/08/01 09:08:45 acasajus Exp $
########################################################################
""" TaskQueueDB class is a front-end to the task queues db
"""

__RCSID__ = "$Id: TaskQueueDB.py,v 1.5 2008/08/01 09:08:45 acasajus Exp $"

import time
import types
import random
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Base.DB import DB

class TaskQueueDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    random.seed()
    DB.__init__( self, 'TaskQueueDB', 'WorkloadManagement/TaskQueueDB', maxQueueSize )
    self.__multiValueFields = ( 'Sites', 'GridCEs', 'GridMiddlewares', 'BannedSites', 'LHCbPlatforms' )
    self.__multiValueMatchFields = ( 'GridCE', 'Site', 'GridMiddleware', 'LHCbPlatform' )
    self.__singleValueFields = ( 'OwnerDN', 'OwnerGroup', 'PilotType', 'Setup', 'CPUTime' )
    self.__minCPUSegments = ( 500, 6000, 100000 )
    self.__maxMatchRetry = 3
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

  def getSingleValueTQDefFields( self ):
    return self.__singleValueFields

  def getMultiValueTQDefFields( self ):
    return self.__multiValueFields

  def getMultiValueMatchFields( self ):
    return self.__multiValueMatchFields

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'tq_TaskQueues' not in tablesInDB:
      tablesD[ 'tq_TaskQueues' ] = { 'Fields' : { 'TQId' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                                  'OwnerDN' : 'VARCHAR(255) NOT NULL',
                                                  'OwnerGroup' : 'VARCHAR(32) NOT NULL',
                                                  'PilotType' : 'VARCHAR(32) NOT NULL',
                                                  'Setup' : 'VARCHAR(32) NOT NULL',
                                                  'CPUTime' : 'BIGINT NOT NULL',
                                                  'Priority' : 'SMALLINT NOT NULL',
                                                   },
                                     'PrimaryKey' : 'TQId',
                                     'Indexes': { 'TQOwner': [ 'OwnerDN', 'OwnerGroup', 'PilotType',
                                                               'Setup', 'CPUTime' ]
                                                }
                                    }

    if 'tq_Jobs' not in tablesInDB:
      tablesD[ 'tq_Jobs' ] = { 'Fields' : { 'TQId' : 'INTEGER NOT NULL',
                                            'JobId' : 'INTEGER NOT NULL',
                                            'Priority' : 'SMALLINT NOT NULL'
                                          },
                               'Indexes': { 'TaskIndex': [ 'TQId' ] },
                             }

    for multiField in self.__multiValueFields:
      tableName = 'tq_TQTo%s' % multiField
      if not tableName in tablesInDB:
        tablesD[ tableName ] = { 'Fields' : { 'TQId' : 'INTEGER NOT NULL',
                                              'Value' : 'VARCHAR(64) NOT NULL',
                                          },
                                 'Indexes': { 'TaskIndex': [ 'TQId' ], '%sIndex' % multiField: [ 'Value' ] },
                               }

    return self._createTables( tablesD )

  def fitCPUTimeToSegments( self, cpuTime ):
    """
    Fit the CPU time to the valid segments
    """
    for iP in range( len( self.__minCPUSegments ) -1, -1, -1 ):
      minCPUTime = self.__minCPUSegments[ iP ]
      if cpuTime >= minCPUTime:
        return minCPUTime
    return self.__minCPUSegments[0]

  def _checkTaskQueueDefinition( self, tqDefDict ):
    """
    Check a task queue definition dict is valid
    """
    for field in self.__singleValueFields:
      if field not in tqDefDict:
        return S_ERROR( "Missing mandatory field '%s' in task queue definition" % field )
      fieldValueType = type( tqDefDict[ field ] )
      if field in [ "CPUTime" ]:
        if fieldValueType not in ( types.IntType, types.LongType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValue ) )
      else:
        if fieldValueType not in ( types.StringType, types.UnicodeType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValue ) )
    for field in self.__multiValueFields:
      if field in tqDefDict:
        fieldValueType = type( tqDefDict[ field ] )
      if fieldValueType not in ( types.ListType, types.TupleType ):
        return S_ERROR( "Multi value field %s value type is not valid: %s" % ( field, fieldValue ) )
    return S_OK( tqDefDict )

  def _checkMatchDefinition( tqMatchDict ):
    """
    Check a task queue match dict is valid
    """
    for field in self.__singleValueFields:
      if field not in tqMatchDict:
        return S_ERROR( "Missing mandatory field '%s' in task queue definition" % field )
      fieldValueType = type( tqMatchDict[ field ] )
      if field in [ "CPUTime" ]:
        if fieldValueType not in ( types.IntType, types.LongType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValue ) )
      else:
        if fieldValueType not in ( types.StringType, types.UnicodeType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValue ) )
    for field in self.__multiValueMatchFields:
      if field in tqMatchDict:
        fieldValueType = type( tqMatchDict[ field ] )
      if fieldValueType not in ( types.StringType, types.UnicodeType ):
        return S_ERROR( "Field %s value type is not valid: %s" % ( field, fieldValue ) )
    return S_OK( tqMatchDict )

  def createTaskQueue( self, tqDefDict, priority = 1, skipDefinitionCheck = False, connObj = False ):
    """
    Create a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    if not skipDefinitionCheck:
      retVal = self._checkTaskQueueDefinition( tqDefDict )
      if not retVal[ 'OK' ]:
        return retVal
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't create task queue: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    minCPUTime = self.fitCPUTimeToSegments( minCPUTime )
    cmd = "INSERT INTO tq_TaskQueues ( TQId, OwnerDN, OwnerGroup, PilotType, Setup, CPUTime, Priority )"
    cmd += " VALUES ( 0, '%s', '%s', '%s', '%s', %s, %s )" % ( ownerDN, ownerGroup, pilotType, Setup, minCPUTime, priority )
    retVal = self._update( cmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    if 'lastRowId' in retVal:
      tqId = retVal['lastRowId']
    else:
      retVal = self._query( "SELECT LAST_INSERT_ID()", conn = connObj )
      if not retVal[ 'OK' ]:
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't determine task queue id after insertion" )
      tqId = retVal[ 'Value' ][0][0]
    for fieldName, values in ( ( 'Sites', sitesList ),
                               ( 'GridCE', gridCEList ),
                               ( 'BannedSites', bannedSitesList ),
                               ( 'LHCbPlatform', LHCbPlatformsList ) ):
      values = List.uniqueElements( [ value for value in values if value.strip() ] )
      cmd = "INSERT INTO tq_TQTo%s ( 'TQId', 'Value' ) VALUES "
      cmd += ", ".join( [ "( %s, '%s' )" % ( tqId, value ) for value in values ] )
      retVal = self._update( cmd, conn = connObj )
      if not retVal[ 'Value' ]:
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't insert values %s for field %s: %s" % ( str( values ), fieldName, retVal[ 'Message' ] ) )
    return S_OK( tqId )

  def cleanOrphanedTaskQueues( self, connObj = False ):
    """
    Delete all empty task queues
    """
    retVal = self._update( "DELETE FROM `tq_TaskQueues` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )", conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    for mvField in self.__multiValueFields:
      retVal = self._update( "DELETE FROM `tq_TQTto%s` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_TaskQueues` )", conn = connObj )
      if not retVal[ 'OK' ]:
        return retVal
    return S_OK()

  def insertJob( self, jobId, tqDefDict, tqPriority, jobPriority ):
    """
    Insert a job in a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    retVal = self._checkTaskQueueDefinition( tqDefDict )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.findTaskQueue( tqDefDict, skipDefinitionCheck = True, connObj = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    tqInfo = retVal[ 'Value' ]
    if not tqInfo[ 'found' ]:
      retVal = self.createTaskQueue( tqDefDict, tqPriority, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqId = retVal[ 'Value' ]
    else:
      tqId = tqInfo[ 'tqId' ]
    return self.insertJobInTaskQueue( jobId, tqId, jobPriority, checkTQExists = False, connObj = connObj )

  def insertJobInTaskQueue( self, jobId, tqId, jobPriority, checkTQExists = True, connObj = False ):
    """
    Insert a job in a given task queue
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    if checkTQExists:
      retVal = self._query( "SELECT tqId FROM `tq_TaskQueues` WHERE TQId = %s" % tqId, conn = connObj )
      if not retVal[ 'OK' ] or len ( retVal[ 'Value' ] ) == 0:
        return S_OK( "Can't find task queue with id %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    return self._update( "INSERT INTO tq_Jobs ( TQId, JobId, Priority ) VALUES ( %s, %s, %s )" % ( tqId, jobId, jobPriority ), conn = connObj )

  def findTaskQueue( self, tqDefDict, skipDefinitionCheck= False, connObj = False ):
    """
      Find a task queue that has exactly the same requirements
    """
    if not skipDefinitionCheck:
      retVal = self._checkTaskQueueDefinition( tqDefDict )
      if not retVal[ 'OK' ]:
        return retVal
    sqlCmd = "SELECT `tq_TaskQueues`.TQId FROM `tq_TaskQueues` WHERE"
    sqlCondList = []
    for field in self.__singleValueFields:
      if field in ( 'CPUTime' ):
        sqlCondList.append( "`tq_TaskQueues`.%s = %s" % ( field, tqDefDict[ field ] ) )
      else:
        sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqDefDict[ field ] ) )
    #MAGIC SUBQUERIES TO ENSURE STRICT MATCH
    for field in self.__multiValueFields:
      tableName = '`tq_TQTo%s`' % multiField
      firstQuery = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId" % ( tableName, tableName, tableName )
      grouping = "GROUP BY `%s`.Value" % tableName
      if field in tqDefDict and tqDefDict[ field ]:
        valuesList = List.uniqueElements( [ value.strip() for value in tqDefDict[ field ] if value.strip() ] )
        numValues = len( valuesList )
        secondQuery = "%s WHERE %s.Value in (%s)" % ( firstQuery, tableName, tableName,
                                                        ",".join( [ "'%s'" % value for value in valuesList ] ) )
        sqlCondList.append( "%s = %s %s" % ( numValues, firstQuery, grouping ) )
        sqlCondList.append( "%s = %s %s" % ( numValues, secondQuery, grouping ) )
      else:
        sqlCondList.append( "0 = (%s %s)" % ( firstQuery, grouping ) )
    #END MAGIC: That was easy ;)
    sqlCmd = "%s %s" % ( sqlCmd, " AND ".join( sqlCondList ) )
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't find task queue: %s" % retVal[ 'Message' ] )
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_OK( { 'found' : False } )
    if len( data ) > 1:
      gLogger.warn( "Found two task queues for the same requirements", str( tqDefDict ) )
    return S_OK( { 'found' : True, 'tqId' : data[0][0] } )

  def matchAndGetJob( self, tqMatchDict ):
    """
    Match a job
    """
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    retVal = self.__generateTQMatchSQL( tqMatchDict )
    if not retVal[ 'OK' ]:
      return retVal
    matchSQL = retVal[ 'Value' ]
    jobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId FROM `tq_Jobs` WHERE `tq_Jobs`.TQId in ( %s )" % ( matchSQL )
    jobSQL = "%s ORDER BY FLOOR( RAND() * `tq_Jobs`.Priority ) DESC, `tq_Jobs`.JobId ASC LIMIT 10" % tqSqlCmd
    for matchTry in range( self.__maxMatchRetry ):
      retVal = self._query( jobSQL, conn = connObj )
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't begin transaction for matching job: %s" % retVal[ 'Message' ] )
      jobTQList = retVal[ 'Value' ]
      if len( jobTQList ) == 0:
        connObj.close()
        return S_OK( { 'matchFound' : False } )
      while len( jobTQList ) > 0:
        jobId, tqId = jobTQList.pop( random.randrange( len( jobTQList ) ) )
        retVal = self.deleteJob( jobId, conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Could not take job %s out from the queue: %s" % ( jobId, retVal[ 'Message' ] ) )
        if retVal( True ):
          self.deleteTaskQueueIfEmpty( tqId, conn = connObj )
          return S_OK( { 'matchFound' : True, 'jobId' : jobId } )
    return S_ERROR( "Could not find a match after %s retries" % self.__maxMatchRetry )

  def __generateTQMatchSQL( self, tqMatchDict ):
    """
    Generate the SQL needed to match a task queue
    """
    retVal = self._checkMatchDefinition( tqMatchDict )
    if not retVal[ 'OK' ]:
      return retVal
    sqlCondList = []
    sqlTables = [ "`tq_TaskQueues`" ]
    #Type of pilot conditions
    if tqMatchDict[ 'PilotType' ] == 'private':
      for field in ( 'OwnerDN', 'OwnerGroup', 'PilotType' ):
        sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqMatchDict[ field ] ) )
    else:
      for field in ( 'PilotType' ):
        sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqMatchDict[ field ] ) )
    #Remaining single value fields
    for field in ( 'CPUTime', 'Setup' ):
      if field in ( 'CPUTime' ):
        sqlCondList.append( "`tq_TaskQueues`.%s <= %s" % ( field, tqMatchDict[ field ] ) )
      else:
        sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqMatchDict[ field ] ) )
    #Match multi value fields
    for field in self.__multiValueFields:
      if field in tqMatchDict:
        fieldValue = tqMatchDict[ field ].strip()
        if not fieldValue:
          continue
        tableName = '`tq_TQTo%s`' % multiField
        sqlTables.append( tableName )
        sqlCondList.append( "%s.TQId = `tq_TaskQueues`.TQId" % table )
        sqlCondList.append( "%s.Value = '%s'" % ( table, fieldValue ) )
        if field == 'Site':
          bannedTable = '`tq_TQToBannedSites`'
          sqlCondList.append( "'%s' not in ( SELECT %s.Value FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId )" % ( fieldValue,
                                                                                                                 bannedTable,
                                                                                                                 bannedTable,
                                                                                                                 bannedTable ) )
    tqSqlCmd = "SELECT `tq_TaskQueues`.TQId FROM %s WHERE %s" % ( ", ".join( sqlTables ), " AND ".join( sqlCondList ) )
    tqSqlCmd = "%s ORDER BY FLOOR( RAND() * `tq_TaskQueues`.Priority ) DESC, `tq_TaskQueues`.TQId ASC LIMIT 10" % tqSqlCmd
    return S_OK( tqSqlCmd )

  def deleteJob( self, jobId, connObj = False ):
    """
    Delete a job from the task queues
    Return S_OK( True/False ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    retVal = self._update( "DELETE FROM `tq_Jobs` WHERE `tq_Jobs`.JobId = %s" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    if connObj.num_rows() == 1:
      return S_OK( True )
    return S_OK( False )

  def deleteTaskQueueIfEmpty( self, tqId, conn = False ):
    """
    Try to delete a task queue if its empty
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    sqlCmd = "%s AND `tq_TaskQueues`.TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )"
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    if connObj.num_rows() == 1:
      for mvField in self.__multiValueFields:
        retVal = self._update( "DELETE FROM `tq_TQTto%s` WHERE TQId = %s" % tqId, conn = connObj )
        if not retVal[ 'OK' ]:
          return retVal
      return S_OK( True )
    return S_OK( False )

  def deleteTaskQueue( self, tqId, conn = False ):
    """
    Try to delete a task queue even if it has jobs
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    sqlCmd = "DELETE FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s" % tqId
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    if connObj.num_rows() == 1:
      for mvField in self.__multiValueFields:
        retVal = self._update( "DELETE FROM `tq_TQTto%s` WHERE TQId = %s" % tqId, conn = connObj )
        if not retVal[ 'OK' ]:
          return retVal
      return S_OK( True )
    return S_OK( False )

  def retrieveTaskQueues( self ):
    """
    Get all the task queues
    """
    sqlSelectEntries = [ "`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority", "COUNT( `tq_Jobs`.TQId )" ]
    for field in self.__singleValueFields:
      sqlSelectEntries.append( "`tq_TaskQueues`.%s" % field )
    sqlCmd = "SELECT %s" % ", ".join( sqlSelectEntries )
    sqlCmd = "%s FROM `tq_TaskQueues`, `tq_Jobs` WHERE tq_TaskQueues`.TQId = `tq_Jobs`.TQId GROUP BY `tq_TaskQueues`.TQId" % sqlCmd

    retVal = sql._query( sqlCmd )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve task queues info: %s" % retVal[ 'Message' ] )
    tqData = {}
    for record in retVal[ 'Value' ]:
      tqId = record[0]
      tq[ tqId ] = { 'Priority' : record[1], 'Jobs' : record[2] }
      record = record[3:]
      for iP in range( len( self.__singleValueFields ) ):
        tq[ tqId ][ self.__singleValueFields[ iP ] ] = record[ iP ]

    tqNeedCleaning = False
    for field in self.__multiValueFields:
      table = "`tq_TQTo%s`" % field
      sqlCmd = "SELECT %s.TQId, %s.Value FROM %s" % ( table, table, table )
      retVal = self._query( sqlCmd )
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't retrieve task queues field % info: %s" % ( field, retVal[ 'Message' ] ) )
      for record in retVal[ 'Value' ]:
        tqId = record[0]
        value = record[1]
        if not tqId in tqData:
          gLogger.warn( "Task Queue %s is defined in field %s but does not exist, triggering a cleaning" % ( tqId, field ) )
          tqNeedCleaning = True
        else:
          if field not in tqData[ tqId ]:
            tqData[ tqId ][ field ] = []
          tqData[ tqId ][ field ].append( value )
    if tqNeedCleaning:
      self.cleanOrphanedTaskQueues()
    return S_OK( tqData )
