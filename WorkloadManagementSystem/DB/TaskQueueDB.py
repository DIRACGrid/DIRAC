########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/TaskQueueDB.py,v 1.24 2008/11/04 15:50:07 acasajus Exp $
########################################################################
""" TaskQueueDB class is a front-end to the task queues db
"""

__RCSID__ = "$Id: TaskQueueDB.py,v 1.24 2008/11/04 15:50:07 acasajus Exp $"

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
    self.__multiValueDefFields = ( 'Sites', 'GridCEs', 'GridMiddlewares', 'BannedSites', 'LHCbPlatforms' )
    self.__multiValueMatchFields = ( 'GridCE', 'Site', 'GridMiddleware', 'LHCbPlatform' )
    self.__singleValueDefFields = ( 'OwnerDN', 'OwnerGroup', 'PilotType', 'Setup', 'CPUTime' )
    self.__mandatoryMatchFields = ( 'PilotType', 'Setup', 'CPUTime' )
    self.maxCPUSegments = ( 500, 5000, 50000, 300000 )
    self.__maxMatchRetry = 3
    self.__jobPriorityBoundaries = ( 1, 10 )
    self.__tqPriorityBoundaries = ( 1, 10 )
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

  def getSingleValueTQDefFields( self ):
    return self.__singleValueDefFields

  def getMultiValueTQDefFields( self ):
    return self.__multiValueDefFields

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

    for multiField in self.__multiValueDefFields:
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
    for iP in range( len( self.maxCPUSegments ) ):
      cpuSegment = self.maxCPUSegments[ iP ]
      if cpuTime <= cpuSegment:
        return cpuSegment
    return self.maxCPUSegments[-1]

  def _checkTaskQueueDefinition( self, tqDefDict ):
    """
    Check a task queue definition dict is valid
    """
    for field in self.__singleValueDefFields:
      if field not in tqDefDict:
        return S_ERROR( "Missing mandatory field '%s' in task queue definition" % field )
      fieldValueType = type( tqDefDict[ field ] )
      if field in [ "CPUTime" ]:
        if fieldValueType not in ( types.IntType, types.LongType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValueType ) )
      else:
        if fieldValueType not in ( types.StringType, types.UnicodeType ):
          return S_ERROR( "Mandatory field %s value type is not valid: %s" % ( field, fieldValueType ) )
    for field in self.__multiValueDefFields:
      if field in tqDefDict:
        fieldValueType = type( tqDefDict[ field ] )
      else:
        continue
      if fieldValueType not in ( types.ListType, types.TupleType ):
        return S_ERROR( "Multi value field %s value type is not valid: %s" % ( field, fieldValueType ) )
    return S_OK( tqDefDict )

  def _checkMatchDefinition( self, tqMatchDict ):
    """
    Check a task queue match dict is valid
    """
    for field in self.__singleValueDefFields:
      if field not in tqMatchDict:
        if field in self.__mandatoryMatchFields:
          return S_ERROR( "Missing mandatory field '%s' in match request definition" % field )
        else:
          continue
      fieldValueType = type( tqMatchDict[ field ] )
      if field in [ "CPUTime" ]:
        if fieldValueType not in ( types.IntType, types.LongType ):
          return S_ERROR( "Match definition field %s value type is not valid: %s" % ( field, fieldValueType ) )
      else:
        if fieldValueType not in ( types.StringType, types.UnicodeType ):
          return S_ERROR( "Match definition field %s value type is not valid: %s" % ( field, fieldValueType ) )
    for field in self.__multiValueMatchFields:
      if field in tqMatchDict:
        fieldValueType = type( tqMatchDict[ field ] )
        if fieldValueType not in ( types.StringType, types.UnicodeType ):
          return S_ERROR( "Match definition field %s value type is not valid: %s" % ( field, fieldValueType ) )
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
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
    sqlSingleFields = [ 'TQId', 'Priority' ]
    sqlValues = [ "0", str( priority ) ]
    for field in self.__singleValueDefFields:
      sqlSingleFields.append( field )
      sqlValues.append( "'%s'" % tqDefDict[ field ] )
    cmd = "INSERT INTO tq_TaskQueues ( %s ) VALUES ( %s)" % ( ", ".join( sqlSingleFields ), ", ".join( sqlValues ) )
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
    for field in self.__multiValueDefFields:
      if field not in tqDefDict:
        continue
      values = List.uniqueElements( [ value for value in tqDefDict[ field ] if value.strip() ] )
      if not values:
        continue
      cmd = "INSERT INTO `tq_TQTo%s` ( TQId, Value ) VALUES " % field
      cmd += ", ".join( [ "( %s, '%s' )" % ( tqId, str( value ) ) for value in values ] )
      retVal = self._update( cmd, conn = connObj )
      if not retVal[ 'OK' ]:
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't insert values %s for field %s: %s" % ( str( values ), field, retVal[ 'Message' ] ) )
    return S_OK( tqId )

  def cleanOrphanedTaskQueues( self, connObj = False ):
    """
    Delete all empty task queues
    """
    retVal = self._update( "LOCK TABLE `tq_TaskQueues`", conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    try:
      retVal = self._update( "DELETE FROM `tq_TaskQueues` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )", conn = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      for mvField in self.__multiValueDefFields:
        retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_TaskQueues` )" % mvField,
                               conn = connObj )
        if not retVal[ 'OK' ]:
          return retVal
    finally:
      retVal = self._update( "UNLOCK TABLE `tq_TaskQueues`", conn = connObj )
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
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
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
    jobPriority = min( max( int( jobPriority ), self.__jobPriorityBoundaries[0] ), self.__jobPriorityBoundaries[1] )
    tqPriority  = min( max( int( tqPriority  ), self.__tqPriorityBoundaries[0]  ), self.__tqPriorityBoundaries[1]  )
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
    for field in self.__singleValueDefFields:
      if field in ( 'CPUTime' ):
        sqlCondList.append( "`tq_TaskQueues`.%s = %s" % ( field, tqDefDict[ field ] ) )
      else:
        sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqDefDict[ field ] ) )
    #MAGIC SUBQUERIES TO ENSURE STRICT MATCH
    for field in self.__multiValueDefFields:
      tableName = '`tq_TQTo%s`' % field
      if field in tqDefDict and tqDefDict[ field ]:
        firstQuery = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId" % ( tableName, tableName, tableName )
        grouping = "GROUP BY %s.TQId" % tableName
        valuesList = List.uniqueElements( [ value.strip() for value in tqDefDict[ field ] if value.strip() ] )
        numValues = len( valuesList )
        secondQuery = "%s AND %s.Value in (%s)" % ( firstQuery, tableName,
                                                        ",".join( [ "'%s'" % str( value ) for value in valuesList ] ) )
        sqlCondList.append( "%s = (%s %s)" % ( numValues, firstQuery, grouping ) )
        sqlCondList.append( "%s = (%s %s)" % ( numValues, secondQuery, grouping ) )
      else:
        sqlCondList.append( "`tq_TaskQueues`.TQId not in ( SELECT DISTINCT %s.TQId from %s )" % ( tableName, tableName ) )
    #END MAGIC: That was easy ;)
    sqlCmd = "%s  %s" % ( sqlCmd, " AND ".join( sqlCondList ) )
    retVal = self._query( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't find task queue: %s" % retVal[ 'Message' ] )
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_OK( { 'found' : False } )
    if len( data ) > 1:
      gLogger.warn( "Found two task queues for the same requirements", str( tqDefDict ) )
    return S_OK( { 'found' : True, 'tqId' : data[0][0] } )

  def matchAndGetJob( self, tqMatchDict, numJobsPerTry = 10, numQueuesPerTry = 10 ):
    """
    Match a job
    """
    retVal = self._checkMatchDefinition( tqMatchDict )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s"
    postJobSQL = " ORDER BY FLOOR( RAND() * `tq_Jobs`.Priority ) DESC, `tq_Jobs`.JobId ASC LIMIT %s" % numJobsPerTry
    for matchTry in range( self.__maxMatchRetry ):
      retVal = self.matchAndGetQueue( tqMatchDict, numQueuesToGet = numQueuesPerTry, skipMatchDictDef = True, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqList = retVal[ 'Value' ]
      if len( tqList ) == 0:
        return S_OK( { 'matchFound' : False } )
      for tqId in tqList:
        retVal = self._query( "%s %s" % ( preJobSQL % tqId, postJobSQL ), conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't begin transaction for matching job: %s" % retVal[ 'Message' ] )
        jobTQList = [ ( row[0], row[1] ) for row in retVal[ 'Value' ] ]
        if len( jobTQList ) == 0:
          gLogger.warn( "Task queue %s seems to be empty, triggering a cleaning" % tqId )
          self.deleteTaskQueueIfEmpty( tqId, connObj = connObj )
          continue
        while len( jobTQList ) > 0:
          jobId, tqId = jobTQList.pop( random.randint( 0, len( jobTQList ) - 1 ) )
          retVal = self.deleteJob( jobId, connObj = connObj )
          if not retVal[ 'OK' ]:
            return S_ERROR( "Could not take job %s out from the queue: %s" % ( jobId, retVal[ 'Message' ] ) )
          if retVal[ 'Value' ] == True :
            self.deleteTaskQueueIfEmpty( tqId, connObj = connObj )
            return S_OK( { 'matchFound' : True, 'jobId' : jobId, 'taskQueueId' : tqId } )
    return S_ERROR( "Could not find a match after %s match retries" % self.__maxMatchRetry )

  def matchAndGetQueue( self, tqMatchDict, numQueuesToGet = 1, skipMatchDictDef = False, connObj = False ):
    """
    Get a queue that matches the requirements
    """
    if not skipMatchDictDef:
      retVal = self._checkMatchDefinition( tqMatchDict )
      if not retVal[ 'OK' ]:
        return retVal
    retVal = self.__generateTQMatchSQL( tqMatchDict, numQueuesToGet = numQueuesToGet )
    if not retVal[ 'OK' ]:
      return retVal
    matchSQL = retVal[ 'Value' ]
    retVal = self._query( matchSQL, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( [ row[0] for row in retVal[ 'Value' ] ] )

  def __generateTQMatchSQL( self, tqMatchDict, numQueuesToGet = 1 ):
    """
    Generate the SQL needed to match a task queue
    """
    sqlCondList = []
    sqlTables = [ "`tq_TaskQueues`" ]
    #Type of pilot conditions
    for field in self.__singleValueDefFields:
      if field in tqMatchDict:
        if field in ( 'CPUTime' ):
          sqlCondList.append( "`tq_TaskQueues`.%s <= %s" % ( field, tqMatchDict[ field ] ) )
        else:
          sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqMatchDict[ field ] ) )
    #Match multi value fields
    for field in self.__multiValueMatchFields:
      if field in tqMatchDict:
        fieldValue = tqMatchDict[ field ].strip()
        if not fieldValue:
          continue
        #It has to be %ss , with an 's' at the end because the columns names
        # are plural and match options are singular
        tableName = '`tq_TQTo%ss`' % field
        # sqlTables.append( tableName )
        sqlMultiCondList = []
        if field != 'GridCE' or 'Site' in tqMatchDict:
          # Jobs for masked sites can be matched if they specified a GridCE
          # Site is removed from tqMatchDict if the Site is mask. In this case we want
          # that the GridCE matches explicetly so the COUNT can not be 0 (that means
          # not specified for the corresponding jobs.
          sqlMultiCondList.append( "( SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId ) = 0 " % (tableName,tableName,tableName ) )
        sqlMultiCondList.append( "'%s' in ( SELECT %s.Value FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId )" % ( fieldValue, tableName, tableName, tableName ) )
        sqlCondList.append( "( %s )" % " OR ".join(sqlMultiCondList) )
        if field == 'Site':
          bannedTable = '`tq_TQToBannedSites`'
          sqlCondList.append( "'%s' not in ( SELECT %s.Value FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId )" % ( fieldValue,
                                                                                                                 bannedTable,
                                                                                                                 bannedTable,
                                                                                                                 bannedTable ) )
    tqSqlCmd = "SELECT `tq_TaskQueues`.TQId FROM %s WHERE %s" % ( ", ".join( sqlTables ), " AND ".join( sqlCondList ) )
    tqSqlCmd = "%s ORDER BY FLOOR( RAND() * `tq_TaskQueues`.Priority ) DESC, `tq_TaskQueues`.TQId ASC LIMIT %s" % ( tqSqlCmd,
                                                                                                                    numQueuesToGet )
    return S_OK( tqSqlCmd )

  def deleteJob( self, jobId, connObj = False ):
    """
    Delete a job from the task queues
    Return S_OK( True/False ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't delete job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    retVal = self._update( "DELETE FROM `tq_Jobs` WHERE `tq_Jobs`.JobId = %s" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    result = retVal[ 'Value' ]
    if retVal[ 'Value' ] == 0:
      return S_OK( False )
    return S_OK( True )

  def deleteTaskQueueIfEmpty( self, tqId, connObj = False ):
    """
    Try to delete a task queue if its empty
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    sqlCmd = "%s AND `tq_TaskQueues`.TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )" % sqlCmd
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    sqlCmd = "SELECT TQId FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    if not retVal['Value']:
      for mvField in self.__multiValueDefFields:
        retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % ( mvField, tqId ), conn = connObj )
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
      for mvField in self.__multiValueDefFields:
        retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % tqId, conn = connObj )
        if not retVal[ 'OK' ]:
          return retVal
      return S_OK( True )
    return S_OK( False )

  def retrieveTaskQueues( self ):
    """
    Get all the task queues
    """
    sqlSelectEntries = [ "`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority", "COUNT( `tq_Jobs`.TQId )" ]
    sqlGroupEntries = [ "`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority" ]
    for field in self.__singleValueDefFields:
      sqlSelectEntries.append( "`tq_TaskQueues`.%s" % field )
      sqlGroupEntries.append( "`tq_TaskQueues`.%s" % field )
    sqlCmd = "SELECT %s FROM `tq_TaskQueues`, `tq_Jobs`" % ", ".join( sqlSelectEntries )
    sqlCmd = "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId GROUP BY %s" % ( sqlCmd, ", ".join( sqlGroupEntries ) )

    retVal = self._query( sqlCmd )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve task queues info: %s" % retVal[ 'Message' ] )
    tqData = {}
    for record in retVal[ 'Value' ]:
      tqId = record[0]
      tqData[ tqId ] = { 'Priority' : record[1], 'Jobs' : record[2] }
      record = record[3:]
      for iP in range( len( self.__singleValueDefFields ) ):
        tqData[ tqId ][ self.__singleValueDefFields[ iP ] ] = record[ iP ]

    tqNeedCleaning = False
    for field in self.__multiValueDefFields:
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
