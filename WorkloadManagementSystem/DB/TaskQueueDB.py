########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/TaskQueueDB.py,v 1.42 2008/12/05 17:05:11 acasajus Exp $
########################################################################
""" TaskQueueDB class is a front-end to the task queues db
"""

__RCSID__ = "$Id: TaskQueueDB.py,v 1.42 2008/12/05 17:05:11 acasajus Exp $"

import time
import types
import random
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security import Properties, CS

class TaskQueueDB(DB):

  defaultGroupShare = 1000

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
    self.__tqPriorityBoundaries = ( 1, 10000 )
    self.__groupShares = {}
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

  def __strDict( self, dDict ):
    lines = []
    for key in sorted( dDict ):
      lines.append( " %s" % key )
      value = dDict[ key ]
      if type( value ) in ( types.ListType, types.TupleType ):
        lines.extend( [ "   %s" % v for v in value ] )
      else:
        lines.append( "   %s" % str(value) )
    return "{\n%s\n}" % "\n".join( lines )

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
    self.log.info( "Creating TQ with requirements", self.__strDict( tqDefDict ) )
    if not skipDefinitionCheck:
      retVal = self._checkTaskQueueDefinition( tqDefDict )
      if not retVal[ 'OK' ]:
        self.log.error( "TQ definition check failed", retVal[ 'Value' ] )
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
      self.log.error( "Can't insert TQ in DB", retVal[ 'Value' ] )
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
        self.log.error( "Failed to insert %s condition" % field, retVal[ 'Message' ] )
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't insert values %s for field %s: %s" % ( str( values ), field, retVal[ 'Message' ] ) )
    self.log.info( "Created TQ %s" % tqId )
    return S_OK( tqId )

  def cleanOrphanedTaskQueues( self, connObj = False ):
    """
    Delete all empty task queues
    """
    self.log.info( "Cleaning orphaned TQs" )
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

  def insertJob( self, jobId, tqDefDict, jobPriority ):
    """
    Insert a job in a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    self.log.info( "Inserting job %s" % jobId )
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    retVal = self._checkTaskQueueDefinition( tqDefDict )
    if not retVal[ 'OK' ]:
      self.log.error( "TQ definition check failed", retVal[ 'Value' ] )
      return retVal
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
    retVal = self.findTaskQueue( tqDefDict, skipDefinitionCheck = True, connObj = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    tqInfo = retVal[ 'Value' ]
    newTq = False
    if not tqInfo[ 'found' ]:
      self.log.info( "Createing a TQ for job %s requirements" % jobId )
      retVal = self.createTaskQueue( tqDefDict, 0, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqId = retVal[ 'Value' ]
      newTq = True
    else:
      tqId = tqInfo[ 'tqId' ]
    jobPriority = min( max( int( jobPriority ), self.__jobPriorityBoundaries[0] ), self.__jobPriorityBoundaries[1] )
    result = self.insertJobInTaskQueue( jobId, tqId, jobPriority, checkTQExists = False, connObj = connObj )
    if not result[ 'OK' ]:
      return result
    if not newTq:
      return result
    return self.recalculateSharesForEntity( tqDefDict[ 'OwnerDN' ], tqDefDict[ 'OwnerGroup' ], connObj = connObj )

  def insertJobInTaskQueue( self, jobId, tqId, jobPriority, checkTQExists = True, connObj = False ):
    """
    Insert a job in a given task queue
    """
    self.log.info( "Inserting job %s in TQ %s with priority %s" % ( jobId, tqId, jobPriority ) )
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
      gLogger.warn( "Found two task queues for the same requirements", self.__strDict( tqDefDict ) )
    return S_OK( { 'found' : True, 'tqId' : data[0][0] } )

  def matchAndGetJob( self, tqMatchDict, numJobsPerTry = 10, numQueuesPerTry = 10 ):
    """
    Match a job
    """
    self.log.info( "Starting match for requirements", self.__strDict( tqMatchDict ) )
    retVal = self._checkMatchDefinition( tqMatchDict )
    if not retVal[ 'OK' ]:
      self.log.error( "TQ match request check failed", retVal[ 'Value' ] )
      return retVal
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't connect to DB: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s"
    postJobSQL = " ORDER BY FLOOR( RAND() * `tq_Jobs`.Priority ) DESC, `tq_Jobs`.JobId ASC LIMIT %s" % numJobsPerTry
    deletedTQ = False
    for matchTry in range( self.__maxMatchRetry ):
      if 'JobID' in tqMatchDict:
        # A certain JobID is required by the resource, so all TQ are to be considered
        retVal = self.matchAndGetQueue( tqMatchDict, numQueuesToGet = 0, skipMatchDictDef = True, connObj = connObj )
        preJobSQL = "%s AND `tq_Jobs`.JobId = %s " % ( preJobSQL, tqMatchDict['JobID'] )
      else:
        retVal = self.matchAndGetQueue( tqMatchDict, numQueuesToGet = numQueuesPerTry, skipMatchDictDef = True, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqList = retVal[ 'Value' ]
      if len( tqList ) == 0:
        self.log.info( "No TQ matches requirements" )
        return S_OK( { 'matchFound' : False } )
      for tqId, tqOwnerDN, tqOwnerGroup in tqList:
        self.log.info( "Trying to extract jobs from TQ %s" % tqId )
        retVal = self._query( "%s %s" % ( preJobSQL % tqId, postJobSQL ), conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't begin transaction for matching job: %s" % retVal[ 'Message' ] )
        jobTQList = [ ( row[0], row[1] ) for row in retVal[ 'Value' ] ]
        if len( jobTQList ) == 0:
          gLogger.info( "Task queue %s seems to be empty, triggering a cleaning" % tqId )
          result = self.deleteTaskQueueIfEmpty( tqId, connObj = connObj )
          if result[ 'OK' ]:
            deletedTQ = result[ 'Value' ]
          continue
        while len( jobTQList ) > 0:
          jobId, tqId = jobTQList.pop( random.randint( 0, len( jobTQList ) - 1 ) )
          retVal = self.deleteJob( jobId, connObj = connObj )
          if not retVal[ 'OK' ]:
            return S_ERROR( "Could not take job %s out from the queue: %s" % ( jobId, retVal[ 'Message' ] ) )
          if retVal[ 'Value' ] == True :
            self.log.info( "Match found with job %s (TQ %s)" % ( jobId, tqId ) )
            result = self.deleteTaskQueueIfEmpty( tqId, connObj = connObj )
            if result[ 'OK' ]:
              deletedTQ = result[ 'Value' ]
            if deletedTQ:
              self.recalculateSharesForEntity( tqOwnerDN, tqOwnerGroup, connObj = connObj )
            return S_OK( { 'matchFound' : True, 'jobId' : jobId, 'taskQueueId' : tqId } )
    if deletedTQ:
      self.recalculateSharesForEntity( tqOwnerDN, tqOwnerGroup, connObj = connObj )
    self.log.info( "Could not find a match after %s match retries" % self.__maxMatchRetry )
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
    return S_OK( [ (row[0],row[1],row[2]) for row in retVal[ 'Value' ] ] )

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
    tqSqlCmd = "SELECT `tq_TaskQueues`.TQId, `tq_TaskQueues`.OwnerDN, `tq_TaskQueues`.OwnerGroup FROM %s WHERE %s" % ( ", ".join( sqlTables ),
                                                                                                                      " AND ".join( sqlCondList ) )
    tqSqlCmd = "%s ORDER BY RAND() / `tq_TaskQueues`.Priority ASC" % tqSqlCmd
    if numQueuesToGet:
      tqSqlCmd = "%s LIMIT %s" % ( tqSqlCmd, numQueuesToGet )
    return S_OK( tqSqlCmd )

  def deleteJob( self, jobId, connObj = False ):
    """
    Delete a job from the task queues
    Return S_OK( True/False ) / S_ERROR
    """
    self.log.info( "Deleting job %s" % jobId )
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
    self.log.info( "Deleting TQ %s if empty" % tqId )
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
      self.log.info( "Could not delete TQ %s" % tqId, retVal[ 'Message' ] )
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
    self.log.info( "Deleting TQ %s" % tqId )
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

  def recalculateSharesForAll(self):
    """
    Recalculate all priorities for TQ's
    """
    self.log.info( "Recalculating shares for all TQs" )
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    result = self._query( "SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`" )
    if not result[ 'OK' ]:
      return result
    for group in [ r[0] for r in result[ 'Value' ] ]:
      self.recalculateSharesForEntity( "all", group )
    return S_OK()

  def recalculateSharesForEntity( self, userDN, userGroup, connObj = False ):
    """
    Recalculate the shares for a userDN/userGroup combo
    """
    self.log.info( "Recalculating shares for %s@%s TQs" % ( userDN, userGroup ) )
    share = gConfig.getValue( "/Security/Groups/%s/JobsShare" % userGroup, self.defaultGroupShare )
    if Properties.JOB_SHARING in CS.getPropertiesForGroup( userGroup ):
      #If group has JobSharing just set prio for that entry, userDN is irrelevant
      return self.setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )

    selSQL = "SELECT OwnerDN, COUNT(OwnerDN) FROM `tq_TaskQueues` WHERE OwnerGroup='%s' GROUP BY OwnerDN" % ( userGroup )
    result = self._query( selSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    #Get owners in this group and the amount of times they appear
    data = [ ( r[0], r[1] ) for r in result[ 'Value' ] if r ]
    numOwners = len( data )
    #If there are no owners do now
    if numOwners == 0:
      return S_OK()
    #Split the share amongst the number of owners
    share /= numOwners
    owners = dict( data )
    #IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
    #(The number of owners didn't change)
    if userDN in owners and owners[ userDN ] > 1:
      return self.setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )
    #Oops the number of owners may have changed so we recalculate the prio for all owners in the group
    for userDN in owners:
      self.setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )
    return S_OK()

  def setPrioritiesForEntity( self, userDN, userGroup, share, connObj = False ):
    """
    Set the priority for a userDN/userGroup combo given a splitted share
    """
    self.log.info( "Setting priorities to %s@%s TQs" % ( userDN, userGroup ) )
    condSQL = "OwnerGroup='%s'" % userGroup
    if Properties.JOB_SHARING not in CS.getPropertiesForGroup( userGroup ):
      condSQL += " AND OwnerDN='%s'" % userDN
    result = self._query( "SELECT COUNT(TQId) FROM `tq_TaskQueues` WHERE %s" % condSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    numTQs = result[ 'Value' ][0][0]
    if numTQs == 0:
      return S_OK()
    prio = share / numTQs
    tqPriority  = min( max( int( prio ), self.__tqPriorityBoundaries[0]  ), self.__tqPriorityBoundaries[1]  )
    updateSQL = "UPDATE `tq_TaskQueues` SET Priority=%s WHERE %s" % ( tqPriority, condSQL )
    return self._update( updateSQL, conn = connObj )

  def getGroupShares(self):
    """
    Get all the shares as a DICT
    """
    result = gConfig.getSections( "/Security/Groups" )
    if result[ 'OK' ]:
      groups = result[ 'Value' ]
    else:
      groups = []
    shares = {}
    for group in groups:
      shares[ group ] = gConfig.getValue( "/Security/Groups/%s/JobsShare" % group, self.defaultGroupShare )
    return shares

  def propagateSharesIfChanged(self):
    """
    If the shares have changed in the CS, recalculate priorities
    """
    shares = self.getGroupShares()
    if shares == self.__groupShares:
      return S_OK()
    self.__groupShares = shares
    return self.recalculateSharesForAll()
