########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/TaskQueueDB.py,v 1.62 2009/01/30 10:13:20 acasajus Exp $
########################################################################
""" TaskQueueDB class is a front-end to the task queues db
"""

__RCSID__ = "$Id: TaskQueueDB.py,v 1.62 2009/01/30 10:13:20 acasajus Exp $"

import time
import types
import random
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.Core.Utilities import List
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security import Properties, CS

class TaskQueueDB(DB):

  defaultGroupShare = 1000

  def __init__( self, maxQueueSize = 10 ):
    random.seed()
    DB.__init__( self, 'TaskQueueDB', 'WorkloadManagement/TaskQueueDB', maxQueueSize )
    self.__multiValueDefFields = ( 'Sites', 'GridCEs', 'GridMiddlewares', 'BannedSites', 'LHCbPlatforms', 'PilotTypes', 'SubmitPools' )
    self.__multiValueMatchFields = ( 'GridCE', 'Site', 'GridMiddleware', 'LHCbPlatform', 'PilotType', 'SubmitPool' )
    self.__singleValueDefFields = ( 'OwnerDN', 'OwnerGroup', 'Setup', 'CPUTime' )
    self.__mandatoryMatchFields = ( 'Setup', 'CPUTime' )
    self.maxCPUSegments = ( 500, 5000, 50000, 300000 )
    self.__maxMatchRetry = 3
    self.__jobPriorityBoundaries = ( 0.001, 10 )
    self.__groupShares = {}
    self.__csSection = getDatabaseSection( "WorkloadManagement/TaskQueueDB" )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ])

  def getSingleValueTQDefFields( self ):
    return self.__singleValueDefFields

  def getMultiValueTQDefFields( self ):
    return self.__multiValueDefFields

  def getMultiValueMatchFields( self ):
    return self.__multiValueMatchFields

  def __getCSOption( self, optionName, defValue ):
    return gConfig.getValue( "%s/%s" % ( self.__csSection, optionName ), defValue )

  def getPrivatePilots( self ):
    return self.__getCSOption( "PrivatePilotTypes", [ 'private' ] )

  def getValidPilotTypes( self ):
    return self.__getCSOption( "AllPilotTypes", [ 'private' ] )

  def __initializeDB(self):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result

    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc[ 'tq_TaskQueues' ] = { 'Fields' : { 'TQId' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                          'OwnerDN' : 'VARCHAR(255) NOT NULL',
                                                          'OwnerGroup' : 'VARCHAR(32) NOT NULL',
                                                          'Setup' : 'VARCHAR(32) NOT NULL',
                                                          'CPUTime' : 'BIGINT UNSIGNED NOT NULL',
                                                          'Priority' : 'FLOAT NOT NULL',
                                                          'Enabled' : 'TINYINT(1) NOT NULL DEFAULT 0'
                                                           },
                                             'PrimaryKey' : 'TQId',
                                             'Indexes': { 'TQOwner': [ 'OwnerDN', 'OwnerGroup',
                                                                       'Setup', 'CPUTime' ]
                                                        }
                                            }

    self.__tablesDesc[ 'tq_Jobs' ] = { 'Fields' : { 'TQId' : 'INTEGER UNSIGNED NOT NULL',
                                                    'JobId' : 'INTEGER UNSIGNED NOT NULL',
                                                    'Priority' : 'INTEGER UNSIGNED NOT NULL',
                                                    'RealPriority' : 'FLOAT NOT NULL'
                                                  },
                                       'PrimaryKey' : 'JobId',
                                       'Indexes': { 'TaskIndex': [ 'TQId' ] },
                                     }

    for multiField in self.__multiValueDefFields:
      tableName = 'tq_TQTo%s' % multiField
      self.__tablesDesc[ tableName ] = { 'Fields' : { 'TQId' : 'INTEGER UNSIGNED NOT NULL',
                                                      'Value' : 'VARCHAR(64) NOT NULL',
                                                  },
                                         'Indexes': { 'TaskIndex': [ 'TQId' ], '%sIndex' % multiField: [ 'Value' ] },
                                       }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def forceRecreationOfTables( self ):
    dropSQL = "DROP TABLE IF EXISTS %s" % ", ".join( self.__tablesDesc )
    result = self._update( dropSQL )
    if not result[ 'OK' ]:
      return result
    return self._createTables( self.__tablesDesc )

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
    if 'PrivatePilots' in tqDefDict:
      validPilotTypes = self.getValidPilotTypes()
      for pilotType in tqDefDict[ 'PrivatePilots' ]:
        if pilotType not in validPilotTypes:
          return S_ERROR( "PilotType %s is invalid" % pilotType )
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

  def __createTaskQueue( self, tqDefDict, priority = 1, skipDefinitionCheck = False, enabled = False, connObj = False ):
    """
    Create a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    if not skipDefinitionCheck:
      result = self._checkTaskQueueDefinition( tqDefDict )
      if not result[ 'OK' ]:
        self.log.error( "TQ definition check failed", result[ 'Value' ] )
        return result
    if not connObj:
      result = self._getConnection()
      if not result[ 'OK' ]:
        return S_ERROR( "Can't create task queue: %s" % result[ 'Message' ] )
      connObj = result[ 'Value' ]
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
    sqlSingleFields = [ 'TQId', 'Priority' ]
    sqlValues = [ "0", str( priority ) ]
    for field in self.__singleValueDefFields:
      sqlSingleFields.append( field )
      sqlValues.append( "'%s'" % tqDefDict[ field ] )
    #Insert the TQ Disabled
    sqlSingleFields.append( "Enabled" )
    sqlValues.append( str( int( enabled ) ) )
    cmd = "INSERT INTO tq_TaskQueues ( %s ) VALUES ( %s )" % ( ", ".join( sqlSingleFields ), ", ".join( sqlValues ) )
    result = self._update( cmd, conn = connObj )
    if not result[ 'OK' ]:
      self.log.error( "Can't insert TQ in DB", result[ 'Value' ] )
      return result
    if 'lastRowId' in result:
      tqId = result['lastRowId']
    else:
      result = self._query( "SELECT LAST_INSERT_ID()", conn = connObj )
      if not result[ 'OK' ]:
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't determine task queue id after insertion" )
      tqId = result[ 'Value' ][0][0]
    for field in self.__multiValueDefFields:
      if field not in tqDefDict:
        continue
      values = List.uniqueElements( [ value for value in tqDefDict[ field ] if value.strip() ] )
      if not values:
        continue
      cmd = "INSERT INTO `tq_TQTo%s` ( TQId, Value ) VALUES " % field
      cmd += ", ".join( [ "( %s, '%s' )" % ( tqId, str( value ) ) for value in values ] )
      result = self._update( cmd, conn = connObj )
      if not result[ 'OK' ]:
        self.log.error( "Failed to insert %s condition" % field, result[ 'Message' ] )
        self.cleanOrphanedTaskQueues( connObj = connObj )
        return S_ERROR( "Can't insert values %s for field %s: %s" % ( str( values ), field, result[ 'Message' ] ) )
    self.log.info( "Created TQ %s" % tqId )
    return S_OK( tqId )

  def cleanOrphanedTaskQueues( self, connObj = False ):
    """
    Delete all empty task queues
    """
    self.log.info( "Cleaning orphaned TQs" )
    result = self._update( "DELETE FROM `tq_TaskQueues` WHERE Enabled AND TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )", conn = connObj )
    if not result[ 'OK' ]:
      return result
    for mvField in self.__multiValueDefFields:
      result = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_TaskQueues` )" % mvField,
                             conn = connObj )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def setTaskQueueState( self, tqId, enabled = True, connObj = False ):
    upSQL = "UPDATE `tq_TaskQueues` SET Enabled=%d WHERE TQId=%d" % ( int( enabled ), tqId )
    result = self._update( upSQL, conn = connObj )
    if not result[ 'OK' ]:
      self.log.error( "Error setting TQ state", "TQ %s State %s: %s" % ( tqId, enabled, result[ 'Message' ] ) )
      return result
    updated = result['Value'] > 0
    if updated:
      self.log.info( "Set enabled = %s for TQ %s" % ( enabled, tqId ) )
    return S_OK( updated )

  def __hackJobPriority( self, jobPriority ):
    jobPriority = min( max( int( jobPriority ), self.__jobPriorityBoundaries[0] ), self.__jobPriorityBoundaries[1] )
    if jobPriority == self.__jobPriorityBoundaries[0]:
      return 10**(-5)
    if jobPriority == self.__jobPriorityBoundaries[1]:
      return 10**6
    return jobPriority

  def insertJob( self, jobId, tqDefDict, jobPriority, skipTQDefCheck = False, numRetries = 10 ):
    """
    Insert a job in a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    if not skipTQDefCheck:
      retVal = self._checkTaskQueueDefinition( tqDefDict )
      if not retVal[ 'OK' ]:
        self.log.error( "TQ definition check failed", retVal[ 'Message' ] )
        return retVal
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
    self.log.info( "Inserting job %s with requirements: %s" % ( jobId, self.__strDict( tqDefDict ) ) )
    retVal = self.findTaskQueue( tqDefDict, skipDefinitionCheck = True, connObj = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    tqInfo = retVal[ 'Value' ]
    newTQ = False
    if not tqInfo[ 'found' ]:
      self.log.info( "Creating a TQ for job %s" % jobId)
      retVal = self.__createTaskQueue( tqDefDict, 1, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqId = retVal[ 'Value' ]
      newTQ = True
    else:
      tqId = tqInfo[ 'tqId' ]
      self.log.info( "Found TQ %s for job %s requirements" % ( tqId, jobId ) )
      result = self.setTaskQueueState( tqId, False )
      if not result[ 'OK' ]:
        return result
      if not result[ 'Value' ]:
        time.sleep(0.1)
        if numRetries <= 0:
          self.log.info( "Couldn't manage to disable TQ %s for job %s insertion, max retries reached. Aborting" % ( tqId, jobId ) )
          return S_ERROR( "Max reties reached for inserting job %s" % jobId )
        self.log.info( "Couldn't manage to disable TQ %s for job %s insertion, retrying" % ( tqId, jobId ) )
        return self.insertJob( jobId, tqDefDict, jobPriority, skipTQDefCheck = True, numRetries = numRetries - 1 )
    result = self.__insertJobInTaskQueue( jobId, tqId, int( jobPriority ), checkTQExists = False, connObj = connObj )
    if not result[ 'OK' ]:
      self.log.error( "Error inserting job in TQ", "Job %s TQ %s: %s" % ( jobId, tqId, result[ 'Message' ] ) )
      return result
    if newTQ:
      self.recalculateTQSharesForEntity( tqDefDict[ 'OwnerDN' ], tqDefDict[ 'OwnerGroup' ], connObj = connObj )
    return self.setTaskQueueState( tqId, True )

  def __insertJobInTaskQueue( self, jobId, tqId, jobPriority, checkTQExists = True, connObj = False ):
    """
    Insert a job in a given task queue
    """
    self.log.info( "Inserting job %s in TQ %s with priority %s" % ( jobId, tqId, jobPriority ) )
    if not connObj:
      result = self._getConnection()
      if not result[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % result[ 'Message' ] )
      connObj = result[ 'Value' ]
    if checkTQExists:
      result = self._query( "SELECT tqId FROM `tq_TaskQueues` WHERE TQId = %s" % tqId, conn = connObj )
      if not result[ 'OK' ] or len ( result[ 'Value' ] ) == 0:
        return S_OK( "Can't find task queue with id %s: %s" % ( tqId, result[ 'Message' ] ) )
    hackedPriority = self.__hackJobPriority( jobPriority )
    return self._update( "INSERT INTO tq_Jobs ( TQId, JobId, Priority, RealPriority ) VALUES ( %s, %s, %s, %f )" % ( tqId, jobId, jobPriority, hackedPriority ), conn = connObj )

  def findTaskQueue( self, tqDefDict, skipDefinitionCheck= False, connObj = False ):
    """
      Find a task queue that has exactly the same requirements
    """
    if not skipDefinitionCheck:
      result = self._checkTaskQueueDefinition( tqDefDict )
      if not result[ 'OK' ]:
        return result
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
    result = self._query( sqlCmd, conn = connObj )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't find task queue: %s" % result[ 'Message' ] )
    data = result[ 'Value' ]
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
    preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s AND `tq_Jobs`.Priority = %s"
    prioSQL = "SELECT `tq_Jobs`.Priority FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s ORDER BY RAND() / `tq_Jobs`.RealPriority ASC LIMIT 1"
    postJobSQL = " ORDER BY `tq_Jobs`.JobId ASC LIMIT %s" % numJobsPerTry
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
        return S_OK( { 'matchFound' : False, 'tqMatch' : tqMatchDict } )
      for tqId, tqOwnerDN, tqOwnerGroup in tqList:
        self.log.info( "Trying to extract jobs from TQ %s" % tqId )
        retVal = self._query( prioSQL % tqId, conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't retrieve winning priority for matching job: %s" % retVal[ 'Message' ] )
        prio = retVal[ 'Value' ][0][0]
        retVal = self._query( "%s %s" % ( preJobSQL % ( tqId, prio ), postJobSQL ), conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't begin transaction for matching job: %s" % retVal[ 'Message' ] )
        jobTQList = [ ( row[0], row[1] ) for row in retVal[ 'Value' ] ]
        if len( jobTQList ) == 0:
          gLogger.info( "Task queue %s seems to be empty, triggering a cleaning" % tqId )
          result = self.deleteTaskQueueIfEmpty( tqId, tqOwnerDN, tqOwnerGroup, connObj = connObj )
          if not result[ 'OK' ]:
            return result
        while len( jobTQList ) > 0:
          jobId, tqId = jobTQList.pop( random.randint( 0, len( jobTQList ) - 1 ) )
          self.log.info( "Trying to extract job %s from TQ %s" % ( jobId, tqId ) )
          retVal = self.deleteJob( jobId, connObj = connObj )
          if not retVal[ 'OK' ]:
            msg = "Could not take job %s out from the TQ %s: %s" % ( jobId, tqId, retVal[ 'Message' ] )
            self.log.error( msg )
            return S_ERROR( msg )
          if retVal[ 'Value' ] == True :
            self.log.info( "Extracted job %s with prio %s from TQ %s" % ( jobId, prio, tqId ) )
            return S_OK( { 'matchFound' : True, 'jobId' : jobId, 'taskQueueId' : tqId, 'tqMatch' : tqMatchDict } )
        self.log.info( "No jobs could be extracted from TQ %s" % tqId )
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
    if 'PilotType' in tqMatchDict:
      pilotType = tqMatchDict[ 'PilotType' ]
      if pilotType in self.getPrivatePilots():
        for field in ( 'OwnerDN', 'OwnerGroup' ):
          sqlCondList.append( "`tq_TaskQueues`.%s = '%s'" % ( field, tqMatchDict[ field ] ) )
    #Type of pilot conditions
    for field in ( 'CPUTime', 'Setup' ):
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
    sqlCondList.append( "Enabled" )
    tqSqlCmd = "SELECT `tq_TaskQueues`.TQId, `tq_TaskQueues`.OwnerDN, `tq_TaskQueues`.OwnerGroup FROM %s WHERE %s" % ( ", ".join( sqlTables ),
                                                                                                                      " AND ".join( sqlCondList ) )
    tqSqlCmd = "%s ORDER BY `tq_TaskQueues`.CPUTime DESC, RAND() / `tq_TaskQueues`.Priority ASC" % tqSqlCmd
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
    retVal = self._query( "SELECT t.TQId, t.OwnerDN, t.OwnerGroup FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE j.JobId = %s AND t.TQId = j.TQId" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not get job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    data = retVal[ 'Value' ]
    if not data:
      return S_OK( False )
    tqId, tqOwnerDN, tqOwnerGroup = data[0]
    retVal = self._update( "DELETE FROM `tq_Jobs` WHERE JobId = %s" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    result = retVal[ 'Value' ]
    if retVal[ 'Value' ] == 0:
      #No job deleted
      return S_OK( False )
    retries = 10
    #Always return S_OK() because job has already been taken out from the TQ
    while retries:
      result = self.deleteTaskQueueIfEmpty( tqId, tqOwnerDN, tqOwnerGroup, connObj = connObj )
      if result[ 'OK' ]:
        return S_OK( True )
      if not result[ 'OK' ]:
        if result[ 'Message' ].find( "try restarting transaction" ) == -1:
          self.log.error( "Error on TQ deletion triggered by job deletion", "Job %s TQ %s : %s" % ( tqId, jobId, result[ 'Message' ] ) )
          return S_OK( True )
      retries -= 1
    self.log.error( "Max retries when trying to delete TQ %s triggered by deletion of job %s" % ( tqId, jobId ) )
    return S_OK( True )

  def getTaskQueueForJob( self, jobId, connObj = False ):
    """
    Return TaskQueue for a given Job
    Return S_OK( [TaskQueueID] ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't delete job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]

    retVal = self._query( 'SELECT TQId FROM `tq_Jobs` WHERE JobId = %s ' % jobId, conn = connObj )

    if not retVal[ 'OK' ]:
      return retVal

    if not retVal['Value']:
      return S_ERROR('Not in TaskQueues')

    return S_OK( retVal['Value'][0][0] )

  def __getOwnerForTaskQueue( self, tqId, connObj = False ):
    retVal = self._query( "SELECT OwnerDN, OwnerGroup from `tq_TaskQueues` WHERE TQId=%s" % tqId, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_OK( False )
    return S_OK( retVal[ 'Value' ][0] )

  def deleteTaskQueueIfEmpty( self, tqId, tqOwnerDN = False, tqOwnerGroup = False, connObj = False ):
    """
    Try to delete a task queue if its empty
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    if not tqOwnerDN or not tqOwnerGroup:
      retVal = self.__getOwnerForTaskQueue( tqId, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      data = retVal[ 'Value' ]
      if not data:
        return S_OK( False )
      tqOwnerDN, tqOwnerGroup = data
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE Enabled AND `tq_TaskQueues`.TQId = %s" % tqId
    sqlCmd = "%s AND `tq_TaskQueues`.TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )" % sqlCmd
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    delTQ = retVal[ 'Value' ]
    if delTQ > 0:
      for mvField in self.__multiValueDefFields:
        retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % ( mvField, tqId ), conn = connObj )
        if not retVal[ 'OK' ]:
          return retVal
      self.recalculateTQSharesForEntity( tqOwnerDN, tqOwnerGroup, connObj = connObj )
      self.log.info( "Deleted empty and enabled TQ %s" % tqId )
      return S_OK( True )
    return S_OK( False )

  def deleteTaskQueue( self, tqId, tqOwnerDN = False, tqOwnerGroup = False, connObj = False ):
    """
    Try to delete a task queue even if it has jobs
    """
    self.log.info( "Deleting TQ %s" % tqId )
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]
    if not tqOwnerDN or not tqOwnerGroup:
      retVal = self.__getOwnerForTaskQueue( tqId, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      data = retVal[ 'Value' ]
      if not data:
        return S_OK( False )
      tqOwnerDN, tqOwnerGroup = data
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    delTQ = retVal[ 'Value' ]
    sqlCmd = "DELETE FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s" % tqId
    retVal = self._update( sqlCmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete task queue %s: %s" % ( tqId, retVal[ 'Message' ] ) )
    for mvField in self.__multiValueDefFields:
      retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % tqId, conn = connObj )
      if not retVal[ 'OK' ]:
        return retVal
    if delTQ > 0:
      self.recalculateTQSharesForEntity( tqOwnerDN, tqOwnerGroup, connObj = connObj )
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
          self.log.warn( "Task Queue %s is defined in field %s but does not exist, triggering a cleaning" % ( tqId, field ) )
          tqNeedCleaning = True
        else:
          if field not in tqData[ tqId ]:
            tqData[ tqId ][ field ] = []
          tqData[ tqId ][ field ].append( value )
    if tqNeedCleaning:
      self.cleanOrphanedTaskQueues()
    return S_OK( tqData )

  def __updateShares(self):
    """
    Update internal structure for shares
    """
    #Update group shares
    self.__groupShares = self.getGroupShares()

  def recalculateTQSharesForAll(self):
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
      self.recalculateTQSharesForEntity( "all", group )
    return S_OK()

  def recalculateTQSharesForEntity( self, userDN, userGroup, connObj = False ):
    """
    Recalculate the shares for a userDN/userGroup combo
    """
    self.__updateShares()
    self.log.info( "Recalculating shares for %s@%s TQs" % ( userDN, userGroup ) )
    share = gConfig.getValue( "/Security/Groups/%s/JobShare" % userGroup, float( self.defaultGroupShare ) )
    if Properties.JOB_SHARING in CS.getPropertiesForGroup( userGroup ):
      #If group has JobSharing just set prio for that entry, userDN is irrelevant
      return self.__setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )

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
      return self.__setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )
    #Oops the number of owners may have changed so we recalculate the prio for all owners in the group
    for userDN in owners:
      self.__setPrioritiesForEntity( userDN, userGroup, share, connObj = connObj )
    return S_OK()

  def __setPrioritiesForEntity( self, userDN, userGroup, share, connObj = False ):
    """
    Set the priority for a userDN/userGroup combo given a splitted share
    """
    self.log.info( "Setting priorities to %s@%s TQs" % ( userDN, userGroup ) )
    tqCond = [ "t.OwnerGroup='%s'" % userGroup ]
    if Properties.JOB_SHARING not in CS.getPropertiesForGroup( userGroup ):
      tqCond.append( "t.OwnerDN='%s'" % userDN )
    tqCond.append( "t.TQId = j.TQId" )
    selectSQL = "SELECT j.TQId, SUM( j.RealPriority ) FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
    selectSQL += " AND ".join( tqCond )
    selectSQL += " GROUP BY t.TQId"
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    tqDict = dict( result[ 'Value' ] )
    if len( tqDict ) == 0:
      return S_OK()
    #Calculate Sum of priorities
    totalPrio = 0
    for k in tqDict:
      totalPrio += tqDict[ k ]
    #Group by priorities
    prioDict = {}
    for tqId in tqDict:
      prio = ( share / totalPrio ) * tqDict[ tqId ]
      if prio not in prioDict:
        prioDict[ prio ] = []
      prioDict[ prio ].append( tqId )
    #Execute updates
    for prio in prioDict:
      tqList = ", ".join( [ str( tqId ) for tqId in prioDict[ prio ] ] )
      updateSQL = "UPDATE `tq_TaskQueues` SET Priority=%.4f WHERE TQId in ( %s )" % ( prio, tqList )
      self._update( updateSQL, conn = connObj )
    return S_OK()

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
      shares[ group ] = gConfig.getValue( "/Security/Groups/%s/JobShare" % group, self.defaultGroupShare )
    return shares

  def propagateTQSharesIfChanged(self):
    """
    If the shares have changed in the CS, recalculate priorities
    """
    shares = self.getGroupShares()
    if shares == self.__groupShares:
      return S_OK()
    self.__groupShares = shares
    return self.recalculateTQSharesForAll()

  def modifyJobsPriorities( self, jobPrioDict ):
    """
    Modify the priority for some jobs
    """
    for jId in jobPrioDict:
      jobPrioDict[jId] = int( jobPrioDict[jId] )
    maxJobsInQuery = 1000
    jobsList = sorted( jobPrioDict )
    prioDict = {}
    for jId in jobsList:
      prio = jobPrioDict[ jId ]
      if not prio in prioDict:
        prioDict[ prio ] = []
      prioDict[ prio ].append( str( jId ) )
    updated = 0
    for prio in prioDict:
      jobsList = prioDict[ prio ]
      for i in range( maxJobsInQuery, 0, len( jobsList ) ):
        jobs = ",".join( jobsList[ i : i + maxJobsInQuery ] )
        updateSQL = "UPDATE `tq_Jobs` SET `Priority`=%s, `RealPriority`=%f WHERE `JobId` in ( %s )" % ( prio, self.__hackJobPriority( prio ), jobs )
        result = self._update( updateSQL )
        if not result[ 'OK' ]:
          return result
        updated += result[ 'Value' ]
    if not updated:
      return S_OK()
    return self.recalculateTQSharesForAll()
