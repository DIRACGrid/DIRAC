""" TaskQueueDB class is a front-end to the task queues db
"""

__RCSID__ = "ebed3a8 (2012-07-06 20:33:11 +0200) Adri Casajs <adria@ecm.ub.es>"

import types
import random
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.private.SharesCorrector import SharesCorrector
from DIRAC.WorkloadManagementSystem.private.Queues import maxCPUSegments
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security import Properties, CS

DEFAULT_GROUP_SHARE = 1000
TQ_MIN_SHARE = 0.001

class TaskQueueDB( DB ):

  def __init__( self, maxQueueSize = 10 ):
    random.seed()
    DB.__init__( self, 'TaskQueueDB', 'WorkloadManagement/TaskQueueDB', maxQueueSize )
    self.__multiValueDefFields = ( 'Sites', 'GridCEs', 'GridMiddlewares', 'BannedSites',
                                   'Platforms', 'PilotTypes', 'SubmitPools', 'JobTypes', 'Tags' )
    self.__multiValueMatchFields = ( 'GridCE', 'Site', 'GridMiddleware', 'Platform',
                                     'PilotType', 'SubmitPool', 'JobType', 'Tag' )
    self.__tagMatchFields = ( 'Tag', )
    self.__bannedJobMatchFields = ( 'Site', )
    self.__strictRequireMatchFields = ( 'SubmitPool', 'Platform', 'PilotType', 'Tag' )
    self.__singleValueDefFields = ( 'OwnerDN', 'OwnerGroup', 'Setup', 'CPUTime' )
    self.__mandatoryMatchFields = ( 'Setup', 'CPUTime' )
    self.__priorityIgnoredFields = ( 'Sites', 'BannedSites' )
    self.__maxJobsInTQ = 5000
    self.__defaultCPUSegments = maxCPUSegments
    self.__maxMatchRetry = 3
    self.__jobPriorityBoundaries = ( 0.001, 10 )
    self.__groupShares = {}
    self.__deleteTQWithDelay = DictCache( self.__deleteTQIfEmpty )
    self.__opsHelper = Operations()
    self.__ensureInsertionIsSingle = False
    self.__sharesCorrector = SharesCorrector( self.__opsHelper )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

  def enableAllTaskQueues( self ):
    """ Enable all Task queues
    """
    return self.updateFields( "tq_TaskQueues", updateDict = { "Enabled" :"1" } )

  def findOrphanJobs( self ):
    """ Find jobs that are not in any task queue
    """
    result = self._query( "select JobID from tq_Jobs WHERE TQId not in (SELECT TQId from tq_TaskQueues)" )
    if not result[ 'OK' ]:
      return result
    return S_OK( [ row[0] for row in result[ 'Value' ] ] )

  def isSharesCorrectionEnabled( self ):
    return self.__getCSOption( "EnableSharesCorrection", False )

  def getSingleValueTQDefFields( self ):
    return self.__singleValueDefFields

  def getMultiValueTQDefFields( self ):
    return self.__multiValueDefFields

  def getMultiValueMatchFields( self ):
    return self.__multiValueMatchFields

  def __getCSOption( self, optionName, defValue ):
    return self.__opsHelper.getValue( "JobScheduling/%s" % optionName, defValue )

  def getPrivatePilots( self ):
    return self.__getCSOption( "PrivatePilotTypes", [ 'private' ] )

  def getValidPilotTypes( self ):
    return self.__getCSOption( "AllPilotTypes", [ 'private' ] )

  def __initializeDB( self ):
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
                                                      'Value' : 'VARCHAR(64) NOT NULL'
                                                    },
                                         'Indexes': { 'TaskIndex': [ 'TQId' ], '%sIndex' % multiField: [ 'Value' ] },
                                       }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def getGroupsInTQs( self ):
    cmdSQL = "SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`"
    result = self._query( cmdSQL )
    if not result[ 'OK' ]:
      return result
    return S_OK( [ row[0] for row in result[ 'Value' ] ] )

  def forceRecreationOfTables( self ):
    dropSQL = "DROP TABLE IF EXISTS %s" % ", ".join( self.__tablesDesc )
    result = self._update( dropSQL )
    if not result[ 'OK' ]:
      return result
    return self._createTables( self.__tablesDesc )

  def __strDict( self, dDict ):
    lines = []
    keyLength = 0
    for key in dDict:
      if len( key ) > keyLength:
        keyLength = len( key )
    for key in sorted( dDict ):
      line = "%s: " % key
      line = line.ljust( keyLength + 2 )
      value = dDict[ key ]
      if type( value ) in ( types.ListType, types.TupleType ):
        line += ','.join( list( value ) )
      else:
        line += str( value )
      lines.append( line )
    return "{\n%s\n}" % "\n".join( lines )

  def fitCPUTimeToSegments( self, cpuTime ):
    """
    Fit the CPU time to the valid segments
    """
    maxCPUSegments = self.__getCSOption( "taskQueueCPUTimeIntervals", self.__defaultCPUSegments )
    try:
      maxCPUSegments = [ int( seg ) for seg in maxCPUSegments ]
      #Check segments in the CS
      last = 0
      for cpuS in maxCPUSegments:
        if cpuS <= last:
          maxCPUSegments = self.__defaultCPUSegments
          break
        last = cpuS
    except:
      maxCPUSegments = self.__defaultCPUSegments
    #Map to a segment
    for iP in range( len( maxCPUSegments ) ):
      cpuSegment = maxCPUSegments[ iP ]
      if cpuTime <= cpuSegment:
        return cpuSegment
    return maxCPUSegments[-1]

  def _checkTaskQueueDefinition( self, tqDefDict ):
    """
    Check a task queue definition dict is valid
    """

    # Confine the LHCbPlatform legacy option here, use Platform everywhere else
    # until the LHCbPlatform is no more used in the TaskQueueDB
    if 'LHCbPlatforms' in tqDefDict and not "Platforms" in tqDefDict:
      tqDefDict['Platforms'] = tqDefDict['LHCbPlatforms']
    if 'SystemConfigs' in tqDefDict and not "Platforms" in tqDefDict:
      tqDefDict['Platforms'] = tqDefDict['SystemConfigs']

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
        result = self._escapeString( tqDefDict[ field ] )
        if not result[ 'OK' ]:
          return result
        tqDefDict[ field ] = result[ 'Value' ]
    for field in self.__multiValueDefFields:
      if field not in tqDefDict:
        continue
      fieldValueType = type( tqDefDict[ field ] )
      if fieldValueType not in ( types.ListType, types.TupleType ):
        return S_ERROR( "Multi value field %s value type is not valid: %s" % ( field, fieldValueType ) )
      result = self._escapeValues( tqDefDict[ field ] )
      if not result[ 'OK' ]:
        return result
      tqDefDict[ field ] = result[ 'Value' ]
    #FIXME: This is not used
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
    def travelAndCheckType( value, validTypes, escapeValues = True ):
      valueType = type( value )
      if valueType in ( types.ListType, types.TupleType ):
        for subValue in value:
          subValueType = type( subValue )
          if subValueType not in validTypes:
            return S_ERROR( "List contained type %s is not valid -> %s" % ( subValueType, validTypes ) )
        if escapeValues:
          return self._escapeValues( value )
        return S_OK( value )
      else:
        if valueType not in validTypes:
          return S_ERROR( "Type %s is not valid -> %s" % ( valueType, validTypes ) )
        if escapeValues:
          return self._escapeString( value )
        return S_OK( value )

    # Confine the LHCbPlatform legacy option here, use Platform everywhere else
    # until the LHCbPlatform is no more used in the TaskQueueDB
    if 'LHCbPlatform' in tqMatchDict and not "Platform" in tqMatchDict:
      tqMatchDict['Platform'] = tqMatchDict['LHCbPlatform']
    if 'SystemConfig' in tqMatchDict and not "Platform" in tqMatchDict:
      tqMatchDict['Platform'] = tqMatchDict['SystemConfig']

    for field in self.__singleValueDefFields:
      if field not in tqMatchDict:
        if field in self.__mandatoryMatchFields:
          return S_ERROR( "Missing mandatory field '%s' in match request definition" % field )
        continue
      fieldValue = tqMatchDict[ field ]
      if field in [ "CPUTime" ]:
        result = travelAndCheckType( fieldValue, ( types.IntType, types.LongType ), escapeValues = False )
      else:
        result = travelAndCheckType( fieldValue, ( types.StringType, types.UnicodeType ) )
      if not result[ 'OK' ]:
        return S_ERROR( "Match definition field %s failed : %s" % ( field, result[ 'Message' ] ) )
      tqMatchDict[ field ] = result[ 'Value' ]
    #Check multivalue
    for multiField in self.__multiValueMatchFields:
      for field in ( multiField, "Banned%s" % multiField ):
        if field in tqMatchDict:
          fieldValue = tqMatchDict[ field ]
          result = travelAndCheckType( fieldValue, ( types.StringType, types.UnicodeType ) )
          if not result[ 'OK' ]:
            return S_ERROR( "Match definition field %s failed : %s" % ( field, result[ 'Message' ] ) )
          tqMatchDict[ field ] = result[ 'Value' ]

    return S_OK( tqMatchDict )

  def __createTaskQueue( self, tqDefDict, priority = 1, connObj = False ):
    """
    Create a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
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
      sqlValues.append( tqDefDict[ field ] )
    #Insert the TQ Disabled
    sqlSingleFields.append( "Enabled" )
    sqlValues.append( "0" )
    cmd = "INSERT INTO tq_TaskQueues ( %s ) VALUES ( %s )" % ( ", ".join( sqlSingleFields ), ", ".join( [ str( v ) for v in sqlValues ] ) )
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
      cmd += ", ".join( [ "( %s, %s )" % ( tqId, str( value ) ) for value in values ] )
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
    result = self._update( "DELETE FROM `tq_TaskQueues` WHERE Enabled >= 1 AND TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )", conn = connObj )
    if not result[ 'OK' ]:
      return result
    for mvField in self.__multiValueDefFields:
      result = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId not in ( SELECT DISTINCT TQId from `tq_TaskQueues` )" % mvField,
                             conn = connObj )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def __setTaskQueueEnabled( self, tqId, enabled = True, connObj = False ):
    if enabled:
      enabled = "+ 1"
    else:
      enabled = "- 1"
    upSQL = "UPDATE `tq_TaskQueues` SET Enabled = Enabled %s WHERE TQId=%d" % ( enabled, tqId )
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
      return 10 ** ( -5 )
    if jobPriority == self.__jobPriorityBoundaries[1]:
      return 10 ** 6
    return jobPriority

  def insertJob( self, jobId, tqDefDict, jobPriority, skipTQDefCheck = False, numRetries = 10 ):
    """
    Insert a job in a task queue
      Returns S_OK( tqId ) / S_ERROR
    """
    try:
      long( jobId )
    except ValueError:
      return S_ERROR( "JobId is not a number!" )
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    if not skipTQDefCheck:
      tqDefDict = dict( tqDefDict )
      retVal = self._checkTaskQueueDefinition( tqDefDict )
      if not retVal[ 'OK' ]:
        self.log.error( "TQ definition check failed", retVal[ 'Message' ] )
        return retVal
      tqDefDict = retVal[ 'Value' ]
    tqDefDict[ 'CPUTime' ] = self.fitCPUTimeToSegments( tqDefDict[ 'CPUTime' ] )
    self.log.info( "Inserting job %s with requirements: %s" % ( jobId, self.__strDict( tqDefDict ) ) )
    retVal = self.__findAndDisableTaskQueue( tqDefDict, skipDefinitionCheck = True, connObj = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    tqInfo = retVal[ 'Value' ]
    newTQ = False
    if not tqInfo[ 'found' ]:
      self.log.info( "Creating a TQ for job %s" % jobId )
      retVal = self.__createTaskQueue( tqDefDict, 1, connObj = connObj )
      if not retVal[ 'OK' ]:
        return retVal
      tqId = retVal[ 'Value' ]
      newTQ = True
    else:
      tqId = tqInfo[ 'tqId' ]
      self.log.info( "Found TQ %s for job %s requirements" % ( tqId, jobId ) )
    try:
      result = self.__insertJobInTaskQueue( jobId, tqId, int( jobPriority ), checkTQExists = False, connObj = connObj )
      if not result[ 'OK' ]:
        self.log.error( "Error inserting job in TQ", "Job %s TQ %s: %s" % ( jobId, tqId, result[ 'Message' ] ) )
        return result
      if newTQ:
        self.recalculateTQSharesForEntity( tqDefDict[ 'OwnerDN' ], tqDefDict[ 'OwnerGroup' ], connObj = connObj )
    finally:
      self.__setTaskQueueEnabled( tqId, True )
    return S_OK()

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
    result = self._update( "INSERT INTO tq_Jobs ( TQId, JobId, Priority, RealPriority ) VALUES ( %s, %s, %s, %f ) ON DUPLICATE KEY UPDATE TQId = %s, Priority = %s, RealPriority = %f" % ( tqId, jobId, jobPriority, hackedPriority, tqId, jobPriority, hackedPriority ), conn = connObj )
    if not result[ 'OK' ]:
      return result
    return S_OK()

  def __generateTQFindSQL( self, tqDefDict, skipDefinitionCheck = False, connObj = False ):
    """
      Find a task queue that has exactly the same requirements
    """
    if not skipDefinitionCheck:
      tqDefDict = dict( tqDefDict )
      result = self._checkTaskQueueDefinition( tqDefDict )
      if not result[ 'OK' ]:
        return result
      tqDefDict = result[ 'Value' ]

    sqlCondList = []
    for field in self.__singleValueDefFields:
      sqlCondList.append( "`tq_TaskQueues`.%s = %s" % ( field, tqDefDict[ field ] ) )
    #MAGIC SUBQUERIES TO ENSURE STRICT MATCH
    for field in self.__multiValueDefFields:
      tableName = '`tq_TQTo%s`' % field
      if field in tqDefDict and tqDefDict[ field ]:
        firstQuery = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = `tq_TaskQueues`.TQId" % ( tableName, tableName, tableName )
        grouping = "GROUP BY %s.TQId" % tableName
        valuesList = List.uniqueElements( [ value.strip() for value in tqDefDict[ field ] if value.strip() ] )
        numValues = len( valuesList )
        secondQuery = "%s AND %s.Value in (%s)" % ( firstQuery, tableName,
                                                        ",".join( [ "%s" % str( value ) for value in valuesList ] ) )
        sqlCondList.append( "%s = (%s %s)" % ( numValues, firstQuery, grouping ) )
        sqlCondList.append( "%s = (%s %s)" % ( numValues, secondQuery, grouping ) )
      else:
        sqlCondList.append( "`tq_TaskQueues`.TQId not in ( SELECT DISTINCT %s.TQId from %s )" % ( tableName, tableName ) )
    #END MAGIC: That was easy ;)
    return S_OK( " AND ".join( sqlCondList ) )


  def __findAndDisableTaskQueue( self, tqDefDict, skipDefinitionCheck = False, retries = 10, connObj = False ):
    """ Disable and find TQ
    """
    for _ in range( retries ):
      result = self.__findSmallestTaskQueue( tqDefDict, skipDefinitionCheck = skipDefinitionCheck, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      data = result[ 'Value' ]
      if not data[ 'found' ]:
        return result
      if data[ 'enabled' ] < 1:
        gLogger.notice( "TaskQueue {tqId} seems to be already disabled ({enabled})".format( **data ) )
      result = self.__setTaskQueueEnabled( data[ 'tqId' ], False )
      if result[ 'OK' ]:
        return S_OK( data )
    return S_ERROR( "Could not disable TQ" )

  def __findSmallestTaskQueue( self, tqDefDict, skipDefinitionCheck = False, connObj = False ):
    """
      Find a task queue that has exactly the same requirements
    """
    result = self.__generateTQFindSQL( tqDefDict, skipDefinitionCheck = skipDefinitionCheck,
                                       connObj = connObj )
    if not result[ 'OK' ]:
      return result

    sqlCmd = "SELECT COUNT( `tq_Jobs`.JobID ), `tq_TaskQueues`.TQId, `tq_TaskQueues`.Enabled FROM `tq_TaskQueues`, `tq_Jobs`"
    sqlCmd = "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId AND %s GROUP BY `tq_Jobs`.TQId ORDER BY COUNT( `tq_Jobs`.JobID ) ASC" % ( sqlCmd, result[ 'Value' ] )
    result = self._query( sqlCmd, conn = connObj )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't find task queue: %s" % result[ 'Message' ] )
    data = result[ 'Value' ]
    if len( data ) == 0 or data[0][0] >= self.__maxJobsInTQ:
      return S_OK( { 'found' : False } )
    return S_OK( { 'found' : True, 'tqId' : data[0][1], 'enabled' : data[0][2], 'jobs' : data[0][0] } )


  def matchAndGetJob( self, tqMatchDict, numJobsPerTry = 50, numQueuesPerTry = 10, negativeCond = {} ):
    """
    Match a job
    """
    #Make a copy to avoid modification of original if escaping needs to be done
    tqMatchDict = dict( tqMatchDict )
    self.log.info( "Starting match for requirements", self.__strDict( tqMatchDict ) )
    retVal = self._checkMatchDefinition( tqMatchDict )
    if not retVal[ 'OK' ]:
      self.log.error( "TQ match request check failed", retVal[ 'Message' ] )
      return retVal
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't connect to DB: %s" % retVal[ 'Message' ] )
    connObj = retVal[ 'Value' ]
    preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s AND `tq_Jobs`.Priority = %s"
    prioSQL = "SELECT `tq_Jobs`.Priority FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s ORDER BY RAND() / `tq_Jobs`.RealPriority ASC LIMIT 1"
    postJobSQL = " ORDER BY `tq_Jobs`.JobId ASC LIMIT %s" % numJobsPerTry
    for _ in range( self.__maxMatchRetry ):
      if 'JobID' in tqMatchDict:
        # A certain JobID is required by the resource, so all TQ are to be considered
        retVal = self.matchAndGetTaskQueue( tqMatchDict, numQueuesToGet = 0, skipMatchDictDef = True, connObj = connObj )
        preJobSQL = "%s AND `tq_Jobs`.JobId = %s " % ( preJobSQL, tqMatchDict['JobID'] )
      else:
        retVal = self.matchAndGetTaskQueue( tqMatchDict,
                                            numQueuesToGet = numQueuesPerTry,
                                            skipMatchDictDef = True,
                                            negativeCond = negativeCond,
                                            connObj = connObj )
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
        if len( retVal[ 'Value' ] ) == 0:
          continue
        prio = retVal[ 'Value' ][0][0]
        retVal = self._query( "%s %s" % ( preJobSQL % ( tqId, prio ), postJobSQL ), conn = connObj )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't begin transaction for matching job: %s" % retVal[ 'Message' ] )
        jobTQList = [ ( row[0], row[1] ) for row in retVal[ 'Value' ] ]
        if len( jobTQList ) == 0:
          gLogger.info( "Task queue %s seems to be empty, triggering a cleaning" % tqId )
          self.__deleteTQWithDelay.add( tqId, 300, ( tqId, tqOwnerDN, tqOwnerGroup ) )
        while len( jobTQList ) > 0:
          jobId, tqId = jobTQList.pop( random.randint( 0, len( jobTQList ) - 1 ) )
          self.log.info( "Trying to extract job %s from TQ %s" % ( jobId, tqId ) )
          retVal = self.deleteJob( jobId, connObj = connObj )
          if not retVal[ 'OK' ]:
            msgFix = "Could not take job"
            msgVar = " %s out from the TQ %s: %s" % ( jobId, tqId, retVal[ 'Message' ] )
            self.log.error( msgFix, msgVar )
            return S_ERROR( msgFix + msgVar )
          if retVal[ 'Value' ] == True :
            self.log.info( "Extracted job %s with prio %s from TQ %s" % ( jobId, prio, tqId ) )
            return S_OK( { 'matchFound' : True, 'jobId' : jobId, 'taskQueueId' : tqId, 'tqMatch' : tqMatchDict } )
        self.log.info( "No jobs could be extracted from TQ %s" % tqId )
    self.log.info( "Could not find a match after %s match retries" % self.__maxMatchRetry )
    return S_ERROR( "Could not find a match after %s match retries" % self.__maxMatchRetry )

  def matchAndGetTaskQueue( self, tqMatchDict, numQueuesToGet = 1, skipMatchDictDef = False,
                                  negativeCond = {}, connObj = False ):
    """
    Get a queue that matches the requirements
    """
    #Make a copy to avoid modification of original if escaping needs to be done
    tqMatchDict = dict( tqMatchDict )
    if not skipMatchDictDef:
      retVal = self._checkMatchDefinition( tqMatchDict )
      if not retVal[ 'OK' ]:
        return retVal
    retVal = self.__generateTQMatchSQL( tqMatchDict, numQueuesToGet = numQueuesToGet, negativeCond = negativeCond )
    if not retVal[ 'OK' ]:
      return retVal
    matchSQL = retVal[ 'Value' ]
    retVal = self._query( matchSQL, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( [ ( row[0], row[1], row[2] ) for row in retVal[ 'Value' ] ] )

  def __generateSQLSubCond( self, sqlString, value, boolOp = 'OR' ):
    if type( value ) not in ( types.ListType, types.TupleType ):
      return sqlString % str( value ).strip()
    sqlORList = []
    for v in value:
      sqlORList.append( sqlString % str( v ).strip() )
    return "( %s )" % ( " %s " % boolOp ).join( sqlORList )

  def __generateNotSQL( self, tableDict, negativeCond ):
    """ Generate negative conditions
        Can be a list of dicts or a dict:
         - list of dicts will be  OR of conditional dicts
         - dicts will be normal conditional dict ( kay1 in ( v1, v2, ... ) AND key2 in ( v3, v4, ... ) )
    """
    condType = type( negativeCond )
    if condType in ( types.ListType, types.TupleType ):
      sqlCond = []
      for cD in negativeCond:
        sqlCond.append( self.__generateNotDictSQL( tableDict, cD ) )
      return " ( %s )" % " OR  ".join( sqlCond )
    elif condType == types.DictType:
      return self.__generateNotDictSQL( tableDict, negativeCond )
    raise RuntimeError( "negativeCond has to be either a list or a dict and it's %s" % condType )

  def __generateNotDictSQL( self, tableDict, negativeCond ):
    """ Generate the negative sql condition from a standard condition dict
        not ( cond1 and cond2 ) = ( not cond1 or not cond 2 )
        For instance: { 'Site': 'S1', 'JobType': [ 'T1', 'T2' ] }
          ( not 'S1' in Sites or ( not 'T1' in JobType and not 'T2' in JobType ) )
          S2 T1 -> not False or ( not True and not False ) -> True or ... -> True -> Eligible
          S1 T3 -> not True or ( not False and not False ) -> False or (True and True ) -> True -> Eligible
          S1 T1 -> not True or ( not True and not False ) -> False or ( False and True ) -> False -> Nop
    """
    condList = []
    for field in negativeCond:
      if field in self.__multiValueMatchFields:
        fullTableN = '`tq_TQTo%ss`' % field
        valList = negativeCond[ field ]
        if type( valList ) not in ( types.TupleType, types.ListType ):
          valList = ( valList, )
        subList = []
        for value in valList:
          value = self._escapeString( value )[ 'Value' ]
          sql = "%s NOT IN ( SELECT %s.Value FROM %s WHERE %s.TQId = tq.TQId )" % ( value,
                                                                    fullTableN, fullTableN, fullTableN )
          subList.append( sql )
        condList.append( "( %s )" % " AND ".join( subList ) )
      elif field in self.__singleValueDefFields:
        for value in negativeCond[field]:
          value = self._escapeString( value )[ 'Value' ]
          sql = "%s != tq.%s " % ( value, field )
          condList.append( sql )
    return "( %s )" % " OR ".join( condList )


  def __generateTablesName( self, sqlTables, field ):
    fullTableName = 'tq_TQTo%ss' % field
    if fullTableName not in sqlTables:
      tableN = field.lower()
      sqlTables[ fullTableName ] = tableN
      return tableN, "`%s`" % fullTableName,
    return  sqlTables[ fullTableName ], "`%s`" % fullTableName

  def __generateTQMatchSQL( self, tqMatchDict, numQueuesToGet = 1, negativeCond = {} ):
    """
    Generate the SQL needed to match a task queue
    """
    #Only enabled TQs
    sqlCondList = []
    sqlTables = { "tq_TaskQueues" : "tq" }
    #If OwnerDN and OwnerGroup are defined only use those combinations that make sense
    if 'OwnerDN' in tqMatchDict and 'OwnerGroup' in tqMatchDict:
      groups = tqMatchDict[ 'OwnerGroup' ]
      if type( groups ) not in ( types.ListType, types.TupleType ):
        groups = [ groups ]
      dns = tqMatchDict[ 'OwnerDN' ]
      if type( dns ) not in ( types.ListType, types.TupleType ):
        dns = [ dns ]
      ownerConds = []
      for group in groups:
        if Properties.JOB_SHARING in CS.getPropertiesForGroup( group.replace( '"', "" ) ):
          ownerConds.append( "tq.OwnerGroup = %s" % group )
        else:
          for dn in dns:
            ownerConds.append( "( tq.OwnerDN = %s AND tq.OwnerGroup = %s )" % ( dn, group ) )
      sqlCondList.append( " OR ".join( ownerConds ) )
    else:
      #If not both are defined, just add the ones that are defined
      for field in ( 'OwnerGroup', 'OwnerDN' ):
        if field in tqMatchDict:
          sqlCondList.append( self.__generateSQLSubCond( "tq.%s = %%s" % field,
                                                         tqMatchDict[ field ] ) )
    #Type of single value conditions
    for field in ( 'CPUTime', 'Setup' ):
      if field in tqMatchDict:
        if field in ( 'CPUTime' ):
          sqlCondList.append( self.__generateSQLSubCond( "tq.%s <= %%s" % field, tqMatchDict[ field ] ) )
        else:
          sqlCondList.append( self.__generateSQLSubCond( "tq.%s = %%s" % field, tqMatchDict[ field ] ) )
    #Match multi value fields
    for field in self.__multiValueMatchFields:
      #It has to be %ss , with an 's' at the end because the columns names
      # are plural and match options are singular
      if field in tqMatchDict and tqMatchDict[ field ]:
        _, fullTableN = self.__generateTablesName( sqlTables, field )
        sqlMultiCondList = []
        # if field != 'GridCE' or 'Site' in tqMatchDict:
          # Jobs for masked sites can be matched if they specified a GridCE
          # Site is removed from tqMatchDict if the Site is mask. In this case we want
          # that the GridCE matches explicitly so the COUNT can not be 0. In this case we skip this
          # condition
        sqlMultiCondList.append( "( SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = tq.TQId ) = 0" % ( fullTableN, fullTableN, fullTableN ) )
        if field in self.__tagMatchFields:
          if tqMatchDict[field] != '"Any"':
            csql = self.__generateTagSQLSubCond( fullTableN, tqMatchDict[field] )
        else:
          csql = self.__generateSQLSubCond( "%%s IN ( SELECT %s.Value FROM %s WHERE %s.TQId = tq.TQId )" % ( fullTableN, fullTableN, fullTableN ), tqMatchDict[ field ] )
        sqlMultiCondList.append( csql )
        sqlCondList.append( "( %s )" % " OR ".join( sqlMultiCondList ) )
        #In case of Site, check it's not in job banned sites
        if field in self.__bannedJobMatchFields:
          fullTableN = '`tq_TQToBanned%ss`' % field
          csql = self.__generateSQLSubCond( "%%s not in ( SELECT %s.Value FROM %s WHERE %s.TQId = tq.TQId )" % ( fullTableN,
                                                                    fullTableN, fullTableN ), tqMatchDict[ field ], boolOp = 'OR' )
          sqlCondList.append( csql )
      #Resource banning
      bannedField = "Banned%s" % field
      if bannedField in tqMatchDict and tqMatchDict[ bannedField ]:
        fullTableN = '`tq_TQTo%ss`' % field
        csql = self.__generateSQLSubCond( "%%s not in ( SELECT %s.Value FROM %s WHERE %s.TQId = tq.TQId )" % ( fullTableN,
                                                                  fullTableN, fullTableN ), tqMatchDict[ bannedField ], boolOp = 'OR' )
        sqlCondList.append( csql )

    #For certain fields, the require is strict. If it is not in the tqMatchDict, the job cannot require it
    for field in self.__strictRequireMatchFields:
      if field in tqMatchDict:
        continue
      fullTableN = '`tq_TQTo%ss`' % field
      sqlCondList.append( "( SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = tq.TQId ) = 0" % ( fullTableN, fullTableN, fullTableN ) )

    # Add extra conditions
    if negativeCond:
      sqlCondList.append( self.__generateNotSQL( sqlTables, negativeCond ) )
    #Generate the final query string
    tqSqlCmd = "SELECT tq.TQId, tq.OwnerDN, tq.OwnerGroup FROM `tq_TaskQueues` tq WHERE %s" % ( " AND ".join( sqlCondList ) )
    #Apply priorities
    tqSqlCmd = "%s ORDER BY RAND() / tq.Priority ASC" % tqSqlCmd
    #Do we want a limit?
    if numQueuesToGet:
      tqSqlCmd = "%s LIMIT %s" % ( tqSqlCmd, numQueuesToGet )
    return S_OK( tqSqlCmd )

  def __generateTagSQLSubCond( self, tableName, tagMatchList ):
    """ Generate SQL condition where ALL the specified multiValue requirements must be
        present in the matching resource list
    """
    sql1 = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId=tq.TQId" % ( tableName, tableName, tableName )
    if type( tagMatchList ) in [types.ListType, types.TupleType]:
      sql2 = sql1 + " AND %s.Value in ( %s )" % ( tableName, ','.join( [ "%s" % v for v in tagMatchList] ) )
    else:
      sql2 = sql1 + " AND %s.Value=%s" % ( tableName, tagMatchList )
    sql = '( '+sql1+' ) = ('+sql2+' )'
    return sql

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
    retVal = self._query( "SELECT t.TQId, t.OwnerDN, t.OwnerGroup FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE j.JobId = %s AND t.TQId = j.TQId" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not get job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    data = retVal[ 'Value' ]
    if not data:
      return S_OK( False )
    tqId, tqOwnerDN, tqOwnerGroup = data[0]
    self.log.info( "Deleting job %s" % jobId )
    retVal = self._update( "DELETE FROM `tq_Jobs` WHERE JobId = %s" % jobId, conn = connObj )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Could not delete job from task queue %s: %s" % ( jobId, retVal[ 'Message' ] ) )
    if retVal['Value'] == 0:
      #No job deleted
      return S_OK( False )
    #Always return S_OK() because job has already been taken out from the TQ
    self.__deleteTQWithDelay.add( tqId, 300, ( tqId, tqOwnerDN, tqOwnerGroup ) )
    return S_OK( True )

  def getTaskQueueForJob( self, jobId, connObj = False ):
    """
    Return TaskQueue for a given Job
    Return S_OK( [TaskQueueID] ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't get TQ for job: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]

    retVal = self._query( 'SELECT TQId FROM `tq_Jobs` WHERE JobId = %s ' % jobId, conn = connObj )

    if not retVal[ 'OK' ]:
      return retVal

    if not retVal['Value']:
      return S_ERROR( 'Not in TaskQueues' )

    return S_OK( retVal['Value'][0][0] )

  def getTaskQueueForJobs( self, jobIDs, connObj = False ):
    """
    Return TaskQueues for a given list of Jobs
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't get TQs for a job list: %s" % retVal[ 'Message' ] )
      connObj = retVal[ 'Value' ]

    jobString = ','.join( [ str( x ) for x in jobIDs ] )
    retVal = self._query( 'SELECT JobId,TQId FROM `tq_Jobs` WHERE JobId in (%s) ' % jobString, conn = connObj )

    if not retVal[ 'OK' ]:
      return retVal

    if not retVal['Value']:
      return S_ERROR( 'Not in TaskQueues' )

    resultDict = {}
    for jobID, TQID in retVal['Value']:
      resultDict[int( jobID )] = int( TQID )

    return S_OK( resultDict )

  def __getOwnerForTaskQueue( self, tqId, connObj = False ):
    retVal = self._query( "SELECT OwnerDN, OwnerGroup from `tq_TaskQueues` WHERE TQId=%s" % tqId, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_OK( False )
    return S_OK( retVal[ 'Value' ][0] )

  def __deleteTQIfEmpty( self, args ):
    ( tqId, tqOwnerDN, tqOwnerGroup ) = args
    retries = 3
    while retries:
      retries -= 1
      result = self.deleteTaskQueueIfEmpty( tqId, tqOwnerDN, tqOwnerGroup )
      if result[ 'OK' ]:
        return
    gLogger.error( "Could not delete TQ %s: %s" % ( tqId, result[ 'Message' ] ) )


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
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE Enabled >= 1 AND `tq_TaskQueues`.TQId = %s" % tqId
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
    for _ in self.__multiValueDefFields:
      retVal = self._update( "DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % tqId, conn = connObj )
      if not retVal[ 'OK' ]:
        return retVal
    if delTQ > 0:
      self.recalculateTQSharesForEntity( tqOwnerDN, tqOwnerGroup, connObj = connObj )
      return S_OK( True )
    return S_OK( False )

  def getMatchingTaskQueues( self, tqMatchDict, negativeCond = False ):
    """
     rename to have the same method as exposed in the Matcher
    """
    return self.retrieveTaskQueuesThatMatch( tqMatchDict, negativeCond = negativeCond )

  def getNumTaskQueues( self ):
    """
     Get the number of task queues in the system
    """
    sqlCmd = "SELECT COUNT( TQId ) FROM `tq_TaskQueues`"
    retVal = self._query( sqlCmd )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( retVal[ 'Value' ][0][0] )

  def retrieveTaskQueuesThatMatch( self, tqMatchDict, negativeCond = False ):
    """
    Get the info of the task queues that match a resource
    """
    result = self.matchAndGetTaskQueue( tqMatchDict, numQueuesToGet = 0, negativeCond = negativeCond )
    if not result[ 'OK' ]:
      return result
    return self.retrieveTaskQueues( [ tqTuple[0] for tqTuple in result[ 'Value' ] ] )

  def retrieveTaskQueues( self, tqIdList = False ):
    """
    Get all the task queues
    """
    sqlSelectEntries = [ "`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority", "COUNT( `tq_Jobs`.TQId )" ]
    sqlGroupEntries = [ "`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority" ]
    for field in self.__singleValueDefFields:
      sqlSelectEntries.append( "`tq_TaskQueues`.%s" % field )
      sqlGroupEntries.append( "`tq_TaskQueues`.%s" % field )
    sqlCmd = "SELECT %s FROM `tq_TaskQueues`, `tq_Jobs`" % ", ".join( sqlSelectEntries )
    sqlTQCond = ""
    if tqIdList != False:
      if len( tqIdList ) == 0:
        return S_OK( {} )
      else:
        sqlTQCond += " AND `tq_TaskQueues`.TQId in ( %s )" % ", ".join( [ str( id_ ) for id_ in tqIdList ] )
    sqlCmd = "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId %s GROUP BY %s" % ( sqlCmd,
                                                                                 sqlTQCond,
                                                                                 ", ".join( sqlGroupEntries ) )
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
          if tqIdList == False or tqId in tqIdList:
            self.log.warn( "Task Queue %s is defined in field %s but does not exist, triggering a cleaning" % ( tqId, field ) )
            tqNeedCleaning = True
        else:
          if field not in tqData[ tqId ]:
            tqData[ tqId ][ field ] = []
          tqData[ tqId ][ field ].append( value )
    if tqNeedCleaning:
      self.cleanOrphanedTaskQueues()
    return S_OK( tqData )

  def __updateGlobalShares( self ):
    """
    Update internal structure for shares
    """
    #Update group shares
    self.__groupShares = self.getGroupShares()
    #Apply corrections if enabled
    if self.isSharesCorrectionEnabled():
      result = self.getGroupsInTQs()
      if not result[ 'OK' ]:
        self.log.error( "Could not get groups in the TQs", result[ 'Message' ] )
      activeGroups = result[ 'Value' ]
      newShares = {}
      for group in activeGroups:
        if group in self.__groupShares:
          newShares[ group ] = self.__groupShares[ group ]
      newShares = self.__sharesCorrector.correctShares( newShares )
      for group in self.__groupShares:
        if group in newShares:
          self.__groupShares[ group ] = newShares[ group ]

  def recalculateTQSharesForAll( self ):
    """
    Recalculate all priorities for TQ's
    """
    if self.isSharesCorrectionEnabled():
      self.log.info( "Updating correctors state" )
      self.__sharesCorrector.update()
    self.__updateGlobalShares()
    self.log.info( "Recalculating shares for all TQs" )
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't insert job: %s" % retVal[ 'Message' ] )
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
    self.log.info( "Recalculating shares for %s@%s TQs" % ( userDN, userGroup ) )
    if userGroup in self.__groupShares:
      share = self.__groupShares[ userGroup ]
    else:
      share = float( DEFAULT_GROUP_SHARE )
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
    entitiesShares = dict( [ ( row[0], share ) for row in data ] )
    #If corrector is enabled let it work it's magic
    if self.isSharesCorrectionEnabled():
      entitiesShares = self.__sharesCorrector.correctShares( entitiesShares, group = userGroup )
    #Keep updating
    owners = dict( data )
    #IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
    #(The number of owners didn't change)
    if userDN in owners and owners[ userDN ] > 1:
      return self.__setPrioritiesForEntity( userDN, userGroup, entitiesShares[ userDN ], connObj = connObj )
    #Oops the number of owners may have changed so we recalculate the prio for all owners in the group
    for userDN in owners:
      self.__setPrioritiesForEntity( userDN, userGroup, entitiesShares[ userDN ], connObj = connObj )
    return S_OK()

  def __setPrioritiesForEntity( self, userDN, userGroup, share, connObj = False, consolidationFunc = "AVG" ):
    """
    Set the priority for a userDN/userGroup combo given a splitted share
    """
    self.log.info( "Setting priorities to %s@%s TQs" % ( userDN, userGroup ) )
    tqCond = [ "t.OwnerGroup='%s'" % userGroup ]
    allowBgTQs = gConfig.getValue( "/Registry/Groups/%s/AllowBackgroundTQs" % userGroup, False )
    if Properties.JOB_SHARING not in CS.getPropertiesForGroup( userGroup ):
      tqCond.append( "t.OwnerDN='%s'" % userDN )
    tqCond.append( "t.TQId = j.TQId" )
    if consolidationFunc == 'AVG':
      selectSQL = "SELECT j.TQId, SUM( j.RealPriority )/COUNT(j.RealPriority) FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
    elif consolidationFunc == 'SUM':
      selectSQL = "SELECT j.TQId, SUM( j.RealPriority ) FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
    else:
      return S_ERROR( "Unknown consolidation func %s for setting priorities" % consolidationFunc )
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
      if tqDict[k] > 0.1 or not allowBgTQs:
        totalPrio += tqDict[ k ]
    #Update prio for each TQ
    for tqId in tqDict:
      if tqDict[ tqId ] > 0.1 or not allowBgTQs:
        prio = ( share / totalPrio ) * tqDict[ tqId ]
      else:
        prio = TQ_MIN_SHARE
      prio = max( prio, TQ_MIN_SHARE )
      tqDict[ tqId ] = prio

    #Generate groups of TQs that will have the same prio=sum(prios) maomenos
    result = self.retrieveTaskQueues( list( tqDict ) )
    if not result[ 'OK' ]:
      return result
    allTQsData = result[ 'Value' ]
    tqGroups = {}
    for tqid in allTQsData:
      tqData = allTQsData[ tqid ]
      for field in ( 'Jobs', 'Priority' ) + self.__priorityIgnoredFields:
        if field in tqData:
          tqData.pop( field )
      tqHash = []
      for f in sorted( tqData ):
        tqHash.append( "%s:%s" % ( f, tqData[ f ] ) )
      tqHash = "|".join( tqHash )
      if tqHash not in tqGroups:
        tqGroups[ tqHash ] = []
      tqGroups[ tqHash ].append( tqid )
    tqGroups = [ tqGroups[ td ] for td in tqGroups ]

    #Do the grouping
    for tqGroup in tqGroups:
      totalPrio = 0
      if len( tqGroup ) < 2:
        continue
      for tqid in tqGroup:
        totalPrio += tqDict[ tqid ]
      for tqid in tqGroup:
        tqDict[ tqid ] = totalPrio

    #Group by priorities
    prioDict = {}
    for tqId in tqDict:
      prio = tqDict[ tqId ]
      if prio not in prioDict:
        prioDict[ prio ] = []
      prioDict[ prio ].append( tqId )

    #Execute updates
    for prio in prioDict:
      tqList = ", ".join( [ str( tqId ) for tqId in prioDict[ prio ] ] )
      updateSQL = "UPDATE `tq_TaskQueues` SET Priority=%.4f WHERE TQId in ( %s )" % ( prio, tqList )
      self._update( updateSQL, conn = connObj )
    return S_OK()

  def getGroupShares( self ):
    """
    Get all the shares as a DICT
    """
    result = gConfig.getSections( "/Registry/Groups" )
    if result[ 'OK' ]:
      groups = result[ 'Value' ]
    else:
      groups = []
    shares = {}
    for group in groups:
      shares[ group ] = gConfig.getValue( "/Registry/Groups/%s/JobShare" % group, DEFAULT_GROUP_SHARE )
    return shares

  def propagateTQSharesIfChanged( self ):
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

