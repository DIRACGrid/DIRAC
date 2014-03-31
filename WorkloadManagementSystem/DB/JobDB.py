""" DIRAC JobDB class is a front-end to the main WMS database containing
    job definitions and status information. It is used in most of the WMS
    components

    The following methods are provided for public usage:

    getJobID()
    getJobAttribute()
    getJobAttributes()
    getAllJobAttributes()
    getDistinctJobAttributes()
    getAttributesForJobList()
    getJobParameter()
    getJobParameters()
    getAllJobParameters()
    getInputData()
    getSubjobs()
    getJobJDL()

    selectJobs()
    selectJobsWithStatus()

    setJobAttribute()
    setJobAttributes()
    setJobParameter()
    setJobParameters()
    setJobJDL()
    setJobStatus()
    setInputData()

    insertNewJobIntoDB()
    removeJobFromDB()

    rescheduleJob()
    rescheduleJobs()

    getMask()
    setMask()
    allowSiteInMask()
    banSiteInMask()

    getCounters()
"""

__RCSID__ = "$Id$"

import sys, types
import operator

from DIRAC                                                       import S_OK, S_ERROR, Time
from DIRAC.ConfigurationSystem.Client.Config                     import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers                    import Registry
from DIRAC.Core.Base.DB                                          import DB
from DIRAC.ConfigurationSystem.Client.Helpers.Resources          import getSites
from DIRAC.ResourceStatusSystem.Client.SiteStatus                import SiteStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest  import JobManifest
from DIRAC.Core.Utilities                                        import Time

DEBUG = False
JOB_STATES = ['Received', 'Checking', 'Staging', 'Waiting', 'Matched',
              'Running', 'Stalled', 'Done', 'Completed', 'Failed']
JOB_FINAL_STATES = ['Done', 'Completed', 'Failed']

JOB_DEPRECATED_ATTRIBUTES = [ 'UserPriority', 'SystemPriority' ]

JOB_STATIC_ATTRIBUTES = [ 'JobID', 'JobType', 'DIRACSetup', 'JobGroup', 'HerdState', 'MasterJobID',
                          'JobName', 'Owner', 'OwnerDN', 'OwnerGroup', 'SubmissionTime', 'VerifiedFlag' ]

JOB_VARIABLE_ATTRIBUTES = [ 'Site', 'RescheduleTime', 'StartExecTime', 'EndExecTime', 'RescheduleCounter',
                           'DeletedFlag', 'KilledFlag', 'FailedFlag',
                           'ISandboxReadyFlag', 'OSandboxReadyFlag', 'RetrievedFlag', 'AccountedFlag' ]

JOB_DYNAMIC_ATTRIBUTES = [ 'LastUpdateTime', 'HeartBeatTime',
                           'Status', 'MinorStatus', 'ApplicationStatus', 'ApplicationNumStatus', 'CPUTime'
                          ]

#############################################################################
class JobDB( DB ):

  _tablesDict = {}
  # Jobs table
  _tablesDict[ 'Jobs' ] = {
                           'Fields' :
                                     {
                                      'JobID'                : 'INTEGER NOT NULL AUTO_INCREMENT',
                                      'JobType'              : 'VARCHAR(32) NOT NULL DEFAULT "normal"',
                                      'DIRACSetup'           : 'VARCHAR(32) NOT NULL',
                                      'JobGroup'             : 'VARCHAR(32) NOT NULL DEFAULT "NoGroup"',
                                      'JobSplitType'         : 'ENUM ("Single","Master","Subjob","DAGNode") NOT NULL DEFAULT "Single"',
                                      'MasterJobID'          : 'INTEGER NOT NULL DEFAULT 0',
                                      'Site'                 : 'VARCHAR(100) NOT NULL DEFAULT "ANY"',
                                      'JobName'              : 'VARCHAR(128) NOT NULL DEFAULT "Unknown"',
                                      'Owner'                : 'VARCHAR(32) NOT NULL DEFAULT "Unknown"',
                                      'OwnerDN'              : 'VARCHAR(255) NOT NULL DEFAULT "Unknown"',
                                      'OwnerGroup'           : 'VARCHAR(128) NOT NULL DEFAULT "lhcb_user"',
                                      'SubmissionTime'       : 'DATETIME',
                                      'RescheduleTime'       : 'DATETIME',
                                      'LastUpdateTime'       : 'DATETIME',
                                      'StartExecTime'        : 'DATETIME',
                                      'HeartBeatTime'        : 'DATETIME',
                                      'EndExecTime'          : 'DATETIME',
                                      'Status'               : 'VARCHAR(32) NOT NULL DEFAULT "Received"',
                                      'MinorStatus'          : 'VARCHAR(128) NOT NULL DEFAULT "Initial insertion"',
                                      'ApplicationStatus'    : 'VARCHAR(256) NOT NULL DEFAULT "Unknown"',
                                      'ApplicationNumStatus' : 'INTEGER NOT NULL DEFAULT 0',
                                      'CPUTime'              : 'FLOAT NOT NULL DEFAULT 0.0',
                                      'UserPriority'         : 'INTEGER NOT NULL DEFAULT 0',
                                      'SystemPriority'       : 'INTEGER NOT NULL DEFAULT 0',
                                      'RescheduleCounter'    : 'INTEGER NOT NULL DEFAULT 0',
                                      'VerifiedFlag'         : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'DeletedFlag'          : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'KilledFlag'           : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'FailedFlag'           : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'ISandboxReadyFlag'    : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'OSandboxReadyFlag'    : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'RetrievedFlag'        : 'ENUM ("True","False") NOT NULL DEFAULT "False"',
                                      'AccountedFlag'        : 'ENUM ("True","False","Failed") NOT NULL DEFAULT "False"'
                                     },
                           'Indexes' :
                                      {
                                       'JobType'           : [ 'JobType' ],
                                       'DIRACSetup'        : [ 'DIRACSetup' ],
                                       'JobGroup'          : [ 'JobGroup' ],
                                       'JobSplitType'      : [ 'JobSplitType' ],
                                       'Site'              : [ 'Site' ],
                                       'Owner'             : [ 'Owner' ],
                                       'OwnerDN'           : [ 'OwnerDN' ],
                                       'OwnerGroup'        : [ 'OwnerGroup' ],
                                       'Status'            : [ 'Status' ],
                                       'StatusSite'        : [ 'Status', 'Site' ],
                                       'MinorStatus'       : [ 'MinorStatus' ],
                                       'ApplicationStatus' : [ 'ApplicationStatus' ]
                                      },
                           'PrimaryKey' : [ 'JobID' ]
                          }
  # JobJDLs table
  _tablesDict[ 'JobJDLs' ] = {
                              'Fields' :
                                        {
                                         'JobID'           : 'INTEGER NOT NULL AUTO_INCREMENT',
                                         'JDL'             : 'BLOB NOT NULL',
                                         'JobRequirements' : 'BLOB NOT NULL',
                                         'OriginalJDL'     : 'BLOB NOT NULL',

                                        },
                              'PrimaryKey' : [ 'JobID' ]
                             }
  # SubJobs table
  _tablesDict[ 'SubJobs' ] = {
                              'Fields' :
                                        {
                                         'JobID'    : 'INTEGER NOT NULL',
                                         'SubJobID' : 'INTEGER NOT NULL',
                                        }
                             }
  # PrecursorJobs table
  _tablesDict[ 'PrecursorJobs' ] = {
                                    'Fields' :
                                              {
                                               'JobID'    : 'INTEGER NOT NULL',
                                               'PreJobID' : 'INTEGER NOT NULL',
                                              }
                                   }
  # InputData table
  _tablesDict[ 'InputData' ] = {
                                'Fields' :
                                          {
                                           'JobID'  : 'INTEGER NOT NULL',
                                           'Status' : 'VARCHAR(32) NOT NULL DEFAULT "AprioriGood"',
                                           'LFN'    : 'VARCHAR(255)'
                                          },
                                'PrimaryKey' : [ 'JobID', 'LFN' ]
                               }
  # JobParameters table
  _tablesDict[ 'JobParameters' ] = {
                                    'Fields' :
                                              {
                                               'JobID' : 'INTEGER NOT NULL',
                                               'Name'  : 'VARCHAR(100) NOT NULL',
                                               'Value' : 'BLOB NOT NULL'
                                              },
                                    'PrimaryKey' : [ 'JobID', 'Name' ]
                                   }
  # OptimizerParameters table
  _tablesDict[ 'OptimizerParameters' ] = {
                                          'Fields' :
                                                    {
                                                     'JobID' : 'INTEGER NOT NULL',
                                                     'Name'  : 'VARCHAR(100) NOT NULL',
                                                     'Value' : 'MEDIUMBLOB NOT NULL'
                                                    },
                                          'PrimaryKey' : [ 'JobID', 'Name' ]
                                         }
  # AtticJobParameters table
  _tablesDict[ 'AtticJobParameters' ] = {
                                         'Fields' :
                                                   {
                                                    'JobID'           : 'INTEGER NOT NULL',
                                                    'RescheduleCycle' : 'INTEGER NOT NULL',
                                                    'Name'            : 'VARCHAR(100) NOT NULL',
                                                    'Value'           : 'BLOB NOT NULL'
                                                   },
                                         'PrimaryKey' : [ 'JobID', 'Name', 'RescheduleCycle' ]
                                        }
  # TaskQueues table
  _tablesDict[ 'TaskQueues' ] = {
                                 'Fields' :
                                           {
                                            'TaskQueueID'  : 'INTEGER NOT NULL AUTO_INCREMENT',
                                            'Priority'     : 'INTEGER NOT NULL DEFAULT 0',
                                            'Requirements' : 'BLOB NOT NULL',
                                            'NumberOfJobs' : 'INTEGER NOT NULL DEFAULT 0'
                                           },
                                 'PrimaryKey' : [ 'TaskQueueID' ]
                                }
  # TaskQueue table
  _tablesDict[ 'TaskQueue' ] = {
                                'Fields' :
                                          {
                                           'TaskQueueID' : 'INTEGER NOT NULL',
                                           'JobID'       : 'INTEGER NOT NULL',
                                           'Rank'        : 'INTEGER NOT NULL DEFAULT 0'
                                          },
                                'PrimaryKey' : [ 'JobID', 'TaskQueueID' ]
                               }
  # SiteMask table
  _tablesDict[ 'SiteMask' ] = {
                               'Fields' :
                                         {
                                          'Site'           : 'VARCHAR(64) NOT NULL',
                                          'Status'         : 'VARCHAR(64) NOT NULL',
                                          'LastUpdateTime' : 'DATETIME NOT NULL',
                                          'Author'         : 'VARCHAR(255) NOT NULL',
                                          'Comment'        : 'BLOB NOT NULL'
                                         },
                               'PrimaryKey' : [ 'Site' ]
                              }
  # SiteMaskLogging table
  _tablesDict[ 'SiteMaskLogging' ] = {
                                      'Fields' :
                                                {
                                                 'Site'       : 'VARCHAR(64) NOT NULL',
                                                 'Status'     : 'VARCHAR(64) NOT NULL',
                                                 'UpdateTime' : 'DATETIME NOT NULL',
                                                 'Author'     : 'VARCHAR(255) NOT NULL',
                                                 'Comment'    : 'BLOB NOT NULL'
                                                }
                                     }
  # HeartBeatLoggingInfo table
  _tablesDict[ 'HeartBeatLoggingInfo' ] = {
                                           'Fields' :
                                                     {
                                                      'JobID'         : 'INTEGER NOT NULL',
                                                      'Name'          : 'VARCHAR(100) NOT NULL',
                                                      'Value'         : 'BLOB NOT NULL',
                                                      'HeartBeatTime' : 'DATETIME NOT NULL'
                                                     },
                                           'Indexes' : { 'JobID' : [ 'JobID' ] }
                                          }
  # JobCommands table
  _tablesDict[ 'JobCommands' ] = {
                                  'Fields' :
                                            {
                                             'JobID'         : 'INTEGER NOT NULL',
                                             'Command'       : 'VARCHAR(100) NOT NULL',
                                             'Arguments'     : 'VARCHAR(100) NOT NULL',
                                             'Status'        : 'VARCHAR(64) NOT NULL DEFAULT "Received"',
                                             'ReceptionTime' : 'DATETIME NOT NULL',
                                             'ExecutionTime' : 'DATETIME',
                                            },
                                  'Indexes' : { 'JobID' : [ 'JobID' ] }
                                 }


  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """

    DB.__init__( self, 'JobDB', 'WorkloadManagement/JobDB', maxQueueSize, debug = DEBUG )

    self.maxRescheduling = gConfig.getValue( self.cs_path + '/MaxRescheduling', 3 )

    self.jobAttributeNames = []
    self.nJobAttributeNames = 0

    result = self.__getAttributeNames()

    if not result['OK']:
      error = 'Can not retrieve job Attributes'
      self.log.fatal( 'JobDB: %s' % error )
      sys.exit( error )
      return

    self.log.info( "MaxReschedule:  %s" % self.maxRescheduling )
    self.log.info( "==================================================" )

    if DEBUG:
      result = self.dumpParameters()

    self.__updateDBSchema()


  def _checkTable( self ):
    """ _checkTable.

    Method called on the MatcherHandler instead of on the JobDB constructor
    to avoid an awful number of unnecessary queries with "show tables".
    """

    return self.__createTables()


  def __createTables( self ):
    """ __createTables

    Writes the schema in the database. If a table is already in the schema, it is
    skipped to avoid problems trying to create a table that already exists.
    """

    # Horrible SQL here !!
    existingTables = self._query( "show tables" )
    if not existingTables[ 'OK' ]:
      return existingTables
    existingTables = [ existingTable[0] for existingTable in existingTables[ 'Value' ] ]

    # Makes a copy of the dictionary _tablesDict
    tables = {}
    tables.update( self._tablesDict )

    for existingTable in existingTables:
      if existingTable in tables:
        del tables[ existingTable ]

    res = self._createTables( tables )
    if not res[ 'OK' ]:
      return res

    # Human readable S_OK message
    if res[ 'Value' ] == 0:
      res[ 'Value' ] = 'No tables created'
    else:
      res[ 'Value' ] = 'Tables created: %s' % ( ','.join( tables.keys() ) )
    return res

  def __updateDBSchema( self ):
    result = self.__checkDBVersion()
    if not result[ 'OK' ]:
      self.log.error( "Can't retrieve schema version: %s" % result[ 'Message' ] )
      return result
    version = result[ 'Value' ]
    while True:
      self.log.info( "Current DB schema version %d" % version )
      version += 1
      try:
        migration = getattr( self, '_JobDB__schemaMigration_%d' % version )
        self.log.info( "Found schema migration" )
      except AttributeError:
        return S_OK( version )
      with self.transaction as commit:
        result = migration()
        if not result[ 'OK' ]:
          self.log.error( "Can't apply migration from schema version %d: %s" % ( version, result[ 'Message' ] ) )
          return result
        result = self._update( "INSERT INTO SchemaVersion VALUES ( %d )" % version )
        if not result[ 'OK' ]:
          self.log.error( "Error saving schema version %d: %s" % ( version, result[ 'Message' ] ) )
          return result
        commit()

    return S_OK( version )

  def __schemaMigration_0( self ):
    self.log.info( "Updating MasterJobIDs..." )
    #Set MasterJobID to the JobID if not set
    result = self._update( "UPDATE `Jobs` SET MasterJobID = JobID WHERE MasterJobID = 0" )
    if not result[ 'OK' ]:
      return result
    #Alter JobSplitType to HerdState
    result = self._query( "DESCRIBE Jobs" )
    if not result[ 'OK' ]:
      return result
    cols = result[ 'Value' ]
    found = False
    for col in cols:
      if col[0] == 'JobSplitType':
        result = self._update( "ALTER TABLE Jobs CHANGE JobSplitType HerdState ENUM ('Single','WillSplit','Splitted') NOT NULL DEFAULT 'Single'" )
        if not result[ 'OK' ]:
          return result
    #Get current tables
    result = self._query( "SHOW TABLES" )
    if not result[ 'OK' ]:
      return result
    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    #Create required tables
    tables = { 'SchemaVersion' : { 'Fields' : { 'Version' : 'INTEGER UNSIGNED' } } }
    tables[ 'LFN' ] = { 'Fields' : { 'LFNID' : 'INTEGER NOT NULL AUTO_INCREMENT',
                                     'JobID' : 'INTEGER UNSIGNED NOT NULL',
                                     'LFN' : 'VARCHAR(512) NOT NULL' ,
                                     'Checksum' : 'VARCHAR(64) DEFAULT ""',
                                     'CreationDate' : 'DATETIME',
                                     'ModificationDate' : 'DATETIME',
                                     'Size' : 'INTEGER UNSIGNED DEFAULT 0' },
                        'PrimaryKey' : 'LFNID',
                        'UniqueIndexes' : { 'joblfn' : [ 'JobID', 'LFN' ] } }
    tables[ 'Replicas' ] = { 'Fields' : { 'LFNID' : 'INTEGER NOT NULL',
                                          'SEName' : 'VARCHAR(64) NOT NULL',
                                          'SURL' : 'VARCHAR(256) NOT NULL',
                                          'Disk' : 'TINYINT(1) NOT NULL' },
                             'PrimaryKey' : [ 'LFNID', 'SEName', 'SURL' ],
                             'Indexes' : { 'LFNID' : [ 'LFNID' ] },
                             'ForeignKeys' : { 'LFNID' : 'LFN' } }
    tables[ 'MasterJDLs' ] = { 'Fields' : { 'JobID' : 'int(11) NOT NULL',
                                            'JDL' : 'BLOB NOT NULL' },
                             'PrimaryKey' : [ 'JobID' ] }

    for t in tablesInDB:
      try:
        tables.pop( t )
      except KeyError:
        pass
    result = self._createTables( tables )
    if not result[ 'OK' ]:
      return result
    #Create foreign keys

    #Drop old tables
    dropTables = []
    for t in [ 'SubJobs', 'PrecursorJobs', 'TaskQueues', 'TaskQueue', 'InputData' ]:
      if t in tablesInDB:
        dropTables.append( t )

    if dropTables:
      self.log.info( "Info dropping tables %s" % ", ".join( dropTables ) )

      result = self._update( "DROP TABLE %s" % ", ".join( dropTables ) )
      if not result[ 'OK' ]:
        return result

    #Migrate to innodb
    result = self._query( "SHOW TABLE STATUS" )
    if not result[ 'OK' ]:
      return result
    tableStatus = dict( [ ( r[0], r[1] ) for r in result[ 'Value' ] ] )
    for tableName in tableStatus:
      if tableStatus[ tableName ] == "MyISAM":
        self.log.info( "Migrating table %s to innodb" % tableName )
        result = self._update( "ALTER TABLE `%s` ENGINE = INNODB" % tableName )
        if not result[ 'OK' ]:
          self.log.error( "Error migrating %s to innodb: %s" % ( tableName, result[ 'Message' ] ) )
    return S_OK()



  def __checkDBVersion( self ):
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result
    tables = [ r[0] for r in result[ 'Value' ] ]
    if 'SchemaVersion' not in tables:
      return S_OK( -1 )
    result = self._query( "SELECT MAX(Version) FROM SchemaVersion" )
    if not result[ 'OK' ]:
      return result
    version = result[ 'Value' ][0][0]
    if version == None:
      version = -1
    return S_OK( version )


  def dumpParameters( self ):
    """  Dump the JobDB connection parameters to the stdout
    """

    print "=================================================="
    print "User:     ", self.dbUser
    print "Host:     ", self.dbHost
    print "Password  ", self.dbPass
    print "DBName    ", self.dbName
    print "MaxQueue  ", self.maxQueueSize
    print "=================================================="

    return S_OK()

  def __getAttributeNames( self ):
    """ get Name of Job Attributes defined in DB
        set self.jobAttributeNames to the list of Names
        return S_OK()
        return S_ERROR upon error
    """

    res = self._query( 'DESCRIBE Jobs' )
    if not res['OK']:
      return res

    self.jobAttributeNames = []
    for row in res['Value']:
      field = row[0]
      self.jobAttributeNames.append( field )

    self.nJobAttributeNames = len( self.jobAttributeNames )

    return S_OK()

#############################################################################
  def getJobID( self ):
    """Get the next unique JobID and prepare the new job insertion
    """

    cmd = 'INSERT INTO Jobs (SubmissionTime) VALUES (UTC_TIMESTAMP())'
    err = 'JobDB.getJobID: Failed to retrieve a new Id.'

    res = self._update( cmd )
    if not res['OK']:
      return S_ERROR( '1 %s\n%s' % ( err, res['Message'] ) )

    if not 'lastRowId' in res['Value']:
      return S_ERROR( '2 %s' % err )

    jobID = int( res['Value']['lastRowId'] )
    self.log.info( 'JobDB: New JobID served "%s"' % jobID )

#############################################################################
  def getAttributesForJobList( self, jobIDList, attrList = None ):
    """ Get attributes for the jobs in the the jobIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[jobID][attribute_name] = attribute_value
    """
    if not jobIDList:
      return S_OK( {} )
    if attrList:
      attrNames = ','.join( [ str( x ) for x in attrList ] )
      attr_tmp_list = attrList
    else:
      attrNames = ','.join( [ str( x ) for x in self.jobAttributeNames ] )
      attr_tmp_list = self.jobAttributeNames
    jobList = ','.join( [str( x ) for x in jobIDList] )

    # FIXME: need to check if the attributes are in the list of job Attributes

    cmd = 'SELECT JobID,%s FROM Jobs WHERE JobID in ( %s )' % ( attrNames, jobList )
    res = self._query( cmd )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        jobID = retValues[0]
        jobDict = {}
        jobDict[ 'JobID' ] = jobID
        attrValues = retValues[1:]
        for i in range( len( attr_tmp_list ) ):
          try:
            jobDict[attr_tmp_list[i]] = attrValues[i].tostring()
          except Exception:
            jobDict[attr_tmp_list[i]] = str( attrValues[i] )
        retDict[int( jobID )] = jobDict
      return S_OK( retDict )
    except Exception, x:
      return S_ERROR( 'JobDB.getAttributesForJobList: Failed\n%s' % str( x ) )


#############################################################################
  def getDistinctJobAttributes( self, attribute, condDict = None, older = None,
                                newer = None, timeStamp = 'LastUpdateTime' ):
    """ Get distinct values of the job attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Jobs', attribute, condDict = condDict,
                                              older = older, newer = newer, timeStamp = timeStamp )


#############################################################################
  def traceJobParameter( self, site, localID, parameter, date = None, until = None ):
    ret = self.traceJobParameters( site, localID, [parameter], None, date, until )
    if not ret['OK']:
      return ret
    returnDict = {}
    for jobID in ret['Value']:
      returnDict[jobID] = ret['Value'][jobID].get( parameter )
    return S_OK( returnDict )

#############################################################################
  def traceJobParameters( self, site, localIDs, paramList = None, attributeList = None, date = None, until = None ):
    import datetime
    exactTime = False
    if not attributeList:
      attributeList = []
    attributeList = list( set( attributeList ) | set( ['StartExecTime', 'SubmissionTime', 'HeartBeatTime',
                                                    'EndExecTime', 'JobName', 'OwnerDN', 'OwnerGroup'] ) )
    try:
      if type( localIDs ) == type( [] ) or type( localIDs ) == type( {} ):
        localIDs = [int( localID ) for localID in localIDs]
      else:
        localIDs = [int( localIDs )]
    except:
      return S_ERROR( "localIDs must be integers" )
    now = datetime.datetime.utcnow()
    if until:
      if until.lower() == 'now':
        until = now
      else:
        try:
          until = datetime.datetime.strptime( until, '%Y-%m-%d' )
        except:
          return S_ERROR( "Error in format for 'until', expected '%Y-%m-%d'" )
    if not date:
      until = now
      since = until - datetime.timedelta( hours = 24 )
    else:
      since = None
      for format in ( '%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S' ):
        try:
          since = datetime.datetime.strptime( date, format )
          break
        except:
          exactTime = True
      if not since:
        return S_ERROR( 'Error in date format' )
      if exactTime:
        exactTime = since
        if not until:
          until = now
      else:
        if not until:
          until = since + datetime.timedelta( hours = 24 )
      if since > now:
        return S_ERROR( 'Cannot find jobs in the future' )
      if until > now:
        until = now
    result = self.selectJobs( {'Site':site}, older = str( until ), newer = str( since ) )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'No jobs found at %s for date %s' % ( site, date ) )
    resultDict = {'Successful':{}, 'Failed':{}}
    for jobID in result['Value']:
      if jobID:
        ret = self.getJobParameter( jobID, 'LocalJobID' )
        if not ret['OK']:
          return ret
        localID = ret['Value']
        if localID and int( localID ) in localIDs:
          attributes = self.getJobAttributes( jobID, attributeList )
          if not attributes['OK']:
            return attributes
          attributes = attributes['Value']
          if exactTime:
            for att in ( 'StartExecTime', 'SubmissionTime' ):
              startTime = attributes.get( att )
              if startTime == 'None':
                startTime = None
              if startTime:
                break
            startTime = datetime.datetime.strptime( startTime , '%Y-%m-%d %H:%M:%S' ) if startTime else now
            for att in ( 'EndExecTime', 'HeartBeatTime' ):
              lastTime = attributes.get( att )
              if lastTime == 'None':
                lastTime = None
              if lastTime:
                break
            lastTime = datetime.datetime.strptime( lastTime, '%Y-%m-%d %H:%M:%S' ) if lastTime else now
            okTime = ( exactTime >= startTime and exactTime <= lastTime )
          else:
            okTime = True
          if okTime:
            ret = self.getJobParameters( jobID, paramList = paramList )
            if not ret['OK']:
              return ret
            attributes.update( ret['Value'] )
            resultDict['Successful'].setdefault( int( localID ), {} )[int( jobID )] = attributes
    for localID in localIDs:
      if localID not in resultDict['Successful']:
        resultDict['Failed'][localID] = 'localID not found'
    return S_OK( resultDict )

#############################################################################
  def getJobParameters( self, jobID, paramList = None ):
    """ Get Job Parameters defined for jobID.
        Returns a dictionary with the Job Parameters.
        If parameterList is empty - all the parameters are returned.
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    e_jobID = ret['Value']

    self.log.debug( 'JobDB.getParameters: Getting Parameters for job %s' % jobID )

    resultDict = {}
    if paramList:
      paramNameList = []
      for x in paramList:
        ret = self._escapeString( x )
        if not ret['OK']:
          return ret
        paramNameList.append( ret['Value'] )
      paramNames = ','.join( paramNameList )
      cmd = "SELECT Name, Value from JobParameters WHERE JobID=%s and Name in (%s)" % ( e_jobID, paramNames )
      result = self._query( cmd )
      if result['OK']:
        if result['Value']:
          for name, value in result['Value']:
            try:
              resultDict[name] = value.tostring()
            except Exception:
              resultDict[name] = value

        return S_OK( resultDict )
      else:
        return S_ERROR( 'JobDB.getJobParameters: failed to retrieve parameters' )

    else:
      result = self.getFields( 'JobParameters', ['Name', 'Value'], {'JobID': jobID} )
      if not result['OK']:
        return result
      else:
        for name, value in result['Value']:
          try:
            resultDict[name] = value.tostring()
          except Exception:
            resultDict[name] = value

        return S_OK( resultDict )

#############################################################################
  def getAtticJobParameters( self, jobID, paramList = None, rescheduleCounter = -1 ):
    """ Get Attic Job Parameters defined for a job with jobID.
        Returns a dictionary with the Attic Job Parameters per each rescheduling cycle.
        If parameterList is empty - all the parameters are returned.
        If recheduleCounter = -1, all cycles are returned.
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    self.log.debug( 'JobDB.getAtticJobParameters: Getting Attic Parameters for job %s' % jobID )

    resultDict = {}
    paramCondition = ''
    if paramList:
      paramNameList = []
      for x in paramList:
        ret = self._escapeString( x )
        if not ret['OK']:
          return ret
        paramNameList.append( x )
      paramNames = ','.join( paramNameList )
      paramCondition = " AND Name in (%s)" % paramNames
    rCounter = ''
    if rescheduleCounter != -1:
      rCounter = ' AND RescheduleCycle=%d' % int( rescheduleCounter )
    cmd = "SELECT Name, Value, RescheduleCycle from AtticJobParameters"
    cmd += " WHERE JobID=%s %s %s" % ( jobID, paramCondition, rCounter )
    result = self._query( cmd )
    if result['OK']:
      if result['Value']:
        for name, value, counter in result['Value']:
          if not resultDict.has_key( counter ):
            resultDict[counter] = {}
          try:
            resultDict[counter][name] = value.tostring()
          except Exception:
            resultDict[counter][name] = value

      return S_OK( resultDict )
    else:
      return S_ERROR( 'JobDB.getAtticJobParameters: failed to retrieve parameters' )

#############################################################################
  def getJobAttributes( self, jobID, attrList = None ):
    """ Get all Job Attributes for a given jobID.
        Return a dictionary with all Job Attributes,
        return an empty dictionary if matching job found
    """
    if not attrList:
      attrList = self.jobAttributeNames
    result = self.getFields( "Jobs", outFields = attrList, condDict = { 'JobID' : jobID } )
    if not result[ 'OK' ]:
      return result
    dbData = result[ 'Value' ]
    if not dbData:
      return S_OK( {} )
    jobData = dbData[0]
    ret = {}
    for iP in range( len( attrList ) ):
      ret[ attrList[iP] ] = jobData[ iP ]
    return S_OK( ret )

#############################################################################
  def getJobInfo( self, jobID, parameters = None ):
    """ Get parameters for job specified by jobID. Parameters can be
        either job attributes ( fields in the Jobs table ) or those
        stored in the JobParameters table.
        The return value is a dictionary of the structure:
        Dict[Name] = Value
    """

    resultDict = {}
    # Parameters are not specified, get them all - parameters + attributes
    if not parameters:
      result = self.getJobAttributes( jobID )
      if result['OK']:
        resultDict = result['value']
      else:
        return S_ERROR( 'JobDB.getJobAttributes: can not retrieve job attributes' )
      result = self.getJobParameters( jobID )
      if result['OK']:
        resultDict.update( result['value'] )
      else:
        return S_ERROR( 'JobDB.getJobParameters: can not retrieve job parameters' )
      return S_OK( resultDict )

    paramList = []
    attrList = []
    for par in parameters:
      if par in self.jobAttributeNames:
        attrList.append( par )
      else:
        paramList.append( par )

    # Get Job Attributes first
    if attrList:
      result = self.getJobAttributes( jobID, attrList )
      if not result['OK']:
        return result
      if len( result['Value'] ) > 0:
        resultDict = result['Value']
      else:
        return S_ERROR( 'Job ' + str( jobID ) + ' not found' )

    # Get Job Parameters
    if paramList:
      result = self.getJobParameters( jobID, paramList )
      if not result['OK']:
        return result
      if len( result['Value'] ) > 0:
        resultDict.update( result['Value'] )

    return S_OK( resultDict )

#############################################################################
  def getJobAttribute( self, jobID, attribute ):
    """ Get the given attribute of a job specified by its jobID
    """

    result = self.getJobAttributes( jobID, [attribute] )
    if result['OK']:
      value = result['Value'][attribute]
      return S_OK( value )
    else:
      return result

#############################################################################
  def getJobParameter( self, jobID, parameter ):
    """ Get the given parameter of a job specified by its jobID
    """

    result = self.getJobParameters( jobID, [parameter] )
    if not result['OK']:
      return result
    return S_OK( result.get( 'Value', {} ).get( parameter ) )

#############################################################################
  def getJobOptParameter( self, jobID, parameter ):
    """ Get optimizer parameters for the given job.
    """

    result = self.getFields( 'OptimizerParameters', ['Value'], {'JobID': jobID, 'Name': parameter} )
    if result['OK']:
      if result['Value']:
        return S_OK( result['Value'][0][0] )
      else:
        return S_ERROR( 'Parameter not found' )
    else:
      return S_ERROR( 'Failed to access database' )

#############################################################################
  def getJobOptParameters( self, jobID, paramList = None ):
    """ Get optimizer parameters for the given job. If the list of parameter names is
        empty, get all the parameters then
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    resultDict = {}

    if paramList:
      paramNameList = []
      for x in paramList:
        ret = self._escapeString( x )
        if not ret['OK']:
          return ret
        paramNameList.append( ret['Value'] )
      paramNames = ','.join( paramNameList )
      cmd = "SELECT Name, Value from OptimizerParameters WHERE JobID=%s and Name in (%s)" % ( jobID, paramNames )
    else:
      cmd = "SELECT Name, Value from OptimizerParameters WHERE JobID=%s" % jobID

    result = self._query( cmd )
    if result['OK']:
      if result['Value']:
        for name, value in result['Value']:
          try:
            resultDict[name] = value.tostring()
          except Exception:
            resultDict[name] = value

      return S_OK( resultDict )
    else:
      return S_ERROR( 'JobDB.getJobOptParameters: failed to retrieve parameters' )

#############################################################################
  def getTimings( self, site, period = 3600 ):
    """ Get CPU and wall clock times for the jobs finished in the last hour
    """
    ret = self._escapeString( site )
    if not ret['OK']:
      return ret
    site = ret['Value']

    date = str( Time.dateTime() - Time.second * period )
    req = "SELECT JobID from Jobs WHERE Site=%s and EndExecTime > '%s' " % ( site, date )
    result = self._query( req )
    jobList = [ str( x[0] ) for x in result['Value'] ]
    jobString = ','.join( jobList )

    req = "SELECT SUM(Value) from JobParameters WHERE Name='TotalCPUTime(s)' and JobID in (%s)" % jobString
    result = self._query( req )
    if not result['OK']:
      return result
    cpu = result['Value'][0][0]
    if not cpu:
      cpu = 0.0

    req = "SELECT SUM(Value) from JobParameters WHERE Name='WallClockTime(s)' and JobID in (%s)" % jobString
    result = self._query( req )
    if not result['OK']:
      return result
    wctime = result['Value'][0][0]
    if not wctime:
      wctime = 0.0

    return S_OK( {"CPUTime":int( cpu ), "WallClockTime":int( wctime )} )

#############################################################################
  def __checkInputDataStructure( self, pDict ):
    if type( pDict ) != types.DictType:
      return S_ERROR( "Input data has to be a dictionary" )
    for lfn in pDict:
      if 'Metadata' not in pDict[ lfn ]:
        return S_ERROR( "Missing metadata for lfn %s" % lfn )
      if 'Replicas' not in pDict[ lfn ]:
        return S_ERROR( "Missing replicas for lfn %s" % lfn )
        replicas = pDict[ lfn ][ 'Replicas' ]
        for seName in replicas:
          if 'SURL' not in replicas or 'Disk' not in replicas:
            return S_ERROR( "Missing SURL or Disk for %s:%s replica" % ( seName, lfn ) )
    return S_OK()

#############################################################################
  def getInputData ( self, jid ):
    """Get input data for the given job
    """
    try:
      jid = int( jid )
    except ValueError:
      return S_ERROR( "Job id needs to be an integer" )
    result = self._query( "SELECT l.LFN, l.Checksum, l.CreationDate, l.ModificationDate, l.Size, r.SURL, r.SEName, r.Disk FROM LFN l, Replicas r WHERE l.JobID = %d AND l.LFNID = r.LFNID" % jid )
    if not result[ 'OK' ]:
      return result
    data = {}
    for lfn, checksum, creationDate, modifDate, size, surl, SEName, onDisk in result[ 'Value' ]:
      if lfn not in data:
        data[ lfn ] = { 'Metadata' : { 'Checksum' : checksum, 'CreationDate' : creationDate,
                                       'ModificationDate' : modifDate, 'Size' : size },
                        'Replicas' : {} }
      rD = data[ lfn ][ 'Replicas' ]
      rD[ SEName ] = { 'SURL' : surl, 'Disk' : onDisk }
    return S_OK( data )

#############################################################################

  def __cleanInputData( self, jid ):
    cmd = "DELETE FROM Replicas WHERE LFNID IN ( SELECT LFNID FROM LFN WHERE JobID = %d )" % jid
    result = self._update( cmd )
    if not result['OK']:
      return S_ERROR( "Could not clean input data for job %d: %s" % ( jid, result[ 'Message' ] ) )
    cmd = "DELETE FROM LFN WHERE JobID = %d" % jid
    result = self._update( cmd )
    if not result['OK']:
      return S_ERROR( "Could not clean input data for job %d: %s" % ( jid, result[ 'Message' ] ) )
    return S_OK()

#############################################################################
  def setInputData ( self, jid, lfnData ):
    """Inserts input data for the given job
    """
    try:
      jid = int( jid )
    except ValueError:
      return S_ERROR( "Job id needs to be an integer" )
    result = self.__checkInputDataStructure( lfnData )
    if not result['OK']:
      return result
    result = self.__cleanInputData( jid )
    if not result['OK']:
      return S_ERROR( "Could not clean input data for job %d: %s" % ( jid, result[ 'Message' ] ) )
    for lfn in lfnData:
      vD = { 'JobID' : jid, 'LFN' : lfn }
      metaDict = lfnData[ lfn ][ 'Metadata' ]
      for k in ( 'Checksum', 'CreationDate', 'ModificationDate', 'Size' ):
        if k in metaDict:
          vD[ k ] = metaDict[ k ]
      result = self.insertFields( "LFN", inDict = vD )
      if not result[ 'OK' ]:
        return S_ERROR( "Can not insert input data: %s" % result[ 'Message' ] )
      lfnid = result[ 'lastRowId' ]
      vL = []
      replicas = lfnData[ lfn ][ 'Replicas' ]
      for seName in replicas:
        result = self._escapeString( replicas[ seName ][ 'SURL' ] )
        if not result[ 'OK' ]:
          return result
        surl = result[ 'Value' ]
        disk = int( replicas[ seName ][ 'Disk' ] )
        result = self._escapeString( seName )
        if not result[ 'OK' ]:
          return result
        seName = result[ 'Value' ]
        vL.append( "%d, %s, %s, %d" % ( lfnid, seName, surl, disk ) )
      values = "),(".join( vL )
      cmd = "INSERT INTO Replicas ( LFNID, SEName, SURL, Disk ) VALUES ( %s )" % values
      result = self._update( cmd )
      if not result[ 'OK' ]:
        return S_ERROR( "Can not insert input data: %s" % result[ 'Message' ] )

    return S_OK()

#############################################################################
  def setOptimizerChain( self, jobID, optimizerList ):
    """ Set the optimizer chain for the given job. The 'TaskQueue'
        optimizer should be the last one in the chain, it is added
        if not present in the optimizerList
    """

    optString = ','.join( optimizerList )
    result = self.setJobOptParameter( jobID, 'OptimizerChain', optString )
    return result

#############################################################################
  def setNextOptimizer( self, jobID, currentOptimizer ):
    """ Set the job status to be processed by the next optimizer in the
        chain
    """

    result = self.getJobOptParameter( jobID, 'OptimizerChain' )
    if not result['OK']:
      return result

    optListString = result['Value']
    optList = optListString.split( ',' )
    try:
      sindex = None
      for i in xrange( len( optList ) ):
        if optList[i] == currentOptimizer:
          sindex = i
      if sindex >= 0:
        if sindex < len( optList ) - 1:
          nextOptimizer = optList[sindex + 1]
        else:
          return S_ERROR( 'Unexpected end of the Optimizer Chain' )
      else:
        return S_ERROR( 'Could not find ' + currentOptimizer + ' in chain' )
    except ValueError:
      return S_ERROR( 'The ' + currentOptimizer + ' not found in the chain' )

    result = self.setJobStatus( jobID, status = "Checking", minor = nextOptimizer )
    if not result[ 'OK' ]:
      return result
    return S_OK( nextOptimizer )

############################################################################
  def countJobs( self, condDict, older = None, newer = None, timeStamp = 'LastUpdateTime' ):
    """ Get the number of jobs matching conditions specified by condDict and time limits
    """
    self.log.debug ( 'JobDB.countJobs: counting Jobs' )
    return self.countEntries( 'Jobs', condDict, older = older, newer = newer, timeStamp = timeStamp )

#############################################################################
  def selectJobs( self, condDict, older = None, newer = None, timeStamp = 'LastUpdateTime',
                  orderAttribute = None, limit = None ):
    """ Select jobs matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by JobID if requested, the result is limited to a given
        number of jobs if requested.
    """

    self.log.debug( 'JobDB.selectJobs: retrieving jobs.' )

    res = self.getFields( 'Jobs', ['JobID'], condDict = condDict, limit = limit,
                            older = older, newer = newer, timeStamp = timeStamp, orderAttribute = orderAttribute )

    if not res['OK']:
      return res

    if not len( res['Value'] ):
      return S_OK( [] )
    return S_OK( [ self._to_value( i ) for i in  res['Value'] ] )

#############################################################################
  def selectJobWithStatus( self, status ):
    """ Get the list of jobs with a given Major Status
    """

    return self.selectJobs( {'Status':status} )

#############################################################################
  def setJobAttribute( self, jobID, attrName, attrValue, update = False, myDate = None ):
    """ Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitly requested
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( attrValue )
    if not ret['OK']:
      return ret
    value = ret['Value']

    # FIXME: need to check the validity of attrName

    if update:
      cmd = "UPDATE Jobs SET %s=%s,LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%s" % ( attrName, value, jobID )
    else:
      cmd = "UPDATE Jobs SET %s=%s WHERE JobID=%s" % ( attrName, value, jobID )

    if myDate:
      cmd += ' AND LastUpdateTime < %s' % myDate

    res = self._update( cmd )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'JobDB.setAttribute: failed to set attribute' )

#############################################################################
  def setJobAttributes( self, jobID, attrNames, attrValues, update = False, myDate = None ):
    """ Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitely requested
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    if len( attrNames ) != len( attrValues ):
      return S_ERROR( 'JobDB.setAttributes: incompatible Argument length' )

    # FIXME: Need to check the validity of attrNames
    attr = []
    for i in range( len( attrNames ) ):
      ret = self._escapeString( attrValues[i] )
      if not ret['OK']:
        return ret
      value = ret['Value']
      attr.append( "%s=%s" % ( attrNames[i], value ) )
    if update:
      attr.append( "LastUpdateTime=UTC_TIMESTAMP()" )
    if len( attr ) == 0:
      return S_ERROR( 'JobDB.setAttributes: Nothing to do' )

    cmd = 'UPDATE Jobs SET %s WHERE JobID=%s' % ( ', '.join( attr ), jobID )

    if myDate:
      cmd += ' AND LastUpdateTime < %s' % myDate

    res = self._update( cmd )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'JobDB.setAttributes: failed to set attribute' )

#############################################################################
  def setJobStatus( self, jobID, status = '', minor = '', application = '', appCounter = None ):
    """ Set status of the job specified by its jobID
    """

    # Do not update the LastUpdate time stamp if setting the Stalled status
    update_flag = True
    if status == "Stalled":
      update_flag = False

    attrNames = []
    attrValues = []
    if status:
      attrNames.append( 'Status' )
      attrValues.append( status )
    if minor:
      attrNames.append( 'MinorStatus' )
      attrValues.append( minor )
    if application:
      attrNames.append( 'ApplicationStatus' )
      attrValues.append( application )
    if appCounter:
      attrNames.append( 'ApplicationNumStatus' )
      attrValues.append( appCounter )

    result = self.setJobAttributes( jobID, attrNames, attrValues, update = update_flag )
    if not result['OK']:
      return result

    return S_OK()

#############################################################################
  def setEndExecTime( self, jobID, endDate = None ):
    """ Set EndExecTime time stamp
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    if endDate:
      ret = self._escapeString( endDate )
      if not ret['OK']:
        return ret
      endDate = ret['Value']
      req = "UPDATE Jobs SET EndExecTime=%s WHERE JobID=%s AND EndExecTime IS NULL" % ( endDate, jobID )
    else:
      req = "UPDATE Jobs SET EndExecTime=UTC_TIMESTAMP() WHERE JobID=%s AND EndExecTime IS NULL" % jobID
    result = self._update( req )
    return result

#############################################################################
  def setStartExecTime( self, jobID, startDate = None ):
    """ Set StartExecTime time stamp
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    if startDate:
      ret = self._escapeString( startDate )
      if not ret['OK']:
        return ret
      startDate = ret['Value']
      req = "UPDATE Jobs SET StartExecTime=%s WHERE JobID=%s AND StartExecTime IS NULL" % ( startDate, jobID )
    else:
      req = "UPDATE Jobs SET StartExecTime=UTC_TIMESTAMP() WHERE JobID=%s AND StartExecTime IS NULL" % jobID
    result = self._update( req )
    return result

#############################################################################
  def setJobParameter( self, jobID, key, value ):
    """ Set a parameter specified by name,value pair for the job JobID
    """

    ret = self._escapeString( key )
    if not ret['OK']:
      return ret
    e_key = ret['Value']
    ret = self._escapeString( value )
    if not ret['OK']:
      return ret
    e_value = ret['Value']

    cmd = 'REPLACE JobParameters (JobID,Name,Value) VALUES (%d,%s,%s)' % ( int( jobID ), e_key, e_value )
    result = self._update( cmd )
    if not result['OK']:
      result = S_ERROR( 'JobDB.setJobParameter: operation failed.' )

    return result

#############################################################################
  def setJobParameters( self, jobID, parameters ):
    """ Set parameters specified by a list of name/value pairs for the job JobID
    """

    if not parameters:
      return S_OK()

    insertValueList = []
    for name, value in parameters:
      ret = self._escapeString( name )
      if not ret['OK']:
        return ret
      e_name = ret['Value']
      ret = self._escapeString( value )
      if not ret['OK']:
        return ret
      e_value = ret['Value']
      insertValueList.append( '(%s,%s,%s)' % ( jobID, e_name, e_value ) )

    cmd = 'REPLACE JobParameters (JobID,Name,Value) VALUES %s' % ', '.join( insertValueList )
    result = self._update( cmd )
    if not result['OK']:
      return S_ERROR( 'JobDB.setJobParameters: operation failed.' )

    return result

#############################################################################
  def setJobOptParameter( self, jobID, name, value ):
    """ Set an optimzer parameter specified by name,value pair for the job JobID
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    e_jobID = ret['Value']

    ret = self._escapeString( name )
    if not ret['OK']:
      return ret
    e_name = ret['Value']

    cmd = 'DELETE FROM OptimizerParameters WHERE JobID=%s AND Name=%s' % ( e_jobID, e_name )
    if not self._update( cmd )['OK']:
      result = S_ERROR( 'JobDB.setJobOptParameter: operation failed.' )

    result = self.insertFields( 'OptimizerParameters', ['JobID', 'Name', 'Value'], [jobID, name, value] )
    if not result['OK']:
      return S_ERROR( 'JobDB.setJobOptParameter: operation failed.' )

    return S_OK()

#############################################################################
  def removeJobOptParameter( self, jobID, name ):
    """ Remove the specified optimizer parameter for jobID
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']
    ret = self._escapeString( name )
    if not ret['OK']:
      return ret
    name = ret['Value']

    cmd = 'DELETE FROM OptimizerParameters WHERE JobID=%s AND Name=%s' % ( jobID, name )
    if not self._update( cmd )['OK']:
      return S_ERROR( 'JobDB.removeJobOptParameter: operation failed.' )
    else:
      return S_OK()

#############################################################################
  def clearAtticParameters( self, jid ):
    try:
      jid = int( jid )
    except ValueError:
      return S_ERROR( "jid has to be a number" )
    return self._update( "DELETE FROM AtticJobParameters WHERE JobID=%s" % jid )

#############################################################################
  def setAtticParameters( self, jid, rescheduleCounter, paramsDict ):
    if not paramsDict:
      return S_OK()

    try:
      jid = int( jid )
      rescheduleCounter = int( rescheduleCounter )
    except ValueError:
      return S_ERROR( "Invalud jid/reschedulecounter. Not a number" )

    entries = []
    for key in paramsDict:
      ret = self._escapeString( paramsDict[ key ] )
      if not ret['OK']:
        return ret
      value = ret['Value']

      ret = self._escapeString( key )
      if not ret['OK']:
        return ret
      key = ret['Value']

      entries.append( "(%d,%d,%s,%s)" % ( jid, rescheduleCounter, key, value, ) )

    return self._update( "INSERT INTO AtticJobParameters VALUES %s" % ",".join( entries ) )

#############################################################################
  def setAtticJobParameter( self, jobID, key, value, rescheduleCounter ):
    """ Set attic parameter for job specified by its jobID when job rescheduling
        for later debugging
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( key )
    if not ret['OK']:
      return ret
    key = ret['Value']

    ret = self._escapeString( value )
    if not ret['OK']:
      return ret
    value = ret['Value']

    ret = self._escapeString( rescheduleCounter )
    if not ret['OK']:
      return ret
    rescheduleCounter = ret['Value']

    cmd = 'INSERT INTO AtticJobParameters (JobID,RescheduleCycle,Name,Value) VALUES(%s,%s,%s,%s)' % \
         ( jobID, rescheduleCounter, key, value )
    result = self._update( cmd )
    if not result['OK']:
      result = S_ERROR( 'JobDB.setAtticJobParameter: operation failed.' )

    return result

#############################################################################
  def setJobJDL( self, jobID, jdl = None, originalJDL = None ):
    """ Insert JDL's for job specified by jobID
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( jdl )
    if not ret['OK']:
      return ret
    e_JDL = ret['Value']

    ret = self._escapeString( originalJDL )
    if not ret['OK']:
      return ret
    e_originalJDL = ret['Value']

    req = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%s" % jobID
    result = self._query( req )
    updateFlag = False
    if result['OK']:
      if len( result['Value'] ) > 0:
        updateFlag = True

    if jdl:

      if updateFlag:
        cmd = "UPDATE JobJDLs Set JDL=%s WHERE JobID=%s" % ( e_JDL, jobID )
      else:
        cmd = "INSERT INTO JobJDLs (JobID,JDL) VALUES (%s,%s)" % ( jobID, e_JDL )
      result = self._update( cmd )
      if not result['OK']:
        return result
    if originalJDL:
      if updateFlag:
        cmd = "UPDATE JobJDLs Set OriginalJDL=%s WHERE JobID=%s" % ( e_originalJDL, jobID )
      else:
        cmd = "INSERT INTO JobJDLs (JobID,OriginalJDL) VALUES (%s,%s)" % ( jobID, e_originalJDL )

      result = self._update( cmd )

    return result

#############################################################################
  def __insertNewJDL( self, jdl ):
    """Insert a new JDL in the system, this produces a new JobID
    """

    err = 'JobDB.__insertNewJDL: Failed to retrieve a new Id.'

    result = self.insertFields( 'JobJDLs' , ['OriginalJDL'], [jdl] )
    if not result['OK']:
      self.log.error( 'Can not insert New JDL', result['Message'] )
      return result

    jid = result[ 'lastRowId' ]
    return S_OK( jid )


#############################################################################
  def getJobJDL( self, jobID, original = False, status = '' ):
    """ Get JDL for job specified by its jobID. By default the current job JDL
        is returned. If 'original' argument is True, original JDL is returned
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( status )
    if not ret['OK']:
      return ret
    e_status = ret['Value']


    if original:
      cmd = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%s" % jobID
    else:
      cmd = "SELECT JDL FROM JobJDLs WHERE JobID=%s" % jobID

    if status:
      cmd = cmd + " AND Status=%s" % e_status

    result = self._query( cmd )
    if result['OK']:
      jdl = result['Value']
      if not jdl:
        return S_OK( jdl )
      else:
        return S_OK( result['Value'][0][0] )
    else:
      return result


  def getJobsInHerd( self, jid ):
    try:
      jid = int( jid )
    except ValueError:
      return S_ERROR( "jid has to be a number" % jid )
    result = self._query( "SELECT JobID FROM Jobs WHERE MasterJobID=%d" % jid )
    if not result[ 'OK' ]:
      return result
    return S_OK( [ row[0] for row in result[ 'Value' ] ] )

  def insertSplittedManifests( self, jid, manifests ):
    jidList = [ jid ]
    with self.transaction as commit:
      result = self.getJobJDL( jid, original = True )
      if not result[ 'OK' ]:
        return result
      sourceManifest = result[ 'Value' ]
      result = self.insertFields( 'MasterJDLs', inDict = { 'JobID' : jid,
                                                           'JDL' : sourceManifest } )
      if not result[ 'OK' ]:
        return result
      jobManifest = manifests[0]
      manifestJDL = jobManifest.dumpAsJDL()
      result = self.updateFields( 'JobJDLs',
                                  condDict = { 'JobID' : jid },
                                  updateDict = { 'JDL' : manifestJDL,
                                                 'OriginalJDL' : manifestJDL,
                                                 'JobRequirements' : '' } )
      if not result[ 'OK' ]:
        return result

      #Get source job input data
      result = self.getInputData( jid )
      if not result[ 'OK' ]:
        return result
      sourceInputData = result[ 'Value' ]

      #Reset source Job Values
      upDict = { 'Status' : 'Received',
                 'MinorStatus' : 'Job accepted',
                 'ApplicationStatus' : 'Unknown',
                 'HerdState' : 'Splitted' }
      for name in ( 'JobName', 'JobType', 'JobGroup', 'Priority' ):
        value = jobManifest.getOption( name )
        if name == 'Priority':
          name = 'UserPriority'
        if value:
          upDict[ name ] = value
      result = self.updateFields( 'Jobs',
                                  condDict = { 'JobID' : jid },
                                  updateDict = upDict )
      if not result[ 'OK' ]:
        return result

      #Reduce source job input data
      if sourceInputData:
        inputData = {}
        for lfn in jobManifest.getOption( "InputData", [] ):
          if lfn not in sourceInputData:
            return S_ERROR( "LFN in splitted manifest does not exist in the original: %s" % lfn )
        inputData[ lfn ] = dict( sourceInputData[ lfn ] )
        result = self.setInputData( jid, inputData )
        if not result[ 'OK' ]:
          return result

      #Get job attributes to copy them to children jobs
      result = self.getJobAttributes( jid, [ 'Owner', 'OwnerDN', 'OwnerGroup', 'DIRACSetup' ] )
      if not result[ 'OK' ]:
        return result
      attrs = result[ 'Value' ]

      #Do the magic!
      for manifest in manifests[1:]:
        result = self.insertNewJobIntoDB( manifest.dumpAsJDL(), attrs[ 'Owner' ], attrs[ 'OwnerDN' ],
                                          attrs[ 'OwnerGroup' ], attrs[ 'DIRACSetup' ], jid )
        if not result[ 'OK' ]:
          return result
        jidList.append( result[ 'Value' ] )
        if sourceInputData:
          inputData = {}
          for lfn in manifest.getOption( "InputData", [] ):
            if lfn not in sourceInputData:
              return S_ERROR( "LFN in splitted manifest does not exist in the original: %s" % lfn )
          inputData[ lfn ] = dict( sourceInputData[ lfn ] )
          result = self.setInputData( jidList[-1], inputData )
          if not result[ 'OK' ]:
            return result
      commit()

    return S_OK( jidList )

#############################################################################
  def insertNewJobIntoDB( self, jdl, owner, ownerDN, ownerGroup, diracSetup, parentJob = None ):
    """ Insert the initial JDL into the Job database,
        Do initial JDL crosscheck,
        Set Initial job Attributes and Status
    """
    jobManifest = JobManifest()
    result = jobManifest.load( jdl )
    if not result['OK']:
      return result
    jobManifest.setOptionsFromDict( { 'OwnerName' : owner,
                                      'OwnerDN' : ownerDN,
                                      'OwnerGroup' : ownerGroup,
                                      'DIRACSetup' : diracSetup } )
    result = jobManifest.check()
    if not result['OK']:
      return result

    # 1.- insert original JDL on DB and get new JobID
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find( '[' ) != 0 :
      jdl = '[' + jdl + ']'
    result = self.__insertNewJDL( jdl )
    if not result[ 'OK' ]:
      return S_ERROR( 'Can not insert manifest into DB: %s' % result[ 'Message' ] )

    jid = result[ 'Value' ]
    result = self.__checkAndPrepareManifest( jobManifest, jid,
                                        owner, ownerDN,
                                        ownerGroup, diracSetup )
    if not result['OK']:
      return result

    jobManifest.remove( 'JobRequirements' )
    result = self.setJobJDL( jid, jobManifest.dumpAsJDL() )
    if not result['OK']:
      return result

    attrs = {}
    attrs[ 'JobID' ] = jid
    attrs[ 'LastUpdateTime' ] = Time.toString()
    attrs[ 'SubmissionTime' ] = Time.toString()
    attrs[ 'Owner' ] = owner
    attrs[ 'OwnerDN' ] = ownerDN
    attrs[ 'OwnerGroup' ] = ownerGroup
    attrs[ 'DIRACSetup' ] = diracSetup
    attrs[ 'VerifiedFlag' ] = True
    attrs[ 'Status' ] = 'Received'
    attrs[ 'MinorStatus' ] = 'Job accepted'

    site = jobManifest.getOption( 'Site', [] )
    if not site:
      attrs[ 'Site' ] = 'ANY'
    elif len( site ) > 1:
      attrs[ 'Site' ] = 'Multiple'
    else:
      attrs[ 'Site' ] = site[0]

    if jobManifest.getOption( "Splitter", "" ):
      attrs[ 'HerdState' ] = "WillSplit"

    if parentJob == None:
      parentJob = jid
    attrs[ 'MasterJobID' ] = parentJob

    for name in ( 'JobName', 'JobType', 'JobGroup', 'Priority' ):
      value = jobManifest.getOption( name )
      if name == 'Priority':
        name = 'UserPriority'
      if value:
        attrs[ name ] = value

    result = self.insertFields( 'Jobs', inDict = attrs )
    if not result['OK']:
      return result

    result = S_OK( jid )
    result[ 'JobID' ] = jid
    result[ 'Status' ] = 'Received'
    result[ 'MinorStatus' ] = 'Job accepted'

    return result

#############################################################################
  def __checkAndPrepareManifest( self, jobManifest, jid, owner, ownerDN,
                                 ownerGroup, DIRACSetup ):
    """
      Check Consistency of Submitted JDL and set some defaults
      Prepare subJDL with Job Requirements
    """
    VO = Registry.getVOForGroup( ownerGroup )

    manifestData = { 'OwnerName' : owner, 'OwnerDN': ownerDN, 'OwnerGroup' : ownerGroup,
                     'DIRACSetup' : DIRACSetup, 'JobID' : jid, 'VirtualOrganization' : VO }

    jobManifest.setOptionsFromDict( manifestData )

    if not jobManifest.isOption( 'SubmitPools' ):
      submitPools = Registry.getVOOption( VO, 'SubmitPools' )
      if submitPools:
        jobManifest.setOption( 'SubmitPools', submitPools )

    voPolicyDict = gConfig.getOptionsDict( '/DIRAC/VOPolicy/%s/%s' % ( VO, DIRACSetup ) )
    if voPolicyDict['OK']:
      voPolicy = voPolicyDict['Value']
      for k in voPolicy:
        if not jobManifest.isOption( k ):
          jobManifest.setOption( k, voPolicy[ k ] )

    jobManifest.remove( "JobRequirements" )
    result = jobManifest.check()
    if not result['OK']:
      return result

    jobManifest.expand()
    return S_OK()


#############################################################################
  def removeJobFromDB( self, jobIDs ):
    """Remove job from DB

       Remove job from the Job DB and clean up all the job related data
       in various tables
    """

    # ret = self._escapeString(jobID)
    # if not ret['OK']:
    #  return ret
    # e_jobID = ret['Value']

    if type( jobIDs ) != type( [] ):
      jobIDList = [jobIDs]
    else:
      jobIDList = jobIDs

    failedTablesList = []
    jobIDString = ','.join( [str( int( j ) ) for j in jobIDList] )
    cmd = "DELETE LFN, Replicas FROM LFN, Replicas WHERE Replicas.LFNID = LFN.LFNID AND LFN.JobID in (%s)" % jobIDString
    result = self._update( cmd )
    if not result[ 'OK' ]:
      return result
    for table in ( 'JobJDLs',
                   'InputData',
                   'JobParameters',
                   'AtticJobParameters',
                   'HeartBeatLoggingInfo',
                   'OptimizerParameters',
                   'Jobs',
                   'MasterJDLs'
                   ):

      cmd = 'DELETE FROM %s WHERE JobID in (%s)' % ( table, jobIDString )
      result = self._update( cmd )
      if not result['OK']:
        failedTablesList.append( table )

    result = S_OK()
    #if failedSubjobList:
    #  result = S_ERROR( 'Errors while job removal' )
    #  result['FailedSubjobs'] = failedSubjobList
    if failedTablesList:
      result = S_ERROR( 'Errors while job removal' )
      result['FailedTables'] = failedTablesList

    return result

#################################################################
  def rescheduleJobs( self, jobIDs ):
    """ Reschedule all the jobs in the given list
    """

    failedJobs = []
    for jobID in jobIDs:
      result = self.rescheduleJob( jobID )
      if not result['OK']:
        failedJobs.append( jobID )

    if failedJobs:
      result = S_ERROR( 'JobDB.rescheduleJobs: Not all the jobs were rescheduled' )
      result['FailedJobs'] = failedJobs

    return S_OK()

#############################################################################
  def __failJob( self, jid, minor, errMsg ):
    result = self.setJobStatus( jid, status = 'Failed', minor = minor )
    ret = S_ERROR( errMsg )
    if result[ 'OK' ]:
      ret.update( { 'Status' : 'Failed', 'MinorStatus' : minor } )
    return ret

#############################################################################
  def resetJob( self, jid ):
    result = self.setJobAttributes( jid, [ 'RescheduleCounter' ], [ -1 ] )
    if not result[ 'OK' ]:
      return self.__failJob( jid, "Error resetting job", "Error setting reschedule counter: %s" % result[ 'Message' ] )
    result = self.clearAtticParameters( jid )
    if not result[ 'OK' ]:
      return self.__failJob( jid, "Error cleaning attic", "Error cleaning attic: %s" % result[ 'Message' ] )
    return self.rescheduleJob( jid )


#############################################################################
  def rescheduleJob ( self, jid ):
    """ Reschedule the given job to run again from scratch. Retain the already
        defined parameters in the parameter Attic
    """
    #I feel dirty after looking at this method

    try:
      jid = int( jid )
    except ValueError:
      return S_ERROR( "jid is not an number! Get out!" )

    # Check Verified Flag
    result = self.getJobAttributes( jid, ['Status', 'MinorStatus', 'VerifiedFlag', 'RescheduleCounter',
                                     'Owner', 'OwnerDN', 'OwnerGroup', 'DIRACSetup'] )
    if not result['OK']:
      return S_ERROR( 'JobDB.getJobAttributes: Job %d does not exist' % jid )
    attrs = result['Value']

    #This is useless?
    if not attrs['VerifiedFlag']:
      return S_ERROR( 'Job %s not Verified: Status = %s, MinorStatus = %s' % (
                                                                             jid,
                                                                             attrs['Status'],
                                                                             attrs['MinorStatus'] ) )


    # Check the Reschedule counter first
    rescheduleCounter = int( attrs['RescheduleCounter'] ) + 1

    self.maxRescheduling = gConfig.getValue( self.cs_path + '/MaxRescheduling', self.maxRescheduling )

    # Exit if the limit of the reschedulings is reached
    if rescheduleCounter > self.maxRescheduling:
<<<<<<< HEAD
      self.log.warn( 'Maximum number of reschedulings is reached', 'Job %s' % jid )
      return self.__failJob( jid, 'Maximum of reschedulings reached',
                             'Maximum number of reschedulings is reached: %s' % self.maxRescheduling )
=======
      self.log.warn( 'Maximum number of reschedulings is reached', 'Job %s' % jobID )
      self.setJobStatus( jobID, status = 'Failed', minor = 'Maximum of reschedulings reached' )
      return S_ERROR( 'Maximum number of reschedulings is reached: %s' % self.maxRescheduling )
>>>>>>> rel-v6r11


    # Save the job parameters for later debugging
    result = self.getJobParameters( jid )
    if result['OK']:
      parDict = result['Value']
      result = self.setAtticParameters( jid, rescheduleCounter - 1, parDict )
      if not result['OK']:
        return self.__failJob( jid, "Error setting parameter", "Can't set attic parameter: %s" % result[ 'Message' ] )

    for tableName in ( 'JobParameters', 'OptimizerParameters' ):
      cmd = 'DELETE FROM JobParameters WHERE JobID=%d' % jid
      res = self._update( cmd )
      if not res['OK']:
        return self.__failJob( jid, "Error cleaning %s" % tableName,
                                    "Can't clean %s: %s" % ( tableName, res[ 'Message' ] ) )

    result = self.__cleanInputData( jid )
    if not result[ 'OK' ]:
      return self.__failJob( jid, "Error rescheduling", "Can't clean input data: %s" % result[ 'Message' ] )

    # the Jobreceiver needs to know if there is InputData ??? to decide which optimizer to call
    # proposal: - use the getInputData method
    res = self.getJobJDL( jid, original = True )
    if not res['OK']:
      return self.__failJob( jid, "Error getting JDL", "Can't retrieve JDL: %s" % res[ 'Value' ] )

    jdl = res['Value']
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find( '[' ) != 0 :
      jdl = '[%s]' % jdl
    jobManifest = JobManifest()
    jobManifest.loadJDL( jdl )

    result = self.__checkAndPrepareManifest( jobManifest, jid, attrs['Owner'],
                                             attrs['OwnerDN'], attrs['OwnerGroup'],
                                             attrs['DIRACSetup'] )

    if not result['OK']:
      return self.__failJob( jid, "Error prepare JDL", "Can't prepare JDL: %s" % res[ 'Value' ] )

    result = self.setJobJDL( jid, jobManifest.dumpAsJDL() )
    if not result['OK']:
      return self.__failJob( jid, "Error setting JDL", "Can't set JDL: %s" % res[ 'Value' ] )


    attrs = {}
    attrs[ 'RescheduleCounter' ] = rescheduleCounter
    attrs[ 'Status' ] = 'Received'
    attrs[ 'MinorStatus' ] = 'Job accepted'
    attrs[ 'ApplicationStatus' ] = 'Unknown'
    attrs[ 'ApplicationNumStatus' ] = 0
    attrs[ 'RescheduleTime' ] = Time.toString()
    attrs[ 'LastUpdateTime' ] = Time.toString()
    attrs[ 'SubmissionTime' ] = Time.toString()

    site = jobManifest.getOption( 'Site', [] )
    if not site:
      attrs[ 'Site' ] = 'ANY'
    elif len( site ) > 1:
      attrs[ 'Site' ] = 'Multiple'
    else:
      attrs[ 'Site' ] = site[0]


    result = self.updateFields( 'Jobs', condDict = { 'JobID' : jid },
                                updateDict = attrs )
    if not result['OK']:
      return self.__failJob( jid, "Error setting attrs", "Can't set attributes: %s" % res[ 'Value' ] )

    retVal = S_OK( jid )
    retVal['JobID'] = jid
    retVal['InputData'] = jobManifest.getOption( "InputData" )
    retVal['RescheduleCounter'] = rescheduleCounter
    retVal['Status'] = 'Received'
    retVal['MinorStatus'] = 'Job Rescheduled'

    return retVal

#############################################################################
  def setSandboxReady( self, jobID, stype = 'InputSandbox' ):
    """ Set the sandbox status ready for the job with jobID
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']


    if stype == "InputSandbox":
      field = "ISandboxReadyFlag"
    elif stype == "OutputSandbox":
      field = "OSandboxReadyFlag"
    else:
      return S_ERROR( 'Illegal Sandbox type: ' + stype )

    cmd = "UPDATE Jobs SET %s='True' WHERE JobID=%s" % ( field, jobID )
    result = self._update( cmd )
    return result

#################################################################################
  def getSiteSummary( self ):
    """ Get the summary of jobs in a given status on all the sites
    """

    waitingList = ['"Submitted"', '"Assigned"', '"Waiting"', '"Matched"']
    waitingString = ','.join( waitingList )

    result = self.getDistinctJobAttributes( 'Site' )
    if not result['OK']:
      return result

    siteList = result['Value']
    siteDict = {}
    totalDict = {'Waiting':0, 'Running':0, 'Stalled':0, 'Done':0, 'Failed':0}

    for site in siteList:
      if site == "ANY":
        continue
      # Waiting
      siteDict[site] = {}
      ret = self._escapeString( site )
      if not ret['OK']:
        return ret
      e_site = ret['Value']

      req = "SELECT COUNT(JobID) FROM Jobs WHERE Status IN (%s) AND Site=%s" % ( waitingString, e_site )
      result = self._query( req )
      if result['OK']:
        count = result['Value'][0][0]
      else:
        return S_ERROR( 'Failed to get Site data from the JobDB' )
      siteDict[site]['Waiting'] = count
      totalDict['Waiting'] += count
      # Running,Stalled,Done,Failed
      for status in ['"Running"', '"Stalled"', '"Done"', '"Failed"']:
        req = "SELECT COUNT(JobID) FROM Jobs WHERE Status=%s AND Site=%s" % ( status, e_site )
        result = self._query( req )
        if result['OK']:
          count = result['Value'][0][0]
        else:
          return S_ERROR( 'Failed to get Site data from the JobDB' )
        siteDict[site][status.replace( '"', '' )] = count
        totalDict[status.replace( '"', '' )] += count

    siteDict['Total'] = totalDict
    return S_OK( siteDict )

#################################################################################
  def getSiteSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of jobs in a given status on all the sites in the standard Web form
    """

    paramNames = ['Site', 'GridType', 'Country', 'Tier', 'MaskStatus']
    paramNames += JOB_STATES
    paramNames += ['Efficiency', 'Status']
    siteT1List = ['CERN', 'IN2P3', 'NIKHEF', 'PIC', 'CNAF', 'RAL', 'GRIDKA']

    # Sort out records as requested
    sortItem = -1
    sortOrder = "ASC"
    if sortList:
      item = sortList[0][0]  # only one item for the moment
      sortItem = paramNames.index( item )
      sortOrder = sortList[0][1]

    last_update = None
    if selectDict.has_key( 'LastUpdateTime' ):
      last_update = selectDict['LastUpdateTime']
      del selectDict['LastUpdateTime']

    result = self.getCounters( 'Jobs', ['Site', 'Status'],
                              {}, newer = last_update,
                              timeStamp = 'LastUpdateTime' )
    last_day = Time.dateTime() - Time.day
    resultDay = self.getCounters( 'Jobs', ['Site', 'Status'],
                                 {}, newer = last_day,
                                 timeStamp = 'EndExecTime' )

    # Get the site mask status
    siteStatus = SiteStatus()
    siteMask = {}
    resultMask = getSites( fullName = True )
    if resultMask['OK']:
      for site in resultMask['Value']:
        siteMask[site] = 'NoMask'
    resultMask = siteStatus.getUsableSites( 'ComputingAccess' )
    if resultMask['OK']:
      for site in resultMask['Value']:
        siteMask[site] = 'Active'
    resultMask = siteStatus.getUnusableSites( 'ComputingAccess' )
    if resultMask['OK']:
      for site in resultMask['Value']:
        siteMask[site] = 'Banned'

    # Sort out different counters
    resultDict = {}
    if result['OK']:
      for attDict, count in result['Value']:
        siteFullName = attDict['Site']
        status = attDict['Status']
        if not resultDict.has_key( siteFullName ):
          resultDict[siteFullName] = {}
          for state in JOB_STATES:
            resultDict[siteFullName][state] = 0
        if status not in JOB_FINAL_STATES:
          resultDict[siteFullName][status] = count
    if resultDay['OK']:
      for attDict, count in resultDay['Value']:
        siteFullName = attDict['Site']
        if not resultDict.has_key( siteFullName ):
          resultDict[siteFullName] = {}
          for state in JOB_STATES:
            resultDict[siteFullName][state] = 0
        status = attDict['Status']
        if status in JOB_FINAL_STATES:
          resultDict[siteFullName][status] = count

    # Collect records now
    records = []
    countryCounts = {}
    for siteFullName in resultDict:
      siteDict = resultDict[siteFullName]
      if siteFullName.find( '.' ) != -1:
        grid, site, country = siteFullName.split( '.' )
      else:
        grid, site, country = 'Unknown', 'Unknown', 'Unknown'

      tier = 'Tier-2'
      if site in siteT1List:
        tier = 'Tier-1'

      if not countryCounts.has_key( country ):
        countryCounts[country] = {}
        for state in JOB_STATES:
          countryCounts[country][state] = 0
      rList = [siteFullName, grid, country, tier]
      if siteMask.has_key( siteFullName ):
        rList.append( siteMask[siteFullName] )
      else:
        rList.append( 'NoMask' )
      for status in JOB_STATES:
        rList.append( siteDict[status] )
        countryCounts[country][status] += siteDict[status]
      efficiency = 0
      total_finished = 0
      for state in JOB_FINAL_STATES:
        total_finished += resultDict[siteFullName][state]
      if total_finished > 0:
        efficiency = float( siteDict['Done'] + siteDict['Completed'] ) / float( total_finished )
      rList.append( '%.1f' % ( efficiency * 100. ) )
      # Estimate the site verbose status
      if efficiency > 0.95:
        rList.append( 'Good' )
      elif efficiency > 0.80:
        rList.append( 'Fair' )
      elif efficiency > 0.60:
        rList.append( 'Poor' )
      elif total_finished == 0:
        rList.append( 'Idle' )
      else:
        rList.append( 'Bad' )
      records.append( rList )

    # Select records as requested
    if selectDict:
      for item in selectDict:
        selectItem = paramNames.index( item )
        values = selectDict[item]
        if type( values ) != type( [] ):
          values = [values]
        indices = range( len( records ) )
        indices.reverse()
        for ind in indices:
          if records[ind][selectItem] not in values:
            del records[ind]

    # Sort records as requested
    if sortItem != -1 :
      if sortOrder.lower() == "asc":
        records.sort( key = operator.itemgetter( sortItem ) )
      else:
        records.sort( key = operator.itemgetter( sortItem ), reverse = True )

    # Collect the final result
    finalDict = {}
    finalDict['ParameterNames'] = paramNames
    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      if startItem + maxItems > len( records ):
        finalDict['Records'] = records[startItem:]
      else:
        finalDict['Records'] = records[startItem:startItem + maxItems]
    else:
      finalDict['Records'] = records

    finalDict['TotalRecords'] = len( records )
    finalDict['Extras'] = countryCounts

    return S_OK( finalDict )

#################################################################################
  def getUserSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of user jobs in a standard form for the Web portal.
        Pagination and global sorting is supported.
    """

    paramNames = ['Owner', 'OwnerDN', 'OwnerGroup']
    paramNames += JOB_STATES
    paramNames += ['TotalJobs']

    # Sort out records as requested
    sortItem = -1
    sortOrder = "ASC"
    if sortList:
      item = sortList[0][0]  # only one item for the moment
      sortItem = paramNames.index( item )
      sortOrder = sortList[0][1]

    last_update = None
    if selectDict.has_key( 'LastUpdateTime' ):
      last_update = selectDict['LastUpdateTime']
      del selectDict['LastUpdateTime']
    if selectDict.has_key( 'Owner' ):
      username = selectDict['Owner']
      del selectDict['Owner']
      result = Registry.getDNForUsername( username )
      if result['OK']:
        selectDict['OwnerDN'] = result['Value']
      else:
        return S_ERROR( 'Unknown user %s' % username )

    result = self.getCounters( 'Jobs', ['OwnerDN', 'OwnerGroup', 'Status'],
                              selectDict, newer = last_update,
                              timeStamp = 'LastUpdateTime' )
    last_day = Time.dateTime() - Time.day
    resultDay = self.getCounters( 'Jobs', ['OwnerDN', 'OwnerGroup', 'Status'],
                                 selectDict, newer = last_day,
                                 timeStamp = 'EndExecTime' )

    # Sort out different counters
    resultDict = {}
    for attDict, count in result['Value']:
      owner = attDict['OwnerDN']
      group = attDict['OwnerGroup']
      status = attDict['Status']

      if not resultDict.has_key( owner ):
        resultDict[owner] = {}
      if not resultDict[owner].has_key( group ):
        resultDict[owner][group] = {}
        for state in JOB_STATES:
          resultDict[owner][group][state] = 0

      resultDict[owner][group][status] = count
    for attDict, count in resultDay['Value']:
      owner = attDict['OwnerDN']
      group = attDict['OwnerGroup']
      status = attDict['Status']
      if status in JOB_FINAL_STATES:
        resultDict[owner][group][status] = count

    # Collect records now
    records = []
    totalUser = {}
    for owner in resultDict:
      totalUser[owner] = 0
      for group in resultDict[owner]:
        result = Registry.getUsernameForDN( owner )
        if result['OK']:
          username = result['Value']
        else:
          username = 'Unknown'
        rList = [username, owner, group]
        count = 0
        for state in JOB_STATES:
          s_count = resultDict[owner][group][state]
          rList.append( s_count )
          count += s_count
        rList.append( count )
        records.append( rList )
        totalUser[owner] += count

    # Sort out records
    if sortItem != -1 :
      if sortOrder.lower() == "asc":
        records.sort( key = operator.itemgetter( sortItem ) )
      else:
        records.sort( key = operator.itemgetter( sortItem ), reverse = True )

    # Collect the final result
    finalDict = {}
    finalDict['ParameterNames'] = paramNames
    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      if startItem + maxItems > len( records ):
        finalDict['Records'] = records[startItem:]
      else:
        finalDict['Records'] = records[startItem:startItem + maxItems]
    else:
      finalDict['Records'] = records

    finalDict['TotalRecords'] = len( records )
    return S_OK( finalDict )

#####################################################################################
  def setHeartBeatData( self, jobID, staticDataDict, dynamicDataDict ):
    """ Add the job's heart beat data to the database
    """

    # Set the time stamp first
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    e_jobID = ret['Value']

    req = "UPDATE Jobs SET HeartBeatTime=UTC_TIMESTAMP(), Status='Running' WHERE JobID=%s" % e_jobID
    result = self._update( req )
    if not result['OK']:
      return S_ERROR( 'Failed to set the heart beat time: ' + result['Message'] )

    ok = True
    # FIXME: It is rather not optimal to use parameters to store the heartbeat info, must find a proper solution
    # Add static data items as job parameters
    result = self.setJobParameters( jobID, staticDataDict.items() )
    if not result['OK']:
      ok = False
      self.log.warn( result['Message'] )

    # Add dynamic data to the job heart beat log
    # start = time.time()
    valueList = []
    for key, value in dynamicDataDict.items():
      result = self._escapeString( key )
      if not result['OK']:
        self.log.warn( 'Failed to escape string ' + key )
        continue
      e_key = result['Value']
      result = self._escapeString( value )
      if not result['OK']:
        self.log.warn( 'Failed to escape string ' + value )
        continue
      e_value = result['Value']
      valueList.append( "( %s, %s,%s,UTC_TIMESTAMP())" % ( e_jobID, e_key, e_value ) )

    if valueList:

      valueString = ','.join( valueList )
      req = "INSERT INTO HeartBeatLoggingInfo (JobID,Name,Value,HeartBeatTime) VALUES "
      req += valueString
      result = self._update( req )
      if not result['OK']:
        ok = False
        self.log.warn( result['Message'] )

    if ok:
      return S_OK()
    else:
      return S_ERROR( 'Failed to store some or all the parameters' )

#####################################################################################
  def getHeartBeatData( self, jobID ):
    """ Retrieve the job's heart beat data
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    cmd = 'SELECT Name,Value,HeartBeatTime from HeartBeatLoggingInfo WHERE JobID=%s' % jobID
    res = self._query( cmd )
    if not res['OK']:
      return res

    if len( res['Value'] ) == 0:
      return S_OK ( [] )

    result = []
    values = res['Value']
    for row in values:
      result.append( ( str( row[0] ), '%.01f' % ( float( row[1].replace( '"', '' ) ) ), str( row[2] ) ) )

    return S_OK( result )

#####################################################################################
  def setJobCommand( self, jobID, command, arguments = None ):
    """ Store a command to be passed to the job together with the
        next heart beat
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( command )
    if not ret['OK']:
      return ret
    command = ret['Value']

    if arguments:
      ret = self._escapeString( arguments )
      if not ret['OK']:
        return ret
      arguments = ret['Value']
    else:
      arguments = "''"

    req = "INSERT INTO JobCommands (JobID,Command,Arguments,ReceptionTime) "
    req += "VALUES (%s,%s,%s,UTC_TIMESTAMP())" % ( jobID, command, arguments )
    result = self._update( req )
    return result

#####################################################################################
  def getJobCommand( self, jobID, status = 'Received' ):
    """ Get a command to be passed to the job together with the
        next heart beat
    """

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( status )
    if not ret['OK']:
      return ret
    status = ret['Value']

    req = "SELECT Command, Arguments FROM JobCommands WHERE JobID=%s AND Status=%s" % ( jobID, status )
    result = self._query( req )
    if not result['OK']:
      return result

    resultDict = {}
    if result['Value']:
      for row in result['Value']:
        resultDict[row[0]] = row[1]

    return S_OK( resultDict )

#####################################################################################
  def setJobCommandStatus( self, jobID, command, status ):
    """ Set the command status
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    ret = self._escapeString( command )
    if not ret['OK']:
      return ret
    command = ret['Value']

    ret = self._escapeString( status )
    if not ret['OK']:
      return ret
    status = ret['Value']

    req = "UPDATE JobCommands SET Status=%s WHERE JobID=%s AND Command=%s" % ( status, jobID, command )
    result = self._update( req )
    return result

#####################################################################################
  def getSummarySnapshot( self, requestedFields = False ):
    """ Get the summary snapshot for a given combination
    """
    if not requestedFields:
      requestedFields = [ 'Status', 'MinorStatus',
                  'Site', 'Owner', 'OwnerGroup', 'JobGroup', 'HerdState' ]
    defFields = [ 'DIRACSetup' ] + requestedFields
    valueFields = [ 'COUNT(JobID)', 'SUM(RescheduleCounter)' ]
    defString = ", ".join( defFields )
    valueString = ", ".join( valueFields )
    sqlCmd = "SELECT %s, %s From Jobs GROUP BY %s" % ( defString, valueString, defString )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( ( ( defFields + valueFields ), result[ 'Value' ] ) )
