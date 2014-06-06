""" JobLoggingDB class is a front-end to the Job Logging Database.
    The following methods are provided

    addLoggingRecord()
    getJobLoggingInfo()
    getWMSTimeStamps()
"""

import time
from types                import StringTypes, IntType, LongType

from DIRAC                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Base.DB   import DB

__RCSID__ = "$Id$"

# Here for debugging purpose; should be initialized by the containing component
gLogger.initialize( 'WMS', '/Databases/JobLoggingDB/Test' )

MAGIC_EPOC_NUMBER = 1270000000

#############################################################################
class JobLoggingDB( DB ):

  _tablesDict = {}
  # LoggingInfo table
  _tablesDict[ 'LoggingInfo' ] = {
                                  'Fields' :
                                            {
                                             'JobID'             : 'INTEGER NOT NULL',
                                             'SeqNum'            : 'INTEGER NOT NULL DEFAULT 0',
                                             'Status'            : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                             'MinorStatus'       : 'VARCHAR(128) NOT NULL DEFAULT ""',
                                             'ApplicationStatus' : 'VARCHAR(256) NOT NULL DEFAULT ""',
                                             'StatusTime'        : 'DATETIME NOT NULL',
                                             'StatusTimeOrder'   : 'DOUBLE(12,3) NOT NULL',
                                             'StatusSource'      : 'VARCHAR(32) NOT NULL DEFAULT "Unknown"',
                                            },
                                  'PrimaryKey': ['JobID', 'SeqNum'],
                                  'Engine': 'InnoDB'
#                                  'Indexes' : { 'JobID' : [ 'JobID' ] }
                                 }

  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """

    DB.__init__( self, 'JobLoggingDB', 'WorkloadManagement/JobLoggingDB', maxQueueSize )
    self.gLogger = gLogger


  def _checkTable( self ):
    """ _checkTable.
     
    Method called on the MatcherHandler instead of on the JobLoggingDB constructor
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


#############################################################################
  def addLoggingRecord( self,
                       jobID,
                       status = 'idem',
                       minor = 'idem',
                       application = 'idem',
                       date = '',
                       source = 'Unknown' ):

    """ Add a new entry to the JobLoggingDB table. One, two or all the three status
        components can be specified. Optionaly the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used.
    """

    event = 'status/minor/app=%s/%s/%s' % ( status, minor, application )
    self.gLogger.info( "Adding record for job " + str( jobID ) + ": '" + event + "' from " + source )

    if not date:
      # Make the UTC datetime string and float
      _date = Time.dateTime()
      epoc = time.mktime( _date.timetuple() ) + _date.microsecond / 1000000. - MAGIC_EPOC_NUMBER
      time_order = round( epoc, 3 )
    else:
      try:
        if type( date ) in StringTypes:
          # The date is provided as a string in UTC
          _date = Time.fromString( date )
          epoc = time.mktime( _date.timetuple() ) + _date.microsecond / 1000000. - MAGIC_EPOC_NUMBER
          time_order = round( epoc, 3 )
        elif type( date ) == Time._dateTimeType:
          _date = date
          epoc = time.mktime( _date.timetuple() ) + _date.microsecond / 1000000. - MAGIC_EPOC_NUMBER
          time_order = round( epoc, 3 )
        else:
          self.gLogger.error( 'Incorrect date for the logging record' )
          _date = Time.dateTime()
          epoc = time.mktime( _date.timetuple() ) - MAGIC_EPOC_NUMBER
          time_order = round( epoc, 3 )
      except:
        self.gLogger.exception( 'Exception while date evaluation' )
        _date = Time.dateTime()
        epoc = time.mktime( _date.timetuple() ) - MAGIC_EPOC_NUMBER
        time_order = round( epoc, 3 )

    cmd = "INSERT INTO LoggingInfo (JobId, Status, MinorStatus, ApplicationStatus, " + \
          "StatusTime, StatusTimeOrder, StatusSource) VALUES (%d,'%s','%s','%s','%s',%f,'%s')" % \
           ( int( jobID ), status, minor, application, str( _date ), time_order, source )

    return self._update( cmd )

#############################################################################
  def getJobLoggingInfo( self, jobID ):
    """ Returns a Status,MinorStatus,ApplicationStatus,StatusTime,StatusSource tuple
        for each record found for job specified by its jobID in historical order
    """

    cmd = 'SELECT Status,MinorStatus,ApplicationStatus,StatusTime,StatusSource FROM' \
          ' LoggingInfo WHERE JobId=%d ORDER BY StatusTimeOrder,StatusTime' % int( jobID )

    result = self._query( cmd )
    if not result['OK']:
      return result
    if result['OK'] and not result['Value']:
      return S_ERROR( 'No Logging information for job %d' % int( jobID ) )

    return_value = []
    status, minor, app = result['Value'][0][:3]
    if app == "idem":
      app = "Unknown"
    for row in result['Value']:
      if row[0] != "idem":
        status = row[0];
      if row[1] != "idem":
        minor = row[1];
      if row[2] != "idem":
        app = row[2];
      return_value.append( ( status, minor, app, str( row[3] ), row[4] ) )

    return S_OK( return_value )

#############################################################################
  def deleteJob( self, jobID ):
    """ Delete logging records for given jobs
    """

    # Make sure that we have a list of jobs
    if type( jobID ) in [ IntType, LongType ]:
      jobList = [ str( jobID ) ]
    elif type( jobID ) in StringTypes:
      jobList = [ jobID ]
    else:
      jobList = list( jobID )

    jobString = ','.join( jobList )
    req = "DELETE FROM LoggingInfo WHERE JobID IN (%s)" % jobString
    result = self._update( req )
    return result

#############################################################################
  def getWMSTimeStamps( self, jobID ):
    """ Get TimeStamps for job MajorState transitions
        return a {State:timestamp} dictionary
    """
    self.gLogger.debug( 'getWMSTimeStamps: Retrieving Timestamps for Job %d' % int( jobID ) )

    result = {}
    cmd = 'SELECT Status,StatusTimeOrder FROM LoggingInfo WHERE JobID=%d' % int( jobID )
    resCmd = self._query( cmd )
    if not resCmd['OK']:
      return resCmd
    if not resCmd['Value']:
      return S_ERROR( 'No Logging Info for job %d' % int( jobID ) )

    for event, etime in resCmd['Value']:
      result[event] = str( etime + MAGIC_EPOC_NUMBER )

    # Get last date and time
    cmd = 'SELECT MAX(StatusTime) FROM LoggingInfo WHERE JobID=%d' % int( jobID )
    resCmd = self._query( cmd )
    if not resCmd['OK']:
      return resCmd
    if len( resCmd['Value'] ) > 0:
      result['LastTime'] = str( resCmd['Value'][0][0] )
    else:
      result['LastTime'] = "Unknown"

    return S_OK( result )
