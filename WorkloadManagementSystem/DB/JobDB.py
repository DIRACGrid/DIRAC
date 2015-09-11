""" DIRAC JobDB class is a front-end to the main WMS database containing
    job definitions and status information. It is used in most of the WMS
    components

    The following methods are provided for public usage:

    getJobAttribute()
    getJobAttributes()
    getAllJobAttributes()
    getDistinctJobAttributes()
    getAttributesForJobList()
    getJobParameter()
    getJobParameters()
    getAllJobParameters()
    getInputData()
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

import sys
import operator

from DIRAC.Core.Utilities.ClassAd.ClassAdLight               import ClassAd
from DIRAC                                                   import S_OK, S_ERROR, Time
from DIRAC.ConfigurationSystem.Client.Config                 import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOForGroup, getVOOption, getGroupOption
from DIRAC.Core.Base.DB                                      import DB
from DIRAC.ConfigurationSystem.Client.Helpers.Resources      import getDIRACPlatform
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest   import JobManifest

DEBUG = False
JOB_STATES = ['Received', 'Checking', 'Staging', 'Waiting', 'Matched',
              'Running', 'Stalled', 'Done', 'Completed', 'Failed']
JOB_FINAL_STATES = ['Done', 'Completed', 'Failed']

JOB_STATIC_ATTRIBUTES = [ 'JobID', 'JobType', 'DIRACSetup', 'JobGroup', 'JobSplitType', 'MasterJobID',
                          'JobName', 'Owner', 'OwnerDN', 'OwnerGroup', 'SubmissionTime', 'VerifiedFlag' ]

JOB_VARIABLE_ATTRIBUTES = [ 'Site', 'RescheduleTime', 'StartExecTime', 'EndExecTime', 'RescheduleCounter',
                           'DeletedFlag', 'KilledFlag', 'FailedFlag',
                           'ISandboxReadyFlag', 'OSandboxReadyFlag', 'RetrievedFlag', 'AccountedFlag' ]

JOB_DYNAMIC_ATTRIBUTES = [ 'LastUpdateTime', 'HeartBeatTime',
                           'Status', 'MinorStatus', 'ApplicationStatus', 'ApplicationNumStatus', 'CPUTime']

#############################################################################
class JobDB( DB ):

  def __init__( self ):
    """ Standard Constructor
    """

    DB.__init__( self, 'JobDB', 'WorkloadManagement/JobDB', debug = DEBUG )

    self.maxRescheduling = self.getCSOption( 'MaxRescheduling', 3 )

    self.jobAttributeNames = []

    result = self.__getAttributeNames()

    if not result['OK']:
      error = 'Can not retrieve job Attributes'
      self.log.fatal( 'JobDB: %s' % error )
      sys.exit( error )
      return

    self.log.info( "MaxReschedule:  %s" % self.maxRescheduling )
    self.log.info( "==================================================" )

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

    return S_OK()

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
      for dFormat in ( '%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S' ):
        try:
          since = datetime.datetime.strptime( date, dFormat )
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

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    if attrList:
      attrNameList = []
      for x in attrList:
        ret = self._escapeString( x )
        if not ret['OK']:
          return ret
        x = "`" + ret['Value'][1:-1] + "`"
        attrNameList.append( x )
      attrNames = ','.join( attrNameList )
    else:
      attrNameList = []
      for x in self.jobAttributeNames:
        ret = self._escapeString( x )
        if not ret['OK']:
          return ret
        x = "`" + ret['Value'][1:-1] + "`"
        attrNameList.append( x )
      attrNames = ','.join( attrNameList )
    self.log.debug( 'JobDB.getAllJobAttributes: Getting Attributes for job = %s.' % jobID )

    cmd = 'SELECT %s FROM Jobs WHERE JobID=%s' % ( attrNames, jobID )
    res = self._query( cmd )
    if not res['OK']:
      return res

    if len( res['Value'] ) == 0:
      return S_OK ( {} )

    values = res['Value'][0]

    attributes = {}
    if attrList:
      for i in range( len( attrList ) ):
        attributes[attrList[i]] = str( values[i] )
    else:
      for i in range( len( self.jobAttributeNames ) ):
        attributes[self.jobAttributeNames[i]] = str( values[i] )

    return S_OK( attributes )

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

  def getInputData( self, jobID ):
    """Get input data for the given job
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']
    cmd = 'SELECT LFN FROM InputData WHERE JobID=%s' % jobID
    res = self._query( cmd )
    if not res['OK']:
      return res

    return S_OK( [ i[0] for i in res['Value'] if i[0].strip() ] )

#############################################################################
  def setInputData( self, jobID, inputData ):
    """Inserts input data for the given job
    """
    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    jobID = ret['Value']
    cmd = 'DELETE FROM InputData WHERE JobID=%s' % ( jobID )
    result = self._update( cmd )
    if not result['OK']:
      result = S_ERROR( 'JobDB.setInputData: operation failed.' )

    for lfn in inputData:
      # some jobs are setting empty string as InputData
      if not lfn:
        continue
      ret = self._escapeString( lfn.strip() )
      if not ret['OK']:
        return ret
      lfn = ret['Value']
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES (%s, %s )' % ( jobID, lfn )
      res = self._update( cmd )
      if not res['OK']:
        return res

    return S_OK( 'Files added' )

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
      return S_ERROR( 'JobDB.setJobOptParameter: operation failed.' )

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
  def __setInitialJobParameters( self, classadJob, jobID ):
    """ Set initial job parameters as was defined in the Classad
    """

    # Extract initital job parameters
    parameters = {}
    if classadJob.lookupAttribute( "Parameters" ):
      parameters = classadJob.getDictionaryFromSubJDL( "Parameters" )
    res = self.setJobParameters( jobID, parameters.items() )

    if not res['OK']:
      return res

    return S_OK()

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

    if not 'lastRowId' in result:
      return S_ERROR( '%s' % err )

    jobID = int( result['lastRowId'] )

    self.log.info( 'JobDB: New JobID served "%s"' % jobID )

    return S_OK( jobID )


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

#############################################################################
  def insertNewJobIntoDB( self, jdl, owner, ownerDN, ownerGroup, diracSetup ):
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
    jobAttrNames = []
    jobAttrValues = []

    # 1.- insert original JDL on DB and get new JobID
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find( '[' ) != 0 :
      jdl = '[' + jdl + ']'
    result = self.__insertNewJDL( jdl )
    if not result[ 'OK' ]:
      return S_ERROR( 'Can not insert JDL in to DB' )
    jobID = result[ 'Value' ]

    jobManifest.setOption( 'JobID', jobID )

    jobAttrNames.append( 'JobID' )
    jobAttrValues.append( jobID )

    jobAttrNames.append( 'LastUpdateTime' )
    jobAttrValues.append( Time.toString() )

    jobAttrNames.append( 'SubmissionTime' )
    jobAttrValues.append( Time.toString() )

    jobAttrNames.append( 'Owner' )
    jobAttrValues.append( owner )

    jobAttrNames.append( 'OwnerDN' )
    jobAttrValues.append( ownerDN )

    jobAttrNames.append( 'OwnerGroup' )
    jobAttrValues.append( ownerGroup )

    jobAttrNames.append( 'DIRACSetup' )
    jobAttrValues.append( diracSetup )

    # 2.- Check JDL and Prepare DIRAC JDL
    classAdJob = ClassAd( jobManifest.dumpAsJDL() )
    classAdReq = ClassAd( '[]' )
    retVal = S_OK( jobID )
    retVal['JobID'] = jobID
    if not classAdJob.isOK():
      jobAttrNames.append( 'Status' )
      jobAttrValues.append( 'Failed' )

      jobAttrNames.append( 'MinorStatus' )
      jobAttrValues.append( 'Error in JDL syntax' )

      result = self.insertFields( 'Jobs', jobAttrNames, jobAttrValues )
      if not result['OK']:
        return result

      retVal['Status'] = 'Failed'
      retVal['MinorStatus'] = 'Error in JDL syntax'
      return retVal

    classAdJob.insertAttributeInt( 'JobID', jobID )
    result = self.__checkAndPrepareJob( jobID, classAdJob, classAdReq,
                                        owner, ownerDN,
                                        ownerGroup, diracSetup,
                                        jobAttrNames, jobAttrValues )
    if not result['OK']:
      return result

    priority = classAdJob.getAttributeInt( 'Priority' )
    jobAttrNames.append( 'UserPriority' )
    jobAttrValues.append( priority )

    for jdlName in 'JobName', 'JobType', 'JobGroup':
      # Defaults are set by the DB.
      jdlValue = classAdJob.getAttributeString( jdlName )
      if jdlValue:
        jobAttrNames.append( jdlName )
        jobAttrValues.append( jdlValue )

    jdlValue = classAdJob.getAttributeString( 'Site' )
    if jdlValue:
      jobAttrNames.append( 'Site' )
      if jdlValue.find( ',' ) != -1:
        jobAttrValues.append( 'Multiple' )
      else:
        jobAttrValues.append( jdlValue )

    jobAttrNames.append( 'VerifiedFlag' )
    jobAttrValues.append( 'True' )

    jobAttrNames.append( 'Status' )
    jobAttrValues.append( 'Received' )

    jobAttrNames.append( 'MinorStatus' )
    jobAttrValues.append( 'Job accepted' )

    reqJDL = classAdReq.asJDL()
    classAdJob.insertAttributeInt( 'JobRequirements', reqJDL )

    jobJDL = classAdJob.asJDL()

    # Replace the JobID placeholder if any
    if jobJDL.find( '%j' ) != -1:
      jobJDL = jobJDL.replace( '%j', str( jobID ) )

    result = self.setJobJDL( jobID, jobJDL )
    if not result['OK']:
      return result

    # Adding the job in the Jobs table
    result = self.insertFields( 'Jobs', jobAttrNames, jobAttrValues )
    if not result['OK']:
      return result

    # Setting the Job parameters
    result = self.__setInitialJobParameters( classAdJob, jobID )
    if not result['OK']:
      return result

    # Looking for the Input Data
    inputData = []
    if classAdJob.lookupAttribute( 'InputData' ):
      inputData = classAdJob.getListFromExpression( 'InputData' )
    values = []

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    e_jobID = ret['Value']

    for lfn in inputData:
      # some jobs are setting empty string as InputData
      if not lfn:
        continue
      ret = self._escapeString( lfn.strip() )
      if not ret['OK']:
        return ret
      lfn = ret['Value']

      values.append( '(%s, %s )' % ( e_jobID, lfn ) )

    if values:
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES %s' % ', '.join( values )
      result = self._update( cmd )
      if not result['OK']:
        return result

    retVal['Status'] = 'Received'
    retVal['MinorStatus'] = 'Job accepted'

    return retVal

  def __checkAndPrepareJob( self, jobID, classAdJob, classAdReq, owner, ownerDN,
                            ownerGroup, diracSetup, jobAttrNames, jobAttrValues ):
    """
      Check Consistency of Submitted JDL and set some defaults
      Prepare subJDL with Job Requirements
    """
    error = ''
    vo = getVOForGroup( ownerGroup )

    jdlDiracSetup = classAdJob.getAttributeString( 'DIRACSetup' )
    jdlOwner = classAdJob.getAttributeString( 'Owner' )
    jdlOwnerDN = classAdJob.getAttributeString( 'OwnerDN' )
    jdlOwnerGroup = classAdJob.getAttributeString( 'OwnerGroup' )
    jdlVO = classAdJob.getAttributeString( 'VirtualOrganization' )

    # The below is commented out since this is always overwritten by the submitter IDs
    # but the check allows to findout inconsistent client environments
    if jdlDiracSetup and jdlDiracSetup != diracSetup:
      error = 'Wrong DIRAC Setup in JDL'
    if jdlOwner and jdlOwner != owner:
      error = 'Wrong Owner in JDL'
    elif jdlOwnerDN and jdlOwnerDN != ownerDN:
      error = 'Wrong Owner DN in JDL'
    elif jdlOwnerGroup and jdlOwnerGroup != ownerGroup:
      error = 'Wrong Owner Group in JDL'
    elif jdlVO and jdlVO != vo:
      error = 'Wrong Virtual Organization in JDL'


    classAdJob.insertAttributeString( 'Owner', owner )
    classAdJob.insertAttributeString( 'OwnerDN', ownerDN )
    classAdJob.insertAttributeString( 'OwnerGroup', ownerGroup )

    submitPools = getGroupOption( ownerGroup, "SubmitPools" )
    if not submitPools and vo:
      submitPools = getVOOption( vo, 'SubmitPools' )
    if submitPools and not classAdJob.lookupAttribute( 'SubmitPools' ):
      classAdJob.insertAttributeString( 'SubmitPools', submitPools )

    if vo:
      classAdJob.insertAttributeString( 'VirtualOrganization', vo )

    classAdReq.insertAttributeString( 'Setup', diracSetup )
    classAdReq.insertAttributeString( 'OwnerDN', ownerDN )
    classAdReq.insertAttributeString( 'OwnerGroup', ownerGroup )
    if vo:
      classAdReq.insertAttributeString( 'VirtualOrganization', vo )

    setup = gConfig.getValue( '/DIRAC/Setup', '' )
    voPolicyDict = gConfig.getOptionsDict( '/DIRAC/VOPolicy/%s/%s' % ( vo, setup ) )
    # voPolicyDict = gConfig.getOptionsDict('/DIRAC/VOPolicy')
    if voPolicyDict['OK']:
      voPolicy = voPolicyDict['Value']
      for param, val in voPolicy.items():
        if not classAdJob.lookupAttribute( param ):
          classAdJob.insertAttributeString( param, val )

    priority = classAdJob.getAttributeInt( 'Priority' )
    platform = classAdJob.getAttributeString( 'Platform' )
    # Legacy check to suite the LHCb logic
    if not platform:
      platform = classAdJob.getAttributeString( 'SystemConfig' )
    cpuTime = classAdJob.getAttributeInt( 'CPUTime' )
    if cpuTime == 0:
      # Just in case check for MaxCPUTime for backward compatibility
      cpuTime = classAdJob.getAttributeInt( 'MaxCPUTime' )
      if cpuTime > 0:
        classAdJob.insertAttributeInt( 'CPUTime', cpuTime )
    classAdReq.insertAttributeInt( 'UserPriority', priority )
    classAdReq.insertAttributeInt( 'CPUTime', cpuTime )

    if platform and platform.lower() != 'any':
      result = getDIRACPlatform( platform )
      if result['OK'] and result['Value']:
        classAdReq.insertAttributeVectorString( 'Platforms', result['Value'] )
      else:
        error = "OS compatibility info not found"

    if error:

      retVal = S_ERROR( error )
      retVal['JobId'] = jobID
      retVal['Status'] = 'Failed'
      retVal['MinorStatus'] = error

      jobAttrNames.append( 'Status' )
      jobAttrValues.append( 'Failed' )

      jobAttrNames.append( 'MinorStatus' )
      jobAttrValues.append( error )
      resultInsert = self.setJobAttributes( jobID, jobAttrNames, jobAttrValues )
      if not resultInsert['OK']:
        retVal['MinorStatus'] += '; %s' % resultInsert['Message']

      return retVal

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
    jobIDString = ','.join( [str( j ) for j in jobIDList] )
    for table in ['InputData',
                  'JobParameters',
                  'AtticJobParameters',
                  'HeartBeatLoggingInfo',
                  'OptimizerParameters',
                  'JobCommands',
                  'Jobs',
                  'JobJDLs']:

      cmd = 'DELETE FROM %s WHERE JobID in (%s)' % ( table, jobIDString )
      result = self._update( cmd )
      if not result['OK']:
        failedTablesList.append( table )

    result = S_OK()
    if failedTablesList:
      result = S_ERROR( 'Errors while job removal' )
      result['FailedTables'] = failedTablesList

    return result

#################################################################
  def rescheduleJobs( self, jobIDs ):
    """ Reschedule all the jobs in the given list
    """

    result = S_OK()

    failedJobs = []
    for jobID in jobIDs:
      result = self.rescheduleJob( jobID )
      if not result['OK']:
        failedJobs.append( jobID )

    if failedJobs:
      result = S_ERROR( 'JobDB.rescheduleJobs: Not all the jobs were rescheduled' )
      result['FailedJobs'] = failedJobs

    return result

#############################################################################
  def rescheduleJob ( self, jobID ):
    """ Reschedule the given job to run again from scratch. Retain the already
        defined parameters in the parameter Attic
    """
    # Check Verified Flag
    result = self.getJobAttributes( jobID, ['Status', 'MinorStatus', 'VerifiedFlag', 'RescheduleCounter',
                                     'Owner', 'OwnerDN', 'OwnerGroup', 'DIRACSetup'] )
    if result['OK']:
      resultDict = result['Value']
    else:
      return S_ERROR( 'JobDB.getJobAttributes: can not retrieve job attributes' )

    if not 'VerifiedFlag' in resultDict:
      return S_ERROR( 'Job ' + str( jobID ) + ' not found in the system' )

    if not resultDict['VerifiedFlag']:
      return S_ERROR( 'Job %s not Verified: Status = %s, MinorStatus = %s' % ( 
                                                                             jobID,
                                                                             resultDict['Status'],
                                                                             resultDict['MinorStatus'] ) )


    # Check the Reschedule counter first
    rescheduleCounter = int( resultDict['RescheduleCounter'] ) + 1

    self.maxRescheduling = self.getCSOption( 'MaxRescheduling', self.maxRescheduling )

    # Exit if the limit of the reschedulings is reached
    if rescheduleCounter > self.maxRescheduling:
      self.log.warn( 'Maximum number of reschedulings is reached', 'Job %s' % jobID )
      self.setJobStatus( jobID, status = 'Failed', minor = 'Maximum of reschedulings reached' )
      return S_ERROR( 'Maximum number of reschedulings is reached: %s' % self.maxRescheduling )

    jobAttrNames = []
    jobAttrValues = []

    jobAttrNames.append( 'RescheduleCounter' )
    jobAttrValues.append( rescheduleCounter )

    # Save the job parameters for later debugging
    result = self.getJobParameters( jobID )
    if result['OK']:
      parDict = result['Value']
      for key, value in parDict.items():
        result = self.setAtticJobParameter( jobID, key, value, rescheduleCounter - 1 )
        if not result['OK']:
          break

    ret = self._escapeString( jobID )
    if not ret['OK']:
      return ret
    e_jobID = ret['Value']

    cmd = 'DELETE FROM JobParameters WHERE JobID=%s' % e_jobID
    res = self._update( cmd )
    if not res['OK']:
      return res

    # Delete optimizer parameters
    cmd = 'DELETE FROM OptimizerParameters WHERE JobID=%s' % ( e_jobID )
    if not self._update( cmd )['OK']:
      return S_ERROR( 'JobDB.removeJobOptParameter: operation failed.' )

    # the Jobreceiver needs to know if there is InputData ??? to decide which optimizer to call
    # proposal: - use the getInputData method
    res = self.getJobJDL( jobID, original = True )
    if not res['OK']:
      return res

    jdl = res['Value']
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find( '[' ) != 0 :
      jdl = '[' + jdl + ']'
    classAdJob = ClassAd( jdl )
    classAdReq = ClassAd( '[]' )
    retVal = S_OK( jobID )
    retVal['JobID'] = jobID

    classAdJob.insertAttributeInt( 'JobID', jobID )
    result = self.__checkAndPrepareJob( jobID, classAdJob, classAdReq, resultDict['Owner'],
                                        resultDict['OwnerDN'], resultDict['OwnerGroup'],
                                        resultDict['DIRACSetup'],
                                        jobAttrNames, jobAttrValues )

    if not result['OK']:
      return result

    priority = classAdJob.getAttributeInt( 'Priority' )
    jobAttrNames.append( 'UserPriority' )
    jobAttrValues.append( priority )

    siteList = classAdJob.getListFromExpression( 'Site' )
    if not siteList:
      site = 'ANY'
    elif len( siteList ) > 1:
      site = "Multiple"
    else:
      site = siteList[0]

    jobAttrNames.append( 'Site' )
    jobAttrValues.append( site )

    jobAttrNames.append( 'Status' )
    jobAttrValues.append( 'Received' )

    jobAttrNames.append( 'MinorStatus' )
    jobAttrValues.append( 'Job Rescheduled' )

    jobAttrNames.append( 'ApplicationStatus' )
    jobAttrValues.append( 'Unknown' )

    jobAttrNames.append( 'ApplicationNumStatus' )
    jobAttrValues.append( 0 )

    jobAttrNames.append( 'LastUpdateTime' )
    jobAttrValues.append( Time.toString() )

    jobAttrNames.append( 'RescheduleTime' )
    jobAttrValues.append( Time.toString() )

    reqJDL = classAdReq.asJDL()
    classAdJob.insertAttributeInt( 'JobRequirements', reqJDL )

    jobJDL = classAdJob.asJDL()
    
    # Replace the JobID placeholder if any
    if jobJDL.find( '%j' ) != -1:
      jobJDL = jobJDL.replace( '%j', str( jobID ) )

    result = self.setJobJDL( jobID, jobJDL )
    if not result['OK']:
      return result

    result = self.__setInitialJobParameters( classAdJob, jobID )
    if not result['OK']:
      return result

    result = self.setJobAttributes( jobID, jobAttrNames, jobAttrValues )
    if not result['OK']:
      return result

    retVal['InputData'] = classAdJob.lookupAttribute( "InputData" )
    retVal['RescheduleCounter'] = rescheduleCounter
    retVal['Status'] = 'Received'
    retVal['MinorStatus'] = 'Job Rescheduled'

    return retVal

#############################################################################
  def getSiteMask( self, siteState = 'Active' ):
    """ Get the currently active site list
    """

    ret = self._escapeString( siteState )
    if not ret['OK']:
      return ret
    siteState = ret['Value']

    if siteState == "All":
      cmd = "SELECT Site FROM SiteMask"
    else:
      cmd = "SELECT Site FROM SiteMask WHERE Status=%s" % siteState

    result = self._query( cmd )
    siteList = []
    if result['OK']:
      siteList = [ x[0] for x in result['Value']]

    return S_OK( siteList )

#############################################################################
  def getSiteMaskStatus( self ):
    """ Get the currently site mask status
    """
    cmd = "SELECT Site,Status FROM SiteMask"

    result = self._query( cmd )
    siteDict = {}
    if result['OK']:
      for site, status in result['Value']:
        siteDict[site] = status

    return S_OK( siteDict )

#############################################################################
  def setSiteMask( self, siteMaskList, authorDN = 'Unknown', comment = 'No comment' ):
    """ Set the Site Mask to the given mask in a form of a list of tuples (site,status)
    """

    for site, status in siteMaskList:
      result = self.__setSiteStatusInMask( site, status, authorDN, comment )
      if not result['OK']:
        return result

    return S_OK()

#############################################################################
  def __setSiteStatusInMask( self, site, status, authorDN = 'Unknown', comment = 'No comment' ):
    """  Set the given site status to 'status' or add a new active site
    """

    result = self._escapeString( site )
    if not result['OK']:
      return result
    site = result['Value']

    result = self._escapeString( status )
    if not result['OK']:
      return result
    status = result['Value']

    result = self._escapeString( authorDN )
    if not result['OK']:
      return result
    authorDN = result['Value']

    result = self._escapeString( comment )
    if not result['OK']:
      return result
    comment = result['Value']



    req = "SELECT Status FROM SiteMask WHERE Site=%s" % site
    result = self._query( req )
    if result['OK']:
      if len( result['Value'] ) > 0:
        current_status = result['Value'][0][0]
        if current_status == status:
          return S_OK()
        else:
          req = "UPDATE SiteMask SET Status=%s,LastUpdateTime=UTC_TIMESTAMP()," \
                 "Author=%s, Comment=%s WHERE Site=%s"
          req = req % ( status, authorDN, comment, site )
      else:
        req = "INSERT INTO SiteMask VALUES (%s,%s,UTC_TIMESTAMP(),%s,%s)" % ( site, status, authorDN, comment )
      result = self._update( req )
      if not result['OK']:
        return S_ERROR( 'Failed to update the Site Mask' )
      # update the site mask logging record
      req = "INSERT INTO SiteMaskLogging VALUES (%s,%s,UTC_TIMESTAMP(),%s,%s)" % ( site, status, authorDN, comment )
      result = self._update( req )
      if not result['OK']:
        self.log.warn( 'Failed to update site mask logging for %s' % site )
    else:
      return S_ERROR( 'Failed to get the Site Status from the Mask' )

    return S_OK()

#############################################################################
  def banSiteInMask( self, site, authorDN = 'Unknown', comment = 'No comment' ):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask( site, 'Banned', authorDN, comment )
    return result

#############################################################################
  def allowSiteInMask( self, site, authorDN = 'Unknown', comment = 'No comment' ):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask( site, 'Active', authorDN, comment )
    return result

#############################################################################
  def removeSiteFromMask( self, site = None ):
    """ Remove the given site from the mask
    """
    if not site:
      req = "DELETE FROM SiteMask"
    else:
      ret = self._escapeString( site )
      if not ret['OK']:
        return ret
      site = ret['Value']
      req = "DELETE FROM SiteMask WHERE Site=%s" % site

    return self._update( req )

#############################################################################
  def getSiteMaskLogging( self, siteList ):
    """ Get the site mask logging history for the list if site names
    """

    if siteList:
      siteString = ','.join( [ "'" + x + "'" for x in siteList ] )
      req = "SELECT Site,Status,UpdateTime,Author,Comment FROM SiteMaskLogging WHERE Site in (%s)" % siteString
    else:
      req = "SELECT Site,Status,UpdateTime,Author,Comment FROM SiteMaskLogging"
    req += " ORDER BY UpdateTime ASC"

    result = self._query( req )
    if not result['OK']:
      return result

    availableSiteList = []
    for row in result['Value']:
      site, status, utime, author, comment = row
      availableSiteList.append( site )

    resultDict = {}
    for site in siteList:
      if not result['Value'] or site not in availableSiteList:
        ret = self._escapeString( site )
        if not ret['OK']:
          continue
        e_site = ret['Value']
        req = "SELECT Status Site,Status,LastUpdateTime,Author,Comment FROM SiteMask WHERE Site=%s" % e_site
        resSite = self._query( req )
        if resSite['OK']:
          if resSite['Value']:
            site, status, lastUpdate, author, comment = resSite['Value'][0]
            resultDict[site] = [( status, str( lastUpdate ), author, comment )]
          else:
            resultDict[site] = [( 'Unknown', '', '', 'Site not present in logging table' )]


    for row in result['Value']:
      site, status, utime, author, comment = row
      if not resultDict.has_key( site ):
        resultDict[site] = []
      resultDict[site].append( ( status, str( utime ), author, comment ) )

    return S_OK( resultDict )

#############################################################################
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
    #FIXME: hack!!!
    siteT1List = ['CERN', 'IN2P3', 'NIKHEF', 'SARA', 'PIC', 'CNAF', 'RAL', 'GRIDKA', 'RRCKI']

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
    siteMask = {}
    resultMask = self.getSiteMask( 'All' )
    if resultMask['OK']:
      for site in resultMask['Value']:
        siteMask[site] = 'NoMask'
    resultMask = self.getSiteMask( 'Active' )
    if resultMask['OK']:
      for site in resultMask['Value']:
        siteMask[site] = 'Active'
    resultMask = self.getSiteMask( 'Banned' )
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
                  'Site', 'Owner', 'OwnerGroup', 'JobGroup', 'JobSplitType' ]
    defFields = [ 'DIRACSetup' ] + requestedFields
    valueFields = [ 'COUNT(JobID)', 'SUM(RescheduleCounter)' ]
    defString = ", ".join( defFields )
    valueString = ", ".join( valueFields )
    sqlCmd = "SELECT %s, %s From Jobs GROUP BY %s" % ( defString, valueString, defString )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( ( ( defFields + valueFields ), result[ 'Value' ] ) )
