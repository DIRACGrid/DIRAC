########################################################################
# $Id$
########################################################################

""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

# This is a global instance of the JobDB class
jobDB = False
logDB = False

JOB_FINAL_STATES = ['Done', 'Completed', 'Failed']

def initializeJobStateUpdateHandler( serviceInfo ):

  global jobDB
  global logDB
  jobDB = JobDB()
  logDB = JobLoggingDB()
  return S_OK()

class JobStateUpdateHandler( RequestHandler ):

  ###########################################################################
  types_updateJobFromStager = [[StringType, IntType, LongType], StringType]
  def export_updateJobFromStager( self, jobID, status ):
    """ Simple call back method to be used by the stager. """
    if status == 'Done':
      jobStatus = 'Checking'
      minorStatus = 'JobScheduling'
    elif status == 'Failed':
      jobStatus = 'Failed'
      minorStatus = 'Staging input files failed'
    else:
      return S_ERROR( "updateJobFromStager: %s status not known." % status )

    result = jobDB.getJobAttributes( jobID, ['Status'] )
    if not result['OK']:
      return result
    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_OK( 'No Matching Job' )
    status = result['Value']['Status']
    if status != 'Staging':
      return S_OK( 'Job is not in Staging' )

    result = self.__setJobStatus( int( jobID ), jobStatus, minorStatus, 'StagerSystem', None )
    if not result['OK']:
      if result['Message'].find( 'does not exist' ) != -1:
        return S_OK()
    return result

  ###########################################################################
  types_setJobStatus = [IntType, StringType, StringType, StringType]
  def export_setJobStatus( self, jobID, status, minorStatus, source = 'Unknown', datetime = None ):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    return self.__setJobStatus( jobID, status, minorStatus, source, datetime )

  ###########################################################################
  types_setJobsStatus = [ListType, StringType, StringType, StringType]
  def export_setJobsStatus( self, jobIDs, status, minorStatus, source = 'Unknown', datetime = None ):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    for jobID in jobIDs:
      self.__setJobStatus( jobID, status, minorStatus, source, datetime )
    return S_OK()

  def __setJobStatus( self, jobID, status, minorStatus, source, datetime ):
    """ update the job status. """
    result = jobDB.setJobStatus( jobID, status, minorStatus )
    if not result['OK']:
      return result

    if status in JOB_FINAL_STATES:
      result = jobDB.setEndExecTime( jobID )

    if status == 'Running' and minorStatus == 'Application':
      result = jobDB.setStartExecTime( jobID )

    result = jobDB.getJobAttributes( jobID, ['Status', 'MinorStatus'] )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Job %d does not exist' % int( jobID ) )

    status = result['Value']['Status']
    minorStatus = result['Value']['MinorStatus']
    if datetime:
      result = logDB.addLoggingRecord( jobID, status, minorStatus, datetime, source )
    else:
      result = logDB.addLoggingRecord( jobID, status, minorStatus, source = source )
    return result

  ###########################################################################
  types_setJobStatusBulk = [IntType, DictType]
  def export_setJobStatusBulk( self, jobID, statusDict ):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """

    dates = statusDict.keys()
    dates.sort()
    status = ""
    minor = ""
    application = ""
    appCounter = ""
    endDate = ''
    startDate = ''
    startFlag = ''

    result = jobDB.getJobAttributes( jobID, ['Status'] )
    if not result['OK']:
      return result

    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_ERROR( 'No Matching Job' )

    new_status = result['Value']['Status']
    if new_status == "Stalled":
      status = 'Running'

    # Get the last status values
    for date in dates:
      if statusDict[date]['Status']:
        status = statusDict[date]['Status']
        if status in JOB_FINAL_STATES:
          endDate = date
        if status == "Running":
          startFlag = 'Running'
      if statusDict[date]['MinorStatus']:
        minor = statusDict[date]['MinorStatus']
        if minor == "Application" and startFlag == 'Running':
          startDate = date
      if statusDict[date]['ApplicationStatus']:
        application = statusDict[date]['ApplicationStatus']
      if 'ApplicationCounter' in statusDict[date] and statusDict[date]['ApplicationCounter']:
        appCounter = statusDict[date]['ApplicationCounter']
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
      attrNames.append( 'ApplicationCounter' )
      attrValues.append( appCounter )
    result = jobDB.setJobAttributes( jobID, attrNames, attrValues, update = True )
    if not result['OK']:
      return result

    if endDate:
      result = jobDB.setEndExecTime( jobID, endDate )
    if startDate:
      result = jobDB.setStartExecTime( jobID, startDate )

    # Update the JobLoggingDB records
    for date, sDict in statusDict.items():

      status = sDict['Status']
      if not status:
        status = 'idem'
      minor = sDict['MinorStatus']
      if not minor:
        minor = 'idem'
      application = sDict['ApplicationStatus']
      if not application:
        application = 'idem'
      else:
        status = "Running"
        minor = "Application"
      source = sDict['Source']
      result = logDB.addLoggingRecord( jobID, status, minor, application, date, source )
      if not result['OK']:
        return result

    return S_OK()

  ###########################################################################
  types_setJobSite = [IntType, StringType]
  def export_setJobSite( self, jobID, site ):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    result = jobDB.setJobAttribute( jobID, 'Site', site )
    return result

  ###########################################################################
  types_setJobFlag = [IntType, StringType]
  def export_setJobFlag( self, jobID, flag ):
    """ Set job flag for job with jobID
    """
    result = jobDB.setJobAttribute( jobID, flag, 'True' )
    return result

  ###########################################################################
  types_unsetJobFlag = [IntType, StringType]
  def export_unsetJobFlag( self, jobID, flag ):
    """ Unset job flag for job with jobID
    """
    result = jobDB.setJobAttribute( jobID, flag, 'False' )
    return result

  ###########################################################################
  types_setJobApplicationStatus = [IntType, StringType, StringType]
  def export_setJobApplicationStatus( self, jobID, appStatus, source = 'Unknown' ):
    """ Set the application status for job specified by its JobId.
    """

    result = jobDB.getJobAttributes( jobID, ['Status', 'MinorStatus'] )
    if not result['OK']:
      return result

    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_ERROR( 'No Matching Job' )

    status = result['Value']['Status']
    if status == "Stalled" or status == "Matched":
      new_status = 'Running'
    else:
      new_status = status
    minorStatus = result['Value']['MinorStatus']

    result = jobDB.setJobStatus( jobID, new_status, application = appStatus )
    if not result['OK']:
      return result

    result = logDB.addLoggingRecord( jobID, new_status, minorStatus, appStatus, source = source )
    return result

  ###########################################################################
  types_setJobParameter = [IntType, StringType, StringType]
  def export_setJobParameter( self, jobID, name, value ):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    result = jobDB.setJobParameter( jobID, name, value )
    return result

  ###########################################################################
  types_setJobsParameter = [DictType]
  def export_setJobsParameter( self, jobsParameterDict ):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    for jobID in jobsParameterDict:
      jobDB.setJobParameter( jobID, str( jobsParameterDict[jobID][0] ), str( jobsParameterDict[jobID][1] ) )
    return S_OK()

  ###########################################################################
  types_setJobParameters = [IntType, ListType]
  def export_setJobParameters( self, jobID, parameters ):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """

    result = jobDB.setJobParameters( jobID, parameters )
    if not result['OK']:
      return S_ERROR( 'Failed to store some of the parameters' )

    return S_OK( 'All parameters stored for job' )

  ###########################################################################
  types_sendHeartBeat = [IntType, DictType, DictType]
  def export_sendHeartBeat( self, jobID, dynamicData, staticData ):
    """ Send a heart beat sign of life for a job jobID
    """

    result = jobDB.setHeartBeatData( jobID, staticData, dynamicData )
    if not result['OK']:
      gLogger.warn( 'Failed to set the heart beat data for job %d ' % jobID )

    # Restore the Running status if necessary
    #result = jobDB.getJobAttributes(jobID,['Status'])
    #if not result['OK']:
    #  return result

    #if not result['Value']:
    #  return S_ERROR('Job %d not found' % jobID)

    #status = result['Value']['Status']
    #if status == "Stalled" or status == "Matched":
    #  result = jobDB.setJobAttribute(jobID,'Status','Running',True)
    #  if not result['OK']:
    #    gLogger.warn('Failed to restore the job status to Running')

    jobMessageDict = {}
    result = jobDB.getJobCommand( jobID )
    if result['OK']:
      jobMessageDict = result['Value']

    if jobMessageDict:
      for key, value in jobMessageDict.items():
        result = jobDB.setJobCommandStatus( jobID, key, 'Sent' )

    return S_OK( jobMessageDict )

