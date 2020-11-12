""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from six.moves import range

__RCSID__ = "$Id$"

import time

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ElasticJobParametersDB import ElasticJobParametersDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus


class JobStateUpdateHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, svcInfoDict):
    """
    Determines the switching of ElasticSearch and MySQL backends
    """
    cls.gJobDB = JobDB()
    cls.gJobLoggingDB = JobLoggingDB()

    cls.gElasticJobParametersDB = None
    useESForJobParametersFlag = Operations().getValue(
	'/Services/JobMonitoring/useESForJobParametersFlag', False)
    if useESForJobParametersFlag:
      cls.gElasticJobParametersDB = ElasticJobParametersDB()
    return S_OK()

  ###########################################################################
  types_updateJobFromStager = [[six.string_types, int], six.string_types]

  @classmethod
  def export_updateJobFromStager(cls, jobID, status):
    """ Simple call back method to be used by the stager. """
    if status == 'Done':
      jobStatus = 'Checking'
      minorStatus = 'JobScheduling'
    elif status == 'Failed':
      jobStatus = 'Failed'
      minorStatus = 'Staging input files failed'
    else:
      return S_ERROR("updateJobFromStager: %s status not known." % status)

    infoStr = None
    trials = 10
    for i in range(trials):
      result = cls.gJobDB.getJobAttributes(jobID, ['Status'])
      if not result['OK']:
        return result
      if not result['Value']:
        # if there is no matching Job it returns an empty dictionary
        return S_OK('No Matching Job')
      status = result['Value']['Status']
      if status == 'Staging':
        if i:
          infoStr = "Found job in Staging after %d seconds" % i
        break
      time.sleep(1)
    if status != 'Staging':
      return S_OK('Job is not in Staging after %d seconds' % trials)

    result = cls.__setJobStatus(
	int(jobID), jobStatus, minorStatus, 'StagerSystem', None)
    if not result['OK']:
      if result['Message'].find('does not exist') != -1:
        return S_OK()
    if infoStr:
      return S_OK(infoStr)
    return result

  ###########################################################################
  types_setJobStatus = [[six.string_types, int]]

  @classmethod
  def export_setJobStatus(cls, jobID, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    return cls.__setJobStatus(int(jobID), status, minorStatus, source, datetime)

  ###########################################################################
  types_setJobsStatus = [list]

  @classmethod
  def export_setJobsStatus(cls, jobIDs, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    for jobID in jobIDs:
      cls.__setJobStatus(int(jobID), status, minorStatus, source, datetime)
    return S_OK()

  @classmethod
  def __setJobStatus(cls, jobID, status, minorStatus, source, datetime):
    """ update the job status. """
    result = cls.gJobDB.setJobStatus(jobID, status, minorStatus)
    if not result['OK']:
      return result

    if status in JobStatus.JOB_FINAL_STATES:
      result = cls.gJobDB.setEndExecTime(jobID)

    if status == 'Running' and minorStatus == 'Application':
      result = cls.gJobDB.setStartExecTime(jobID)

    result = cls.gJobDB.getJobAttributes(jobID, ['Status', 'MinorStatus'])
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Job %d does not exist' % int(jobID))

    status = result['Value']['Status']
    minorStatus = result['Value']['MinorStatus']
    if datetime:
      result = cls.gJobLoggingDB.addLoggingRecord(
	  jobID, status, minorStatus, datetime, source)
    else:
      result = cls.gJobLoggingDB.addLoggingRecord(
	  jobID, status, minorStatus, source=source)
    return result

  ###########################################################################
  types_setJobStatusBulk = [[six.string_types, int], dict]

  @classmethod
  def export_setJobStatusBulk(cls, jobID, statusDict):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """

    status = ''
    minor = ''
    application = ''
    appCounter = ''
    jobID = int(jobID)

    result = cls.gJobDB.getJobAttributes(
	jobID, ['Status', 'StartExecTime', 'EndExecTime'])
    if not result['OK']:
      return result

    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_ERROR('No Matching Job')
    # If the current status is Stalled and we get an update, it should probably be "Running"
    if result['Value']['Status'] == JobStatus.STALLED:
      status = JobStatus.RUNNING
    startTime = result['Value'].get('StartExecTime', '')
    endTime = result['Value'].get('EndExecTime', '')

    # Get the latest WN time stamps of status updates
    result = cls.gJobLoggingDB.getWMSTimeStamps(int(jobID))
    if not result['OK']:
      return result
    lastTime = max([float(t) for s, t in result['Value'].items() if s != 'LastTime'])
    lastTime = Time.toString(Time.fromEpoch(lastTime))

    dates = sorted(statusDict)
    # Pick up start and end times from all updates, if they don't exist
    for date in dates:
      sDict = statusDict[date]
      status = sDict.get('Status', status)
      if status in JobStatus.JOB_FINAL_STATES and not endTime:
        endTime = date
      minor = sDict.get('MinorStatus', minor)
      # Pick up the start date
      if minor == "Application" and status == JobStatus.RUNNING and not startTime:
        startTime = date

    # We should only update the status if its time stamp is more recent than the last update
    if dates[-1] >= lastTime:
      # Get the last status values
      for date in [date for date in dates if date >= lastTime]:
        sDict = statusDict[date]
        status = sDict.get('Status', status)
        minor = sDict.get('MinorStatus', minor)
        application = sDict.get('ApplicationStatus', application)
        appCounter = sDict.get('ApplicationCounter', appCounter)

      attrNames = []
      attrValues = []
      if status:
        attrNames.append('Status')
        attrValues.append(status)
      if minor:
        attrNames.append('MinorStatus')
        attrValues.append(minor)
      if application:
        attrNames.append('ApplicationStatus')
        attrValues.append(application)
      if appCounter:
        attrNames.append('ApplicationCounter')
        attrValues.append(appCounter)
      result = cls.gJobDB.setJobAttributes(jobID, attrNames, attrValues, update=True)
      if not result['OK']:
        return result

    # Update start and end time if needed
    if endTime:
      result = cls.gJobDB.setEndExecTime(jobID, endTime)
    if startTime:
      result = cls.gJobDB.setStartExecTime(jobID, startTime)

    # Update the JobLoggingDB records
    for date in dates:
      sDict = statusDict[date]
      status = sDict['Status'] if sDict['Status'] else 'idem'
      minor = sDict['MinorStatus'] if sDict['MinorStatus'] else 'idem'
      application = sDict['ApplicationStatus'] if sDict['ApplicationStatus'] else 'idem'
      source = sDict['Source']
      result = cls.gJobLoggingDB.addLoggingRecord(
	  jobID, status, minor, application, date, source)
      if not result['OK']:
        return result

    return S_OK()

  ###########################################################################
  types_setJobSite = [[six.string_types, int], six.string_types]

  @classmethod
  def export_setJobSite(cls, jobID, site):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    return cls.gJobDB.setJobAttribute(int(jobID), 'Site', site)

  ###########################################################################
  types_setJobFlag = [[six.string_types, int], six.string_types]

  @classmethod
  def export_setJobFlag(cls, jobID, flag):
    """ Set job flag for job with jobID
    """
    return cls.gJobDB.setJobAttribute(int(jobID), flag, 'True')

  ###########################################################################
  types_unsetJobFlag = [[six.string_types, int], six.string_types]

  @classmethod
  def export_unsetJobFlag(cls, jobID, flag):
    """ Unset job flag for job with jobID
    """
    return cls.gJobDB.setJobAttribute(int(jobID), flag, 'False')

  ###########################################################################
  types_setJobApplicationStatus = [[six.string_types, int], six.string_types, six.string_types]

  @classmethod
  def export_setJobApplicationStatus(cls, jobID, appStatus, source='Unknown'):
    """ Set the application status for job specified by its JobId.
    """

    result = cls.gJobDB.getJobAttributes(int(jobID), ['Status', 'MinorStatus'])
    if not result['OK']:
      return result

    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_ERROR('No Matching Job')

    status = result['Value']['Status']
    if status == "Stalled" or status == "Matched":
      newStatus = 'Running'
    else:
      newStatus = status
    minorStatus = result['Value']['MinorStatus']

    result = cls.gJobDB.setJobStatus(
	int(jobID), status=newStatus, minor=minorStatus, application=appStatus)
    if not result['OK']:
      return result

    result = cls.gJobLoggingDB.addLoggingRecord(
	int(jobID), newStatus, minorStatus, appStatus, source=source)
    return result

  ###########################################################################
  types_setJobParameter = [[six.string_types, int], six.string_types, six.string_types]

  @classmethod
  def export_setJobParameter(cls, jobID, name, value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    if cls.gElasticJobParametersDB:
      return cls.gElasticJobParametersDB.setJobParameter(int(jobID), name, value)  # pylint: disable=no-member

    return cls.gJobDB.setJobParameter(int(jobID), name, value)

  ###########################################################################
  types_setJobsParameter = [dict]

  @classmethod
  @ignoreEncodeWarning
  def export_setJobsParameter(cls, jobsParameterDict):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    failed = False

    for jobID in jobsParameterDict:

      if cls.gElasticJobParametersDB:
	res = cls.gElasticJobParametersDB.setJobParameter(
	    jobID,
	    str(jobsParameterDict[jobID][0]),
	    str(jobsParameterDict[jobID][1]))
        if not res['OK']:
	  gLogger.error('Failed to add Job Parameter to cls.gElasticJobParametersDB', res['Message'])
          failed = True
          message = res['Message']

      else:
	res = cls.gJobDB.setJobParameter(jobID,
					 str(jobsParameterDict[jobID][0]),
					 str(jobsParameterDict[jobID][1]))
        if not res['OK']:
	  gLogger.error('Failed to add Job Parameter to MySQL', res['Message'])
          failed = True
          message = res['Message']

    if failed:
      return S_ERROR(message)
    return S_OK()

  ###########################################################################
  types_setJobParameters = [[six.string_types, int], list]

  @classmethod
  @ignoreEncodeWarning
  def export_setJobParameters(cls, jobID, parameters):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """
    failed = False

    if cls.gElasticJobParametersDB:
      for key, value in parameters:  # FIXME: should use a bulk method
	res = cls.gElasticJobParametersDB.setJobParameter(jobID, key, value)
        if not res['OK']:
	  gLogger.error('Failed to add Job Parameters to cls.gElasticJobParametersDB', res['Message'])
          failed = True
          message = res['Message']

    else:
      result = cls.gJobDB.setJobParameters(int(jobID), parameters)
      if not result['OK']:
	gLogger.error('Failed to add Job Parameters to MySQL', res['Message'])
        failed = True
        message = res['Message']

    if failed:
      return S_ERROR(message)
    return S_OK()

  ###########################################################################
  types_sendHeartBeat = [[six.string_types, int], dict, dict]

  @classmethod
  def export_sendHeartBeat(cls, jobID, dynamicData, staticData):
    """ Send a heart beat sign of life for a job jobID
    """

    result = cls.gJobDB.setHeartBeatData(int(jobID), staticData, dynamicData)
    if not result['OK']:
      gLogger.warn('Failed to set the heart beat data', 'for job %d ' % int(jobID))

    # Restore the Running status if necessary
    result = cls.gJobDB.getJobAttributes(jobID, ['Status'])
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('Job %d not found' % jobID)

    status = result['Value']['Status']
    if status == "Stalled" or status == "Matched":
      result = cls.gJobDB.setJobAttribute(jobID, 'Status', 'Running', True)
      if not result['OK']:
	gLogger.warn('Failed to restore the job status to Running')

    jobMessageDict = {}
    result = cls.gJobDB.getJobCommand(int(jobID))
    if result['OK']:
      jobMessageDict = result['Value']

    if jobMessageDict:
      for key, _value in jobMessageDict.items():
	result = cls.gJobDB.setJobCommandStatus(int(jobID), key, 'Sent')

    return S_OK(jobMessageDict)
