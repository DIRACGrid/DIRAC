""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

from __future__ import absolute_import
import six
from six.moves import range

__RCSID__ = "$Id$"

import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ElasticJobDB import ElasticJobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus

# This is a global instance of the JobDB class
jobDB = False
logDB = False
elasticJobDB = False


def initializeJobStateUpdateHandler(serviceInfo):

  global jobDB
  global logDB
  jobDB = JobDB()
  logDB = JobLoggingDB()
  return S_OK()


class JobStateUpdateHandler(RequestHandler):

  def initialize(self):
    """
    Flags gESFlag and gMySQLFlag have bool values (True/False)
    derived from dirac.cfg configuration file

    Determines the switching of ElasticSearch and MySQL backends
    """
    global elasticJobDB

    useESForJobParametersFlag = Operations().getValue('/Services/JobMonitoring/useESForJobParametersFlag', False)
    if useESForJobParametersFlag:
      elasticJobDB = ElasticJobDB()
      self.log.verbose("Using ElasticSearch for JobParameters")

    return S_OK()

  ###########################################################################
  types_updateJobFromStager = [[six.string_types, int], six.string_types]

  def export_updateJobFromStager(self, jobID, status):
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
      result = jobDB.getJobAttributes(jobID, ['Status'])
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

    result = self.__setJobStatus(int(jobID), jobStatus, minorStatus, 'StagerSystem', None)
    if not result['OK']:
      if result['Message'].find('does not exist') != -1:
        return S_OK()
    if infoStr:
      return S_OK(infoStr)
    return result

  ###########################################################################
  types_setJobStatus = [[six.string_types, int]]

  def export_setJobStatus(self, jobID, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    return self.__setJobStatus(int(jobID), status, minorStatus, source, datetime)

  ###########################################################################
  types_setJobsStatus = [list]

  def export_setJobsStatus(self, jobIDs, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    for jobID in jobIDs:
      self.__setJobStatus(int(jobID), status, minorStatus, source, datetime)
    return S_OK()

  def __setJobStatus(self, jobID, status, minorStatus, source, datetime):
    """ update the job status. """
    result = jobDB.setJobStatus(jobID, status, minorStatus)
    if not result['OK']:
      return result

    if status in JobStatus.JOB_FINAL_STATES:
      result = jobDB.setEndExecTime(jobID)

    if status == 'Running' and minorStatus == 'Application':
      result = jobDB.setStartExecTime(jobID)

    result = jobDB.getJobAttributes(jobID, ['Status', 'MinorStatus'])
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Job %d does not exist' % int(jobID))

    status = result['Value']['Status']
    minorStatus = result['Value']['MinorStatus']
    if datetime:
      result = logDB.addLoggingRecord(jobID, status, minorStatus, datetime, source)
    else:
      result = logDB.addLoggingRecord(jobID, status, minorStatus, source=source)
    return result

  ###########################################################################
  types_setJobStatusBulk = [[six.string_types, int], dict]

  def export_setJobStatusBulk(self, jobID, statusDict):
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

    result = jobDB.getJobAttributes(jobID, ['Status', 'StartExecTime', 'EndExecTime'])
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
    result = logDB.getWMSTimeStamps(int(jobID))
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
      result = jobDB.setJobAttributes(jobID, attrNames, attrValues, update=True)
      if not result['OK']:
        return result

    # Update start and end time if needed
    if endTime:
      result = jobDB.setEndExecTime(jobID, endTime)
    if startTime:
      result = jobDB.setStartExecTime(jobID, startTime)

    # Update the JobLoggingDB records
    for date in dates:
      sDict = statusDict[date]
      status = sDict['Status'] if sDict['Status'] else 'idem'
      minor = sDict['MinorStatus'] if sDict['MinorStatus'] else 'idem'
      application = sDict['ApplicationStatus'] if sDict['ApplicationStatus'] else 'idem'
      source = sDict['Source']
      result = logDB.addLoggingRecord(jobID, status, minor, application, date, source)
      if not result['OK']:
        return result

    return S_OK()

  ###########################################################################
  types_setJobSite = [[six.string_types, int], six.string_types]

  def export_setJobSite(self, jobID, site):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    result = jobDB.setJobAttribute(int(jobID), 'Site', site)
    return result

  ###########################################################################
  types_setJobFlag = [[six.string_types, int], six.string_types]

  def export_setJobFlag(self, jobID, flag):
    """ Set job flag for job with jobID
    """
    result = jobDB.setJobAttribute(int(jobID), flag, 'True')
    return result

  ###########################################################################
  types_unsetJobFlag = [[six.string_types, int], six.string_types]

  def export_unsetJobFlag(self, jobID, flag):
    """ Unset job flag for job with jobID
    """
    result = jobDB.setJobAttribute(int(jobID), flag, 'False')
    return result

  ###########################################################################
  types_setJobApplicationStatus = [[six.string_types, int], six.string_types, six.string_types]

  def export_setJobApplicationStatus(self, jobID, appStatus, source='Unknown'):
    """ Set the application status for job specified by its JobId.
    """

    result = jobDB.getJobAttributes(int(jobID), ['Status', 'MinorStatus'])
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

    result = jobDB.setJobStatus(int(jobID), status=newStatus, minor=minorStatus, application=appStatus)
    if not result['OK']:
      return result

    result = logDB.addLoggingRecord(int(jobID), newStatus, minorStatus, appStatus, source=source)
    return result

  ###########################################################################
  types_setJobParameter = [[six.string_types, int], six.string_types, six.string_types]

  def export_setJobParameter(self, jobID, name, value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    if elasticJobDB:
      return elasticJobDB.setJobParameter(int(jobID), name, value)

    return jobDB.setJobParameter(int(jobID), name, value)

  ###########################################################################
  types_setJobsParameter = [dict]

  @ignoreEncodeWarning
  def export_setJobsParameter(self, jobsParameterDict):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    for jobID in jobsParameterDict:

      if elasticJobDB:
        res = elasticJobDB.setJobParameter(jobID,
                                           str(jobsParameterDict[jobID][0]),
                                           str(jobsParameterDict[jobID][1]))
        if not res['OK']:
          self.log.error('Failed to add Job Parameter to elasticJobDB', res['Message'])

      else:
        res = jobDB.setJobParameter(jobID,
                                    str(jobsParameterDict[jobID][0]),
                                    str(jobsParameterDict[jobID][1]))
        if not res['OK']:
          self.log.error('Failed to add Job Parameter to MySQL', res['Message'])

    return S_OK()

  ###########################################################################
  types_setJobParameters = [[six.string_types, int], list]

  @classmethod
  @ignoreEncodeWarning
  def export_setJobParameters(cls, jobID, parameters):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """

    result = jobDB.setJobParameters(int(jobID), parameters)
    if not result['OK']:
      return S_ERROR('Failed to store some of the parameters')

    return S_OK('All parameters stored for job')

  ###########################################################################
  types_sendHeartBeat = [[six.string_types, int], dict, dict]

  def export_sendHeartBeat(self, jobID, dynamicData, staticData):
    """ Send a heart beat sign of life for a job jobID
    """

    result = jobDB.setHeartBeatData(int(jobID), staticData, dynamicData)
    if not result['OK']:
      self.log.warn('Failed to set the heart beat data', 'for job %d ' % int(jobID))

    # Restore the Running status if necessary
    result = jobDB.getJobAttributes(jobID, ['Status'])
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('Job %d not found' % jobID)

    status = result['Value']['Status']
    if status == "Stalled" or status == "Matched":
      result = jobDB.setJobAttribute(jobID, 'Status', 'Running', True)
      if not result['OK']:
        self.log.warn('Failed to restore the job status to Running')

    jobMessageDict = {}
    result = jobDB.getJobCommand(int(jobID))
    if result['OK']:
      jobMessageDict = result['Value']

    if jobMessageDict:
      for key, _value in jobMessageDict.items():
        result = jobDB.setJobCommandStatus(int(jobID), key, 'Sent')

    return S_OK(jobMessageDict)
