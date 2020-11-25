""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

from __future__ import absolute_import
import time
import six
from six.moves import range

__RCSID__ = "$Id$"


from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ElasticJobParametersDB import ElasticJobParametersDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.Core.Utilities.Decorators import deprecated

# This is a global instance of the JobDB class
jobDB = False
logDB = False
elasticJobParametersDB = False


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
    global elasticJobParametersDB

    useESForJobParametersFlag = Operations().getValue('/Services/JobMonitoring/useESForJobParametersFlag', False)
    if useESForJobParametersFlag:
      elasticJobParametersDB = ElasticJobParametersDB()
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

    result = self.__setJobStatus(int(jobID), status=jobStatus, minorStatus=minorStatus, source='StagerSystem')
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
    return self.__setJobStatus(int(jobID), status=status, minorStatus=minorStatus, source=source, datetime=datetime)

  ###########################################################################
  types_setJobsStatus = [list]

  @deprecated("unused")
  def export_setJobsStatus(self, jobIDs, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    for jobID in jobIDs:
      self.__setJobStatus(int(jobID), status=status, minorStatus=minorStatus, source=source, datetime=datetime)
    return S_OK()

  def __setJobStatus(self, jobID, status=None, minorStatus=None, appStatus=None, source=None, datetime=None):
    """ update the job provided statuses (major, minor and application)
        If sets also the source and the time stamp (or current time)
        This method calls the bulk method internally
    """
    sDict = {}
    if status:
      sDict['Status'] = status
    if minorStatus:
      sDict['MinorStatus'] = minorStatus
    if appStatus:
      sDict['ApplicationStatus'] = appStatus
    if sDict:
      if source:
        sDict['Source'] = source
      if not datetime:
        datetime = Time.toString()
      return self.__setJobStatusBulk(jobID, {datetime: sDict})
    return S_OK()

  ###########################################################################
  types_setJobStatusBulk = [[six.string_types, int], dict]

  def export_setJobStatusBulk(self, jobID, statusDict):
    """ Set various job status fields with a time stamp and a source
    """
    return self.__setJobStatusBulk(jobID, statusDict)

  def __setJobStatusBulk(self, jobID, statusDict):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """
    status = ''
    minor = ''
    application = ''
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
    log = self.log.getSubLogger('JobStatusBulk/Job-%s' % jobID)
    log.debug("*** New call ***", "Last update time %s - Sorted new times %s" % (lastTime, dates))
    # Remove useless items in order to make it simpler later, although should not be there
    for sDict in statusDict.values():
      for item in ('Status', 'MinorStatus', 'ApplicationStatus'):
        if not sDict.get(item):
          sDict.pop(item, None)
    # Pick up start and end times from all updates, if they don't exist
    for date in dates:
      sDict = statusDict[date]
      status = sDict.get('Status', '')
      if status in JobStatus.JOB_FINAL_STATES and not endTime:
        endTime = date
      minor = sDict.get('MinorStatus', '')
      # Pick up the start date
      if minor == "Application" and status == JobStatus.RUNNING and not startTime:
        startTime = date

    # We should only update the status if its time stamp is more recent than the last update
    if dates[-1] >= lastTime:
      # Get the last status values
      for date in [dt for dt in dates if dt >= lastTime]:
        sDict = statusDict[date]
        log.debug("\t", "Time %s - Statuses %s" % (date, str(sDict)))
        status = sDict.get('Status', status)
        minor = sDict.get('MinorStatus', minor)
        application = sDict.get('ApplicationStatus', application)

      log.debug("Final statuses:", "status '%s', minor '%s', application '%s'" %
                (status, minor, application))
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
      result = jobDB.setJobAttributes(jobID, attrNames, attrValues, update=True)
      if not result['OK']:
        return result

    # Update start and end time if needed
    if endTime:
      result = jobDB.setEndExecTime(jobID, endTime)
      if not result['OK']:
        return result
    if startTime:
      result = jobDB.setStartExecTime(jobID, startTime)
      if not result['OK']:
        return result

    # Update the JobLoggingDB records
    for date in dates:
      sDict = statusDict[date]
      status = sDict.get('Status', 'idem')
      minor = sDict.get('MinorStatus', 'idem')
      application = sDict.get('ApplicationStatus', 'idem')
      source = sDict.get('Source', 'Unknown')
      result = logDB.addLoggingRecord(jobID, status, minor, application, date, source)
      if not result['OK']:
        return result

    return S_OK()

  ###########################################################################
  types_setJobAttribute = [[six.string_types, int], six.string_types, six.string_types]

  def export_setJobAttribute(self, jobID, attribute, value):
    """Set a job attribute
    """
    return jobDB.setJobAttribute(int(jobID), attribute, value)

  ###########################################################################
  types_setJobSite = [[six.string_types, int], six.string_types]

  def export_setJobSite(self, jobID, site):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    return jobDB.setJobAttribute(int(jobID), 'Site', site)

  ###########################################################################
  types_setJobFlag = [[six.string_types, int], six.string_types]

  def export_setJobFlag(self, jobID, flag):
    """ Set job flag for job with jobID
    """
    return jobDB.setJobAttribute(int(jobID), flag, 'True')

  ###########################################################################
  types_unsetJobFlag = [[six.string_types, int], six.string_types]

  def export_unsetJobFlag(self, jobID, flag):
    """ Unset job flag for job with jobID
    """
    return jobDB.setJobAttribute(int(jobID), flag, 'False')

  ###########################################################################
  types_setJobApplicationStatus = [[six.string_types, int], six.string_types, six.string_types]

  def export_setJobApplicationStatus(self, jobID, appStatus, source='Unknown'):
    """ Set the application status for job specified by its JobId.
        Internally calling the bulk method
    """
    return self.__setJobStatus(jobID, appStatus=appStatus, source=source)

  ###########################################################################
  types_setJobParameter = [[six.string_types, int], six.string_types, six.string_types]

  def export_setJobParameter(self, jobID, name, value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    if elasticJobParametersDB:
      return elasticJobParametersDB.setJobParameter(int(jobID), name, value)

    return jobDB.setJobParameter(int(jobID), name, value)

  ###########################################################################
  types_setJobsParameter = [dict]

  @ignoreEncodeWarning
  def export_setJobsParameter(self, jobsParameterDict):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    failed = False

    for jobID in jobsParameterDict:

      if elasticJobParametersDB:
        res = elasticJobParametersDB.setJobParameter(jobID,
                                                     str(jobsParameterDict[jobID][0]),
                                                     str(jobsParameterDict[jobID][1]))
        if not res['OK']:
          self.log.error('Failed to add Job Parameter to elasticJobParametersDB', res['Message'])
          failed = True
          message = res['Message']

      else:
        res = jobDB.setJobParameter(jobID,
                                    str(jobsParameterDict[jobID][0]),
                                    str(jobsParameterDict[jobID][1]))
        if not res['OK']:
          self.log.error('Failed to add Job Parameter to MySQL', res['Message'])
          failed = True
          message = res['Message']

    if failed:
      return S_ERROR(message)
    return S_OK()

  ###########################################################################
  types_setJobParameters = [[six.string_types, int], list]

  @ignoreEncodeWarning
  def export_setJobParameters(self, jobID, parameters):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """
    failed = False

    if elasticJobParametersDB:
      for key, value in parameters:  # FIXME: should use a bulk method
        res = elasticJobParametersDB.setJobParameter(jobID, key, value)
        if not res['OK']:
          self.log.error('Failed to add Job Parameters to elasticJobParametersDB', res['Message'])
          failed = True
          message = res['Message']

    else:
      result = jobDB.setJobParameters(int(jobID), parameters)
      if not result['OK']:
        self.log.error('Failed to add Job Parameters to MySQL', res['Message'])
        failed = True
        message = res['Message']

    if failed:
      return S_ERROR(message)
    return S_OK()

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
