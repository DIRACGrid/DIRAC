""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import six
from six.moves import range

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.Core.Utilities.Decorators import deprecated


class JobStateUpdateHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, svcInfoDict):
    """
    Determines the switching of ElasticSearch and MySQL backends
    """
    try:
      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
      if not result['OK']:
        return result
      cls.jobDB = result['Value']()

      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
      if not result['OK']:
        return result
      cls.jobLoggingDB = result['Value']()

    except RuntimeError as excp:
      return S_ERROR("Can't connect to DB: %s" % excp)

    cls.elasticJobParametersDB = None
    useESForJobParametersFlag = Operations().getValue(
        '/Services/JobMonitoring/useESForJobParametersFlag', False)
    if useESForJobParametersFlag:
      try:
        result = ObjectLoader().loadObject(
            "WorkloadManagementSystem.DB.ElasticJobParametersDB", "ElasticJobParametersDB"
        )
        if not result['OK']:
          return result
        cls.elasticJobParametersDB = result['Value']()
      except RuntimeError as excp:
        return S_ERROR("Can't connect to DB: %s" % excp)
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
      result = cls.jobDB.getJobAttributes(jobID, ['Status'])
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
        int(jobID), status=jobStatus, minorStatus=minorStatus, source='StagerSystem')
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
    return cls.__setJobStatus(
        int(jobID), status=status, minorStatus=minorStatus, source=source, datetime=datetime)

  ###########################################################################
  types_setJobsStatus = [list]

  @classmethod
  @deprecated("unused")
  def export_setJobsStatus(cls, jobIDs, status='', minorStatus='', source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    for jobID in jobIDs:
      cls.__setJobStatus(
          int(jobID), status=status, minorStatus=minorStatus, source=source, datetime=datetime)
    return S_OK()

  @classmethod
  def __setJobStatus(cls, jobID, status=None, minorStatus=None, appStatus=None, source=None, datetime=None):
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
      return cls.__setJobStatusBulk(jobID, {datetime: sDict})
    return S_OK()

  ###########################################################################
  types_setJobStatusBulk = [[six.string_types, int], dict]

  @classmethod
  def export_setJobStatusBulk(cls, jobID, statusDict):
    """ Set various job status fields with a time stamp and a source
    """
    return cls.__setJobStatusBulk(jobID, statusDict)

  @classmethod
  def __setJobStatusBulk(cls, jobID, statusDict):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """
    status = ''
    minor = ''
    application = ''
    jobID = int(jobID)

    result = cls.jobDB.getJobAttributes(
        jobID, ['Status', 'StartExecTime', 'EndExecTime'])
    if not result['OK']:
      return result

    if not result['Value']:
      # if there is no matching Job it returns an empty dictionary
      return S_ERROR('No Matching Job')
    # If the current status is Stalled and we get an update, it should probably be "Running"
    currentStatus = result['Value']['Status']
    if currentStatus == JobStatus.STALLED:
      status = JobStatus.RUNNING
    startTime = result['Value'].get('StartExecTime')
    endTime = result['Value'].get('EndExecTime')
    # getJobAttributes only returns strings :(
    if startTime == 'None':
      startTime = None
    if endTime == 'None':
      endTime = None

    # Get the latest WN time stamps of status updates
    result = cls.jobLoggingDB.getWMSTimeStamps(int(jobID))
    if not result['OK']:
      return result
    lastTime = max([float(t) for s, t in result['Value'].items() if s != 'LastTime'])
    lastTime = Time.toString(Time.fromEpoch(lastTime))

    dates = sorted(statusDict)
    # If real updates, start from the current status
    if dates[0] >= lastTime and not status:
      status = currentStatus
    log = gLogger.getSubLogger('JobStatusBulk/Job-%s' % jobID)
    log.debug("*** New call ***", "Last update time %s - Sorted new times %s" % (lastTime, dates))
    # Remove useless items in order to make it simpler later, although there should not be any
    for sDict in statusDict.values():
      for item in sorted(sDict):
        if not sDict[item]:
          sDict.pop(item, None)
    # Pick up start and end times from all updates, if they don't exist
    newStat = status
    for date in dates:
      sDict = statusDict[date]
      # This is to recover Matched jobs that set the application status: they are running!
      if sDict.get('ApplicationStatus') and newStat == JobStatus.MATCHED:
        sDict['Status'] = JobStatus.RUNNING
      newStat = sDict.get('Status', newStat)
      if newStat == JobStatus.RUNNING and not startTime:
        # Pick up the start date when the job starts running if not existing
        startTime = date
        log.debug("Set job start time", startTime)
      elif newStat in JobStatus.JOB_FINAL_STATES and not endTime:
        # Pick up the end time when the job is in a final status
        endTime = date
        log.debug("Set job end time", endTime)

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
      result = cls.jobDB.setJobAttributes(jobID, attrNames, attrValues, update=True)
      if not result['OK']:
        return result

    # Update start and end time if needed
    if endTime:
      result = cls.jobDB.setEndExecTime(jobID, endTime)
      if not result['OK']:
        return result
    if startTime:
      result = cls.jobDB.setStartExecTime(jobID, startTime)
      if not result['OK']:
        return result

    # Update the JobLoggingDB records
    for date in dates:
      sDict = statusDict[date]
      status = sDict.get('Status', 'idem')
      minor = sDict.get('MinorStatus', 'idem')
      application = sDict.get('ApplicationStatus', 'idem')
      source = sDict.get('Source', 'Unknown')
      result = cls.jobLoggingDB.addLoggingRecord(jobID,
                                                 status=status,
                                                 minorStatus=minor,
                                                 applicationStatus=application,
                                                 date=date,
                                                 source=source)
      if not result['OK']:
        return result

    return S_OK()

  ###########################################################################
  types_setJobAttribute = [[six.string_types, int], six.string_types, six.string_types]

  @classmethod
  def export_setJobAttribute(cls, jobID, attribute, value):
    """Set a job attribute
    """
    return cls.jobDB.setJobAttribute(int(jobID), attribute, value)

  ###########################################################################
  types_setJobSite = [[six.string_types, int], six.string_types]

  @classmethod
  def export_setJobSite(cls, jobID, site):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    return cls.jobDB.setJobAttribute(int(jobID), 'Site', site)

  ###########################################################################
  types_setJobFlag = [[six.string_types, int], six.string_types]

  @classmethod
  def export_setJobFlag(cls, jobID, flag):
    """ Set job flag for job with jobID
    """
    return cls.jobDB.setJobAttribute(int(jobID), flag, 'True')

  ###########################################################################
  types_unsetJobFlag = [[six.string_types, int], six.string_types]

  @classmethod
  def export_unsetJobFlag(cls, jobID, flag):
    """ Unset job flag for job with jobID
    """
    return cls.jobDB.setJobAttribute(int(jobID), flag, 'False')

  ###########################################################################
  types_setJobApplicationStatus = [[six.string_types, int], six.string_types, six.string_types]

  @classmethod
  def export_setJobApplicationStatus(cls, jobID, appStatus, source='Unknown'):
    """ Set the application status for job specified by its JobId.
        Internally calling the bulk method
    """
    return cls.__setJobStatus(jobID, appStatus=appStatus, source=source)

  ###########################################################################
  types_setJobParameter = [[six.string_types, int], six.string_types, six.string_types]

  @classmethod
  def export_setJobParameter(cls, jobID, name, value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    if cls.elasticJobParametersDB:
      return cls.elasticJobParametersDB.setJobParameter(int(jobID), name, value)  # pylint: disable=no-member

    return cls.jobDB.setJobParameter(int(jobID), name, value)

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

      if cls.elasticJobParametersDB:
        res = cls.elasticJobParametersDB.setJobParameter(
            jobID,
            str(jobsParameterDict[jobID][0]),
            str(jobsParameterDict[jobID][1]))
        if not res['OK']:
          gLogger.error('Failed to add Job Parameter to cls.elasticJobParametersDB', res['Message'])
          failed = True
          message = res['Message']

      else:
        res = cls.jobDB.setJobParameter(jobID,
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
    if cls.elasticJobParametersDB:
      result = cls.elasticJobParametersDB.setJobParameters(jobID, parameters)
      if not result['OK']:
        gLogger.error('Failed to add Job Parameters to ElasticJobParametersDB', result['Message'])
    else:
      result = cls.jobDB.setJobParameters(int(jobID), parameters)
      if not result['OK']:
        gLogger.error('Failed to add Job Parameters to MySQL', result['Message'])

    return result

  ###########################################################################
  types_sendHeartBeat = [[six.string_types, int], dict, dict]

  @classmethod
  def export_sendHeartBeat(cls, jobID, dynamicData, staticData):
    """ Send a heart beat sign of life for a job jobID
    """

    result = cls.jobDB.setHeartBeatData(int(jobID), dynamicData)
    if not result['OK']:
      gLogger.warn('Failed to set the heart beat data', 'for job %d ' % int(jobID))

    if cls.elasticJobParametersDB:
      for key, value in staticData.items():
        result = cls.elasticJobParametersDB.setJobParameter(int(jobID), key, value)
        if not result['OK']:
          gLogger.error('Failed to add Job Parameters to ElasticSearch', result['Message'])
    else:
      result = cls.jobDB.setJobParameters(int(jobID), list(staticData.items()))
      if not result['OK']:
        gLogger.error('Failed to add Job Parameters to MySQL', result['Message'])

    # Restore the Running status if necessary
    result = cls.jobDB.getJobAttributes(jobID, ['Status'])
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('Job %d not found' % jobID)

    status = result['Value']['Status']
    if status in (JobStatus.STALLED, JobStatus.MATCHED):
      result = cls.jobDB.setJobAttribute(jobID, 'Status', JobStatus.RUNNING, True)
      if not result['OK']:
        gLogger.warn('Failed to restore the job status to Running')

    jobMessageDict = {}
    result = cls.jobDB.getJobCommand(int(jobID))
    if result['OK']:
      jobMessageDict = result['Value']

    if jobMessageDict:
      for key, _value in jobMessageDict.items():
        result = cls.jobDB.setJobCommandStatus(int(jobID), key, 'Sent')

    return S_OK(jobMessageDict)
