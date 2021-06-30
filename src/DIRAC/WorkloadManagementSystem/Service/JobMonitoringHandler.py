""" JobMonitoringHandler is the implementation of the JobMonitoring service
    in the DISET framework

    The following methods are available in the Service interface
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import six
from datetime import timedelta

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
import DIRAC.Core.Utilities.Time as Time
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, RIGHT_GET_INFO

SUMMARY = []


class JobMonitoringHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, svcInfoDict):
    """ initialize DBs
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

      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB")
      if not result['OK']:
        return result
      cls.taskQueueDB = result['Value']()

    except RuntimeError as excp:
      return S_ERROR("Can't connect to DB: %s" % excp)

    cls.elasticJobParametersDB = None
    useESForJobParametersFlag = Operations().getValue(
        "/Services/JobMonitoring/useESForJobParametersFlag", False)
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

  def initialize(self):
    """ initialize jobPolicy
    """

    credDict = self.getRemoteCredentials()
    ownerDN = credDict['DN']
    ownerGroup = credDict['group']
    operations = Operations(group=ownerGroup)
    self.globalJobsInfo = operations.getValue(
        '/Services/JobMonitoring/GlobalJobsInfo', True)
    self.jobPolicy = JobPolicy(ownerDN, ownerGroup, self.globalJobsInfo)
    self.jobPolicy.jobDB = self.jobDB

    return S_OK()

  @classmethod
  def getJobsAttributes(cls, *args, **kwargs):
    """ Utility function for unpacking
    """
    res = cls.jobDB.getJobsAttributes(*args, **kwargs)
    if not res['OK']:
      return res
    return S_OK(strToIntDict(res['Value']))


##############################################################################
  types_getApplicationStates = []

  @classmethod
  def export_getApplicationStates(cls, condDict=None, older=None, newer=None):
    """ Return Distinct Values of ApplicationStatus job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('ApplicationStatus', condDict, older, newer)

##############################################################################
  types_getJobTypes = []

  @classmethod
  def export_getJobTypes(cls, condDict=None, older=None, newer=None):
    """ Return Distinct Values of JobType job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('JobType', condDict, older, newer)

##############################################################################
  types_getOwners = []

  @classmethod
  def export_getOwners(cls, condDict=None, older=None, newer=None):
    """
    Return Distinct Values of Owner job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('Owner', condDict, older, newer)

##############################################################################
  types_getOwnerGroup = []

  @classmethod
  def export_getOwnerGroup(cls):
    """
    Return Distinct Values of OwnerGroup from the JobDB
    """
    return cls.jobDB.getDistinctJobAttributes('OwnerGroup')

##############################################################################
  types_getProductionIds = []

  @classmethod
  @deprecated("Unused")
  def export_getProductionIds(cls, condDict=None, older=None, newer=None):
    """
    Return Distinct Values of ProductionId job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('JobGroup', condDict, older, newer)

##############################################################################
  types_getJobGroups = []

  @classmethod
  def export_getJobGroups(cls, condDict=None, older=None, cutDate=None):
    """
    Return Distinct Values of ProductionId job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('JobGroup', condDict, older, newer=cutDate)

##############################################################################
  types_getSites = []

  @classmethod
  def export_getSites(cls, condDict=None, older=None, newer=None):
    """
    Return Distinct Values of Site job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('Site', condDict, older, newer)

##############################################################################
  types_getStates = []

  @classmethod
  def export_getStates(cls, condDict=None, older=None, newer=None):
    """
    Return Distinct Values of Status job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('Status', condDict, older, newer)

##############################################################################
  types_getMinorStates = []

  @classmethod
  def export_getMinorStates(cls, condDict=None, older=None, newer=None):
    """
    Return Distinct Values of Minor Status job Attribute in WMS
    """
    return cls.jobDB.getDistinctJobAttributes('MinorStatus', condDict, older, newer)

##############################################################################
  types_getJobs = []

  @classmethod
  def export_getJobs(cls, attrDict=None, cutDate=None):
    """
    Return list of JobIds matching the condition given in attrDict
    """
    # queryDict = {}

    # if attrDict:
    #  if type ( attrDict ) != dict:
    #    return S_ERROR( 'Argument must be of Dict Type' )
    #  for attribute in self.queryAttributes:
    #    # Only those Attribute in self.queryAttributes can be used
    #    if attrDict.has_key(attribute):
    #      queryDict[attribute] = attrDict[attribute]

    return cls.jobDB.selectJobs(attrDict, newer=cutDate)

##############################################################################
  types_getCounters = [list]

  @classmethod
  def export_getCounters(cls, attrList, attrDict=None, cutDate=''):
    """
    Retrieve list of distinct attributes values from attrList
    with attrDict as condition.
    For each set of distinct values, count number of occurences.
    Return a list. Each item is a list with 2 items, the list of distinct
    attribute values and the counter
    """

    # Check that Attributes in attrList and attrDict, they must be in
    # self.queryAttributes.

    # for attr in attrList:
    #  try:
    #    self.queryAttributes.index(attr)
    #  except:
    #    return S_ERROR( 'Requested Attribute not Allowed: %s.' % attr )
    #
    # for attr in attrDict:
    #  try:
    #    self.queryAttributes.index(attr)
    #  except:
    #    return S_ERROR( 'Condition Attribute not Allowed: %s.' % attr )

    cutDate = str(cutDate)
    if not attrDict:
      attrDict = {}

    return cls.jobDB.getCounters('Jobs', attrList, attrDict, newer=cutDate, timeStamp='LastUpdateTime')

##############################################################################
  types_getCurrentJobCounters = []

  @classmethod
  @deprecated("Unused")
  def export_getCurrentJobCounters(cls, attrDict=None):
    """ Get job counters per Status with attrDict selection. Final statuses are given for
        the last day.
    """

    if not attrDict:
      attrDict = {}
    result = cls.jobDB.getCounters('Jobs', ['Status'], attrDict, timeStamp='LastUpdateTime')
    if not result['OK']:
      return result
    last_update = Time.dateTime() - Time.day
    resultDay = cls.jobDB.getCounters('Jobs', ['Status'], attrDict, newer=last_update,
                                      timeStamp='LastUpdateTime')
    if not resultDay['OK']:
      return resultDay

    resultDict = {}
    for statusDict, count in result['Value']:
      status = statusDict['Status']
      resultDict[status] = count
      if status in JobStatus.JOB_FINAL_STATES + JobStatus.JOB_REALLY_FINAL_STATES:
        resultDict[status] = 0
        for statusDayDict, ccount in resultDay['Value']:
          if status == statusDayDict['Status']:
            resultDict[status] = ccount
          break

    return S_OK(resultDict)

##############################################################################
  types_getJobStatus = [int]

  @classmethod
  @deprecated("Use getJobsStatus")
  def export_getJobStatus(cls, jobID):

    return cls.jobDB.getJobAttribute(jobID, 'Status')

##############################################################################
  types_getJobOwner = [int]

  @classmethod
  def export_getJobOwner(cls, jobID):

    return cls.jobDB.getJobAttribute(jobID, 'Owner')

##############################################################################
  types_getJobSite = [int]

  @classmethod
  def export_getJobSite(cls, jobID):

    return cls.jobDB.getJobAttribute(jobID, 'Site')

##############################################################################
  types_getJobJDL = [int, bool]

  @classmethod
  def export_getJobJDL(cls, jobID, original):

    return cls.jobDB.getJobJDL(jobID, original=original)

##############################################################################
  types_getJobLoggingInfo = [int]

  @classmethod
  def export_getJobLoggingInfo(cls, jobID):

    return cls.jobLoggingDB.getJobLoggingInfo(jobID)

##############################################################################
  types_getJobsParameters = [[six.string_types, int, list], list]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsParameters(cls, jobIDs, parameters):
    return cls.getJobsAttributes(jobIDs, parameters)

##############################################################################
  types_getJobsStates = [[six.string_types, int, list]]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsStates(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, ['Status', 'MinorStatus', 'ApplicationStatus'])

##############################################################################
  types_getJobsStatus = [[six.string_types, int, list]]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsStatus(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, ['Status'])

##############################################################################
  types_getJobsMinorStatus = [[six.string_types, int, list]]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsMinorStatus(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, ['MinorStatus'])

##############################################################################
  types_getJobsApplicationStatus = [[six.string_types, int, list]]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsApplicationStatus(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, ['ApplicationStatus'])

##############################################################################
  types_getJobsSites = [[six.string_types, int, list]]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobsSites(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, ['Site'])

##############################################################################
  types_getJobSummary = [int]

  @classmethod
  def export_getJobSummary(cls, jobID):
    return cls.jobDB.getJobAttributes(jobID, SUMMARY)

##############################################################################
  types_getJobPrimarySummary = [int]

  @classmethod
  @deprecated("Use getJobSummary")
  def export_getJobPrimarySummary(cls, jobID):
    return cls.jobDB.getJobAttributes(jobID, [])

##############################################################################
  types_getJobsSummary = [list]

  @classmethod
  def export_getJobsSummary(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, SUMMARY)

##############################################################################
  types_getJobPageSummaryWeb = [dict, list, int, int]

  def export_getJobPageSummaryWeb(self, selectDict, sortList, startItem, maxItems, selectJobs=True):
    """ Get the summary of the job information for a given page in the
        job monitor in a generic format
    """
    resultDict = {}
    startDate = selectDict.get('FromDate', None)
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get('LastUpdate', None)
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get('ToDate', None)
    if endDate:
      del selectDict['ToDate']

    # Provide JobID bound to a specific PilotJobReference
    # There is no reason to have both PilotJobReference and JobID in selectDict
    # If that occurs, use the JobID instead of the PilotJobReference
    pilotJobRefs = selectDict.get('PilotJobReference')
    if pilotJobRefs:
      del selectDict['PilotJobReference']
      if 'JobID' not in selectDict or not selectDict['JobID']:
        if not isinstance(pilotJobRefs, list):
          pilotJobRefs = [pilotJobRefs]
        selectDict['JobID'] = []
        for pilotJobRef in pilotJobRefs:
          res = PilotManagerClient().getPilotInfo(pilotJobRef)
          if res['OK'] and 'Jobs' in res['Value'][pilotJobRef]:
            selectDict['JobID'].extend(res['Value'][pilotJobRef]['Jobs'])

    result = self.jobPolicy.getControlledUsers(RIGHT_GET_INFO)
    if not result['OK']:
      return result
    if result['Value'] != 'ALL':
      selectDict[('Owner', 'OwnerGroup')] = result['Value']

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    result = self.jobDB.getCounters('Jobs', ['Status'], selectDict,
                                    newer=startDate,
                                    older=endDate,
                                    timeStamp='LastUpdateTime')
    if not result['OK']:
      return result

    statusDict = {}
    nJobs = 0
    for stDict, count in result['Value']:
      nJobs += count
      statusDict[stDict['Status']] = count

    resultDict['TotalRecords'] = nJobs
    if nJobs == 0:
      return S_OK(resultDict)

    resultDict['Extras'] = statusDict

    if selectJobs:
      iniJob = startItem
      if iniJob >= nJobs:
        return S_ERROR('Item number out of range')

      result = self.jobDB.selectJobs(selectDict, orderAttribute=orderAttribute,
                                     newer=startDate, older=endDate, limit=(maxItems, iniJob))
      if not result['OK']:
        return result

      summaryJobList = result['Value']
      if not self.globalJobsInfo:
        validJobs, _invalidJobs, _nonauthJobs, _ownJobs = self.jobPolicy.evaluateJobRights(summaryJobList,
                                                                                           RIGHT_GET_INFO)
        summaryJobList = validJobs

      result = self.getJobsAttributes(summaryJobList, SUMMARY)
      if not result['OK']:
        return result

      summaryDict = result['Value']
      # If no jobs can be selected after the properties check
      if not summaryDict:
        return S_OK(resultDict)

      # Evaluate last sign of life time
      for jobID, jobDict in summaryDict.items():
        if (
            'HeartBeatTime' not in jobDict
            or not jobDict['HeartBeatTime']
            or jobDict['HeartBeatTime'] == "None"
        ):
          jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']
        else:
          lastTime = (
              Time.fromString(jobDict['LastUpdateTime'])
              if isinstance(jobDict['LastUpdateTime'], str)
              else jobDict['LastUpdateTime']
          )
          hbTime = (
              Time.fromString(jobDict['HeartBeatTime'])
              if isinstance(jobDict['HeartBeatTime'], str)
              else jobDict['HeartBeatTime']
          )
          # Not only Stalled jobs but also Failed jobs because Stalled
          if (
              (hbTime - lastTime) > timedelta(0)
              or jobDict["Status"] == JobStatus.STALLED
              or jobDict["MinorStatus"].startswith("Job stalled")
              or jobDict["MinorStatus"].startswith("Stalling")
          ):
            jobDict['LastSignOfLife'] = jobDict['HeartBeatTime']
          else:
            jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']

      # prepare the standard structure now
      key = list(summaryDict)[0]
      paramNames = list(summaryDict[key])

      records = []
      for jobID, jobDict in summaryDict.items():
        jParList = []
        for pname in paramNames:
          jParList.append(jobDict[pname])
        records.append(jParList)

      resultDict['ParameterNames'] = paramNames
      resultDict['Records'] = records

    return S_OK(resultDict)

##############################################################################
  types_getJobStats = [six.string_types, dict]

  @classmethod
  def export_getJobStats(cls, attribute, selectDict):
    """ Get job statistics distribution per attribute value with a given selection
    """
    startDate = selectDict.get('FromDate', None)
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get('LastUpdate', None)
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get('ToDate', None)
    if endDate:
      del selectDict['ToDate']

    result = cls.jobDB.getCounters('Jobs', [attribute], selectDict,
                                   newer=startDate,
                                   older=endDate,
                                   timeStamp='LastUpdateTime')
    if not result['OK']:
      return result

    resultDict = {}
    for cDict, count in result['Value']:
      resultDict[cDict[attribute]] = count

    return S_OK(resultDict)

##############################################################################
  types_getJobsPrimarySummary = [list]

  @classmethod
  @ignoreEncodeWarning
  @deprecated("Use getJobsSummary")
  def export_getJobsPrimarySummary(cls, jobIDs):
    return cls.getJobsAttributes(jobIDs, [])

##############################################################################
  types_getJobParameter = [six.string_types + six.integer_types, six.string_types]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobParameter(cls, jobID, parName):
    """
    :param str/int jobID: one single Job ID
    :param str parName: one single parameter name
    """
    if cls.elasticJobParametersDB:
      res = cls.elasticJobParametersDB.getJobParameters(jobID, [parName])
      if not res['OK']:
        return res
      if res['Value'].get(int(jobID)):
        return S_OK(res['Value'][int(jobID)])

    res = cls.jobDB.getJobParameters(jobID, [parName])
    if not res['OK']:
      return res
    return S_OK(res['Value'].get(int(jobID), {}))

##############################################################################
  types_getJobOptParameters = [int]

  @classmethod
  def export_getJobOptParameters(cls, jobID):
    return cls.jobDB.getJobOptParameters(jobID)

##############################################################################
  types_getJobParameters = [six.string_types + six.integer_types + (list,)]

  @classmethod
  @ignoreEncodeWarning
  def export_getJobParameters(cls, jobIDs, parName=None):
    """
    :param str/int/list jobIDs: one single job ID or a list of them
    :param str parName: one single parameter name, a list or None (meaning all of them)
    """
    if cls.elasticJobParametersDB:
      if not isinstance(jobIDs, list):
        jobIDs = [jobIDs]
      parameters = {}
      for jobID in jobIDs:
        res = cls.elasticJobParametersDB.getJobParameters(jobID, parName)
        if not res['OK']:
          return res
        parameters.update(res['Value'])

      # Need anyway to get also from JobDB, for those jobs with parameters registered in MySQL or in both backends
      res = cls.jobDB.getJobParameters(jobIDs, parName)
      if not res['OK']:
        return res
      parametersM = res['Value']

      # and now combine
      final = dict(parametersM)
      # if job in JobDB, update with parameters from ES if any
      for jobID in final:
        final[jobID].update(parameters.get(jobID, {}))
      # if job in ES and not in JobDB, take ES
      for jobID in parameters:
        if jobID not in final:
          final[jobID] = parameters[jobID]
      return S_OK(final)

    return cls.jobDB.getJobParameters(jobIDs, parName)

##############################################################################
  types_getAtticJobParameters = [list(six.integer_types)]

  @classmethod
  def export_getAtticJobParameters(cls, jobID, parameters=None, rescheduleCycle=-1):
    if not parameters:
      parameters = []
    return cls.jobDB.getAtticJobParameters(jobID, parameters, rescheduleCycle)

##############################################################################
  types_getJobAttributes = [int]

  @classmethod
  def export_getJobAttributes(cls, jobID, attrList=None):
    """
    :param int jobID: one single Job ID
    :param list attrList: optional list of attributes
    """

    return cls.jobDB.getJobAttributes(jobID, attrList=attrList)

##############################################################################
  types_getJobAttribute = [int, six.string_types]

  @classmethod
  def export_getJobAttribute(cls, jobID, attribute):
    """
    :param int jobID: one single Job ID
    :param str attribute: one single attribute name
    """

    return cls.jobDB.getJobAttribute(jobID, attribute)

##############################################################################
  types_getSiteSummary = []

  @classmethod
  def export_getSiteSummary(cls):
    return cls.jobDB.getSiteSummary()

##############################################################################
  types_getJobHeartBeatData = [int]

  @classmethod
  def export_getJobHeartBeatData(cls, jobID):
    return cls.jobDB.getHeartBeatData(jobID)

##############################################################################
  types_getInputData = [list(six.integer_types)]

  @classmethod
  def export_getInputData(cls, jobID):
    """ Get input data for the specified jobs
    """
    return cls.jobDB.getInputData(jobID)
