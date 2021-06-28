"""
This is the interface to DIRAC PilotAgentsDB.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Utilities.Time as Time

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getPilotLoggingInfo,\
    getGridJobOutput, killPilotsInQueues


FINAL_STATES = ['Done', 'Aborted', 'Cleared', 'Deleted', 'Stalled']


class PilotManagerHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ Initialization of DB objects
    """

    try:
      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
      if not result['OK']:
        return result
      cls.pilotAgentsDB = result['Value']()

    except RuntimeError as excp:
      return S_ERROR("Can't connect to DB: %s" % excp)

    cls.pilotsLoggingDB = None
    enablePilotsLogging = Operations().getValue(
        '/Services/JobMonitoring/usePilotsLoggingFlag', False)
    if enablePilotsLogging:
      try:
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotsLoggingDB", "PilotsLoggingDB")
        if not result['OK']:
          return result
        cls.pilotsLoggingDB = result['Value']()
      except RuntimeError as excp:
        return S_ERROR("Can't connect to DB: %s" % excp)

    return S_OK()

  ##############################################################################
  types_getCurrentPilotCounters = [dict]

  @classmethod
  def export_getCurrentPilotCounters(cls, attrDict={}):
    """ Get pilot counters per Status with attrDict selection. Final statuses are given for
        the last day.
    """

    result = cls.pilotAgentsDB.getCounters(
        'PilotAgents', ['Status'], attrDict, timeStamp='LastUpdateTime')
    if not result['OK']:
      return result
    last_update = Time.dateTime() - Time.day
    resultDay = cls.pilotAgentsDB.getCounters(
        'PilotAgents', ['Status'], attrDict,
        newer=last_update, timeStamp='LastUpdateTime')
    if not resultDay['OK']:
      return resultDay

    resultDict = {}
    for statusDict, count in result['Value']:
      status = statusDict['Status']
      resultDict[status] = count
      if status in FINAL_STATES:
        resultDict[status] = 0
        for statusDayDict, ccount in resultDay['Value']:
          if status == statusDayDict['Status']:
            resultDict[status] = ccount
          break

    return S_OK(resultDict)

  ##########################################################################################
  types_addPilotTQReference = [list, six.integer_types, six.string_types, six.string_types]

  @classmethod
  def export_addPilotTQReference(cls, pilotRef, taskQueueID, ownerDN, ownerGroup,
                                 broker='Unknown', gridType='DIRAC', pilotStampDict={}):
    """ Add a new pilot job reference
    """
    return cls.pilotAgentsDB.addPilotTQReference(
        pilotRef, taskQueueID, ownerDN, ownerGroup,
        broker, gridType, pilotStampDict)

  ##############################################################################
  types_getPilotOutput = [six.string_types]

  @staticmethod
  def export_getPilotOutput(pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    return getGridJobOutput(pilotReference)

  ##############################################################################
  types_getPilotInfo = [(list,) + six.string_types]

  @classmethod
  def export_getPilotInfo(cls, pilotReference):
    """ Get the info about a given pilot job reference
    """
    return cls.pilotAgentsDB.getPilotInfo(pilotReference)

  ##############################################################################
  types_selectPilots = [dict]

  @classmethod
  def export_selectPilots(cls, condDict):
    """ Select pilots given the selection conditions
    """
    return cls.pilotAgentsDB.selectPilots(condDict)

  ##############################################################################
  types_storePilotOutput = [six.string_types, six.string_types, six.string_types]

  @classmethod
  def export_storePilotOutput(cls, pilotReference, output, error):
    """ Store the pilot output and error
    """
    return cls.pilotAgentsDB.storePilotOutput(pilotReference, output, error)

  ##############################################################################
  types_getPilotLoggingInfo = [six.string_types]

  @classmethod
  def export_getPilotLoggingInfo(cls, pilotReference):
    """ Get the pilot logging info for the Grid job reference
    """

    result = cls.pilotAgentsDB.getPilotInfo(pilotReference)
    if not result['OK'] or not result['Value']:
      return S_ERROR('Failed to determine owner for pilot ' + pilotReference)

    pilotDict = result['Value'][pilotReference]
    owner = pilotDict['OwnerDN']
    group = pilotDict['OwnerGroup']
    gridType = pilotDict['GridType']
    pilotStamp = pilotDict['PilotStamp']

    # Add the pilotStamp to the pilot Reference, some CEs may need it to retrieve the logging info
    pilotReference = pilotReference + ':::' + pilotStamp
    return getPilotLoggingInfo(gridType, pilotReference,  # pylint: disable=unexpected-keyword-arg
                               proxyUserDN=owner, proxyUserGroup=group)

  ##############################################################################
  types_getPilotSummary = []

  @classmethod
  def export_getPilotSummary(cls, startdate='', enddate=''):
    """ Get summary of the status of the LCG Pilot Jobs
    """

    return cls.pilotAgentsDB.getPilotSummary(startdate, enddate)

  ##############################################################################
  types_getPilotMonitorWeb = [dict, list, six.integer_types, list(six.integer_types)]

  @classmethod
  def export_getPilotMonitorWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    return cls.pilotAgentsDB.getPilotMonitorWeb(selectDict, sortList, startItem, maxItems)

  ##############################################################################
  types_getPilotMonitorSelectors = []

  @classmethod
  def export_getPilotMonitorSelectors(cls):
    """ Get all the distinct selector values for the Pilot Monitor web portal page
    """

    return cls.pilotAgentsDB.getPilotMonitorSelectors()

  ##############################################################################
  types_getPilotSummaryWeb = [dict, list, six.integer_types, list(six.integer_types)]

  @classmethod
  def export_getPilotSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    return cls.pilotAgentsDB.getPilotSummaryWeb(selectDict, sortList, startItem, maxItems)

  ##############################################################################
  types_getPilots = [six.string_types + six.integer_types]

  @classmethod
  def export_getPilots(cls, jobID):
    """ Get pilot references and their states for :
      - those pilots submitted for the TQ where job is sitting
      - (or) the pilots executing/having executed the Job
    """

    pilots = []
    result = cls.pilotAgentsDB.getPilotsForJobID(int(jobID))
    if not result['OK']:
      if result['Message'].find('not found') == -1:
        return S_ERROR('Failed to get pilot: ' + result['Message'])
    else:
      pilots += result['Value']
    if not pilots:
      # Pilots were not found try to look in the Task Queue
      taskQueueID = 0
      try:
        result = ObjectLoader().loadObject(
            "WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB"
        )
        if not result['OK']:
          return result
        tqDB = result['Value']()
      except RuntimeError as excp:
        return S_ERROR("Can't connect to DB: %s" % excp)
      result = tqDB.getTaskQueueForJob(int(jobID))
      if result['OK'] and result['Value']:
        taskQueueID = result['Value']
      if taskQueueID:
        result = cls.pilotAgentsDB.getPilotsForTaskQueue(taskQueueID, limit=10)
        if not result['OK']:
          return S_ERROR('Failed to get pilot: ' + result['Message'])
        pilots += result['Value']

    if not pilots:
      return S_ERROR('Failed to get pilot for Job %d' % int(jobID))

    return cls.pilotAgentsDB.getPilotInfo(pilotID=pilots)

  ##############################################################################
  types_killPilot = [six.string_types + (list,)]

  @classmethod
  def export_killPilot(cls, pilotRefList):
    """ Kill the specified pilots
    """
    # Make a list if it is not yet
    pilotRefs = list(pilotRefList)
    if isinstance(pilotRefList, six.string_types):
      pilotRefs = [pilotRefList]

    # Regroup pilots per site and per owner
    pilotRefDict = {}
    for pilotReference in pilotRefs:
      result = cls.pilotAgentsDB.getPilotInfo(pilotReference)
      if not result['OK'] or not result['Value']:
        return S_ERROR('Failed to get info for pilot ' + pilotReference)

      pilotDict = result['Value'][pilotReference]
      owner = pilotDict['OwnerDN']
      group = pilotDict['OwnerGroup']
      queue = '@@@'.join([owner, group, pilotDict['GridSite'], pilotDict['DestinationSite'], pilotDict['Queue']])
      gridType = pilotDict['GridType']
      pilotRefDict.setdefault(queue, {})
      pilotRefDict[queue].setdefault('PilotList', [])
      pilotRefDict[queue]['PilotList'].append(pilotReference)
      pilotRefDict[queue]['GridType'] = gridType

    failed = killPilotsInQueues(pilotRefDict)

    if failed:
      return S_ERROR('Failed to kill at least some pilots')

    return S_OK()

  ##############################################################################
  types_setJobForPilot = [six.string_types + six.integer_types, six.string_types]

  @classmethod
  def export_setJobForPilot(cls, jobID, pilotRef, destination=None):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """

    result = cls.pilotAgentsDB.setJobForPilot(int(jobID), pilotRef)
    if not result['OK']:
      return result
    result = cls.pilotAgentsDB.setCurrentJobID(pilotRef, int(jobID))
    if not result['OK']:
      return result
    if destination:
      result = cls.pilotAgentsDB.setPilotDestinationSite(pilotRef, destination)

    return result

  ##########################################################################################
  types_setPilotBenchmark = [six.string_types, float]

  @classmethod
  def export_setPilotBenchmark(cls, pilotRef, mark):
    """ Set the pilot agent benchmark
    """
    return cls.pilotAgentsDB.setPilotBenchmark(pilotRef, mark)

  ##########################################################################################
  types_setAccountingFlag = [six.string_types]

  @classmethod
  def export_setAccountingFlag(cls, pilotRef, mark='True'):
    """ Set the pilot AccountingSent flag
    """
    return cls.pilotAgentsDB.setAccountingFlag(pilotRef, mark)

  ##########################################################################################
  types_setPilotStatus = [six.string_types, six.string_types]

  @classmethod
  def export_setPilotStatus(cls, pilotRef, status, destination=None, reason=None, gridSite=None, queue=None):
    """ Set the pilot agent status
    """

    return cls.pilotAgentsDB.setPilotStatus(
        pilotRef, status, destination=destination,
        statusReason=reason, gridSite=gridSite, queue=queue)

  ##########################################################################################
  types_countPilots = [dict]

  @classmethod
  def export_countPilots(cls, condDict, older=None, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    return cls.pilotAgentsDB.countPilots(condDict, older, newer, timeStamp)

  ##########################################################################################
  types_getCounters = [six.string_types, list, dict]

  @classmethod
  def export_getCounters(cls, table, keys, condDict, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    return cls.pilotAgentsDB.getCounters(table, keys, condDict, newer=newer, timeStamp=timeStamp)

##############################################################################
  types_getPilotStatistics = [six.string_types, dict]

  @classmethod
  def export_getPilotStatistics(cls, attribute, selectDict):
    """ Get pilot statistics distribution per attribute value with a given selection
    """

    startDate = selectDict.get('FromDate', None)
    if startDate:
      del selectDict['FromDate']

    if startDate is None:
      startDate = selectDict.get('LastUpdate', None)
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get('ToDate', None)
    if endDate:
      del selectDict['ToDate']

    result = cls.pilotAgentsDB.getCounters(
        'PilotAgents', [attribute], selectDict,
        newer=startDate, older=endDate, timeStamp='LastUpdateTime')
    statistics = {}
    if result['OK']:
      for status, count in result['Value']:
        if "OwnerDN" in status:
          userName = getUsernameForDN(status['OwnerDN'])
          if userName['OK']:
            status['OwnerDN'] = userName['Value']
          statistics[status['OwnerDN']] = count
        else:
          statistics[status[attribute]] = count

    return S_OK(statistics)

  ##############################################################################
  types_deletePilots = [(list, ) + six.integer_types + six.string_types]

  @classmethod
  def export_deletePilots(cls, pilotIDs):

    if isinstance(pilotIDs, six.string_types):
      return cls.pilotAgentsDB.deletePilot(pilotIDs)

    if isinstance(pilotIDs, six.integer_types):
      pilotIDs = [pilotIDs, ]

    result = cls.pilotAgentsDB.deletePilots(pilotIDs)
    if not result['OK']:
      return result
    if cls.pilotsLoggingDB:
      pilotIDs = result['Value']
      pilots = cls.pilotAgentsDB.getPilotInfo(pilotID=pilotIDs)
      if not pilots['OK']:
        return pilots
      pilotRefs = []
      for pilot in pilots:
        pilotRefs.append(pilot['PilotJobReference'])
      result = cls.pilotsLoggingDB.deletePilotsLogging(pilotRefs)
      if not result['OK']:
        return result

    return S_OK()

##############################################################################
  types_clearPilots = [six.integer_types, six.integer_types]

  @classmethod
  def export_clearPilots(cls, interval=30, aborted_interval=7):

    result = cls.pilotAgentsDB.clearPilots(interval, aborted_interval)
    if not result['OK']:
      return result
    if cls.pilotsLoggingDB:
      pilotIDs = result['Value']
      pilots = cls.pilotAgentsDB.getPilotInfo(pilotID=pilotIDs)
      if not pilots['OK']:
        return pilots
      pilotRefs = []
      for pilot in pilots:
        pilotRefs.append(pilot['PilotJobReference'])
      result = cls.pilotsLoggingDB.deletePilotsLogging(pilotRefs)
      if not result['OK']:
        return result

    return S_OK()
