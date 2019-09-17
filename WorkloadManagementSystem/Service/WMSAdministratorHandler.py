"""
This is a DIRAC WMS administrator interface.
"""

__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridJobOutput

# imports for interacting with PilotAgentsDB -- moved to PilotManagerHandler and deprecated
import DIRAC.Core.Utilities.Time as Time
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB import PilotsLoggingDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getPilotLoggingInfo, killPilotsInQueues

# This is a global instance of the database classes
jobDB = None
taskQueueDB = None
enablePilotsLogging = False

FINAL_STATES = ['Done', 'Aborted', 'Cleared', 'Deleted', 'Stalled']


def initializeWMSAdministratorHandler(serviceInfo):
  """  WMS AdministratorService initialization
  """

  global jobDB
  global taskQueueDB

  jobDB = JobDB()
  taskQueueDB = TaskQueueDB()

  return S_OK()


class WMSAdministratorHandler(RequestHandler):

  types_setSiteMask = [list]

  def export_setSiteMask(self, siteList):
    """ Set the site mask for matching. The mask is given in a form of Classad string.
    """
    result = self.getRemoteCredentials()
    dn = result['DN']

    maskList = [(site, 'Active') for site in siteList]
    result = jobDB.setSiteMask(maskList, dn, 'No comment')
    return result

##############################################################################
  types_getSiteMask = []

  @classmethod
  def export_getSiteMask(cls, siteState='Active'):
    """ Get the site mask
    """
    return jobDB.getSiteMask(siteState)

  types_getSiteMaskStatus = []

  @classmethod
  def export_getSiteMaskStatus(cls, sites=None):
    """ Get the site mask of given site(s) with columns 'site' and 'status' only
    """

    return jobDB.getSiteMaskStatus(sites)

  ##############################################################################
  types_getAllSiteMaskStatus = []

  @classmethod
  def export_getAllSiteMaskStatus(cls):
    """ Get all the site parameters in the site mask
    """
    return jobDB.getAllSiteMaskStatus()

##############################################################################
  types_banSite = [basestring]

  def export_banSite(self, site, comment='No comment'):
    """ Ban the given site in the site mask
    """

    result = self.getRemoteCredentials()
    dn = result['DN']
    result = getUsernameForDN(dn)
    if result['OK']:
      author = result['Value']
    else:
      author = dn
    result = jobDB.banSiteInMask(site, author, comment)
    return result

##############################################################################
  types_allowSite = [basestring]

  def export_allowSite(self, site, comment='No comment'):
    """ Allow the given site in the site mask
    """

    result = self.getRemoteCredentials()
    dn = result['DN']
    result = getUsernameForDN(dn)
    if result['OK']:
      author = result['Value']
    else:
      author = dn
    result = jobDB.allowSiteInMask(site, author, comment)
    return result

##############################################################################
  types_clearMask = []

  @classmethod
  def export_clearMask(cls):
    """ Clear up the entire site mask
    """

    return jobDB.removeSiteFromMask(None)

  ##############################################################################
  types_getSiteMaskLogging = [(basestring, list)]

  @classmethod
  def export_getSiteMaskLogging(cls, sites):
    """ Get the site mask logging history
    """

    if isinstance(sites, basestring):
      sites = [sites]

    return jobDB.getSiteMaskLogging(sites)

##############################################################################
  types_getSiteMaskSummary = []

  @classmethod
  def export_getSiteMaskSummary(cls):
    """ Get the mask status for all the configured sites
    """

    # Get all the configured site names
    result = gConfig.getSections('/Resources/Sites')
    if not result['OK']:
      return result
    grids = result['Value']
    sites = []
    for grid in grids:
      result = gConfig.getSections('/Resources/Sites/%s' % grid)
      if not result['OK']:
        return result
      sites += result['Value']

    # Get the current mask status
    result = jobDB.getSiteMaskStatus()
    siteDict = result['Value']
    for site in sites:
      if site not in siteDict:
        siteDict[site] = 'Unknown'

    return S_OK(siteDict)

  ##############################################################################
  types_getJobPilotOutput = [(basestring, int, long)]

  def export_getJobPilotOutput(self, jobID):
    """ Get the pilot job standard output and standard error files for the DIRAC
        job reference
    """

    pilotReference = ''
    # Get the pilot grid reference first from the job parameters
    result = jobDB.getJobParameter(int(jobID), 'Pilot_Reference')
    if result['OK']:
      pilotReference = result['Value']

    if not pilotReference:
      # Failed to get the pilot reference, try to look in the attic parameters
      result = jobDB.getAtticJobParameters(int(jobID), ['Pilot_Reference'])
      if result['OK']:
        c = -1
        # Get the pilot reference for the last rescheduling cycle
        for cycle in result['Value']:
          if cycle > c:
            pilotReference = result['Value'][cycle]['Pilot_Reference']
            c = cycle

    if pilotReference:
      return getGridJobOutput(pilotReference)
    return S_ERROR('No pilot job reference found')

  ##############################################################################
  types_getSiteSummaryWeb = [dict, list, (int, long), (int, long)]

  @classmethod
  def export_getSiteSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the jobs running on sites in a generic format
    """

    result = jobDB.getSiteSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

##############################################################################
  types_getSiteSummarySelectors = []

  @classmethod
  def export_getSiteSummarySelectors(cls):
    """ Get all the distinct selector values for the site summary web portal page
    """

    resultDict = {}
    statusList = ['Good', 'Fair', 'Poor', 'Bad', 'Idle']
    resultDict['Status'] = statusList
    maskStatus = ['Active', 'Banned', 'NoMask', 'Reduced']
    resultDict['MaskStatus'] = maskStatus

    gridTypes = []
    result = gConfig.getSections('Resources/Sites/', [])
    if result['OK']:
      gridTypes = result['Value']

    resultDict['GridType'] = gridTypes
    siteList = []
    for grid in gridTypes:
      result = gConfig.getSections('Resources/Sites/%s' % grid, [])
      if result['OK']:
        siteList += result['Value']

    countryList = []
    for site in siteList:
      if site.find('.') != -1:
        country = site.split('.')[2].lower()
        if country not in countryList:
          countryList.append(country)
    countryList.sort()
    resultDict['Country'] = countryList
    siteList.sort()
    resultDict['Site'] = siteList

    return S_OK(resultDict)

  ##############################################################################
  # Methods below moved to PilotManagerHandler -- all marked as deprecated
  ##############################################################################

  types_getCurrentPilotCounters = [dict]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getCurrentPilotCounters(cls, attrDict={}):
    """ Get pilot counters per Status with attrDict selection. Final statuses are given for
        the last day.
    """

    result = PilotAgentsDB().getCounters('PilotAgents', ['Status'], attrDict, timeStamp='LastUpdateTime')
    if not result['OK']:
      return result
    last_update = Time.dateTime() - Time.day
    resultDay = PilotAgentsDB().getCounters('PilotAgents', ['Status'], attrDict, newer=last_update,
                                            timeStamp='LastUpdateTime')
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
  types_addPilotTQReference = [list, (int, long), basestring, basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_addPilotTQReference(cls, pilotRef, taskQueueID, ownerDN, ownerGroup, broker='Unknown',
                                 gridType='DIRAC', pilotStampDict={}):
    """ Add a new pilot job reference """
    return PilotAgentsDB().addPilotTQReference(pilotRef, taskQueueID,
                                               ownerDN, ownerGroup,
                                               broker, gridType, pilotStampDict)

  ##############################################################################
  types_getPilotOutput = [basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotOutput(cls, pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    return getGridJobOutput(pilotReference)

  ##############################################################################
  types_getPilotInfo = [(list, basestring)]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotInfo(cls, pilotReference):
    """ Get the info about a given pilot job reference
    """
    return PilotAgentsDB().getPilotInfo(pilotReference)

  ##############################################################################
  types_selectPilots = [dict]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_selectPilots(cls, condDict):
    """ Select pilots given the selection conditions
    """
    return PilotAgentsDB().selectPilots(condDict)

  ##############################################################################
  types_storePilotOutput = [basestring, basestring, basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_storePilotOutput(cls, pilotReference, output, error):
    """ Store the pilot output and error
    """
    return PilotAgentsDB().storePilotOutput(pilotReference, output, error)

  ##############################################################################
  types_getPilotLoggingInfo = [basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotLoggingInfo(cls, pilotReference):
    """ Get the pilot logging info for the Grid job reference
    """

    result = PilotAgentsDB().getPilotInfo(pilotReference)
    if not result['OK'] or not result['Value']:
      return S_ERROR('Failed to determine owner for pilot ' + pilotReference)

    pilotDict = result['Value'][pilotReference]
    owner = pilotDict['OwnerDN']
    group = pilotDict['OwnerGroup']
    gridType = pilotDict['GridType']

    return getPilotLoggingInfo(gridType, pilotReference,  # pylint: disable=unexpected-keyword-arg
                               proxyUserDN=owner, proxyUserGroup=group)

  ##############################################################################
  types_getPilotSummary = []

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotSummary(cls, startdate='', enddate=''):
    """ Get summary of the status of the LCG Pilot Jobs
    """

    result = PilotAgentsDB().getPilotSummary(startdate, enddate)
    return result

  ##############################################################################
  types_getPilotMonitorWeb = [dict, list, (int, long), [int, long]]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotMonitorWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    result = PilotAgentsDB().getPilotMonitorWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getPilotMonitorSelectors = []

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotMonitorSelectors(cls):
    """ Get all the distinct selector values for the Pilot Monitor web portal page
    """

    result = PilotAgentsDB().getPilotMonitorSelectors()
    return result

  ##############################################################################
  types_getPilotSummaryWeb = [dict, list, (int, long), [int, long]]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    result = PilotAgentsDB().getPilotSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getPilots = [(basestring, int, long)]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilots(cls, jobID):
    """ Get pilot references and their states for :
      - those pilots submitted for the TQ where job is sitting
      - (or) the pilots executing/having executed the Job
    """

    pilots = []
    result = PilotAgentsDB().getPilotsForJobID(int(jobID))
    if not result['OK']:
      if result['Message'].find('not found') == -1:
        return S_ERROR('Failed to get pilot: ' + result['Message'])
    else:
      pilots += result['Value']
    if not pilots:
      # Pilots were not found try to look in the Task Queue
      taskQueueID = 0
      result = TaskQueueDB().getTaskQueueForJob(int(jobID))
      if result['OK'] and result['Value']:
        taskQueueID = result['Value']
      if taskQueueID:
        result = PilotAgentsDB().getPilotsForTaskQueue(taskQueueID, limit=10)
        if not result['OK']:
          return S_ERROR('Failed to get pilot: ' + result['Message'])
        pilots += result['Value']

    if not pilots:
      return S_ERROR('Failed to get pilot for Job %d' % int(jobID))

    return PilotAgentsDB().getPilotInfo(pilotID=pilots)

  ##############################################################################
  types_killPilot = [(basestring, list)]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_killPilot(cls, pilotRefList):
    """ Kill the specified pilots
    """
    # Make a list if it is not yet
    pilotRefs = list(pilotRefList)
    if isinstance(pilotRefList, basestring):
      pilotRefs = [pilotRefList]

    # Regroup pilots per site and per owner
    pilotRefDict = {}
    for pilotReference in pilotRefs:
      result = PilotAgentsDB().getPilotInfo(pilotReference)
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
  types_setJobForPilot = [(basestring, int, long), basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_setJobForPilot(cls, jobID, pilotRef, destination=None):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """

    result = PilotAgentsDB().setJobForPilot(int(jobID), pilotRef)
    if not result['OK']:
      return result
    result = PilotAgentsDB().setCurrentJobID(pilotRef, int(jobID))
    if not result['OK']:
      return result
    if destination:
      result = PilotAgentsDB().setPilotDestinationSite(pilotRef, destination)

    return result

  ##########################################################################################
  types_setPilotBenchmark = [basestring, float]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_setPilotBenchmark(cls, pilotRef, mark):
    """ Set the pilot agent benchmark
    """
    return PilotAgentsDB().setPilotBenchmark(pilotRef, mark)

  ##########################################################################################
  types_setAccountingFlag = [basestring]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_setAccountingFlag(cls, pilotRef, mark='True'):
    """ Set the pilot AccountingSent flag
    """
    return PilotAgentsDB().setAccountingFlag(pilotRef, mark)

  ##########################################################################################
  types_setPilotStatus = [basestring, basestring]

  @deprecated("Moved to PilotManagerHandler")
  def export_setPilotStatus(self, pilotRef, status, destination=None, reason=None, gridSite=None, queue=None):
    """ Set the pilot agent status
    """

    return PilotAgentsDB().setPilotStatus(pilotRef, status, destination=destination,
                                          statusReason=reason, gridSite=gridSite, queue=queue)

  ##########################################################################################
  types_countPilots = [dict]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_countPilots(cls, condDict, older=None, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    return PilotAgentsDB().countPilots(condDict, older, newer, timeStamp)

  ##########################################################################################
  types_getCounters = [basestring, list, dict]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getCounters(cls, table, keys, condDict, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    return PilotAgentsDB().getCounters(table, keys, condDict, newer=newer, timeStamp=timeStamp)

##############################################################################
  types_getPilotStatistics = [basestring, dict]

  @staticmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_getPilotStatistics(attribute, selectDict):
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

    result = PilotAgentsDB().getCounters('PilotAgents', [attribute], selectDict,
                                         newer=startDate,
                                         older=endDate,
                                         timeStamp='LastUpdateTime')
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
  types_deletePilots = [(list, int, long, basestring)]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_deletePilots(cls, pilotIDs):

    if isinstance(pilotIDs, basestring):
      return PilotAgentsDB().deletePilot(pilotIDs)

    if isinstance(pilotIDs, (int, long)):
      pilotIDs = [pilotIDs, ]

    result = PilotAgentsDB().deletePilots(pilotIDs)
    if not result['OK']:
      return result
    if enablePilotsLogging:
      pilotIDs = result['Value']
      pilots = PilotAgentsDB().getPilotInfo(pilotID=pilotIDs)
      if not pilots['OK']:
        return pilots
      pilotRefs = []
      for pilot in pilots:
        pilotRefs.append(pilot['PilotJobReference'])
      result = PilotsLoggingDB().deletePilotsLogging(pilotRefs)
      if not result['OK']:
        return result

    return S_OK()

##############################################################################
  types_clearPilots = [(int, long), (int, long)]

  @classmethod
  @deprecated("Moved to PilotManagerHandler")
  def export_clearPilots(cls, interval=30, aborted_interval=7):

    result = PilotAgentsDB().clearPilots(interval, aborted_interval)
    if not result['OK']:
      return result
    if enablePilotsLogging:
      pilotIDs = result['Value']
      pilots = PilotAgentsDB().getPilotInfo(pilotID=pilotIDs)
      if not pilots['OK']:
        return pilots
      pilotRefs = []
      for pilot in pilots:
        pilotRefs.append(pilot['PilotJobReference'])
      result = PilotsLoggingDB().deletePilotsLogging(pilotRefs)
      if not result['OK']:
        return result

    return S_OK()
