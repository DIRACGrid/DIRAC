"""
This is a DIRAC WMS administrator interface.
"""

__RCSID__ = "$Id$"

from past.builtins import long
import six
from DIRAC import gConfig, S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridJobOutput

# This is a global instance of the database classes
jobDB = None
taskQueueDB = None
enablePilotsLogging = False

FINAL_STATES = ['Done', 'Aborted', 'Cleared', 'Deleted', 'Stalled']


def initializeWMSAdministratorHandler(serviceInfo):
  """ WMS AdministratorService initialization

      :param dict serviceInfo: service information dictionary

      :return: S_OK()/S_ERROR()
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

        :param list siteList: site, status

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    maskList = [(site, 'Active') for site in siteList]
    result = jobDB.setSiteMask(maskList, credDict['DN'], 'No comment')
    return result

##############################################################################
  types_getSiteMask = []

  @classmethod
  def export_getSiteMask(cls, siteState='Active'):
    """ Get the site mask

        :param str siteState: site status

        :return: S_OK(list)/S_ERROR()
    """
    return jobDB.getSiteMask(siteState)

  types_getSiteMaskStatus = []

  @classmethod
  def export_getSiteMaskStatus(cls, sites=None):
    """ Get the site mask of given site(s) with columns 'site' and 'status' only

        :param sites: list of sites or site
        :type sites: list or str

        :return: S_OK()/S_ERROR() -- S_OK contain dict or str
    """
    return jobDB.getSiteMaskStatus(sites)

  ##############################################################################
  types_getAllSiteMaskStatus = []

  @classmethod
  def export_getAllSiteMaskStatus(cls):
    """ Get all the site parameters in the site mask

        :return: dict
    """
    return jobDB.getAllSiteMaskStatus()

##############################################################################
  types_banSite = [basestring]

  def export_banSite(self, site, comment='No comment'):
    """ Ban the given site in the site mask

        :param str site: site
        :param str comment: comment

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    author = credDict['username'] if credDict['username'] != 'anonymous' else credDict['DN']
    result = jobDB.banSiteInMask(site, author, comment)
    return result

##############################################################################
  types_allowSite = [basestring]

  def export_allowSite(self, site, comment='No comment'):
    """ Allow the given site in the site mask

        :param str site: site
        :param str comment: comment

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    author = credDict['username'] if credDict['username'] != 'anonymous' else credDict['DN']
    result = jobDB.allowSiteInMask(site, author, comment)
    return result

##############################################################################
  types_clearMask = []

  @classmethod
  def export_clearMask(cls):
    """ Clear up the entire site mask

        :return: S_OK()/S_ERROR()
    """
    return jobDB.removeSiteFromMask(None)

  ##############################################################################
  types_getSiteMaskLogging = [(basestring, list)]

  @classmethod
  def export_getSiteMaskLogging(cls, sites):
    """ Get the site mask logging history

        :param list sites: sites

        :return: S_OK(dict)/S_ERROR()
    """
    if isinstance(sites, six.string_types):
      sites = [sites]

    return jobDB.getSiteMaskLogging(sites)

##############################################################################
  types_getSiteMaskSummary = []

  @classmethod
  def export_getSiteMaskSummary(cls):
    """ Get the mask status for all the configured sites

        :return: S_OK(dict)/S_ERROR()
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

        :param str jobID: job ID

        :return: S_OK(dict)/S_ERROR()
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
  types_getSiteSummaryWeb = [dict, list, six.integer_types, six.integer_types]

  @classmethod
  def export_getSiteSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the jobs running on sites in a generic format

        :param dict selectDict: selectors
        :param list sortList: sorting list
        :param int startItem: start item number
        :param int maxItems: maximum of items

        :return: S_OK(dict)/S_ERROR()
    """
    result = jobDB.getSiteSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

##############################################################################
  types_getSiteSummarySelectors = []

  @classmethod
  def export_getSiteSummarySelectors(cls):
    """ Get all the distinct selector values for the site summary web portal page

        :return: S_OK(dict)/S_ERROR()
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
