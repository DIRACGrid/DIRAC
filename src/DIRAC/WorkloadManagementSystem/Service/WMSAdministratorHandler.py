"""
This is a DIRAC WMS administrator interface.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridJobOutput


class WMSAdministratorHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, svcInfoDict):
    """ WMS AdministratorService initialization
    """
    try:
      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
      if not result['OK']:
        return result
      cls.jobDB = result['Value']()
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

  types_setSiteMask = [list]

  def export_setSiteMask(self, siteList):
    """ Set the site mask for matching. The mask is given in a form of Classad string.

        :param list siteList: site, status

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    maskList = [(site, 'Active') for site in siteList]
    return self.jobDB.setSiteMask(maskList, credDict['DN'], 'No comment')

##############################################################################
  types_getSiteMask = []

  @classmethod
  def export_getSiteMask(cls, siteState='Active'):
    """ Get the site mask

        :param str siteState: site status

        :return: S_OK(list)/S_ERROR()
    """
    return cls.jobDB.getSiteMask(siteState)

  types_getSiteMaskStatus = []

  @classmethod
  def export_getSiteMaskStatus(cls, sites=None):
    """ Get the site mask of given site(s) with columns 'site' and 'status' only

        :param sites: list of sites or site
        :type sites: list or str

        :return: S_OK()/S_ERROR() -- S_OK contain dict or str
    """
    return cls.jobDB.getSiteMaskStatus(sites)

  ##############################################################################
  types_getAllSiteMaskStatus = []

  @classmethod
  def export_getAllSiteMaskStatus(cls):
    """ Get all the site parameters in the site mask

        :return: dict
    """
    return cls.jobDB.getAllSiteMaskStatus()

##############################################################################
  types_banSite = [six.string_types]

  def export_banSite(self, site, comment='No comment'):
    """ Ban the given site in the site mask

        :param str site: site
        :param str comment: comment

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    author = credDict['username'] if credDict['username'] != 'anonymous' else credDict['DN']
    return self.jobDB.banSiteInMask(site, author, comment)

##############################################################################
  types_allowSite = [six.string_types]

  def export_allowSite(self, site, comment='No comment'):
    """ Allow the given site in the site mask

        :param str site: site
        :param str comment: comment

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    author = credDict['username'] if credDict['username'] != 'anonymous' else credDict['DN']
    return self.jobDB.allowSiteInMask(site, author, comment)

##############################################################################
  types_clearMask = []

  @classmethod
  def export_clearMask(cls):
    """ Clear up the entire site mask

        :return: S_OK()/S_ERROR()
    """
    return cls.jobDB.removeSiteFromMask(None)

  ##############################################################################
  types_getSiteMaskLogging = [six.string_types + (list,)]

  @classmethod
  def export_getSiteMaskLogging(cls, sites):
    """ Get the site mask logging history

        :param list sites: sites

        :return: S_OK(dict)/S_ERROR()
    """
    if isinstance(sites, six.string_types):
      sites = [sites]

    return cls.jobDB.getSiteMaskLogging(sites)

##############################################################################
  types_getSiteMaskSummary = []

  @classmethod
  def export_getSiteMaskSummary(cls):
    """ Get the mask status for all the configured sites

        :return: S_OK(dict)/S_ERROR()
    """
    # Get all the configured site names
    res = getSites()
    if not res['OK']:
      return res
    sites = res['Value']

    # Get the current mask status
    result = cls.jobDB.getSiteMaskStatus()
    siteDict = result['Value']
    for site in sites:
      if site not in siteDict:
        siteDict[site] = 'Unknown'

    return S_OK(siteDict)

  ##############################################################################
  types_getJobPilotOutput = [six.string_types + six.integer_types]

  def export_getJobPilotOutput(self, jobID):
    """ Get the pilot job standard output and standard error files for the DIRAC
        job reference

        :param str jobID: job ID

        :return: S_OK(dict)/S_ERROR()
    """
    pilotReference = ''
    # Get the pilot grid reference first from the job parameters

    if self.elasticJobParametersDB:
      res = self.elasticJobParametersDB.getJobParameters(int(jobID), 'Pilot_Reference')
      if not res['OK']:
        return res
      if res['Value'].get(int(jobID)):
        pilotReference = res['Value'][int(jobID)]['Pilot_Reference']

    if not pilotReference:
      res = self.jobDB.getJobParameter(int(jobID), 'Pilot_Reference')
      if not res['OK']:
        return res
      pilotReference = res['Value']

    if not pilotReference:
      # Failed to get the pilot reference, try to look in the attic parameters
      res = self.jobDB.getAtticJobParameters(int(jobID), ['Pilot_Reference'])
      if res['OK']:
        c = -1
        # Get the pilot reference for the last rescheduling cycle
        for cycle in res['Value']:
          if cycle > c:
            pilotReference = res['Value'][cycle]['Pilot_Reference']
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
    return cls.jobDB.getSiteSummaryWeb(selectDict, sortList, startItem, maxItems)

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

    res = getSites()
    if not res['OK']:
      return res
    siteList = res['Value']

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
