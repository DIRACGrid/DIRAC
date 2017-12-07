"""
This class gets information about storage space tokens from the FC recording,
from SRM and if available from storage dumps
It reports for each site its availability and usage
"""

import time

from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Base import Script

from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement


class SpaceTokenUsage(object):
  """
  Class used to get information about Space token occupancy
  """

  def __init__(self):
    """ c'tor """
    import lcg_util
    self.lcg_util = lcg_util
    self.storageusage = RPCClient('DataManagement/StorageUsage')
    self.spaceTokenInfo = {}
    self.sitesSEs = {}
    self.storageElementSet = DMSHelpers().getStorageElements()
    self.dmsHelper = DMSHelpers()
    self.shortSiteNames = self.dmsHelper.getShortSiteNames(tier=(0, 1))
    self.storageSummary = None

  def execute(self, unit):
    """ Parse the request and execute the command """

    sites = None
    for switch in Script.getUnprocessedSwitches():
      if switch[0] in ("u", "Unit"):
        unit = switch[1]
      if switch[0] in ("S", "Sites"):
        sites = switch[1].split(',')

    if not sites:
      # Tier0 and all Tier1s
      sites = sorted(self.dmsHelper.getTiers(tier=(0, 1)))
    else:
      # Translate in case it is a short name
      allSites = self.dmsHelper.getSites()
      sites = [self.shortSiteNames.get(site, site) for site in sites]
      badSites = set(sites) - set(allSites)
      if badSites:
        gLogger.warn("Some sites do not exist", str(sorted(badSites)))
        sites = [site for site in sites if site in allSites]

    scaleDict = {'MB': 1000 * 1000.0,
                 'GB': 1000 * 1000 * 1000.0,
                 'TB': 1000 * 1000 * 1000 * 1000.0,
                 'PB': 1000 * 1000 * 1000 * 1000 * 1000.0}
    if unit not in scaleDict:
      Script.showHelp()
    scaleFactor = scaleDict[unit]

    for site in sites:
      self.sitesSEs[site] = {}
      # Get SEs at site
      seList = self.dmsHelper.getSEsAtSite(site).get('Value', [])
      for se in seList:
        spaceToken = StorageElement(se).getStorageParameters(protocol='srm')
        if spaceToken['OK']:
          spaceToken = spaceToken['Value']
          st = spaceToken['SpaceToken']
          self.sitesSEs[site].setdefault(st, {}).setdefault('SEs', []).append(se)
          # Fill in the endpoints
          ep = 'httpg://%s:%s%s' % (spaceToken['Host'], spaceToken['Port'], spaceToken['WSUrl'].split('?')[0])
          self.spaceTokenInfo.setdefault(site.split('.')[1], {}).setdefault(ep, set()).add(st)

    lfcUsage = {}
    srmUsage = {}
    sdUsage = {}
    for site in sites:
      # retrieve space usage from LFC
      lfcUsage[site] = self.getFCUsage(site)

      # retrieve SRM usage
      srmResult = self.getSrmUsage(site)
      if srmResult != -1:
        srmUsage[site] = srmResult
      else:
        return 1

      # retrieve space usage from storage dumps:
      sdResult = self.getSDUsage(site)
      if sdResult != -1:
        sdUsage[site] = sdResult
      else:
        return 1

      gLogger.notice("Storage usage summary for site %s - %s " % (site.split('.')[1], time.asctime()))
      for st in self.sitesSEs[site]:
        gLogger.notice("Space token %s " % st)
        gLogger.notice("\tFrom FC: Files: %d, Size: %.2f %s" %
                       (lfcUsage[site][st]['Files'],
                        lfcUsage[site][st]['Size'] / scaleFactor, unit))
        if site in srmUsage and st in srmUsage[site]:
          gLogger.notice("\tFrom SRM: Total Assigned Space: %.2f %s, Used Space: %.2f %s, Free Space: %.2f %s " %
                         (srmUsage[site][st]['SRMTotal'] / scaleFactor, unit,
                          srmUsage[site][st]['SRMUsed'] / scaleFactor, unit,
                          srmUsage[site][st]['SRMFree'] / scaleFactor, unit))
        else:
          gLogger.notice("\tFrom SRM: Information not available")
        if site in sdUsage and st in sdUsage[site]:
          gLogger.notice("\tFrom storage dumps: Files: %d, Size: %.2f %s - last update %s " %
                         (sdUsage[site][st]['Files'],
                          sdUsage[site][st]['Size'] / scaleFactor, unit,
                          sdUsage[site][st]['LastUpdate']))
        else:
          gLogger.notice("\tFrom storage dumps: Information not available")
    return 0

  def getSrmUsage(self, lcgSite):
    """Get space usage via SRM interface
    """
    try:
      site = lcgSite.split('.')[1]
    except IndexError:
      site = lcgSite
    if site not in self.spaceTokenInfo:
      gLogger.error("ERROR: information not available for site %s. Space token information from CS: %s "
                    % (site, sorted(self.spaceTokenInfo)))
      return -1

    result = {}
    for ep, stList in self.spaceTokenInfo[site].iteritems():
      for st in stList:
        result[st] = {}
        srm = self.lcg_util.lcg_stmd(st, ep, True, 0)
        if srm[0]:
          # This SpaceToken doesn't exist at this endPoint
          continue
        srmVal = srm[1][0]
        srmTotSpace = srmVal['totalsize']
        # correct for the 6% overhead due to castor setup at RAL
        if 'gridpp' in ep:
          srmTotSpace = (srmVal['totalsize']) * 0.94
          gLogger.warn('WARNING! apply a 0.94 factor to total space for RAL!')
        srmFree = srmVal['unusedsize']
        srmUsed = srmTotSpace - srmFree
        result[st]['SRMUsed'] = srmUsed
        result[st]['SRMFree'] = srmFree
        result[st]['SRMTotal'] = srmTotSpace
    return result

  # .................................................

  def getSDUsage(self, lcgSite):
    """ get storage usage from storage dumps
    """
    try:
      site = lcgSite.split('.')[1]
    except IndexError:
      site = lcgSite
    res = self.storageusage.getSTSummary(site)
    if not res['OK']:
      gLogger.error("ERROR: Cannot get storage dump information for site %s :" % site, res['Message'])
      return -1
    if not res['Value']:
      gLogger.warn(" No information available for site %s from storage dumps" % site)
    sdUsage = {}
    for row in res['Value']:
      site, spaceTokenWithID, totalSpace, totalFiles, lastUpdate = row
      for st in self.sitesSEs[lcgSite]:
        sdUsage.setdefault(st, {})
        if st in spaceTokenWithID:
          sdUsage[st]['Size'] = totalSpace
          sdUsage[st]['Files'] = totalFiles
          sdUsage[st]['LastUpdate'] = lastUpdate
          break
    return sdUsage

  def getFCUsage(self, lcgSite):
    """ get storage usage from LFC
    """
    if self.storageSummary is None:
      res = self.storageusage.getStorageSummary()
      if not res['OK']:
        gLogger.error('ERROR in getStorageSummary ', res['Message'])
        return {}
      self.storageSummary = res['Value']

    usage = {}
    for st in self.sitesSEs[lcgSite]:
      usage[st] = {'Files': 0, 'Size': 0}
      for se in self.sitesSEs[lcgSite][st]['SEs']:
        if se in self.storageSummary:
          usage[st]['Files'] += self.storageSummary[se]['Files']
          usage[st]['Size'] += self.storageSummary[se]['Size']
        else:
          gLogger.error("No FC storage information for SE", se)

    return usage
