"""  The SitesDIRACGOCDBmapping module performs the necessary CS gymnastics to
     resolve sites DIRAC-GOCDB names.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""
__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Utilities.CSHelpers import getSEHost


#############################################################################


def getGOCSiteName(diracSiteName):
  """
  Get GOC DB site name, given the DIRAC site name, as it stored in the CS

  :params:
    :attr:`diracSiteName` - string: DIRAC site name (e.g. 'LCG.CERN.ch')
  """
  gocDBName = gConfig.getValue('/Resources/Sites/%s/%s/Name' % (diracSiteName.split('.')[0],
                                                                diracSiteName))
  if not gocDBName:
    return S_ERROR("No GOC site name for %s in CS (Not a grid site ?)" % diracSiteName)
  else:
    return S_OK(gocDBName)


def getGOCSites(diracSites=None):

  if diracSites is None:
    diracSites = getSites()
    if not diracSites['OK']:
      return diracSites
    diracSites = diracSites['Value']

  gocSites = []

  for diracSite in diracSites:
    gocSite = getGOCSiteName(diracSite)
    if not gocSite['OK']:
      continue
    gocSites.append(gocSite['Value'])

  return S_OK(list(set(gocSites)))


def getGOCFTSName(diracFTSName):
  """
  Get GOC DB FTS server URL, given the DIRAC FTS server name, as it stored in the CS

  :params:
    :attr:`diracFTSName` - string: DIRAC FTS server name (e.g. 'CERN-FTS3')
  """

  csPath = "/Resources/FTSEndpoints/FTS3"
  gocFTSName = gConfig.getValue("%s/%s" % (csPath, diracFTSName))
  if not gocFTSName:
    return S_ERROR("No GOC FTS server name for %s in CS (Not a grid site ?)" % diracFTSName)
  else:
    return S_OK(gocFTSName)


#############################################################################

def getDIRACSiteName(gocSiteName):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params:
    :attr:`gocSiteName` - string: GOC DB site name (e.g. 'CERN-PROD')
  """
  diracSites = []
  result = gConfig.getSections("/Resources/Sites")
  if not result['OK']:
    return result
  gridList = result['Value']
  for grid in gridList:
    result = gConfig.getSections("/Resources/Sites/%s" % grid)
    if not result['OK']:
      return result
    sitesList = result['Value']
    tmpList = [(site, gConfig.getValue("/Resources/Sites/%s/%s/Name" % (grid, site))) for site in sitesList]
    diracSites += [dirac for (dirac, goc) in tmpList if goc == gocSiteName]

  if diracSites:
    return S_OK(diracSites)

  return S_ERROR("There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName)


def getDIRACSesForHostName(hostName):
  """ returns the DIRAC SEs that share the same hostName

      :param str hostName: host name, e.g. 'storm-fe-lhcb.cr.cnaf.infn.it'

      :return: S_OK with list of DIRAC SE names, or S_ERROR
  """

  seNames = DMSHelpers().getStorageElements()

  resultDIRACSEs = []
  for seName in seNames:
    res = getSEHost(seName)
    if not res['OK']:
      return res
    if hostName in res['Value']:
      resultDIRACSEs.append(seName)

  return S_OK(resultDIRACSEs)


def getDIRACGOCDictionary():
  """
  Create a dictionary containing DIRAC site names and GOCDB site names
  using a configuration provided by CS.

  :return:  A dictionary of DIRAC site names (key) and GOCDB site names (value).
  """

  log = gLogger.getSubLogger('getDIRACGOCDictionary')
  log.debug('Begin function ...')

  result = gConfig.getConfigurationTree('/Resources/Sites', 'Name')
  if not result['OK']:
    log.error("getConfigurationTree() failed with message: %s" % result['Message'])
    return S_ERROR('Configuration is corrupted')
  siteNamesTree = result['Value']

  dictionary = dict()
  PATHELEMENTS = 6  # site names have 6 elements in the path, i.e.:
  #    /Resource/Sites/<GRID NAME>/<DIRAC SITE NAME>/Name
  # [0]/[1]     /[2]  /[3]        /[4]              /[5]

  for path, gocdbSiteName in siteNamesTree.iteritems():
    elements = path.split('/')
    if len(elements) != PATHELEMENTS:
      continue

    diracSiteName = elements[PATHELEMENTS - 2]
    dictionary[diracSiteName] = gocdbSiteName

  log.debug('End function.')
  return S_OK(dictionary)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
