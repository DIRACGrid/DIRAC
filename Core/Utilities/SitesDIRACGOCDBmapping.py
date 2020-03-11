"""  The SitesDIRACGOCDBmapping module performs the necessary CS gymnastics to
     resolve sites DIRAC-GOCDB names.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""
__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


#############################################################################


def getGOCSiteName(diracSiteName):
  """
  Get GOC DB site name, given the DIRAC site name, as it is stored in the CS

  :param str`diracSiteName: DIRAC site name (e.g. 'LCG.CERN.ch')
  :returns S_OK/S_ERROR structure
  """
  gocDBName = gConfig.getValue('/Resources/Sites/%s/%s/Name' % (diracSiteName.split('.')[0],
                                                                diracSiteName))
  if not gocDBName:
    return S_ERROR("No GOC site name for %s in CS (Not a grid site ?)" % diracSiteName)
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

  :param str diracFTSName: DIRAC FTS server name (e.g. 'CERN-FTS3')
  :returns S_OK/S_ERROR structure
  """

  csPath = "/Resources/FTSEndpoints/FTS3"
  gocFTSName = gConfig.getValue("%s/%s" % (csPath, diracFTSName))
  if not gocFTSName:
    return S_ERROR("No GOC FTS server name for %s in CS (Not a grid site ?)" % diracFTSName)
  return S_OK(gocFTSName)


#############################################################################

def getDIRACSiteName(gocSiteName):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params str gocSiteName: GOC DB site name (e.g. 'CERN-PROD')
  :returns S_OK/S_ERROR structure
  """
  res = getSites()
  if not res['OK']:
    return res
  sitesList = res['Value']

  tmpList = [(site, gConfig.getValue("/Resources/Sites/%s/%s/Name" % (site.split('.')[0],
								      site))) for site in sitesList]
  diracSites = [dirac for (dirac, goc) in tmpList if goc == gocSiteName]

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
    res = getSEHosts(seName)
    if not res['OK']:
      return res
    if hostName in res['Value']:
      resultDIRACSEs.extend(seName)

  return S_OK(resultDIRACSEs)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
