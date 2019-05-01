"""  The SiteCEMapping module performs the necessary CS gymnastics to
     resolve site and CE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id$"

import re

from DIRAC import gConfig, gLogger, S_OK, S_ERROR


def __getGridTypes(gridName=None):
  gridTypes = gConfig.getSections('Resources/Sites/', [])
  if not gridTypes['OK']:
    gLogger.error('Problem retrieving sections in /Resources/Sites')
    return gridTypes
  gridTypes = gridTypes['Value']
  if gridName:
    if gridName not in gridTypes:
      errMsg = 'Could not get sections for /Resources/Sites/%s' % gridName
      gLogger.error(errMsg)
      return S_ERROR(errMsg)
    gridTypes = [gridName]
  return S_OK(gridTypes)

#############################################################################


def getSites(gridName=None):
  gridTypes = __getGridTypes(gridName)
  if not gridTypes['OK']:
    return gridTypes
  gridTypes = gridTypes['Value']

  siteList = []
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' % grid, [])
    if not sites['OK']:
      gLogger.error('Problem retrieving /Resources/Sites/%s section' % grid, sites['Message'])
      return sites
    siteList += sites['Value']
  return S_OK(siteList)

#############################################################################


def getSiteCEMapping(gridName=None):
  """ Returns a dictionary of all sites and their CEs as a list, e.g.
      {'LCG.CERN.ch':['ce101.cern.ch',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  sites = getSites(gridName=gridName)
  if not sites['OK']:
    return sites

  siteCEMapping = {}
  for candidate in sites['Value']:
    grid = candidate.split('.')[0]
    candidateCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' % (grid, candidate), [])
    if candidateCEs:
      siteCEMapping[candidate] = candidateCEs
    else:
      gLogger.debug('No CEs defined for site %s' % candidate)

  return S_OK(siteCEMapping)

#############################################################################


def getCESiteMapping(gridName=None):
  """ Returns a dictionary of all CEs and their associated site, e.g.
      {'ce101.cern.ch':'LCG.CERN.ch', ...]}
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  sites = getSites(gridName=gridName)
  if not sites['OK']:
    return sites

  ceSiteMapping = {}
  for candidate in sites['Value']:
    grid = candidate.split('.')[0]
    siteCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' % (grid, candidate), [])
    for ce in siteCEs:
      if ce in ceSiteMapping:
        current = ceSiteMapping[ce]
        gLogger.error('CE %s already has a defined site %s but it is also defined for %s' % (ce, current, candidate))
      else:
        ceSiteMapping[ce] = candidate

  return S_OK(ceSiteMapping)

#############################################################################


def getSiteForCE(computingElement):
  """ Given a Grid CE name this method returns the DIRAC site name.

      WARNING: if two or more sites happen to have the same ceName/queueName, then only the first found is returned
  """
  sites = getSites()
  if not sites['OK']:
    return sites

  for candidate in sites['Value']:
    grid = candidate.split('.')[0]
    siteCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' % (grid, candidate), [])
    if computingElement in siteCEs:
      finalSite = candidate
      return S_OK(finalSite)
  # FIXME: this is strange but this was how it was coded
  return S_OK('')

#############################################################################


def getCEsForSite(siteName):
  """ Given a DIRAC site name this method returns a list of corresponding CEs.
  """
  if not re.search('.', siteName):
    return S_ERROR('%s is not a valid site name' % siteName)
  gridName = siteName.split('.')[0]
  return S_OK(gConfig.getValue('/Resources/Sites/%s/%s/CE' % (gridName, siteName), []))

#############################################################################


def getQueueInfo(ceUniqueID, diracSiteName=''):
  """
    Extract information from full CE Name including associate DIRAC Site
  """
  try:
    subClusterUniqueID = ceUniqueID.split('/')[0].split(':')[0]
    queueID = ceUniqueID.split('/')[1]
  except BaseException:
    return S_ERROR('Wrong full queue Name')

  if not diracSiteName:
    gLogger.debug("SiteName not given, looking in /LocaSite/Site")
    diracSiteName = gConfig.getValue('/LocalSite/Site', '')

    if not diracSiteName:
      gLogger.debug("Can't find LocalSite name, looking in CS")
      result = getSiteForCE(subClusterUniqueID)
      if not result['OK']:
        return result
      diracSiteName = result['Value']

      if not diracSiteName:
        gLogger.error('Can not find corresponding Site in CS')
        return S_ERROR('Can not find corresponding Site in CS')

  gridType = diracSiteName.split('.')[0]

  siteCSSEction = '/Resources/Sites/%s/%s/CEs/%s' % (gridType, diracSiteName, subClusterUniqueID)
  queueCSSection = '%s/Queues/%s' % (siteCSSEction, queueID)

  resultDict = {'SubClusterUniqueID': subClusterUniqueID,
                'QueueID': queueID,
                'SiteName': diracSiteName,
                'Grid': gridType,
                'SiteCSSEction': siteCSSEction,
                'QueueCSSection': queueCSSection}

  return S_OK(resultDict)
