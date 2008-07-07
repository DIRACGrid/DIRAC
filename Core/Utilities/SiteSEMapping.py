########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/SiteSEMapping.py,v 1.1 2008/07/07 21:28:59 paterson Exp $
# File :   SiteSEMapping.py
########################################################################

"""  The SiteSEMapping module performs the necessary CS gymnastics to
     resolve site and SE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id: SiteSEMapping.py,v 1.1 2008/07/07 21:28:59 paterson Exp $"

import string,re

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################
def getSiteSEMapping(gridName=''):
  """ Returns a dictionary of all sites and their localSEs as a list, e.g.
      {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteSEMapping = {}
  gridTypes = gConfig.getSections('Resources/Sites/',[])
  if not gridTypes['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites')
    return gridTypes

  gridTypes = gridTypes['Value']
  if gridName:
    if not gridName in gridTypes:
      return S_ERROR('Could not get sections for /Resources/Sites/%s' %gridName)
    gridTypes = [gridName]

  gLogger.debug('Grid Types are: %s' %(string.join(gridTypes,', ')))
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' %grid,[])
    if not sites['OK']:
      gLogger.warn('Problem retrieving /Resources/Sites/%s section' %grid)
      return sites
    for candidate in sites['Value']:
      candidateSEs = gConfig.getValue('/Resources/Sites/%s/%s/SE' %(grid,candidate),[])
      if candidateSEs:
        siteSEMapping[candidate]=candidateSEs
      else:
        gLogger.debug('No SEs defined for site %s' %candidate)

  return S_OK(siteSEMapping)

#############################################################################
def getSESiteMapping(gridName=''):
  """ Returns a dictionary of all SEs and their associated site(s), e.g.
      {'CERN-RAW':'LCG.CERN.ch','CERN-RDST':'LCG.CERN.ch',...]}
      Although normally one site exists for a given SE, it is possible over all
      Grid types to have
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  seSiteMapping = {}
  gridTypes = gConfig.getSections('/Resources/Sites/',[])
  if not gridTypes['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites')
    return gridTypes

  gridTypes = gridTypes['Value']
  if gridName:
    if not gridName in gridTypes:
      return S_ERROR('Could not get sections for /Resources/Sites/%s' %gridName)
    gridTypes = [gridName]

  gLogger.debug('Grid Types are: %s' %(string.join(gridTypes,', ')))
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' %grid,[])
    if not sites['OK']: #gConfig returns S_ERROR for empty sections until version
      gLogger.warn('Problem retrieving /Resources/Sites/%s section' %grid)
      return sites
    if sites:
      for candidate in sites['Value']:
        siteSEs = gConfig.getValue('/Resources/Sites/%s/%s/SE' %(grid,candidate),[])
        for se in siteSEs:
          if seSiteMapping.has_key(se):
            current = seSiteMapping[se]
            current.append(candidate)
            seSiteMapping[se]=current
          else:
            seSiteMapping[se]=candidate

  return S_OK(seSiteMapping)

#############################################################################
def getSitesForSE(storageElement):
  """ Given a DIRAC SE name this method returns a list of corresponding sites.
  """
  finalSites = []
  gridTypes = gConfig.getSections('/Resources/Sites/',[])
  if not gridTypes['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites')
    return gridTypes

  gridTypes = gridTypes['Value']
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' %grid,[])
    if not sites['OK']: #gConfig returns S_ERROR for empty sections until version
      gLogger.warn('Problem retrieving /Resources/Sites/%s section' %grid)
      return sites
    if sites:
      siteList = sites['Value']
      for candidate in siteList:
        siteSEs = gConfig.getValue('/Resources/Sites/%s/%s/SE' %(grid,candidate),[])
        if storageElement in siteSEs:
          finalSites.append(candidate)

  return S_OK(finalSites)

#############################################################################
def getSEsForSite(siteName):
  """ Given a DIRAC site name this method returns a list of corresponding SEs.
  """
  if not re.search('.',siteName):
    return S_ERROR('%s is not a valid site name' %siteName)
  gridName = string.split(siteName,'.')[0]
  siteSection = '/Resources/Sites/%s/%s/SE' %(gridName,siteName)
  ses = gConfig.getValue(siteSection,[])
  return S_OK(ses)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#