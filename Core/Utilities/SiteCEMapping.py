########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/SiteCEMapping.py,v 1.1 2008/07/21 10:14:26 paterson Exp $
# File :   SiteCEMapping.py
########################################################################

"""  The SiteCEMapping module performs the necessary CS gymnastics to
     resolve site and CE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id: SiteCEMapping.py,v 1.1 2008/07/21 10:14:26 paterson Exp $"

import string,re

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################
def getSiteCEMapping(gridName=''):
  """ Returns a dictionary of all sites and their CEs as a list, e.g.
      {'LCG.CERN.ch':['ce101.cern.ch',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteCEMapping = {}
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
      candidateCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' %(grid,candidate),[])
      if candidateCEs:
        siteCEMapping[candidate]=candidateCEs
      else:
        gLogger.debug('No CEs defined for site %s' %candidate)

  return S_OK(siteCEMapping)

#############################################################################
def getCESiteMapping(gridName=''):
  """ Returns a dictionary of all CEs and their associated site, e.g.
      {'ce101.cern.ch':'LCG.CERN.ch', ...]}
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  ceSiteMapping = {}
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
        siteCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' %(grid,candidate),[])
        for ce in siteCEs:
          if ceSiteMapping.has_key(ce):
            current = ceSiteMapping[ce]
            current.append(candidate)
            ceSiteMapping[ce]=current
          else:
            ceSiteMapping[ce]=candidate

  return S_OK(ceSiteMapping)

#############################################################################
def getSiteForCE(computingElement):
  """ Given a Grid CE name this method returns the DIRAC site name.
  """
  finalSite = ''
  gridTypes = gConfig.getSections('/Resources/Sites/',[])
  if not gridTypes['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites')
    return gridTypes

  gridTypes = gridTypes['Value']
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' %grid,[])
    if not sites['OK']:
      gLogger.warn('Problem retrieving /Resources/Sites/%s section' %grid)
      return sites
    if sites:
      siteList = sites['Value']
      for candidate in siteList:
        siteCEs = gConfig.getValue('/Resources/Sites/%s/%s/CE' %(grid,candidate),[])
        if computingElement in siteCEs:
          finalSite = candidate
          break

  return S_OK(finalSite)

#############################################################################
def getCEsForSite(siteName):
  """ Given a DIRAC site name this method returns a list of corresponding CEs.
  """
  if not re.search('.',siteName):
    return S_ERROR('%s is not a valid site name' %siteName)
  gridName = string.split(siteName,'.')[0]
  siteSection = '/Resources/Sites/%s/%s/CE' %(gridName,siteName)
  ces = gConfig.getValue(siteSection,[])
  return S_OK(ces)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#