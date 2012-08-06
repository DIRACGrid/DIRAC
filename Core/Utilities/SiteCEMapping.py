########################################################################
# $HeadURL$
# File :   SiteCEMapping.py
########################################################################

"""  The SiteCEMapping module performs the necessary CS gymnastics to
     resolve site and CE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id$"

import re

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################
def getSiteCEMapping( gridName = '' ):
  """ Returns a dictionary of all sites and their CEs as a list, e.g.
      {'LCG.CERN.ch':['ce101.cern.ch',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteCEMapping = {}
  gridTypes = gConfig.getSections( 'Resources/Sites/', [] )
  if not gridTypes['OK']:
    gLogger.warn( 'Problem retrieving sections in /Resources/Sites' )
    return gridTypes

  gridTypes = gridTypes['Value']
  if gridName:
    if not gridName in gridTypes:
      return S_ERROR( 'Could not get sections for /Resources/Sites/%s' % gridName )
    gridTypes = [gridName]

  gLogger.debug( 'Grid Types are: %s' % ( ', '.join( gridTypes ) ) )
  for grid in gridTypes:
    sites = gConfig.getSections( '/Resources/Sites/%s' % grid, [] )
    if not sites['OK']:
      gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
      return sites
    for candidate in sites['Value']:
      candidateCEs = gConfig.getValue( '/Resources/Sites/%s/%s/CE' % ( grid, candidate ), [] )
      if candidateCEs:
        siteCEMapping[candidate] = candidateCEs
      else:
        gLogger.debug( 'No CEs defined for site %s' % candidate )

  return S_OK( siteCEMapping )

#############################################################################
def getCESiteMapping( gridName = '' ):
  """ Returns a dictionary of all CEs and their associated site, e.g.
      {'ce101.cern.ch':'LCG.CERN.ch', ...]}
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  ceSiteMapping = {}
  gridTypes = gConfig.getSections( '/Resources/Sites/', [] )
  if not gridTypes['OK']:
    gLogger.warn( 'Problem retrieving sections in /Resources/Sites' )
    return gridTypes

  gridTypes = gridTypes['Value']
  if gridName:
    if not gridName in gridTypes:
      return S_ERROR( 'Could not get sections for /Resources/Sites/%s' % gridName )
    gridTypes = [gridName]

  gLogger.debug( 'Grid Types are: %s' % ( ', '.join( gridTypes ) ) )
  for grid in gridTypes:
    sites = gConfig.getSections( '/Resources/Sites/%s' % grid, [] )
    if not sites['OK']: #gConfig returns S_ERROR for empty sections until version
      gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
      return sites
    if sites:
      for candidate in sites['Value']:
        siteCEs = gConfig.getValue( '/Resources/Sites/%s/%s/CE' % ( grid, candidate ), [] )
        for ce in siteCEs:
          if ceSiteMapping.has_key( ce ):
            current = ceSiteMapping[ce]
            gLogger.warn( 'CE %s already has a defined site %s but it is also defined for %s' % ( ce, current, candidate ) )
          else:
            ceSiteMapping[ce] = candidate

  return S_OK( ceSiteMapping )

#############################################################################
def getSiteForCE( computingElement ):
  """ Given a Grid CE name this method returns the DIRAC site name.
  """
  finalSite = ''
  gridTypes = gConfig.getSections( '/Resources/Sites/', [] )
  if not gridTypes['OK']:
    gLogger.warn( 'Problem retrieving sections in /Resources/Sites' )
    return gridTypes

  gridTypes = gridTypes['Value']
  for grid in gridTypes:
    sites = gConfig.getSections( '/Resources/Sites/%s' % grid, [] )
    if not sites['OK']:
      gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
      return sites
    if sites:
      siteList = sites['Value']
      for candidate in siteList:
        siteCEs = gConfig.getValue( '/Resources/Sites/%s/%s/CE' % ( grid, candidate ), [] )
        if computingElement in siteCEs:
          finalSite = candidate
          break

  return S_OK( finalSite )

#############################################################################
def getCEsForSite( siteName ):
  """ Given a DIRAC site name this method returns a list of corresponding CEs.
  """
  if not re.search( '.', siteName ):
    return S_ERROR( '%s is not a valid site name' % siteName )
  gridName = siteName.split( '.' )[0]
  siteSection = '/Resources/Sites/%s/%s/CE' % ( gridName, siteName )
  ces = gConfig.getValue( siteSection, [] )
  return S_OK( ces )

#############################################################################
def getQueueInfo( ceUniqueID ):
  """
    Extract information from full CE Name including associate DIRAC Site
  """
  try:
    subClusterUniqueID = ceUniqueID.split( '/' )[0].split( ':' )[0]
    queueID = ceUniqueID.split( '/' )[1]
  except:
    return S_ERROR( 'Wrong full queue Name' )

  result = getSiteForCE( subClusterUniqueID )
  if not result['OK']:
    return result
  diracSiteName = result['Value']

  if not diracSiteName:
    return S_ERROR( 'Can not find corresponding Site in CS' )

  gridType = diracSiteName.split( '.' )[0]

  siteCSSEction = '/Resources/Sites/%s/%s/CEs/%s' % ( gridType, diracSiteName, subClusterUniqueID )
  queueCSSection = '%s/Queues/%s' % ( siteCSSEction, queueID )

  resultDict = { 'SubClusterUniqueID': subClusterUniqueID,
                 'QueueID': queueID,
                 'SiteName': diracSiteName,
                 'Grid': gridType,
                 'SiteCSSEction': siteCSSEction,
                 'QueueCSSection': queueCSSection }

  return S_OK( resultDict )
