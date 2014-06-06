########################################################################
# $HeadURL$
# File :   SiteCEMapping.py
########################################################################

"""  The SiteCEMapping module performs the necessary CS gymnastics to
     resolve site and CE combinations.  These manipulations are necessary
     in several components.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSiteFullNames, \
                                                               getSiteForResource, getSiteName

#############################################################################
def getSiteCEMapping():
  """ Returns a dictionary of all sites and their CEs as a list, e.g.
      {'LCG.CERN.ch':['ce101.cern.ch',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteCEMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleSites()
  if not result['OK']:
    return result
  sites = result['Value']
  
  for site in sites:
    result = resourceHelper.getEligibleResources( 'Computing', {'Site':site} )
    if not result['OK']:
      continue
    ceList = result['Value']
    
    result = getSiteFullNames( site )
    if not result['OK']:
      continue
    for sName in result['Value']:
      siteCEMapping[sName] = ceList   

  return S_OK( siteCEMapping )

#############################################################################
def getCESiteMapping():
  """ Returns a dictionary of all CEs and their associated site, e.g.
      {'ce101.cern.ch':'LCG.CERN.ch', ...]}
  """
  ceSiteMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleResources( 'Computing' )
  if not result['OK']:
    return result
  ceList = result['Value']
  for ce in ceList:
    result = getSiteForCE( ce )
    if not result['OK']:
      continue
    site = result['Value']
    ceSiteMapping[ce] = site

  return S_OK( ceSiteMapping )

#############################################################################
def getSiteForCE( computingElement ):
  """ Given a Grid CE name this method returns the DIRAC site name.
  """
  result = getSiteForResource( computingElement )
  if not result['OK']:
    return result
  site = result['Value']
  result = getSiteFullNames( site )
  if not result['OK']:
    return result
  siteFullName = result['Value'][0]

  return S_OK( siteFullName )

#############################################################################
def getCEsForSite( siteName ):
  """ Given a DIRAC site name this method returns a list of corresponding CEs.
  """
  resourceHelper = Resources()
  result = resourceHelper.getEligibleResources( 'Computing', {'Site':siteName} )
  if not result['OK']:
    return result
  ceList = result['Value']
  
  return S_OK( ceList )  

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
  
  resourceHelper = Resources()
  result = getSiteName( diracSiteName )
  site = result['Value']
  domain = result.get( 'Domain', 'Unknonw' )
  country = result.get( 'Country', 'xx' )
  
  result = resourceHelper.getQueueOptionsDict( site, subClusterUniqueID, queueID )
  if not result['OK']:
    return result
  queueDict = result['Value']
  maxCPUTime = queueDict.get( 'maxCPUTime', 0 )
  SI00 = queueDict.get( 'SI00', 0 ) 
  
  if not maxCPUTime or not SI00:
    result = resourceHelper.getComputingOptionsDict( site, subClusterUniqueID )
    if not result['OK']:
      return result
    ceDict = result['Value']
    if not maxCPUTime:
      maxCPUTime = ceDict.get( 'maxCPUTime', 0 )
    if not SI00:
      SI00 = ceDict.get( 'SI00', 0 )   

  resultDict = { 'SubClusterUniqueID': subClusterUniqueID,
                 'QueueID': queueID,
                 'SiteName': diracSiteName,
                 'Domain': domain,
                 'Country': country,
                 'maxCPUTime': maxCPUTime,
                 'SI00': SI00 }

  return S_OK( resultDict )
