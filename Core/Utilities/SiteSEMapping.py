########################################################################
# $HeadURL$
# File :   SiteSEMapping.py
########################################################################

"""  The SiteSEMapping module performs the necessary CS gymnastics to
     resolve site and SE combinations.  These manipulations are necessary
     in several components.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSiteFullNames

#############################################################################
def getSiteSEMapping():
  """ Returns a dictionary of all sites and their localSEs as a list, e.g.
      {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteSEMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleSites()
  if not result['OK']:
    return result
  sites = result['Value']
  
  for site in sites:
    result = resourceHelper.getEligibleResources( 'Storage', {'Site':site} )
    if not result['OK']:
      continue
    seList = result['Value']
    
    result = getSiteFullNames( site )
    if not result['OK']:
      continue
    for sName in result['Value']:
      siteSEMapping[sName] = seList   

  # Add Sites from the SiteToLocalSEMapping in the CS
  opsHelper = Operations()
  result = opsHelper.getSiteMapping( 'Storage', 'LocalSE' )
  if result['OK']:
    mapping = result['Value']
    for site in mapping:
      if site not in siteSEMapping:
        siteSEMapping[site] = mapping[site]
      else:  
        for se in mapping[site]:
          if se not in siteSEMapping[site]:
            siteSEMapping[site].append( se )

  return S_OK( siteSEMapping )

#############################################################################
def getSESiteMapping( gridName = '' ):
  """ Returns a dictionary of all SEs and their associated site(s), e.g.
      {'CERN-RAW':'LCG.CERN.ch','CERN-RDST':'LCG.CERN.ch',...]}
      Although normally one site exists for a given SE, it is possible over all
      Grid types to have multiple entries.
      If gridName is specified, result is restricted to that Grid type.
  """
  seSiteMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleResources( 'Storage' )
  if not result['OK']:
    return result
  seList = result['Value']
  for se in seList:
    result = getSitesForSE( se )
    if not result['OK']:
      continue
    site = result['Value']
    seSiteMapping[se] = site

  return S_OK( seSiteMapping )

#############################################################################
def getSitesForSE( storageElement, gridName = '' ):
  """ Given a DIRAC SE name this method returns a list of corresponding sites.
      Optionally restrict to Grid specified by name.
  """

  result = getSiteSEMapping( gridName )
  if not result['OK']:
    return result

  mapping = result['Value']

  finalSites = []

  for site in mapping:
    if storageElement in mapping[site]:
      finalSites.append( site )
  return S_OK( finalSites )


#############################################################################
def getSEsForSite( siteName ):
  """ Given a DIRAC site name this method returns a list of corresponding SEs.
  """
  result = getSiteSEMapping()
  if not result['OK']:
    return result

  mapping = result['Value']
  if siteName in mapping:
    return S_OK( mapping[siteName] )

  return S_OK( [] )

#############################################################################
def isSameSiteSE( se1, se2 ):
  """ Check if the 2 SEs are from the same site
  """
  if se1 == se2:
    return S_OK( True )

  result = getSitesForSE( se1 )
  if not result['OK']:
    return result
  sites1 = result['Value']
  result = getSitesForSE( se2 )
  if not result['OK']:
    return result
  sites2 = result['Value']

  for site in sites1:
    if site in sites2:
      return S_OK( True )

  return S_OK( False )

#############################################################################
def getSEsForCountry( country ):
  """ Determines the associated SEs from the country code
  """
  mappedCountries = [country]
  opsHelper = Operations()
  while True:
    mappedCountry = opsHelper.getValue( '/Countries/%s/AssignedTo' % country, country )
    if mappedCountry == country:
      break
    elif mappedCountry in mappedCountries:
      return S_ERROR( 'Circular mapping detected for %s' % country )
    else:
      country = mappedCountry
      mappedCountries.append( mappedCountry )
  res = opsHelper.getOptionsDict( '/Countries/%s/AssociatedSEs' % country )
  if not res['OK']:
    return S_ERROR( 'Failed to obtain AssociatedSEs for %s' % country )
  return S_OK( res['Value'].values() )

