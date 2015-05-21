"""  The SiteSEMapping module performs the necessary CS gymnastics to
     resolve site and SE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


#############################################################################
def getSiteSEMapping( gridName = '', withSiteLocalSEMapping = False ):
  """ Returns a dictionary of all sites and their localSEs as a list, e.g.
      {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  result = DMSHelpers().getSiteSEMapping()
  if not result['OK']:
    return result
  if withSiteLocalSEMapping:
    mapping = result['Value'][2]
  else:
    mapping = result['Value'][1]
  if gridName:
    for site in mapping.keys():
      if site.split( '.' )[0] != gridName:
        del mapping[site]
  return S_OK( mapping )


#############################################################################
def getSESiteMapping( gridName = '', withSiteLocalSEMapping = False ):
  """ Returns a dictionary of all SEs and their associated site(s), e.g.
      {'CERN-RAW':'LCG.CERN.ch','CERN-RDST':'LCG.CERN.ch',...]}
      Although normally one site exists for a given SE, it is possible over all
      Grid types to have multiple entries.
      If gridName is specified, result is restricted to that Grid type.
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  dmsHelper = DMSHelpers()
  storageElements = dmsHelper.getStorageElements()
  return S_OK( dict( ( se,
                      getSitesForSE( se, gridName = gridName,
                                    withSiteLocalSEMapping = withSiteLocalSEMapping ).get( 'Value', [] ) ) \
                    for se in storageElements ) )

#############################################################################
def getSitesForSE( storageElement, gridName = '', withSiteLocalSEMapping = False ):
  """ Given a DIRAC SE name this method returns a list of corresponding sites.
      Optionally restrict to Grid specified by name.
  """

  result = DMSHelpers().getSitesForSE( storageElement, connectionLevel = 'DOWNLOAD' if withSiteLocalSEMapping else 'LOCAL' )
  if not result['OK'] or not gridName:
    return result

  return S_OK( [site for site in result['Value'] if site.split( '.' )[0] == gridName] )


#############################################################################
def getSEsForSite( siteName, withSiteLocalSEMapping = False ):
  """ Given a DIRAC site name this method returns a list of corresponding SEs.
  """
  return DMSHelpers().getSEsForSite( siteName, connectionLevel = 'DOWNLOAD' if withSiteLocalSEMapping else 'LOCAL' )

#############################################################################
def isSameSiteSE( se1, se2 ):
  """ Check if the 2 SEs are at the same site
  """
  dmsHelper = DMSHelpers()
  site1 = dmsHelper.getLocalSiteForSE( se1 ).get( 'Value' )
  site2 = dmsHelper.getLocalSiteForSE( se2 ).get( 'Value' )
  return site1 and site2 and site1 == site2

#############################################################################
def getSEsForCountry( country ):
  """ Determines the associated SEs from the country code
  """
  return DMSHelpers().getSEsAtCountry( country )
