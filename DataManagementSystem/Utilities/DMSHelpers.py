"""
  This module contains helper methods for accessing operational attributes or parameters of DMS objects

"""

from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
# from Core.Utilities import SitesDIRACGOCDBmapping

LOCAL = 1
PROTOCOL = 2
DOWNLOAD = 3

def resolveSEGroup( seGroupList ):
  seList = []
  if type( seGroupList ) != type( [] ):
    seGroupList = [seGroupList]
  for se in seGroupList:
    seConfig = gConfig.getValue( '/Resources/StorageElementGroups/%s' % se, se )
    if seConfig != se:
      seList += [se.strip() for se in seConfig.split( ',' )]
      # print seList
    else:
      seList.append( se )
    res = gConfig.getSections( '/Resources/StorageElements' )
    if not res['OK']:
      gLogger.fatal( 'Error getting list of SEs from CS', res['Message'] )
      return []
    for se in seList:
      if se not in res['Value']:
        gLogger.fatal( '%s is not a valid SE' % se )
        seList = []
        break

  return seList


class DMSHelpers():

  def __init__( self ):
    self.siteSEMapping = {}
    self.storageElementSet = set()
    self.siteSet = set()
    self.__opsHelper = Operations()


  def getSiteSEMapping( self ):
    """ Returns a dictionary of all sites and their localSEs as a list, e.g.
        {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
        If gridName is specified, result is restricted to that Grid type.
    """
    if self.siteSEMapping:
      return S_OK( self.siteSEMapping )

    siteSEMapping = {}
    gridTypes = gConfig.getSections( 'Resources/Sites/' )
    if not gridTypes['OK']:
      gLogger.warn( 'Problem retrieving sections in /Resources/Sites' )
      return gridTypes

    gridTypes = gridTypes['Value']

    gLogger.debug( 'Grid Types are: %s' % ( ', '.join( gridTypes ) ) )
    # Get a list of sites and their local SEs
    siteSet = set()
    storageElementSet = set()
    siteSEMapping[LOCAL] = {}
    for grid in gridTypes:
      result = gConfig.getSections( '/Resources/Sites/%s' % grid )
      if not result['OK']:
        gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
        return result
      sites = result['Value']
      siteSet.update( sites )
      for site in sites:
        candidateSEs = gConfig.getValue( '/Resources/Sites/%s/%s/SE' % ( grid, site ), [] )
        if candidateSEs:
          siteSEMapping[LOCAL].setdefault( site, set() ).update( candidateSEs )
          storageElementSet.update( candidateSEs )
        else:
          gLogger.debug( 'No SEs defined for site %s' % site )

    # Add Sites from the SiteSEMappingByProtocol in the CS
    siteSEMapping[PROTOCOL] = {}
    cfgLocalSEPath = cfgPath( 'SiteSEMappingByProtocol' )
    result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if result['OK']:
      sites = result['Value']
      for site in sites:
        candidates = self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] )
        ses = set( candidates )
        # If a candidate is a site, then all local SEs are eligible
        for candidate in ses & siteSet:
          ses.remove( candidate )
          ses.update( siteSEMapping[LOCAL][candidate] )
        siteSEMapping[PROTOCOL].setdefault( site, set() ).update( ses )

    # Add Sites from the SiteSEMappingByDownload in the CS, else SiteLocalSEMapping (old convention)
    siteSEMapping[DOWNLOAD] = {}
    cfgLocalSEPath = cfgPath( 'SiteSEMappingByDownload' )
    result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if not result['OK']:
      cfgLocalSEPath = cfgPath( 'SiteLocalSEMapping' )
      result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if result['OK']:
      sites = result['Value']
      for site in sites:
        candidates = self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] )
        ses = set( candidates )
        for candidate in ses & siteSet:
          ses.remove( candidate )
          ses.update( siteSEMapping[LOCAL][candidate] )
        siteSEMapping[ DOWNLOAD].setdefault( site, set() ).update( ses )

    self.siteSEMapping = siteSEMapping
    self.storageElementSet = storageElementSet
    self.siteSet = siteSet
    return S_OK( siteSEMapping )


  def isSEFailover( self, storageElement ):
    self.getSiteSEMapping()
    if storageElement not in self.storageElementSet:
      return False
    seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsUsedForFailover', [] ) )
    return storageElement in resolveSEGroup( seList )

  def isSEForJobs( self, storageElement ):
    self.getSiteSEMapping()
    if storageElement not in self.storageElementSet:
      return False
    seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsNotToBeUsedForJobs', [] ) )
    return storageElement not in resolveSEGroup( seList )

  def isSEArchive( self, storageElement ):
    self.getSiteSEMapping()
    if storageElement not in self.storageElementSet:
      return False
    seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsUsedForArchive', [] ) )
    return storageElement in resolveSEGroup( seList )

  def getSitesForSE( self, storageElement, connectionLevel = None ):
    if connectionLevel in None:
      connectionLevel = 'DOWNLOAD'
    if isinstance( connectionLevel, basestring ):
      connectionLevel = connectionLevel.upper()
    if connectionLevel == 'LOCAL':
      return self._getLocalSitesForSE( storageElement )
    if connectionLevel == 'PROTOCOL':
      return self.getProtocolSitesForSE( storageElement )
    if connectionLevel == 'DOWNLOAD':
      return self.getDownloadSitesForSE( storageElement )
    return S_ERROR( "Unknown connection level" )

  def getLocalSiteForSE( self, se ):
    sites = self._getLocalSitesForSE( se )
    if not sites['OK']:
      return sites
    return S_OK( sites['Value'][0] )

  def _getLocalSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    if se not in self.storageElementSet:
      return S_ERROR( 'Non-existing SE' )
    mapping = mapping['Value'][LOCAL]
    sites = [site for site in mapping if se in mapping[site]]
    if len( sites ) != 1:
      return S_ERROR( 'SE is at more than one site' )
    return S_OK( sites )

  def getProtocolSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    if se not in self.storageElementSet:
      return S_ERROR( 'Non-existing SE' )
    mapping = mapping['Value'][PROTOCOL]
    sites = self._getLocalSitesForSE( se )
    if not sites['OK']:
      return sites
    sites = set( sites['Value'] )
    sites.update( [site for site in mapping if se in mapping[site]] )
    return S_OK( sorted( sites ) )

  def getDownloadSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    if se not in self.storageElementSet:
      return S_ERROR( 'Non-existing SE' )
    mapping = mapping['Value'][DOWNLOAD]
    sites = self.getProtocolSitesForSE( se )
    if not sites['OK']:
      return sites
    sites = set( sites['Value'] )
    sites.update( [site for site in mapping if se in mapping[site]] )
    return S_OK( sorted( sites ) )

  def getSEsAtSite( self, site ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    if site not in self.siteSet:
      site = None
      for s in self.siteSet:
        if '.%s.' in s:
          site = s
          break
    if site is None:
      return S_ERROR( "Unknown site" )
    ses = mapping['Value'][LOCAL].get( site, [] )
    if not ses:
      return S_ERROR( 'No SE at site' )
    return S_OK( sorted( ses ) )

  def getSEInGroupAtSite( self, seGroup, site ):
    if type( seGroup ) == type( '' ):
      seList = gConfig.getValue( '/Resources/StorageElementGroups/%s' % seGroup, [] )
    else:
      seList = list( seGroup )
    if not seList:
      return S_ERROR( 'SEGroup does not exist' )
    sesAtSite = self.getSEsAtSite( site )
    if not sesAtSite['OK']:
      return sesAtSite
    sesAtSite = sesAtSite['Value']
    se = set( seList ) & set( sesAtSite )
    if not se:
      gLogger.warn( 'No SE found at that site', 'in group %s at %s' % ( seGroup, site ) )
      return S_OK()
    return S_OK( list( se )[0] )
