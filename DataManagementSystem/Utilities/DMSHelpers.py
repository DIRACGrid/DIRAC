"""
  This module contains helper methods for accessing operational attributes or parameters of DMS objects

"""

from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
# from Core.Utilities import SitesDIRACGOCDBmapping

LOCAL = 1
PROTOCOL = LOCAL + 1
DOWNLOAD = PROTOCOL + 1

def resolveSEGroup( seGroupList, allSEs = None ):
  """
  Resolves recursively a (list of) SEs that can be groupSEs

  :param seGroupList: list of SEs to resolve or comma-separated SEs
  :type seGroupList: list or string
  :param allSEs: if provised, list of all known SEs
  :type allSEs: list

  :return : list of resolved SEs or [] if error
  """
  if allSEs is None:
    res = gConfig.getSections( '/Resources/StorageElements' )
    if not res['OK']:
      gLogger.fatal( 'Error getting list of SEs from CS', res['Message'] )
      return []
    allSEs = res['Value']
  seList = []
  if isinstance( seGroupList, basestring ):
    seGroupList = [se.strip() for se in seGroupList.split( ',' ) if se.strip()]
  for se in seGroupList:
    seConfig = gConfig.getValue( '/Resources/StorageElementGroups/%s' % se, se )
    if seConfig != se:
      newSEs = [se1.strip() for se1 in seConfig.split( ',' ) if se1.strip()]
      # print seList
    else:
      newSEs = [se]
    for se1 in list( newSEs ):
      if se1 not in allSEs:
        # Here means se is not a group and is not an SE either, fatal!
        if se1 == se:
          gLogger.fatal( '%s is not a valid SE' % se1 )
          return []
        # If not an SE, it may be a group
        recursive = resolveSEGroup( se1, allSEs = allSEs )
        if not recursive:
          return []
        newSEs += recursive
    seList += newSEs

  return seList

def siteGridName( site ):
  """ Returns the Grid name for a site"""
  if not isinstance( site, basestring ):
    return None
  siteSplit = site.split( '.' )
  if len( siteSplit ) < 3:
    return None
  return siteSplit[0]

def siteCountryName( site ):
  """ Returns the Grid name for a site"""
  if not isinstance( site, basestring ):
    return None
  siteSplit = site.split( '.' )
  if len( siteSplit ) < 3:
    return None
  return site.split( '.' )[-1].lower()

def _getConnectionIndex( connectionLevel, default = None ):
  if connectionLevel is None:
    connectionLevel = default
  if isinstance( connectionLevel, ( int, long ) ):
    return connectionLevel
  if isinstance( connectionLevel, basestring ):
    connectionLevel = connectionLevel.upper()
  return {'LOCAL':LOCAL, 'PROTOCOL':PROTOCOL, 'DOWNLOAD':DOWNLOAD}.get( connectionLevel )


class DMSHelpers( object ):

  def __init__( self, vo = False ):
    self.siteSEMapping = {}
    self.storageElementSet = set()
    self.siteSet = set()
    self.__opsHelper = Operations( vo = vo )
    self.failoverSEs = None
    self.archiveSEs = None
    self.notForJobSEs = None


  def getSiteSEMapping( self ):
    """ Returns a dictionary of all sites and their localSEs as a list, e.g.
        {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
    """
    if self.siteSEMapping:
      return S_OK( self.siteSEMapping )

    # Get the list of SEs and keep a mapping of those using an Alias or a BaseSE
    storageElements = gConfig.getSections( 'Resources/StorageElements' )
    if not storageElements['OK']:
      gLogger.warn( 'Problem retrieving storage elements', storageElements['Message'] )
      return storageElements
    storageElements = storageElements['Value']
    equivalentSEs = {}
    for se in storageElements:
      for option in ( 'BaseSE', 'Alias' ):
        originalSE = gConfig.getValue( 'Resources/StorageElements/%s/%s' % ( se, option ) )
        if originalSE:
          equivalentSEs.setdefault( originalSE, [] ).append( se )
          break

    siteSEMapping = {}
    gridTypes = gConfig.getSections( 'Resources/Sites/' )
    if not gridTypes['OK']:
      gLogger.warn( 'Problem retrieving sections in /Resources/Sites', gridTypes['Message'] )
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
          candidateSEs += [eqSE for se in candidateSEs for eqSE in equivalentSEs.get( se, [] )]
          siteSEMapping[LOCAL].setdefault( site, set() ).update( candidateSEs )
          storageElementSet.update( candidateSEs )

    # Add Sites from the SiteSEMappingByProtocol in the CS
    siteSEMapping[PROTOCOL] = {}
    cfgLocalSEPath = cfgPath( 'SiteSEMappingByProtocol' )
    result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if result['OK']:
      sites = result['Value']
      for site in sites:
        candidates = set( self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] ) )
        ses = set( resolveSEGroup( candidates - siteSet ) ) | ( candidates & siteSet )
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
        candidates = set( self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] ) )
        ses = set( resolveSEGroup( candidates - siteSet ) ) | ( candidates & siteSet )
        # If a candidate is a site, then all local SEs are eligible
        for candidate in ses & siteSet:
          ses.remove( candidate )
          ses.update( siteSEMapping[LOCAL][candidate] )
        siteSEMapping[DOWNLOAD].setdefault( site, set() ).update( ses )

    self.siteSEMapping = siteSEMapping
    # Add storage elements that may not be associated with a site
    result = gConfig.getSections( '/Resources/StorageElements' )
    if not result['OK']:
      gLogger.warn( 'Problem retrieving /Resources/StorageElements section', result['Message'] )
      return result
    self.storageElementSet = storageElementSet | set( result['Value'] )
    self.siteSet = siteSet
    return S_OK( siteSEMapping )

  def getSites( self ):
    self.getSiteSEMapping()
    return sorted( self.siteSet )

  def getTiers( self, withStorage = False, tier = None ):
    sites = sorted( self.getShortSiteNames( withStorage = withStorage, tier = tier ).values() )
    if sites and isinstance( sites[0], list ):
      # List of lists, flatten it
      sites = [s for sl in sites for s in sl]
    return sites

  def getShortSiteNames( self, withStorage = True, tier = None ):
    siteDict = {}
    result = self.getSiteSEMapping()
    if result['OK']:
      for site in self.siteSEMapping[LOCAL] if withStorage else self.siteSet:
        grid, shortSite, _country = site.split( '.' )
        if isinstance( tier, ( int, long ) ) and ( grid != 'LCG' or gConfig.getValue( '/Resources/Sites/%s/%s/MoUTierLevel' % ( grid, site ), 999 ) != tier ):
          continue
        if isinstance( tier, ( list, tuple, dict, set ) ) and ( grid != 'LCG' or gConfig.getValue( '/Resources/Sites/%s/%s/MoUTierLevel' % ( grid, site ), 999 ) not in tier ):
          continue
        if withStorage or tier is not None:
          siteDict[shortSite] = site
        else:
          siteDict.setdefault( shortSite, [] ).append( site )
    return siteDict

  def getStorageElements( self ):
    self.getSiteSEMapping()
    return sorted( self.storageElementSet )

  def isSEFailover( self, storageElement ):
    if self.failoverSEs is None:
      seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsUsedForFailover', [] ) )
      self.failoverSEs = resolveSEGroup( seList )
    # FIXME: remove string test at some point
    return storageElement in self.failoverSEs or ( not self.failoverSEs and isinstance( storageElement, basestring ) and 'FAILOVER' in storageElement.upper() )

  def isSEForJobs( self, storageElement, checkSE = True ):
    if checkSE:
      self.getSiteSEMapping()
      if storageElement not in self.storageElementSet:
        return False
    if self.notForJobSEs is None:
      seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsNotToBeUsedForJobs', [] ) )
      self.notForJobSEs = resolveSEGroup( seList )
    return storageElement not in self.notForJobSEs

  def isSEArchive( self, storageElement ):
    if self.archiveSEs is None:
      seList = resolveSEGroup( self.__opsHelper.getValue( 'DataManagement/SEsUsedForArchive', [] ) )
      self.archiveSEs = resolveSEGroup( seList )
    # FIXME: remove string test at some point
    return storageElement in self.archiveSEs or ( not self.archiveSEs and isinstance( storageElement, basestring ) and 'ARCHIVE' in storageElement.upper() )

  def getSitesForSE( self, storageElement, connectionLevel = None ):
    connectionIndex = _getConnectionIndex( connectionLevel, default = DOWNLOAD )
    if connectionIndex == LOCAL:
      return self._getLocalSitesForSE( storageElement )
    if connectionIndex == PROTOCOL:
      return self.getProtocolSitesForSE( storageElement )
    if connectionIndex == DOWNLOAD:
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
      if self.__opsHelper.getValue( 'DataManagement/ForceSingleSitePerSE', True ):
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

  def getSEsForSite( self, site, connectionLevel = None ):
    connectionIndex = _getConnectionIndex( connectionLevel, default = DOWNLOAD )
    if connectionIndex is None:
      return S_ERROR( "Unknown connection level" )
    if not self.siteSet:
      self.getSiteSEMapping()
    if site not in self.siteSet:
      siteList = [s for s in self.siteSet if '.%s.' % site in s]
    else:
      siteList = [site]
    if not siteList:
      return S_ERROR( "Unknown site" )
    return self._getSEsForSItes( siteList, connectionIndex = connectionIndex )

  def _getSEsForSItes( self, siteList, connectionIndex ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    ses = []
    for index in range( LOCAL, connectionIndex + 1 ):
      for site in siteList:
        ses += mapping['Value'][index].get( site, [] )
    if not ses:
      return S_ERROR( 'No SE found' )
    return S_OK( sorted( ses ) )

  def getSEsAtSite( self, site ):
    return self.getSEsForSite( site, connectionLevel = LOCAL )

  def isSameSiteSE( self, se1, se2 ):
    res = self.getLocalSiteForSE( se1 )
    if not res['OK']:
      return res
    site1 = res['Value']
    res = self.getLocalSiteForSE( se2 )
    if not res['OK']:
      return res
    site2 = res['Value']
    return S_OK( site1 == site2 )

  def getSEsAtCountry( self, country, connectionLevel = None ):
    connectionIndex = _getConnectionIndex( connectionLevel, default = DOWNLOAD )
    if connectionIndex is None:
      return S_ERROR( "Unknown connection level" )
    if not self.siteSet:
      self.getSiteSEMapping()
    siteList = [site for site in self.siteSet if siteCountryName( site ) == country.lower()]
    if not siteList:
      return S_ERROR( "No SEs found in country" )
    return self._getSEsForSItes( siteList, connectionIndex )

  def getSEInGroupAtSite( self, seGroup, site ):
    if isinstance( seGroup, basestring ):
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

  def getRegistrationProtocols( self ):
    """ Returns the Favorite registration protocol defined in the CS, or 'srm' as default """
    return self.__opsHelper.getValue( 'DataManagement/RegistrationProtocols', ['srm', 'dips'] )

  def getThirdPartyProtocols( self ):
    """ Returns the Favorite third party protocol defined in the CS, or 'srm' as default """
    return self.__opsHelper.getValue( 'DataManagement/ThirdPartyProtocols', ['srm'] )

  def getAccessProtocols( self ):
    """ Returns the Favorite access protocol defined in the CS, or 'srm' as default """
    return self.__opsHelper.getValue( 'DataManagement/AccessProtocols', ['srm', 'dips'] )


  def getWriteProtocols( self ):
    """ Returns the Favorite Write protocol defined in the CS, or 'srm' as default """
    return self.__opsHelper.getValue( 'DataManagement/WriteProtocols', ['srm', 'dips'] )

