"""
  This module contains helper methods for accessing operational attributes or parameters of DMS objects

"""

from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from Core.Utilities import SitesDIRACGOCDBmapping

LOCAL = 1
PROTOCOL = 2
DOWNLOAD = 3

def _resolveSEGroup( seGroupList ):
  seList = []
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


class CSHelpers():

  def __init__( self ):
    self.local = LOCAL
    self.protocol = PROTOCOL
    self.download = DOWNLOAD
    self.siteSEMapping = {}
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
    for grid in gridTypes:
      sites = gConfig.getSections( '/Resources/Sites/%s' % grid )
      if not sites['OK']:
        gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
        return sites
      for candidate in sites['Value']:
        candidateSEs = gConfig.getValue( '/Resources/Sites/%s/%s/SE' % ( grid, candidate ), [] )
        if candidateSEs:
          siteSEMapping.setdefault( self.local, {} ).setdefault( candidate, set() ).add( candidateSEs )
        else:
          gLogger.debug( 'No SEs defined for site %s' % candidate )

    # Add Sites from the SiteLocalSEMapping in the CS
    cfgLocalSEPath = cfgPath( 'SiteLocalSEMapping' )
    result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if not result['OK']:
      cfgLocalSEPath = cfgPath( 'SiteSEMappingByProtocol' )
      result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if result['OK']:
      sites = result['Value']
      for site in sites:
        ses = self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] )
        siteSEMapping.setdefault( self.protocol, {} ).setdefault( site, set() ).update( ses )

    # Add Sites from the SiteLocalSEMapping in the CS
    cfgLocalSEPath = cfgPath( 'SiteSEMappingByDownload' )
    result = self.__opsHelper.getOptionsDict( cfgLocalSEPath )
    if result['OK']:
      sites = result['Value']
      for site in sites:
        ses = self.__opsHelper.getValue( cfgPath( cfgLocalSEPath, site ), [] )
        siteSEMapping.setdefault( self.download, {} ).setdefault( site, set() ).update( ses )

    self.siteSEMapping = siteSEMapping
    return S_OK( siteSEMapping )


  def isSEFailover( self, storageElement ):
    seList = self.__opsHelper.getValue( 'DataManagement/SEsUsedForFailover', [] )
    return storageElement in _resolveSEGroup( seList )

  def isSEForJobs( self, storageElement ):
    seList = self.__opsHelper.getValue( 'DataManagement/SEsNotToBeUsedForJobs', [] )
    return storageElement not in _resolveSEGroup( seList )

  def isSEArchive( self, storageElement ):
    seList = self.__opsHelper.getValue( 'DataManagement/SEsUsedForArchive', [] )
    return storageElement in _resolveSEGroup( seList )

  def getSitesForSE( self, storageElement, connectionLevel = None ):
    if not connectionLevel:
      connectionLevel = self.download
    if connectionLevel == self.local:
      return self.getLocalSitesForSE( storageElement )
    if connectionLevel == self.protocol:
      return self.getProtocolSitesForSE( storageElement )
    if connectionLevel == self.download:
      return self.getDownloadSitesForSE( storageElement )
    return S_ERROR( "Unknown connection level, connectionLevel" )

  def getLocalSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    mapping = mapping['Value'][self.local]
    sites = set( [site for site in mapping if mapping[site] == se] )
    return S_OK( sorted( sites ) )

  def getProtocolSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    mapping = mapping['Value'][self.protocol]
    sites = self.getLocalSitesForSE( se )
    if not sites['OK']:
      return sites
    sites = set( sites['Value'] )
    sites = sites.update( [site for site in mapping if mapping[site] == se] )
    return S_OK( sorted( sites ) )

  def getDownloadSitesForSE( self, se ):
    mapping = self.getSiteSEMapping()
    if not mapping['OK']:
      return mapping
    mapping = mapping['Value'][self.protocol]
    sites = self.getProtocolSitesForSE( se )
    if not sites['OK']:
      return sites
    sites = set( sites['Value'] )
    sites = sites.update( [site for site in mapping if mapping[site] == se] )
    return S_OK( sorted( sites ) )
