# $HeadURL$
""" SiteStatus helper

  Provides methods to easily interact with the RSS

"""

# DIRAC
from DIRAC                                                  import S_OK
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.ResourceStatusSystem.Utilities.ElementStatus     import ElementStatus
from DIRAC.ResourceStatusSystem.Utilities.RSSCache          import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration
from DIRAC.ConfigurationSystem.Client.Helpers.Resources     import getSiteNamesDict, getSiteFullNames

__RCSID__ = '$Id: $'

class SiteStatus( ElementStatus ):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.
  
  It provides four methods to interact with the site statuses:
  * getSiteStatuses
  * getSiteStatus 
  * isUsableSite  
  * getUsableSites
  """
  
  __metaclass__ = DIRACSingleton
  
  def __init__( self ):
    """
    Constructor, initializes the logger, rssClient and cache.
    
    examples
      >>> siteStatus = SiteStatus()
    """
    
    super( SiteStatus, self ).__init__()
    
    # RSSCache initialization
    cacheLifeTime   = int( RssConfiguration().getConfigCache() )
    self.siteCache  = RSSCache( 'Site', cacheLifeTime, self.__updateSiteCache )


  def getSiteStatuses( self, siteNames, statusTypes = None ):
    """
    Method that queries the RSSCache for Site-Status-related information. If any
    of the inputs is None, it is interpreted as * ( all ).
    
    If match is positive, the output looks like:
    { 
     siteA : { statusType1 : status1, statusType2 : status2 },
     siteB : { statusType1 : status1, statusType2 : status2 },
    }
    
    There are ALWAYS the same keys inside the site dictionaries.
    
    examples
      >>> siteStatus.getSiteStatuses( 'LCG.CERN.ch', None )
          S_OK( { 'LCG.CERN.ch' : { 'ComputingAccess' : 'Active', 'StorageAccess' : 'Degraded' } }  )
      >>> siteStatus.getSiteStatuses( 'RubbishSite', None )
          S_ERROR( ... )            
      >>> siteStaus.getSiteStatuses( 'LCG.CERN.ch', 'ComputingAccess' )
          S_OK( { 'LCG.CERN.ch' : { 'ComputingAccess' : 'Active' } }  )    
      >>> siteStatus.getSiteStatuses( [ 'LCG.CERN.ch', 'LCG.IN2P3.fr' ], 'ComputingAccess' )
          S_OK( { 'LCG.CERN.ch'  : { 'ComputingAccess' : 'Active' },
                  'LCG.IN2P3.fr' : { 'ComputingAccess' : 'Active' } }  )    
      >>> siteStatus.getSiteStatuses( None, 'ComputingAccess' )
          S_OK( { 'LCG.CERN.ch'  : { 'ComputingAccess' : 'Active' },
                  'LCG.IN2P3.fr' : { 'ComputingAccess' : 'Active' },
                  ... }  )            

    :Parameters:
      **siteNames** - [ None, `string`, `list` ]
        name(s) of the sites to be matched
      **statusTypes** - [ None, `string`, `list` ]
        name(s) of the statusTypes to be matched
    
    :return: S_OK() || S_ERROR()                 
    """
    
    siteDict = {}
    
    self.log.debug( 'getSiteStatus' )
    
    if siteNames is not None:
      translatedNames = getSiteNamesDict( siteNames )
      if not translatedNames[ 'OK' ]:
        return translatedNames
      siteDict  = translatedNames[ 'Value' ]
      siteNames = list( set( siteDict.values() ) )  
    
    result = self.siteCache.match( siteNames, statusTypes )
    if not result['OK']:
      return result
    
    self.log.debug( result )
    if not siteDict:
      # We do not have to translate back the site names
      return result
    
    resultDict = {}
    for key in siteDict.iterkeys():
      resultDict[ key ] = result[ 'Value' ][ siteDict[ key ] ]
      
    return S_OK( resultDict )
  

  def getSiteStatus( self, siteName, statusType ):
    """
    Given a site and a statusType, it returns its status from the cache.
    
    examples
      >>> siteStatus.getSiteStatus( 'LCG.CERN.ch', 'StorageAccess' )
          S_OK( 'Active' )
      >>> siteStatus.getSiteStatus( 'LCG.CERN.ch', None )
          S_ERROR( ... )
    
    :Parameters:
      **siteName** - `string`
        name of the site to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
  
    return self.getElementStatus( 'Site', siteName, statusType )

  def isUsableSite( self, siteName, statusType ):
    """
    Similar method to getSiteStatus. The difference is the output.
    Given a site name, returns a bool if the site is usable: 
      status is Active or Degraded outputs True
      anything else outputs False
    
    examples
      >>> siteStatus.isUsableSite( 'LCG.CERN.ch', 'StorageAccess' )
          True
      >>> siteStatus.isUsableSite( 'LCG.CERN.ch', 'ComputingAccess' )
          False # May be banned   
      >>> siteStatus.isUsableSite( 'LCG.CERN.ch', None )
          False    
      >>> siteStatus.isUsableSite( 'RubbishSite', 'StorageAccess' )
          False
      >>> siteStatus.isUsableSite( 'LCG.CERN.ch', 'RubbishAccess' )
          False        
    
    :Parameters:
      **siteName** - `string`
        name of the site to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()    
    """
    
    return self.isUsableElement( 'Site', siteName, statusType )

  def getUsableSites( self, statusType ):
    """
    For a given statusType, returns all sites that are usable: their status
    for that particular statusType is either Active or Degraded; in a list.
    
    examples
      >>> siteStatus.getUsableSites( 'ComputingAccess' )
          S_OK( [ 'LCG.CERN.ch', 'LCG.IN2P3.fr',... ] )
      >>> siteStatus.getUsableSites( None )
          S_ERROR( ... )
      >>> siteStatus.getUsableSites( 'RubbishAccess' )
          S_ERROR( ... )    
    
    :Parameters:
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    result = self.getUsableElements( 'Site', statusType )
    if not result['OK']:
      return result
    
    resultList = []
    for site in result['Value']:
      resultNames = getSiteFullNames( site )
      if result['OK']:
        resultList += resultNames['Value']
        
    return S_OK( resultList )    
  
  def getUnusableSites( self, statusType ):
    """
    For a given statusType, returns all sites that are usable: their status
    for that particular statusType is either Banned or Probing; in a list.
    
    examples
      >>> siteStatus.getUnusableSites( 'ComputingAccess' )
          S_OK( [ 'LCG.CERN.ch', 'LCG.IN2P3.fr',... ] )
      >>> siteStatus.getUnsusableSites( None )
          S_ERROR( ... )
      >>> siteStatus.getUnusableSites( 'RubbishAccess' )
          S_ERROR( ... )    
    
    :Parameters:
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    result = self.getUnusableElements( 'Site', statusType )
    if not result['OK']:
      return result

    resultList = []
    for site in result['Value']:
      resultNames = getSiteFullNames( site )
      if result['OK']:
        resultList += resultNames['Value']
        
    return S_OK( resultList )    
 
  #.............................................................................
  # Private methods
 
  def __updateSiteCache( self ):
    """
      Method used to update the SiteCache.
    """   

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }   
    rawCache  = self.rssClient.selectStatusElement( 'Site', 'Status', meta = meta )
    
    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( self.getCacheDictFromRawData( rawCache[ 'Value' ] ) )
  
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF