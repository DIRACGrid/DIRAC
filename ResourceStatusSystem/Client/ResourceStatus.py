# $HeadURL: $
""" ResourceStatus

  Module use to switch between the CS and the RSS.

"""

from DIRAC                                                 import S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                   import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers              import Resources
from DIRAC.ResourceStatusSystem.Client.SiteStatus          import SiteStatus
from DIRAC.ResourceStatusSystem.Utilities.ElementStatus    import ElementStatus
from DIRAC.ResourceStatusSystem.Utilities.RSSCache         import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration

__RCSID__ = '$Id: $'

class ResourceStatus( ElementStatus ):
  """
  ResourceStatus helper that connects to CS if RSS flag is not Active. It keeps
  the connection to the db / server as an object member, to avoid creating a new
  one massively.
  """

  __metaclass__ = DIRACSingleton
  
  def __init__( self ):
    """
    Constructor, initializes the logger, rssClient and caches.

    examples
      >>> resourceStatus = ResourceStatus()
    """

    super( ResourceStatus, self ).__init__()
    
    self.siteStatus = SiteStatus()
    
    # We can set CacheLifetime and CacheHistory from CS, so that we can tune them.
    cacheLifeTime = int( RssConfiguration().getConfigCache() )
    
    # RSSCaches, one per elementType ( StorageElement, ComputingElement )
    # Should be generated on the fly, instead of being hardcoded ?
    self.seCache = RSSCache( 'Storage', cacheLifeTime, self._updateSECache )
    self.ceCache = RSSCache( 'Computing', cacheLifeTime, self._updateCECache )

  #.............................................................................
  # ComputingElement methods

  def getComputingStatuses( self, ceNames, statusTypes = None ):
    """
    Method that queries the RSSCache for ComputingElement-Status-related information.
    If any of the inputs is None, it is interpreted as * ( all ).
    If match is positive, the output looks like:
      {
        computingElementA : { statusType1 : status1, statusType2 : status2 },
        computingElementB : { statusType1 : status1, statusType2 : status2 },
      }
    There are ALWAYS the same keys inside the site dictionaries.
    
    examples:
      >>> resourceStatus.getComputingStatuses( 'ce207.cern.ch', None )
          S_OK( { 'ce207.cern.ch' : { 'all' : 'Active' } } )
      >>> resourceStatus.getComputingStatuses( 'RubbishCE', None )
          S_ERROR( ... )
      >>> resourceStaus.getComputingStatuses( 'ce207.cern.ch', 'all' )
          S_OK( { 'ce207.cern.ch' : { 'all' : 'Active' } } )
      >>> resourceStatus.getComputingStatuses( [ 'ce206.cern.ch', 'ce207.cern.ch' ], 'all' )
          S_OK( { 'ce206.cern.ch' : { 'all' : 'Active' },
                  'ce207.cern.ch' : { 'all' : 'Active' } } )
      >>> resourceStatus.getComputingStatuses( None, 'all' )
          S_OK( { 'ce206.cern.ch' : { 'all' : 'Active' },
                  'ce207.cern.ch' : { 'all' : 'Active' },
                  ... } )

    :Parameters:
      **ceNames** - [ None, `string`, `list` ]
        name(s) of the computing elements to be matched
      **statusTypes** - [ None, `string`, `list` ]
        name(s) of the statusTypes to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    cacheMatch = self.ceCache.match( ceNames, statusTypes )
    if not cacheMatch[ 'OK' ]:
      return cacheMatch
    
    cacheMatch = cacheMatch[ 'Value' ]
    
    for ceName, ceDict in cacheMatch.items():
      
      if not self.__getSiteAccess( ceName, 'ComputingAccess' )[ 'OK' ]:
        
        cacheMatch[ ceName ] = dict( zip( ceDict.keys(), [ 'Banned' ] * len( ceDict ) ) )
          
    return S_OK( cacheMatch )

  def getComputingStatus( self, ceName, statusType ):
    """
    Given a ce and a statusType, it returns its status from the cache.
    
    examples:
      >>> resourceStatus.getComputingStatus( 'ce207.cern.ch', 'all' )
          S_OK( 'Active' )
      >>> resourceStatus.getComputingStatus( 'ce207.cern.ch', None )
          S_ERROR( ... )

    :Parameters:
      **ceName** - `string`
        name of the computing element to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
  
    return self.getElementStatus( 'Computing', ceName, statusType )
  
  def isUsableComputing( self, ceName, statusType ):
    """
    Similar method to getComputingStatus. The difference is the output.
    Given a ce name, returns a bool if the ce is usable:
    status is Active or Degraded outputs True
    anything else outputs False
    
    examples:
      >>> resourceStatus.isUsableComputing( 'ce207.cern.ch', 'all' )
          True
      >>> resourceStatus.isUsableComputing( 'ce207.cern.ch', 'all' )
          False # May be banned
      >>> resourceStatus.isUsableComputing( 'ce207.cern.ch', None )
          False
      >>> resourceStatus.isUsableComputing( 'RubbishCE', 'all' )
          False
      >>> resourceStatus.isUsableComputing( 'ce207.cern.ch', 'RubbishAccess' )
          False
    
    :Parameters:
      **ceName** - `string`
        name of the computing element to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    return self.isUsableElement( 'Computing', ceName, statusType )

  def getUsableComputings( self, statusType ):
    """
    For a given statusType, returns all computing elements that are usable: their
    status for that particular statusType is either Active or Degraded; in a list.
    
    examples:
      >>> resourceStatus.getUsableComputings( 'all' )
          S_OK( [ 'ce206.cern.ch', 'ce207.cern.ch',... ] )
      >>> resourceStatus.getUsableComputings( None )
          S_ERROR( ... )
      >>> resourceStatus.getUsableComputings( 'RubbishAccess' )
          S_ERROR( ... )
    
    :Parameters:
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    return self.getUsableElements( 'Computing', statusType )

  #.............................................................................
  # StorageElement methods

  def getStorageStatuses( self, seNames, statusTypes = None ):
    """
    Method that queries the RSSCache for StorageElement-Status-related information.
    If any of the inputs is None, it is interpreted as * ( all ).
    If match is positive, the output looks like:
    {
      storageElementA : { statusType1 : status1, statusType2 : status2 },
      storageElementB : { statusType1 : status1, statusType2 : status2 },
    }
    There are ALWAYS the same keys inside the site dictionaries.
    
    examples:
      >>> resourceStatus.getStorageStatuses( 'CERN-USER', None )
          S_OK( { 'CERN-USER' : { 'ReadAccess' : 'Active', 'WriteAccess' : 'Degraded',... } } )
      >>> resourceStatus.getStorageStatuses( 'RubbishCE', None )
          S_ERROR( ... )
      >>> resourceStaus.getStorageStatuses( 'CERN-USER', 'ReadAccess' )
          S_OK( { 'CERN-USER' : { 'ReadAccess' : 'Active' } } )
      >>> resourceStatus.getStorageStatuses( [ 'CERN-USER', 'PIC-USER' ], 'ReadAccess' )
          S_OK( { 'CERN-USER' : { 'ReadAccess' : 'Active' },
                  'PIC-USER' : { 'ReadAccess' : 'Active' } } )
      >>> resourceStatus.getStorageStatuses( None, 'ReadAccess' )
          S_OK( { 'CERN-USER' : { 'ReadAccess' : 'Active' },
                  'PIC-USER' : { 'ReadAccess' : 'Active' },
                  ... } )

    :Parameters:
      **seNames** - [ None, `string`, `list` ]
        name(s) of the storage elements to be matched
      **statusTypes** - [ None, `string`, `list` ]
        name(s) of the statusTypes to be matched
        
    :return: S_OK() || S_ERROR()
    """
    
    cacheMatch = self.seCache.match( seNames, statusTypes )
    if not cacheMatch[ 'OK' ]:
      return cacheMatch
    
    cacheMatch = cacheMatch[ 'Value' ]
    
    for seName, seDict in cacheMatch.items():
      
      if not self.__getSiteAccess( seName, 'StorageAccess' )[ 'OK' ]:
        
        cacheMatch[ seName ] = dict( zip( seDict.keys(), [ 'Banned' ] * len( seDict ) ) )
          
    return S_OK( cacheMatch )


  def getStorageStatus( self, seName, statusType ):
    """
    Given a se and a statusType, it returns its status from the cache.
    
    examples:
      >>> resourceStatus.getComputingElementStatus( 'CERN-USER', 'ReadAccess' )
          S_OK( 'Active' )
      >>> resourceStatus.getComputingElementStatus( 'CERN-USER', None )
          S_ERROR( ... )
    
    :Parameters:
      **seName** - `string`
        name of the storage element to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
  
    return self.getElementStatus( 'Storage', seName, statusType )
  
  def isUsableStorage( self, seName, statusType ):
    """
    Similar method to getStorageStatus. The difference is the output.
    Given a se name, returns a bool if the se is usable:
    status is Active or Degraded outputs True
    anything else outputs False
    
    examples:
      >>> resourceStatus.isUsableStorage( 'CERN-USER', 'ReadAccess' )
          True
      >>> resourceStatus.isUsableStorage( 'CERN-ARCHIVE', 'ReadAccess' )
          False # May be banned
      >>> resourceStatus.isUsableStorage( 'CERN-USER', None )
          False
      >>> resourceStatus.isUsableStorage( 'RubbishCE', 'ReadAccess' )
          False
      >>> resourceStatus.isUsableStorage( 'CERN-USER', 'RubbishAccess' )
          False
    
    :Parameters:
      **seName** - `string`
        name of the storage element to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    return self.isUsableElement( 'Storage', seName, statusType )

  def getUsableStorages( self, statusType ):
    """
    For a given statusType, returns all storage elements that are usable: their
    status for that particular statusType is either Active or Degraded; in a list.
    
    examples:
      >>> resourceStatus.getUsableStorages( 'ReadAccess' )
          S_OK( [ 'CERN-USER', 'PIC-USER',... ] )
      >>> resourceStatus.getUsableStorages( None )
          S_ERROR( ... )
      >>> resourceStatus.getUsableStorages( 'RubbishAccess' )
          S_ERROR( ... )
    
    :Parameters:
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    return self.getUsableElements( 'Storage', statusType )
  
  
  #.............................................................................
  # update Cache methods

  def _updateCECache( self ):
    """
    Method used to update the ComputingElementCache.
    """
    return self.__updateCache( 'Computing' )

  
  def _updateSECache( self ):
    """
    Method used to update the StorageElementCache.
    """
    return self.__updateCache( 'Storage' )
    
          
  #.............................................................................
  # Private methods
  

  def __updateCache( self, elementType ):

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }
    rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                    elementType = elementType,
                                                    meta = meta )
    
    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( self.getCacheDictFromRawData( rawCache[ 'Value' ] ) )  
  
  
  def __getSiteAccess( self, elementName, siteAccess ):
    """
    Method that given a resourceType and an elementName, finds the site name
    that owes it. Once that is done, the site access <siteAccess> is checked
    and returned.
    
    :Parameters:
      **resourceType** - `string`
        name of the resource type ( StorageElement, ComputingElement.. )
      **elementName** - `string`
        name of the resource of type <resourceType>
      **siteAccess** - `string`
        site access ( StorageAccess, ComputingAccess .. )
        
    :return: S_OK() || S_ERROR()
    """
    
    siteName = Resources.getSiteForResource( elementName )
    if not siteName[ 'OK' ]:
      return siteName
    siteName = siteName[ 'Value' ]
    
    if not self.siteStatus.isUsableSite( siteName, siteAccess ):
      return S_ERROR( 'Site %s is not usable for Computing' % siteName )
    
    return S_OK()
  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF