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
      
      if not self.__getSiteAccess( 'Computing', ceName, 'ComputingAccess' )[ 'OK' ]:
        
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
      
      if not self.__getSiteAccess( 'Storage', seName, 'StorageAccess' )[ 'OK' ]:
        
        cacheMatch[ seName ] = dict( zip( seDict.keys(), [ 'Banned' ] * len( seDict ) ) )
          
    return S_OK( cacheMatch )

  #FIXME: rename to getStorageElementStatus
  def getStorageElementStatus( self, seName, statusType ):
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
  # Private methods
  
  def __getSiteAccess( self, resourceType, elementName, siteAccess ):
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
    
    siteName = Resources.getSiteForResource( resourceType, elementName )
    if not siteName[ 'OK' ]:
      return siteName
    siteName = siteName[ 'Value' ]
    
    if not self.siteStatus.isUsableSite( siteName, siteAccess ):
      return S_ERROR( 'Site %s is not usable for Computing' % siteName )
    
    return S_OK()

  #.............................................................................
  #.............................................................................
  #.............................................................................
  #.............................................................................
  # Old code, to be deleted / refactored soon.
            
# def getStorageElementStatus( self, elementName, statusType = None ):
# """
# Helper with dual access, tries to get information from the RSS for the given
# StorageElement, otherwise, it gets it from the CS.
#
# example:
# >>> getStorageElementStatus( 'CERN-USER', 'Read' )
# S_OK( { 'CERN-USER' : { 'Read': 'Active' } } )
# >>> getStorageElementStatus( 'CERN-USER', 'Write' )
# S_OK( { 'CERN-USER' : {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'}} )
# >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
# S_ERROR( xyz.. )
# >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
# S_OK( 'Unknown' )
#
# """
#
# if self.__getMode():
# # We do not apply defaults. If is not on the cache, S_ERROR is returned.
# return self.__getRSSStorageElementStatus( elementName, statusType )
# else:
# return self.__getCSStorageElementStatus( elementName, statusType )

  # FIXME: to be deleted !!! ONLY RSS ( scripts, agents and web portal ) should set statuses
# def setStorageElementStatus( self, elementName, statusType, status, reason = None,
# tokenOwner = None ):
#
# """
# Helper with dual access, tries set information in RSS and in CS.
#
# example:
# >>> getStorageElementStatus( 'CERN-USER', 'Read' )
# S_OK( { 'Read': 'Active' } )
# >>> getStorageElementStatus( 'CERN-USER', 'Write' )
# S_OK( {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'} )
# >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
# S_ERROR( xyz.. )
# >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' )
# S_OK( 'Unknown' )
# """
#
# #if self.__getMode():
# #return self.__setRSSStorageElementStatus( elementName, statusType, status, reason, tokenOwner )
# #else:
# # return self.__setCSStorageElementStatus( elementName, statusType, status )

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

 
  def __updateCache( self, elementType ):

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }
    rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                    elementType = elementType,
                                                    meta = meta )
    
    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( self.getCacheDictFromRawData( rawCache[ 'Value' ] ) )
 
  #.............................................................................
  #.............................................................................
  #.............................................................................
  #.............................................................................
  # TODO : delete all this
  
# def __getRSSStorageElementStatus( self, elementName, statusType ):
# """
# Gets from the cache or the RSS the StorageElements status. The cache is a
# copy of the DB table. If it is not on the cache, most likely is not going
# to be on the DB.
#
# There is one exception: item just added to the CS, e.g. new StorageElement.
# The period between it is added to the DB and the changes are propagated
# to the cache will be inconsisten, but not dangerous. Just wait <cacheLifeTime>
# minutes.
# """
#
# siteAccess = self.__getSiteAccess( 'StorageElement', elementName, 'StorageAccess' )
# if not siteAccess[ 'OK' ]:
# self.log.error( siteAccess[ 'Message' ] )
# return siteAccess
#
# cacheMatch = self.seCache.match( elementName, statusType )
#
# self.log.debug( '__getRSSStorageElementStatus' )
# self.log.debug( cacheMatch )
#
# return cacheMatch

# def __getCSStorageElementStatus( self, elementName, statusType, default = None ):
# """
# Gets from the CS the StorageElements status
# """
#
# cs_path = "/Resources/StorageElements"
#
# if not isinstance( elementName, list ):
# elementName = [ elementName ]
#
# statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
#
# result = {}
# for element in elementName:
#
# if statusType is not None:
# # Added Active by default
# res = gConfig.getOption( "%s/%s/%s" % ( cs_path, element, statusType ), 'Active' )
# if res[ 'OK' ] and res[ 'Value' ]:
# result[ element ] = { statusType : res[ 'Value' ] }
#
# else:
# res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
# if res[ 'OK' ] and res[ 'Value' ]:
# elementStatuses = {}
# for elementStatusType, value in res[ 'Value' ].items():
# if elementStatusType in statuses:
# elementStatuses[ elementStatusType ] = value
#
# # If there is no status defined in the CS, we add by default Read and
# # Write as Active.
# if elementStatuses == {}:
# elementStatuses = { 'ReadAccess' : 'Active', 'WriteAccess' : 'Active' }
#
# result[ element ] = elementStatuses
#
# if result:
# return S_OK( result )
#
# if default is not None:
#
# # sec check
# if statusType is None:
# statusType = 'none'
#
# defList = [ [ el, statusType, default ] for el in elementName ]
# return S_OK( getDictFromList( defList ) )
#
# _msg = "StorageElement '%s', with statusType '%s' is unknown for CS."
# return S_ERROR( _msg % ( elementName, statusType ) )

# def __setRSSStorageElementStatus( self, elementName, statusType, status, reason, tokenOwner ):
# """
# Sets on the RSS the StorageElements status
# """
#
# expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )
#
# self.seCache.acquireLock()
# try:
# res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName,
# statusType = statusType, status = status,
# reason = reason, tokenOwner = tokenOwner,
# tokenExpiration = expiration )
# if res[ 'OK' ]:
# self.seCache.refreshCache()
#
# if not res[ 'OK' ]:
# _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, status )
# gLogger.warn( 'RSS: %s' % _msg )
#
# return res
#
# finally:
# # Release lock, no matter what.
# self.seCache.releaseLock()

# def __setCSStorageElementStatus( self, elementName, statusType, status ):
# """
# Sets on the CS the StorageElements status
# """
#
# statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
# if not statusType in statuses:
# gLogger.error( "%s is not a valid statusType" % statusType )
# return S_ERROR( "%s is not a valid statusType: %s" % ( statusType, statuses ) )
#
# csAPI = CSAPI()
#
# cs_path = "/Resources/StorageElements"
#
# csAPI.setOption( "%s/%s/%s" % ( cs_path, elementName, statusType ), status )
#
# res = csAPI.commitChanges()
# if not res[ 'OK' ]:
# gLogger.warn( 'CS: %s' % res[ 'Message' ] )
#
# return res

# def __getMode( self ):
# """
# Get's flag defined ( or not ) on the RSSConfiguration. If defined as 1,
# we use RSS, if not, we use CS.
# """
#
# res = self.rssConfig.getConfigState()
#
# if res == 'Active':
#
# if self.rssClient is None:
# self.rssClient = ResourceStatusClient()
# return True
#
# self.rssClient = None
# return False

################################################################################

#def getDictFromList( fromList ):
# '''
# Auxiliary method that given a list returns a dictionary of dictionaries:
# { site1 : { statusType1 : st1, statusType2 : st2 }, ... }
# '''
#
# res = {}
# for listElement in fromList:
# site, sType, status = listElement
# if not res.has_key( site ):
# res[ site ] = {}
# res[ site ][ sType ] = status
# return res
 
#def getCacheDictFromRawData( rawList ):
# """
# Formats the raw data list, which we know it must have tuples of three elements.
# ( element1, element2, element3 ) into a list of tuples with the format
# ( ( element1, element2 ), element3 ). Then, it is converted to a dictionary,
# which will be the new Cache.
#
# It happens that element1 is elementName, element2 is statusType and element3
# is status.
#
# :Parameters:
# **rawList** - `list`
# list of three element tuples [( element1, element2, element3 ),... ]
#
# :return: dict of the form { ( elementName, statusType ) : status, ... }
# """
#
# res = [ ( ( name, sType ), status ) for name, sType, status in rawList ]
# return dict( res )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF