# $HeadURL:  $
""" ResourceStatus

  Module use to switch between the CS and the RSS.

"""

import datetime

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton 
from DIRAC.ConfigurationSystem.Client.CSAPI                 import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations 
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCache          import RSSCache 
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration

__RCSID__  = '$Id: $'

class ResourceStatus( object ):
  """
  ResourceStatus helper that connects to CS if RSS flag is not Active. It keeps
  the connection to the db / server as an object member, to avoid creating a new
  one massively.
  """

  __metaclass__ = DIRACSingleton
  
  def __init__( self ):
    """
    Constructor, initializes the rssClient.
    """

    self.log        = gLogger.getSubLogger( self.__class__.__name__ )
    self.rssConfig  = RssConfiguration()
    self.__opHelper = Operations()    
    self.rssClient  = None 
    
    # We can set CacheLifetime and CacheHistory from CS, so that we can tune them.
    cacheLifeTime   = int( self.rssConfig.getConfigCache() )
    
    # RSSCache only affects the calls directed to RSS, if using the CS it is not
    # used.  
    self.seCache   = RSSCache( 'StorageElement', cacheLifeTime, self.__updateSECache )       
            
  def getStorageElementStatus( self, elementName, statusType = None, default = None ):
    """
    Helper with dual access, tries to get information from the RSS for the given
    StorageElement, otherwise, it gets it from the CS. 
    
    example:
      >>> getStorageElementStatus( 'CERN-USER', 'Read' )
          S_OK( { 'CERN-USER' : { 'Read': 'Active' } } )
      >>> getStorageElementStatus( 'CERN-USER', 'Write' )
          S_OK( { 'CERN-USER' : {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'}} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' ) 
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' ) 
          S_OK( 'Unknown' ) 
    
    """
  
    if self.__getMode():
      # We do not apply defaults. If is not on the cache, S_ERROR is returned.
      return self.__getRSSStorageElementStatus( elementName, statusType )
    else:
      return self.__getCSStorageElementStatus( elementName, statusType, default )

  def setStorageElementStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):
  
    """
    Helper with dual access, tries set information in RSS and in CS. 
    
    example:
      >>> getStorageElementStatus( 'CERN-USER', 'Read' )
          S_OK( { 'Read': 'Active' } )
      >>> getStorageElementStatus( 'CERN-USER', 'Write' )
          S_OK( {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' ) 
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' ) 
          S_OK( 'Unknown' ) 
    """
  
    if self.__getMode():
      return self.__setRSSStorageElementStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return self.__setCSStorageElementStatus( elementName, statusType, status )

################################################################################

  def __updateSECache( self ):
    """
      Method used to update the StorageElementCache.
    """   

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }   
    rawCache  = self.rssClient.selectStatusElement( 'Resource', 'Status', 
                                                    elementType = 'StorageElement', 
                                                    meta = meta )
    
    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )
 
################################################################################
  
  def __getRSSStorageElementStatus( self, elementName, statusType ):
    """
    Gets from the cache or the RSS the StorageElements status. The cache is a
    copy of the DB table. If it is not on the cache, most likely is not going
    to be on the DB.
    
    There is one exception: item just added to the CS, e.g. new StorageElement.
    The period between it is added to the DB and the changes are propagated
    to the cache will be inconsisten, but not dangerous. Just wait <cacheLifeTime>
    minutes.
    """
  
    cacheMatch = self.seCache.match( elementName, statusType )

    self.log.debug( '__getRSSStorageElementStatus' )
    self.log.debug( cacheMatch )
    
    return cacheMatch

  def __getCSStorageElementStatus( self, elementName, statusType, default ):
    """
    Gets from the CS the StorageElements status
    """
  
    cs_path     = "/Resources/StorageElements"
  
    if not isinstance( elementName, list ):
      elementName = [ elementName ]

    statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
       
    result = {}
    for element in elementName:
    
      if statusType is not None:
        # Added Active by default
        res = gConfig.getOption( "%s/%s/%s" % ( cs_path, element, statusType ), 'Active' )
        if res[ 'OK' ] and res[ 'Value' ]:
          result[ element ] = { statusType : res[ 'Value' ] }
        
      else:
        res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          elementStatuses = {}
          for elementStatusType, value in res[ 'Value' ].items():
            if elementStatusType in statuses:
              elementStatuses[ elementStatusType ] = value
          
          # If there is no status defined in the CS, we add by default Read and 
          # Write as Active.
          if elementStatuses == {}:
            elementStatuses = { 'ReadAccess' : 'Active', 'WriteAccess' : 'Active' }
                
          result[ element ] = elementStatuses             
    
    if result:
      return S_OK( result )
                
    if default is not None:
    
      # sec check
      if statusType is None:
        statusType = 'none'
    
      defList = [ [ el, statusType, default ] for el in elementName ]
      return S_OK( getDictFromList( defList ) )

    _msg = "StorageElement '%s', with statusType '%s' is unknown for CS."
    return S_ERROR( _msg % ( elementName, statusType ) )

  def __setRSSStorageElementStatus( self, elementName, statusType, status, reason, tokenOwner ):
    """
    Sets on the RSS the StorageElements status
    """
  
    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )
      
    self.seCache.acquireLock()
    try:
      res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName, 
                                                statusType = statusType, status = status,
                                                reason = reason, tokenOwner = tokenOwner,
                                                tokenExpiration = expiration )
      if res[ 'OK' ]:
        self.seCache.refreshCache()
        
      if not res[ 'OK' ]:
        _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, status )
        gLogger.warn( 'RSS: %s' % _msg )
    
      return res

    finally:
      # Release lock, no matter what.   
      self.seCache.releaseLock()

  def __setCSStorageElementStatus( self, elementName, statusType, status ):
    """
    Sets on the CS the StorageElements status
    """

    statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
    if not statusType in statuses:
      gLogger.error( "%s is not a valid statusType" % statusType )
      return S_ERROR( "%s is not a valid statusType: %s" % ( statusType, statuses ) )    

    csAPI = CSAPI()
  
    cs_path     = "/Resources/StorageElements"
    
    csAPI.setOption( "%s/%s/%s" % ( cs_path, elementName, statusType ), status )  
  
    res = csAPI.commitChanges()
    if not res[ 'OK' ]:
      gLogger.warn( 'CS: %s' % res[ 'Message' ] )
    
    return res

  def __getMode( self ):
    """
      Get's flag defined ( or not ) on the RSSConfiguration. If defined as 1, 
      we use RSS, if not, we use CS.
    """
  
    res = self.rssConfig.getConfigState() 

    if res == 'Active':
    
      if self.rssClient is None:
        self.rssClient = ResourceStatusClient() 
      return True
    
    self.rssClient = None
    return False

################################################################################

def getDictFromList( fromList ):
  '''
  Auxiliary method that given a list returns a dictionary of dictionaries:
  { site1 : { statusType1 : st1, statusType2 : st2 }, ... }
  '''
    
  res = {}
  for listElement in fromList:
    site, sType, status = listElement
    if not res.has_key( site ):
      res[ site ] = {}
    res[ site ][ sType ] = status
  return res  
 
def getCacheDictFromRawData( rawList ):
  """
  Formats the raw data list, which we know it must have tuples of three elements.
  ( element1, element2, element3 ) into a list of tuples with the format
  ( ( element1, element2 ), element3 ). Then, it is converted to a dictionary,
  which will be the new Cache.
  
  It happens that element1 is elementName, element2 is statusType and element3
  is status.
    
  :Parameters:
    **rawList** - `list`
      list of three element tuples [( element1, element2, element3 ),... ]
    
  :return: dict of the form { ( elementName, statusType ) : status, ... }
  """
      
  res = [ ( ( name, sType ), status ) for name, sType, status in rawList ]
  return dict( res )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF