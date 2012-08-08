# $HeadURL:  $
''' ResourceStatus

  Module use to switch between the CS and the RSS.

'''

import datetime

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton 
from DIRAC.ConfigurationSystem.Client.CSAPI                 import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations 
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCache          import RSSCache 
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration

__RCSID__  = '$Id:  $'

class ResourceStatus( object ):
  '''
  ResourceStatus helper that connects to CS if RSS flag is not Active. It keeps
  the connection to the db / server as an object member, to avoid creating a new
  one massively.
  '''

  __metaclass__ = DIRACSingleton
  def __init__( self ):
    '''
    Constructor, initializes the rssClient.
    '''

    self.rssConfig  = RssConfiguration()
    self.__opHelper = Operations()    
    self.rssClient  = None 

    # RSSCache only affects the calls directed to RSS, if using the CS it is not
    # used.  
    self.seCache   = RSSCache( 300, updateFunc = self.__updateSECache, cacheHistoryLifeTime = 24 ) 
    self.seCache.startRefreshThread()           
            
  def getStorageElementStatus( self, elementName, statusType = None, default = None ):
    '''
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
    
    '''
  
    if self.__getMode():
      return self.__getRSSStorageElementStatus( elementName, statusType, default )
    else:
      return self.__getCSStorageElementStatus( elementName, statusType, default )

  def setStorageElementStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):
  
    '''
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
    '''
  
    if self.__getMode():
      return self.__setRSSStorageElementStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return self.__setCSStorageElementStatus( elementName, statusType, status )

################################################################################

  def __updateSECache( self ):
    '''
      Method used to update the StorageElementCache.
    '''  
    
    if not self.__getMode():
      # We are using the CS, we do not care about the cache.
      return { 'OK' : False, 'Message' : 'RSS flag is inactive' }
    

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }   
    rawCache  = self.rssClient.selectStatusElement( 'Resource', 'Status', 
                                                    elementType = 'StorageElement', 
                                                    meta = meta )  
    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    if not rawCache[ 'OK' ]:
      return rawCache
    
    return S_OK( getCacheDictFromList( rawCache[ 'Value' ] ) )     
  
  def __cacheMatch( self, resourceNames, statusTypes ):
    '''
      Method that given a resourceName and a statusType, gives the match with the
      cache. Both arguments can be None, String of list( String, ). Being string,
      if not present in the cache, there is no match.
      
      Keys in the cache are stored as:
        ( <resourceName>, <statusType> )
        ( <resourceName>, <statusType> )
        ( <resourceName>, <statusType> )
      so, we first need some processing to see which of all possible combinations
      of resourceName and statusType are in the cache. ( If any of them is None,
      it is interpreted as all ).  
    '''
    
    cacheKeys = self.seCache.getCacheKeys()
    if not cacheKeys[ 'OK' ]:
      return cacheKeys
     
    cacheKeys = cacheKeys[ 'Value' ] 
      
    elementCandidates = cacheKeys
    
    if resourceNames is not None:
      
      elementCandidates = []
      
      if isinstance( resourceNames, str ):       
        resourceNames = [ resourceNames ]
        
      for resourceName in resourceNames:
        found = False
          
        for cK in cacheKeys:
          
          if cK[ 0 ] == resourceName:
            elementCandidates.append( cK )
            found = True
            
        if not found:
          return S_ERROR( '%s not found in the cache' % resourceName )  
    
    statusTypeCandidates = elementCandidates 
    
    # now we loop over elementCandidates, saves lots of iterations.        
    if statusTypes is not None:

      statusTypeCandidates = []
      
      if isinstance( statusTypes, str ):
        statusTypes = [ statusTypes ]  
               
      for elementCandidate in elementCandidates:
        for statusType in statusTypes:  
                  
          if elementCandidate[ 1 ] == statusType:  
            statusTypeCandidates.append( elementCandidate )
                  
    return S_OK( statusTypeCandidates )   

  def __getFromCache( self, elementName, statusType ):
    '''
    Given an elementName and a statusType, matches the cache, and in case
    of positive match, formats the output and returns
    '''

    match = self.__cacheMatch( elementName, statusType )
    
    if not match[ 'OK' ]:
      return match
    
    cacheMatches = self.seCache.getBulk( match[ 'Value' ] )
    if not cacheMatches[ 'OK' ]:
      return cacheMatches

    cacheMatches = cacheMatches[ 'Value' ]
    if not cacheMatches:
      return S_ERROR( 'Empty cache for ( %s, %s )' % ( elementName, statusType ) )   
    
    # We undo the key into <resourceName> and <statusType>
    fromList = [ list( key ) + [ value ] for key, value in cacheMatches.items() ]
    return S_OK( getDictFromList( fromList ) )
  
################################################################################
  
  def __getRSSStorageElementStatus( self, elementName, statusType, default ):
    '''
    Gets from the cache or the RSS the StorageElements status
    '''
  
    #Checks cache first
    cache = self.__getFromCache( elementName, statusType )
    if cache[ 'OK' ]:
      return cache
            
    #Humm, seems cache did not work     
    gLogger.info( 'Cache miss with %s %s' % ( elementName, statusType ) )          
    
    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    res = self.rssClient.selectStatusElement( 'Resource', 'Status', 
                                              elementType = 'StorageElement',
                                              name = elementName,
                                              statusType = statusType, 
                                              meta = meta )      
      
    if res[ 'OK' ] and res[ 'Value' ]:
      return S_OK( getDictFromList( res[ 'Value' ] ) )
  
    if not isinstance( elementName, list ):
      elementName = [ elementName ]
  
    if default is not None:
    
      # sec check
      if statusType is None:
        statusType = ''
    
      defList = [ [ el, statusType, default ] for el in elementName ]
      return S_OK( getDictFromList( defList ) )

    if elementName == [ None ]:
      elementName = [ '' ]

    _msg = "StorageElement '%s', with statusType '%s' is unknown for RSS."
    return S_ERROR( _msg % ( ','.join( elementName ), statusType ) )

  def __getCSStorageElementStatus( self, elementName, statusType, default ):
    '''
    Gets from the CS the StorageElements status
    '''
  
    cs_path     = "/Resources/StorageElements"
  
    if not isinstance( elementName, list ):
      elementName = [ elementName ]

    statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
    #statuses = self.__opHelper.getOptionsDict( 'RSSConfiguration/GeneralConfig/Resources/StorageElement' )
    #statuses = gConfig.getOptionsDict( '/Operations/RSSConfiguration/GeneralConfig/Resources/StorageElement' )
    
    if statuses[ 'OK' ]:
      statuses = statuses[ 'Value' ][ 'StatusType' ]
    else:
      statuses = [ 'ReadAccess', 'WriteAccess' ]  
    
    result = {}
    for element in elementName:
    
      if statusType is not None:
        # Added Allowed by default
        res = gConfig.getOption( "%s/%s/%s" % ( cs_path, element, statusType ), 'Allowed' )
        if res[ 'OK' ] and res[ 'Value' ]:
          result[ element ] = { statusType : res[ 'Value' ] }
        
      else:
        res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          elementStatuses = {}
          for elementStatusType, value in res[ 'Value' ].items():
            #k = k.replace( 'Access', '' )
            if elementStatusType in statuses:
              elementStatuses[ elementStatusType ] = value
          
          # If there is no status defined in the CS, we add by default Read and 
          # Write as Allowed.
          if elementStatuses == {}:
            elementStatuses = { 'ReadAccess' : 'Allowed', 'WriteAccess' : 'Allowed' }
                
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
    '''
    Sets on the RSS the StorageElements status
    '''
  
    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )
      
    self.seCache.acquireLock()
    
    res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName, 
                                              statusType = statusType, status = status,
                                              reason = reason, tokenOwner = tokenOwner,
                                              tokenExpiration = expiration )
    if res[ 'OK' ]:
      self.seCache.refreshCacheAndHistory()
    
    # Looks dirty, but this way we avoid retaining the lock when using gLogger.   
    self.seCache.releaseLock()
    
    if not res[ 'OK' ]:
      _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, status )
      gLogger.warn( 'RSS: %s' % _msg )
    
    return res

  @staticmethod
  def __setCSStorageElementStatus( elementName, statusType, status ):
    '''
    Sets on the CS the StorageElements status
    '''

    csAPI = CSAPI()
  
    cs_path     = "/Resources/StorageElements"
    
    csAPI.setOption( "%s/%s/%sAccess" % ( cs_path, elementName, statusType ), status )  
  
    res = csAPI.commitChanges()
    if not res[ 'OK' ]:
      gLogger.warn( 'CS: %s' % res[ 'Message' ] )
    
    return res

  def __getMode( self ):
    '''
      Get's flag defined ( or not ) on the RSSConfiguration. If defined as 1, 
      we use RSS, if not, we use CS.
    '''
  
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
  Auxiliar method that given a list returns a dictionary of dictionaries:
  { site1 : { statusType1 : st1, statusType2 : st2 }, ... }
  '''
    
  res = {}
  for listElement in fromList:
    site, sType, status = listElement
    if not res.has_key( site ):
      res[ site ] = {}
    res[ site ][ sType ] = status
  return res  

def getCacheDictFromList( rawList ):
  '''
  Auxiliar method that given a list returns a dictionary looking like:
  { 
    ( <resourceName>, <statusType> )  : status,
    ( <resourceName>, <statusType1> ) : status1
    ...
  }
  '''
    
  #res = [ ( '%s#%s' % ( name, sType ), status ) for name, sType, status in rawList ]
  res = [ ( ( name, sType ), status ) for name, sType, status in rawList ]
  return dict( res )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF