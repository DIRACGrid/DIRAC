# $HeadURL:  $
''' ResourceStatus

  Module use to switch between the CS and the RSS.

'''

import datetime
import threading

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton 
from DIRAC.ConfigurationSystem.Client.CSAPI                 import CSAPI
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCache          import RSSCache 

__RCSID__  = '$Id: $'

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
    self.rssClient = None 
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
  
    try:
   
      if self.__getMode():
        return self.__getRSSStorageElementStatus( elementName, statusType, default )
      else:
        return self.__getCSStorageElementStatus( elementName, statusType, default )

    except Exception, e:
    
      _msg = "Error getting StorageElement '%s', with statusType '%s'."
      gLogger.error( _msg % ( elementName, statusType ) )
      gLogger.exception( e )
      return S_ERROR( _msg % ( elementName, statusType ) )  

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
  
    try:
    
      if self.__getMode():
        return self.__setRSSStorageElementStatus( elementName, statusType, status, reason, tokenOwner )
      else:
        return self.__setCSStorageElementStatus( elementName, statusType, status )

    except Exception, e:
    
      _msg = "Error setting StorageElement '%s' status '%s', with statusType '%s'."
      gLogger.error( _msg % ( elementName, status, statusType ) )
      gLogger.exception( e )
      return S_ERROR( _msg % ( elementName, status, statusType ) ) 

################################################################################

  def __updateSECache( self ):
    '''
      Method used to update the StorageElementCache.
    '''  
    
    if not self.__getMode():
      # We are using the CS, we do not care about the cache.
      return { 'OK' : False, 'Message' : 'RSS flag is inactive' }
    
    meta = { 'columns' : [ 'StorageElementName','StatusType','Status' ] }
  
    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    rawCache = self.rssClient.getElementStatus( 'StorageElement', meta = meta )
    if not rawCache[ 'OK' ]:
      return rawCache
    
    return S_OK( getCacheDictFromList( rawCache[ 'Value' ] ) )     
  
################################################################################
  
  def __getRSSStorageElementStatus( self, elementName, statusType, default ):
    '''
    Gets from RSS the StorageElements status
    '''
   
    meta        = { 'columns' : [ 'StorageElementName','StatusType','Status' ] }
    kwargs      = { 
                    'elementName' : elementName,
                    'statusType'  : statusType, 
                    'meta'        : meta 
                  }
  
    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    res = self.rssClient.getElementStatus( 'StorageElement', **kwargs )
    if res[ 'OK' ] and res['Value']:
      return S_OK( getDictFromList( res['Value'] ) )
  
    if not isinstance( elementName, list ):
      elementName = [ elementName ]
  
    if default is not None:
    
      # sec check
      if statusType is None:
        statusType = 'none'
    
      defList = [ [ el, statusType, default ] for el in elementName ]
      return S_OK( getDictFromList( defList ) )

    _msg = "StorageElement '%s', with statusType '%s' is unknown for RSS."
    return S_ERROR( _msg % ( elementName, statusType ) )

  def __getCSStorageElementStatus( self, elementName, statusType, default ):
    '''
    Gets from the CS the StorageElements status
    '''
  
    cs_path     = "/Resources/StorageElements"
  
    if not isinstance( elementName, list ):
      elementName = [ elementName ]

    statuses = gConfig.getOptionsDict( '/Operations/RSSConfiguration/GeneralConfig/Resources/StorageElement' )
    
    if statuses[ 'OK' ]:
      statuses = statuses[ 'Value' ][ 'StatusType' ]
    else:
      statuses = [ 'Read', 'Write' ]  
    
    r = {}
    for element in elementName:
    
      if statusType is not None:
        res = gConfig.getOption( "%s/%s/%sAccess" % ( cs_path, element, statusType ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          r[ element ] = { statusType : res[ 'Value' ] }
        
      else:
        res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          r2 = {}
          for k,v in res['Value'].items():
            k = k.replace( 'Access', '' )
            if k in statuses:
              r2[ k ] = v
              
          r[ element ] = r2             
    
    if r:
      return S_OK( r )
                
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
  
    kwargs = {
              'status'          : status, 
              'reason'          : reason,
              'tokenOwner'      : tokenOwner, 
              'tokenExpiration' : expiration 
              }
    
    res = self.rssClient.modifyElementStatus( 'StorageElement', elementName, statusType, **kwargs )
  
    if not res[ 'OK' ]:
      _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, str( kwargs ))
      gLogger.warn( 'RSS: %s' % _msg )
    
    return res

  def __setCSStorageElementStatus( self, elementName, statusType, status ):
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
  
    res = gConfig.getValue( 'Operations/RSSConfiguration/Status', 'InActive' )
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
    <resourceName>#<statusType> : status,
    <resourceName>#<statusType1> : status1
    ...
  }
  '''
    
  res = [ ( '%s#%s' % ( name, sType ), status ) for name, sType, status in rawList ]
  return dict( res )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF