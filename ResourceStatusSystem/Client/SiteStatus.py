# $HeadURL$
""" SiteStatus helper

  Provides methods to easily interact with the RSS

"""

from DIRAC                                                  import gLogger, S_ERROR, S_OK 
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread  import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration

__RCSID__ = '$Id: $'

class SiteStatus( object ):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.
  """
  
  __metaclass__ = DIRACSingleton
  
  def __init__( self ):
    """
    Constructor, initializes the rssClient.
    """
    self.log       = gLogger.getSubLogger( self.__class__.__name__ )
    self.rssConfig = RssConfiguration()
    
    self.rssClient = ResourceStatusClient()
    
    # RSSCache initialization
    cacheLifeTime   = int( self.rssConfig.getConfigCache() )
    # FIXME: we need to define the types in the CS : Site => {Computing,Storage,..}Access
    self.siteCache  = RSSCache( None, cacheLifeTime, self.__updateSiteCache )

  def getSiteStatuses( self, siteNames, statusTypes ):
    """
    Method that queries the RSSCache for Site-Status-related information. If any
    of the inputs is None, it is interpreted as * ( all ).
    
    If match is positive, the output looks like:
    { 
     siteA : { statusType1 : status1, statusType2 : status2 },
     siteB : { statusType1 : status1, statusType2 : status2 },
    }
    
    There are ALWAYS the same keys inside the site dictionaries.
    
    :Parameters:
      **siteNames** - [ None, `string`, `list` ]
        name(s) of the sites to be matched
      **statusTypes** - [ None, `string`, `list` ]
        name(s) of the statusTypes to be matched
    
    :return: S_OK() || S_ERROR()       
    """
    
    cacheMatch = self.siteCache.match( siteNames, statusTypes )

    self.log.debug( 'getSiteStatus' )
    self.log.debug( cacheMatch )
    
    return cacheMatch        
  
  def getSiteStatus( self, siteName, statusType ):
    """
    Given a site and a statusType, it returns its status from the cache.
    
    :Parameters:
      **siteName** - `string`
        name of the site to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
  
    if not isinstance( siteName, str ):
      self.log.error( "getSiteStatus expects str for siteName" )
      return S_ERROR( "getSiteStatus expects str for siteName" )
    if not isinstance( statusType, str ):
      self.log.error( "getSiteStatus expects str for statusType" )
      return S_ERROR( "getSiteStatus expects str for statusType" )
    
    result = self.getSiteStatuses( siteName, statusType )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      return result
    
    return S_OK( result[ 'Value' ][ siteName ][ statusType ] )

  def isUsableSite( self, siteName, statusType ):
    """
    Similar method to getSiteStatus. The difference is the output.
    Given a site name, returns a bool if the site is usable: 
      status is Active or Degraded outputs True
      anything else outputs False
    
    :Parameters:
      **siteName** - `string`
        name of the site to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()    
    """
    
    self.log.debug( ( siteName, statusType ) )
    
    siteStatus = self.getSiteStatus( siteName, statusType )
    if not siteStatus[ 'OK' ]:
      self.log.error( siteStatus[ 'Message' ] )
      return False
    
    if siteStatus[ 'Value' ] in ( 'Active', 'Degraded' ):
      self.log.debug( 'IsUsable' )
      return True
    
    self.log.debug( 'Is NOT Usable' )
    return False  
    
  def getUsableSites( self, statusType ):
    """
    For a given statusType, returns all sites that are usable: their status
    for that particular statusType is either Active or Degraded; in a list.
    
    :Parameters:
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    if not isinstance( statusType, str ):
      self.log.error( "getUsableSites expects str for statusType" )
      return S_ERROR( "getUsableSites expects str for statusType" )       
    
    result = self.getSiteStatuses( None, statusType )
    if not result[ 'OK' ]:
      self.log.error( result )
      return result
    result = result[ 'Value' ]
    
    self.log.debug( result )
    
    usableSites = []
    
    for siteDict in result:
      for siteName, statusDict in siteDict.items():
        
        if statusDict[ statusType ] in ( 'Active', 'Degraded' ):
        
          usableSites.append( siteName )
    
    return S_OK( usableSites )
 
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
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) ) 

#...............................................................................

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
  
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF