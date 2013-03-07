# $HeadURL$
""" SiteStatus helper

  Provides methods to easily interact with the RSS

"""

from DIRAC                                                  import S_ERROR, S_OK 
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__ = '$Id: $'

class SiteStatus( object ):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.
  """
  
  __metaclass__ = DIRACSingleton
  
  #FIXME: add cache. As it is now, is querying directly the server
  
  def __init__( self ):
    '''
    Constructor, initializes the rssClient.
    '''
    self.rssClient = ResourceStatusClient()
  
  def getSiteStatus( self, siteName ):
    ''' 
    Given a siteName, returns its status: Unknown, Active, Degraded, Probing, Banned
    and Error.
    '''
    
    if not isinstance( siteName, str ):
      return S_ERROR( '%s is not of type str' % siteName )
    
    res = self.rssClient.selectStatusElement( 'Site', 'Status', name = siteName,
                                              meta = { 'columns' : [ 'Status' ] } )
    
    if not res[ 'OK' ]:
      return res
    return S_OK( res[ 'Value' ][ 0 ][ 0 ] )

  def isSiteUsable( self, siteName ):
    '''
    Given a site name, returns a bool if the site is usable: status is Active or
    Degraded, or not ( encapsulated in a S_OK object ). 
    '''
    
    siteStatus = self.getSiteStatus( siteName )
    if not siteStatus[ 'OK' ]:
      return siteStatus
    
    if siteStatus[ 'Value' ] in ( 'Active', 'Degraded' ):
      return S_OK( True )
    
    return S_OK( False )  
    
  def getUsableSites( self ):
    '''
    Returns all site names which status is either Active or Degraded, in a list.
    '''
    
    res = self.rssClient.selectStatusElement( 'Site', 'Status', status = [ 'Active', 'Degraded' ],  
                                              meta = { 'columns' : [ 'Name' ] } )
    if not res[ 'OK' ]:
      return res
    
    return S_OK( [ siteTuple[0] for siteTuple in res[ 'Value' ] ] )
  
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF