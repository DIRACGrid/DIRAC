""" PublisherHandler

This service has been built to provide the RSS web views with all the information
they need. NO OTHER COMPONENT THAN Web controllers should make use of it.

"""

from datetime import datetime, timedelta
from types    import NoneType

# DIRAC
from DIRAC                                                      import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers, Utils
ResourceManagementClient = getattr(Utils.voimport( 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient' ),'ResourceManagementClient')

__RCSID__ = '$Id:$'

# RSS Clients
rsClient  = None
rmClient  = None

def initializePublisherHandler( _serviceInfo ):
  """
  Handler initialization in the usual horrible way.
  """
  
  global rsClient 
  rsClient = ResourceStatusClient()
  
  global rmClient 
  rmClient = ResourceManagementClient()
  
  return S_OK()
  
class PublisherHandler( RequestHandler ):
  """
  RPCServer used to deliver data to the web portal.
      
  """  

  def __init__( self, *args, **kwargs ):
    """
    Constructor
    """
    super( PublisherHandler, self ).__init__( *args, **kwargs )
  
  # ResourceStatusClient .......................................................
  
  types_getSites = []
  def export_getSites( self ):  
    """
    Returns list of all sites considered by RSS
    
    :return: S_OK( [ sites ] ) | S_ERROR
    """
    
    gLogger.info( 'getSites' )
    return CSHelpers.getSites()

  types_getSitesResources = [ ( str, list, NoneType ) ]
  def export_getSitesResources( self, siteNames ):
    """
    Returns dictionary with SEs and CEs for the given site(s). If siteNames is
    None, all sites are taken into account.
    
    :return: S_OK( { site1 : { ces : [ ces ], 'ses' : [ ses  ] },... } ) | S_ERROR 
    """
    
    gLogger.info( 'getSitesResources' )
        
    if siteNames is None:
      siteNames = CSHelpers.getSites()
      if not siteNames[ 'OK' ]:
        return siteNames
      siteNames = siteNames[ 'Value' ]
    
    if isinstance( siteNames, str ):
      siteNames = [ siteNames ]
    
    sitesRes = {}
    
    for siteName in siteNames:
      
      res = {}      
      res[ 'ces' ] = CSHelpers.getSiteComputingElements( siteName )
      # Convert StorageElements to host names
      ses          = CSHelpers.getSiteStorageElements( siteName )
      sesHosts     = CSHelpers.getStorageElementsHosts( ses )
      if not sesHosts[ 'OK' ]:
        return sesHosts
      # Remove duplicates
      res[ 'ses' ] = list( set( sesHosts[ 'Value' ] ) )
          
      sitesRes[ siteName ] = res
    
    return S_OK( sitesRes )

  types_getElementStatuses = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ),
                               ( str, list, NoneType ) ]
  def export_getElementStatuses( self, element, name, elementType, statusType, status, tokenOwner ):
    """
    Returns element statuses from the ResourceStatusDB
    """
    
    gLogger.info( 'getElementStatuses' )
    return rsClient.selectStatusElement( element, 'Status', name = name, elementType = elementType, 
                                         statusType = statusType, status = status, 
                                         tokenOwner = tokenOwner ) 

  types_getElementHistory = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                              ( str, list, NoneType ) ]
  def export_getElementHistory( self, element, name, elementType, statusType ):
    """
    Returns element history from ResourceStatusDB
    """
    
    gLogger.info( 'getElementHistory' )
    columns = [ 'Status', 'DateEffective', 'Reason' ]
    return rsClient.selectStatusElement( element, 'History', name = name, elementType = elementType,
                                         statusType = statusType,
                                         meta = { 'columns' : columns } ) 

  types_getElementPolicies = [ str, ( str, list, NoneType ), ( str, list, NoneType ) ]
  def export_getElementPolicies( self, element, name, statusType ):
    """
    Returns policies for a given element
    """
    
    gLogger.info( 'getElementPolicies' )
    columns = [ 'Status', 'PolicyName', 'DateEffective', 'LastCheckTime', 'Reason' ]
    return rmClient.selectPolicyResult( element = element, name = name, 
                                        statusType = statusType, 
                                        meta = { 'columns' : columns } )

  types_getNodeStatuses = []
  def export_getNodeStatuses( self ):
      return rsClient.selectStatusElement( 'Node', 'Status' ) 

  types_getTree = [ str, str, str ]
  def export_getTree( self, element, elementType, elementName ):
    """
    Given an element, finds its parent site and returns all descendants of that
    site.
    """

    gLogger.info( 'getTree' )

    site = self.getSite( element, elementType, elementName )        
    if not site:
      return S_ERROR( 'No site' )
    
    siteStatus = rsClient.selectStatusElement( 'Site', 'Status', name = site, 
                                               meta = { 'columns' : [ 'StatusType', 'Status' ] } )
    if not siteStatus[ 'OK' ]:
      return siteStatus      

    tree = { site : { 'statusTypes' : dict( siteStatus[ 'Value' ] ) } }
    
    ces = CSHelpers.getSiteComputingElements( site )    
    cesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ces,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not cesStatus[ 'OK' ]:
      return cesStatus
  
    ses = CSHelpers.getSiteStorageElements( site )
    sesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ses,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not sesStatus[ 'OK' ]:
      return sesStatus   
    
    def feedTree( elementsList ):
      
      elements = {}
      for elementTuple in elementsList[ 'Value' ]:
        name, statusType, status = elementTuple
        
        if not name in elements:
          elements[ name ] = {}
        elements[ name ][ statusType ] = status
      
      return elements
    
    tree[ site ][ 'ces' ] = feedTree( cesStatus )
    tree[ site ][ 'ses' ] = feedTree( sesStatus )
    
    return S_OK( tree )
    
  #-----------------------------------------------------------------------------  
  types_setToken = [ str ] * 7
  def export_setToken( self, element, name, statusType, token, elementType, username, lastCheckTime ):

    lastCheckTime = datetime.strptime( lastCheckTime, '%Y-%m-%d %H:%M:%S' )

    credentials = self.getRemoteCredentials()
    gLogger.info( credentials )

    elementInDB =rsClient.selectStatusElement( element, 'Status', name = name,
                                               statusType = statusType,
                                               elementType = elementType,
                                               lastCheckTime = lastCheckTime )
    if not elementInDB[ 'OK' ]:
      return elementInDB
    elif not elementInDB[ 'Value' ]:
      return S_ERROR( 'Your selection has been modified. Please refresh.' )



    if token == 'Acquire':
      tokenOwner = username
      tokenExpiration = datetime.utcnow() + timedelta( days = 1 )
    elif token == 'Release':
      tokenOwner = 'rs_svc'
      tokenExpiration = datetime.max
    else:
      return S_ERROR( '%s is unknown token action' % token )

    reason = 'Token %sd by %s ( web )' % ( token, username )

    newStatus = rsClient.addOrModifyStatusElement( element, 'Status', name = name,
                                                   statusType = statusType,
                                                   elementType = elementType,
                                                   reason = reason,
                                                   tokenOwner = tokenOwner,
                                                   tokenExpiration = tokenExpiration )
    if not newStatus[ 'OK' ]:
      return newStatus

    return S_OK( reason )
    
  def getSite( self, element, elementType, elementName ):
    """
    Given an element, return its site
    """
    
    if elementType == 'StorageElement':
      elementType = 'SE'

    domainNames = gConfig.getSections( 'Resources/Sites' )
    if not domainNames[ 'OK' ]:
      return domainNames
    domainNames = domainNames[ 'Value' ]
  
    for domainName in domainNames:
      
      sites = gConfig.getSections( 'Resources/Sites/%s' % domainName )
      if not sites[ 'OK' ]:
        continue
      
      for site in sites[ 'Value' ]:
      
        elements = gConfig.getValue( 'Resources/Sites/%s/%s/%s' % ( domainName, site, elementType ), '' )
        if elementName in elements:
          return site          

    return ''

  # ResourceManagementClient ...................................................
  
  types_getDowntimes = [ str, str, str ]
  def export_getDowntimes( self, element, elementType, name ):
    
    if elementType == 'StorageElement':
      name = CSHelpers.getSEHost( name )
      if not name['OK']:
        return name
      name = name['Value']
    
    return rmClient.selectDowntimeCache( element = element, name = name, 
                                         meta = { 'columns' : [ 'StartDate', 'EndDate', 
                                                                'Link', 'Description', 
                                                                'Severity' ] } )

  types_getCachedDowntimes = [ ( str, NoneType, list ), ( str, NoneType, list ), ( str, NoneType, list ),
                               ( str, NoneType, list ), datetime, datetime ]
  def export_getCachedDowntimes( self, element, elementType, name, severity, startDate, endDate ):
    
    if elementType == 'StorageElement':
      name = CSHelpers.getSEHost( name )
      if not name['OK']:
        return name
      name = name['Value']
   
    if startDate > endDate:
      return S_ERROR( 'startDate > endDate' )
    
    res = rmClient.selectDowntimeCache( element = element, name = name, severity = severity,
                                        meta = { 'columns' : [ 'Element', 'Name', 'StartDate',
                                                               'EndDate', 'Severity',
                                                               'Description', 'Link' ] } )
    if not res[ 'OK' ]:
      return res
    
    downtimes = []
    
    for dt in res[ 'Value' ]:
      
      dtDict = dict( zip( res[ 'Columns' ], dt ) ) 
    
      if dtDict[ 'StartDate' ] < endDate and dtDict[ 'EndDate' ] > startDate:
        downtimes.append( dt )
    
    result = S_OK( downtimes )
    result[ 'Columns' ] = res[ 'Columns' ]
    
    return result    

  types_setStatus = [ str ] * 7
  def export_setStatus( self, element, name, statusType, status, elementType, username, lastCheckTime ):

    lastCheckTime = datetime.strptime( lastCheckTime, '%Y-%m-%d %H:%M:%S' )

    credentials = self.getRemoteCredentials()
    gLogger.info( credentials )

    elementInDB =rsClient.selectStatusElement( element, 'Status', name = name,
                                               statusType = statusType,
                                            #   status = status,
                                               elementType = elementType,
                                               lastCheckTime = lastCheckTime )
    if not elementInDB[ 'OK' ]:
      return elementInDB
    elif not elementInDB[ 'Value' ]:
      return S_ERROR( 'Your selection has been modified. Please refresh.' )

    reason          = 'Status %s forced by %s ( web )' % ( status, username )
    tokenExpiration = datetime.utcnow() + timedelta( days = 1 )

    newStatus = rsClient.addOrModifyStatusElement( element, 'Status', name = name,
                                                   statusType = statusType,
                                                   status = status,
                                                   elementType = elementType,
                                                   reason = reason,
                                                   tokenOwner = username,
                                                   tokenExpiration = tokenExpiration )
    if not newStatus[ 'OK' ]:
      return newStatus

    return S_OK( reason )    
    
  #-----------------------------------------------------------------------------  

# TO-DO: to restore when v7r0 will be properly integrated
#   # ResourceManagementClient ...................................................
#   types_getSpaceTokenOccupancy = [ ( str, NoneType, list ) ] * 2
#   def export_getSpaceTokenOccupancy( self, site, token ):
#
#     # Ugly thing
#     #...........................................................................
#
#     endpoint2Site = {}
#
#     ses = CSHelpers.getStorageElements()
#     if not ses[ 'OK' ]:
#       gLogger.error( ses[ 'Message' ] )
#
#     for seName in ses[ 'Value' ]:
#       # Ugly, ugly, ugly.. waiting for DIRAC v7r0 to do it properly
#       if ( not '-' in seName ) or ( '_' in seName ):
#         continue
#
#       res = CSHelpers.getStorageElementEndpoint( seName )
#       if not res[ 'OK' ]:
#         return res
#
#       if not res[ 'Value' ] in endpoint2Site:
#         endpoint2Site[ res[ 'Value' ] ] = seName.split( '-', 1 )[ 0 ]
#
#     #...........................................................................
#
#     endpointSet = set()
#
#     if site:
#
#       if isinstance( site, str ):
#         site = [ site ]
#
#       for ep, siteName in endpoint2Site.items():
#         if siteName in site:
#           endpointSet.add( ep )
#
#     if endpointSet:
#       endpoint = list( endpointSet )
#     else:
#       endpoint = None
#
#     res = rmClient.selectSpaceTokenOccupancyCache( endpoint = endpoint, token = token )
#     if not res[ 'OK' ]:
#       return res
#
#     spList = [ dict( zip( res[ 'Columns' ], sp ) ) for sp in res[ 'Value' ] ]
#
#     for spd in spList:
#
#       try:
#         spd[ 'Site' ] = endpoint2Site[ spd[ 'Endpoint' ] ]
#       except KeyError:
#         spd[ 'Site' ] = 'Unknown'
#
#     return S_OK( spList )
#
# #...............................................................................
# #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
