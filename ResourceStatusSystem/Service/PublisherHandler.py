''' LHCbDIRAC.ResourceStatusSystem.Service.PublisherHandler

   initializePublisherHandler
   PublisherHandler.__bases__:
     DIRAC.Core.DISET.RequestHandler.RequestHandler

'''

from datetime import datetime, timedelta
from types    import NoneType

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers                   import Resources
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers
from DIRAC.ConfigurationSystem.Client.Helpers.Resources         import getSiteForResource



__RCSID__ = '$Id: PublisherHandler.py 65921 2013-05-14 13:05:43Z ubeda $'
rsClient  = None
rmClient  = None

def initializePublisherHandler( _serviceInfo ):
  '''
    Handler initialization in the usual horrible way.
  '''
  global rsClient 
  rsClient = ResourceStatusClient()
  
  global rmClient 
  rmClient = ResourceManagementClient()
  
  return S_OK()
  
class PublisherHandler( RequestHandler ):
  '''
    RPCServer used to deliver data to the web portal.
      
    So far it contains only examples, probably some of them will be used by the 
    web portal, but not all of them.
  '''  
  
  # ResourceStatusClient .......................................................
  
  types_getSites = []
  def export_getSites( self ):  
    '''
      Returns list of all sites considered by RSS
    '''
    gLogger.info( 'getSites' )
    return Resources.getSites()

  types_getSitesResources = [ ( str, list, NoneType ) ]
  def export_getSitesResources( self, siteNames ):
    
    resources = Resources.Resources()
    
    if siteNames is None:
      siteNames = Resources.getSites()
      if not siteNames[ 'OK' ]:
        return siteNames
      siteNames = siteNames[ 'Value' ]
    
    if isinstance( siteNames, str ):
      siteNames = [ siteNames ]
    
    sitesRes = {}
    
    for siteName in siteNames:
      
      res = {}         
      res[ 'ces' ] = resources.getEligibleResources( 'Computing', { 'Site': siteName } )
      ses          = resources.getEligibleStorageElements( { 'Site': siteName } )
      sesHosts = CSHelpers.getStorageElementsHosts( ses )
      if not sesHosts[ 'OK' ]:
        return sesHosts
      res[ 'ses' ] = list( set( sesHosts[ 'Value' ] ) )
          
      sitesRes[ siteName ] = res
    
    return S_OK( sitesRes )

  types_getElementStatuses = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ), ( str, list, NoneType ),
                            ( str, list, NoneType ) ]
  def export_getElementStatuses( self, element, name, elementType, statusType, status, tokenOwner ):
      return rsClient.selectStatusElement( element, 'Status', name = name, elementType = elementType,
                                           statusType = statusType, status = status,
                                           tokenOwner = tokenOwner ) 

  types_getElementHistory = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ) ]
  def export_getElementHistory( self, element, name, elementType, statusType ):
      return rsClient.selectStatusElement( element, 'History', name = name, elementType = elementType,
                                           statusType = statusType,
                                           meta = { 'columns' : [ 'Status', 'DateEffective', 'Reason' ] } ) 

  types_getElementPolicies = [ str, ( str, list, NoneType ), ( str, list, NoneType ) ]
  def export_getElementPolicies( self, element, name, statusType ):
    return rmClient.selectPolicyResult( element = element, name = name, 
                                        statusType = statusType, 
                                        meta = { 'columns' : [ 'Status', 'PolicyName', 'DateEffective',
                                                               'LastCheckTime', 'Reason' ]} )

  types_getNodeStatuses = []
  def export_getNodeStatuses( self ):
      return rsClient.selectStatusElement( 'Node', 'Status' ) 

  types_getTree = [ str, str, str ]
  def export_getTree( self, element, elementType, elementName ):

    tree = {}

    resources = Resources.Resources()

    #site = self.getSite( element, elementType, elementName )
    result = getSiteForResource( elementName )
    if not result['OK']:
      return S_ERROR( 'Can not get site name: %s' % result[ 'Message' ] ) 
    site = result['Value']       
    if not site:
      return S_ERROR( 'No site' )
    
    siteStatus = rsClient.selectStatusElement( 'Site', 'Status', name = site, 
                                               meta = { 'columns' : [ 'StatusType', 'Status' ] } )
    if not siteStatus[ 'OK' ]:
      return siteStatus      

    tree[ site ] = { 'statusTypes' : dict( siteStatus[ 'Value' ] ) }
      
    ces = resources.getEligibleResources( 'Computing', { 'Site': site } )
    cesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ces,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not cesStatus[ 'OK' ]:
      return cesStatus
    
    tree[ site ][ 'ces' ] = {}
    for ceTuple in cesStatus[ 'Value' ]:
      name, statusType, status = ceTuple
      if not name in tree[ site ][ 'ces' ]:
        tree[ site ][ 'ces' ][ name ] = {}
      tree[ site ][ 'ces' ][ name ][ statusType ] = status   
    
    ses = resources.getEligibleStorageElements( { 'Site': site } )
    sesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ses,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not sesStatus[ 'OK' ]:
      return sesStatus
    
    tree[ site ][ 'ses' ] = {}
    for seTuple in sesStatus[ 'Value' ]:
      name, statusType, status = seTuple
      if not name in tree[ site ][ 'ses' ]:
        tree[ site ][ 'ses' ][ name ] = {}
      tree[ site ][ 'ses' ][ name ][ statusType ] = status   

    return S_OK( tree )

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
    
#  def getSite( self, element, elementType, elementName ):
#    
#    if elementType == 'StorageElement':
#      elementType = 'SE'
#
#    domainNames = gConfig.getSections( 'Resources/Sites' )
#    if not domainNames[ 'OK' ]:
#      return domainNames
#    domainNames = domainNames[ 'Value' ]
#  
#    for domainName in domainNames:
#      
#      sites = gConfig.getSections( 'Resources/Sites/%s' % domainName )
#      if not sites[ 'OK' ]:
#        continue
#      
#      for site in sites[ 'Value' ]:
#      
#        elements = gConfig.getValue( 'Resources/Sites/%s/%s/%s' % ( domainName, site, elementType ), '' )
#        if elementName in elements:
#          return site          
#
#    return ''

  # ResourceManagementClient ...................................................
  
  types_getDowntimes = [ str, str, str ]
  def export_getDowntimes( self, element, elementType, elementName ):
    
    if elementType == 'StorageElement':
      result = CSHelpers.getSEProtocolOption( elementName, 'Host' )
      if not result['OK']:
        return S_ERROR( 'StorageElement %s host not found' % elementName )
      name = result['Value']
    
    return rmClient.selectDowntimeCache( element = element, name = name, 
                                         meta = { 'columns' : [ 'StartDate', 'EndDate', 
                                                                'Link', 'Description', 
                                                                'Severity' ] } )

  types_getCachedDowntimes = [ ( str, NoneType, list ), ( str, NoneType, list ), ( str, NoneType, list ),
                               ( str, NoneType, list ), datetime, datetime ]
  def export_getCachedDowntimes( self, element, elementType, elementName, severity, startDate, endDate ):
    
    if elementType == 'StorageElement':
      result = CSHelpers.getSEProtocolOption( elementName, 'Host' )
      if not result['OK']:
        return S_ERROR( 'StorageElement %s host not found' % elementName )
      name = result['Value']
   
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

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF