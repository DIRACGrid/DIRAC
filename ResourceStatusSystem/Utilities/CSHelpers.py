# $HeadURL:  $
''' CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.  
'''

from DIRAC                                               import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources  import getGOCSiteName
from DIRAC.ResourceStatusSystem.Utilities                import Utils
from DIRAC.ConfigurationSystem.Client.Helpers            import Resources  

__RCSID__ = '$Id:  $'

def warmUp():
  '''
    gConfig has its own dark side, it needs some warm up phase.  
  '''
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()

## Main functions ##############################################################

def getSites():
  '''
    Gets all sites from /Resources/Sites
  '''

  return Resources.getSites()

#  _basePath = 'Resources/Sites'
#  
#  sites = []
#  
#  domainNames = gConfig.getSections( _basePath )
#  if not domainNames[ 'OK' ]:
#    return domainNames
#  domainNames = domainNames[ 'Value' ]
#  
#  for domainName in domainNames:
#    domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
#    if not domainSites[ 'OK' ]:
#      return domainSites
#    
#    domainSites = domainSites[ 'Value' ]
#    
#    sites.extend( domainSites )  
#
#  # Remove duplicated ( just in case )
#  sites = list( set ( sites ) )
#  return S_OK( sites )

def getGOCSites( diracSites = None ):
  
  if diracSites is None:
    diracSites = getSites()
    if not diracSites[ 'OK' ]:
      return diracSites
    diracSites = diracSites[ 'Value' ]
  
  gocSites = []

  for diracSite in diracSites:
    gocSite = getGOCSiteName( diracSite )      
    if not gocSite[ 'OK' ]:
      continue
    gocSites.append( gocSite[ 'Value' ] ) 
  
  return S_OK( list( set( gocSites ) ) )

################################################################################

def getStorageElements():
  '''
    Gets all storage elements from /Resources/StorageElements
  '''
  
  #_basePath = 'Resources/StorageElements'
    
  #seNames = gConfig.getSections( _basePath )
  #return seNames 

  resources = Resources()
  result = resources.getEligibleStorageElements()
  return result

def getStorageElementsHosts( seNames = None ):
  
  seHosts = []
  
  if seNames is None:
    seNames = getStorageElements()
    if not seNames[ 'OK' ]:
      return seNames
    seNames = seNames[ 'Value' ]
  
  for seName in seNames:
    
    result = getSEHost( seName )
    if result['OK']:
      seHosts.append( result['Value'] )
      
  return S_OK( list( set( seHosts ) ) )    
  
#def getSEToken( se ):
#  ''' 
#    Get StorageElement token 
#  '''
#  
#  _basePath = '/Resources/StorageElements/%s/AccessProtocol.1/SpaceToken'
#  
#  #FIXME: return S_OK, S_ERROR
#  return gConfig.getValue( _basePath % se, '' )

def getSEHost( se ):
  ''' 
    Get StorageElement token 
  '''
  
  #_basePath = '/Resources/StorageElements/%s/AccessProtocol.1/Host'
  
  #FIXME: return S_OK, S_ERROR
  #return gConfig.getValue( _basePath % se, '' )

  return __getSEProtocolOption( se, 'Host' )

def __getSEProtocolOption( se, optionName ):
  """ 
    Get option of the Storage Element access protocol
  """
  resources = Resources()
  result = resources.getAccessProtocols( se )
  if not result['OK']:
    return S_ERROR( "Acces Protocol for SE %s not found: " % ( se, result['Message'] ) )
  ap = result['Value'][0]
  return resources.getAccessProtocolOption( ap, optionName )

def getStorageElementSpaceToken( storageElement ):
  
  #_basePath = '/Resources/StorageElements/%s/AccessProtocol.1/SpaceToken' % storageElement
  
  #res = gConfig.getValue( _basePath, '' )
#  if not res:
#    return S_ERROR( '%s not found' % _basePath )
  #return S_OK( res )

  return __getSEProtocolOption( storageElement, 'SpaceToken' )

def getStorageElementEndpoint( storageElement ):
  
  #_basePath = '/Resources/StorageElements/%s/AccessProtocol.1' % storageElement
  
  #host  = gConfig.getValue( _basePath + '/Host' )
  #port  = gConfig.getValue( _basePath + '/Port' ) 
  #wsurl = gConfig.getValue( _basePath + '/WSUrl', '' )
  
  resources = Resources()
  result = resources.getAccessProtocols( storageElement )
  if not result['OK']:
    return result
  # FIXME: There can be several access protocols for the same SE !
  ap = result['Value'][0]
  
  result = resources.getAccessProtocolOptionsDict( ap )
  result = resources.getAccessProtocols( storageElement )
  if not result['OK']:
    return result
  host = result['Value'].get( 'Host', '' )
  port = result['Value'].get( 'Port', '' )
  wsurl = result['Value'].get( 'WSUrl', '' )
  
  # MAYBE wusrl is not defined
  #if host and port and wsurl:
  if host and port:
     
    url = 'httpg://%s:%s%s' % ( host, port, wsurl )
    url = url.replace( '?SFN=', '' )
    return S_OK( url )
  
  return S_ERROR( ( host, port, wsurl ) )

def getStorageElementEndpoints( storageElements = None ):
  
  if storageElements is None:
    storageElements = getStorageElements()
    if not storageElements[ 'OK' ]:
      return storageElements
    storageElements = storageElements[ 'Value' ]

  storageElementEndpoints = []
  
  for se in storageElements:
    
    seEndpoint = getStorageElementEndpoint( se )
    if not seEndpoint[ 'OK' ]:
      continue
    storageElementEndpoints.append( seEndpoint[ 'Value' ] )
  
  return S_OK( list( set( storageElementEndpoints ) ) )

def getSpaceTokens():
  ''' Get Space Tokens '''
  return  S_OK( ( 'LHCb_USER', 'LHCb-Disk', 'LHCb-Tape', 'LHCb-EOS' ) )

def getSpaceTokenEndpoints():
  ''' Get Space Token Endpoints '''
  
  return Utils.getCSTree( 'Shares/Disk' )

#def getVOMSEndpoints():
#  ''' Get VOMS endpoints '''
#  
#  endpoints = gConfig.getSections( '/Registry/VOMS/Servers/lhcb' )
#  if endpoints[ 'OK' ]:
#    return endpoints[ 'Value' ]
#  return [] 

#def getFileCatalogs():
#  '''
#    Gets all storage elements from /Resources/FileCatalogs
#  '''
#  
#  _basePath = 'Resources/FileCatalogs'
#    
#  fileCatalogs = gConfig.getSections( _basePath )
#  return fileCatalogs 

def getComputingElements():
  '''
    Gets all computing elements from /Resources/Sites/<>/<>/CE
  '''
  #_basePath = 'Resources/Sites'
  
  #ces = []
  
  #domainNames = gConfig.getSections( _basePath )
  #if not domainNames[ 'OK' ]:
  #  return domainNames
  #domainNames = domainNames[ 'Value' ]
  
  #for domainName in domainNames:
  #  domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
  #  if not domainSites[ 'OK' ]:
  #    return domainSites
  #  domainSites = domainSites[ 'Value' ]
  #  
  #  for site in domainSites:
  #    siteCEs = gConfig.getSections( '%s/%s/%s/CEs' % ( _basePath, domainName, site ) )
  #    if not siteCEs[ 'OK' ]:
  #      #return siteCEs
  #      gLogger.error( siteCEs[ 'Message' ] )
  #      continue
  #    siteCEs = siteCEs[ 'Value' ]
  #    ces.extend( siteCEs )  

  # Remove duplicated ( just in case )
  #ces = list( set ( ces ) )
    
  #return S_OK( ces ) 

  resources = Resources()
  result = resources.getEligibleComputingElements()
  return result

##
# Quick functions implemented for Andrew

def getSiteComputingElements( siteName ):
  '''
    Gets all computing elements from /Resources/Sites/<>/<siteName>/CE
  '''
  
  #_basePath = 'Resources/Sites'
  # 
  #domainNames = gConfig.getSections( _basePath )
  #if not domainNames[ 'OK' ]:
  #  return domainNames
  #domainNames = domainNames[ 'Value' ]
  
  #for domainName in domainNames:
  #  ces = gConfig.getValue( '%s/%s/%s/CE' % ( _basePath, domainName, siteName ), '' )
  #  if ces:
  #    return ces.split( ', ' )
      
  #return []
  
  resources = Resources()
  result = resources.getEligibleStorageElements( { 'Site': siteName } )
  return result
  
def getSiteStorageElements( siteName ):
  '''
    Gets all computing elements from /Resources/Sites/<>/<siteName>/SE
  '''
  
  #_basePath = 'Resources/Sites'
  #
  #domainNames = gConfig.getSections( _basePath )
  #if not domainNames[ 'OK' ]:
  #  return domainNames
  #domainNames = domainNames[ 'Value' ]
  # 
  #for domainName in domainNames:
  #  ses = gConfig.getValue( '%s/%s/%s/SE' % ( _basePath, domainName, siteName ), '' )
  #  if ses:
  #    return ses.split( ', ' )
      
  #return []
  
  resources = Resources()
  result = resources.getEligibleStorageElements( { 'Site': siteName } )
  return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF