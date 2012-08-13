# $HeadURL:  $
''' CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.  
'''

from DIRAC                                import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities import Utils

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

  _basePath = 'Resources/Sites'
  
  sites = []
  
  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]
  
  for domainName in domainNames:
    domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
    if not domainSites[ 'OK' ]:
      return domainSites
    
    domainSites = domainSites[ 'Value' ]
    
    sites.extend( domainSites )  

  # Remove duplicated ( just in case )
  sites = list( set ( sites ) )
  return S_OK( sites )

def getDomainSites():
  '''
    Gets all sites from /Resources/Sites
  '''

  _basePath = 'Resources/Sites'
  
  sites = {}
  
  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]
  
  for domainName in domainNames:
    domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
    if not domainSites[ 'OK' ]:
      return domainSites
    
    domainSites = domainSites[ 'Value' ]
    
    sites[ domainName ] = domainSites  

  return S_OK( sites )

def getResources():
  '''
    Gets all resources
  '''
  
  resources = []
  
  ses = getStorageElements()
  if ses[ 'OK' ]:
    resources = resources + ses[ 'Value' ]
  
  fts = getFTS()
  if fts[ 'OK' ]:
    resources = resources + fts[ 'Value' ]
  
  fc = getFileCatalogs()
  if fc[ 'OK' ]:
    resources = resources + fc[ 'Value' ]
  
  ce = getComputingElements() 
  if ce[ 'OK' ]:
    resources = resources + ce[ 'Value' ]

  return S_OK( resources )

def getNodes():
  '''
    Gets all nodes
  '''
  
  nodes = []
  
  queues = getQueues()
  if queues[ 'OK' ]:
    nodes = nodes + queues[ 'Value' ] 
  
  return S_OK( nodes )

################################################################################

def getStorageElements():
  '''
    Gets all storage elements from /Resources/StorageElements
  '''
  
  _basePath = 'Resources/StorageElements'
    
  seNames = gConfig.getSections( _basePath )
  return seNames 

def getSEToken( se ):
  ''' 
    Get StorageElement token 
  '''
  
  _basePath = '/Resources/StorageElements/%s/AccessProtocol.1/SpaceToken'
  
  #FIXME: return S_OK, S_ERROR
  return gConfig.getValue( _basePath % se, '' )

def getStorageElementSpaceToken( storageElement ):
  
  _basePath = '/Resources/StorageElements/%s/AccessProtocol.1/SpaceToken' % storageElement
  
  res = gConfig.getValue( _basePath  )
  if not res:
    return S_ERROR( '%s not found' % _basePath )
  return S_OK( res )

def getStorageElementEndpoint( storageElement ):
  
  _basePath = '/Resources/StorageElements/%s/AccessProtocol.1' % storageElement
  
  host  = gConfig.getValue( _basePath + '/Host' )
  port  = gConfig.getValue( _basePath + '/Port' ) 
  wsurl = gConfig.getValue( _basePath + '/WSUrl' )
  
  if host and port and wsurl:
     
    url = 'httpg://%s:%s/%s' % ( host, port, wsurl )
    return S_OK( url )
  
  return S_ERROR( ( host, port, wsurl ) )
  
def getFTS():
  '''
    Gets all storage elements from /Resources/FTSEndpoints
  '''
  
  _basePath = 'Resources/FTSEndpoints'
    
  ftsEndpoints = gConfig.getOptions( _basePath )
  return ftsEndpoints 

def getSpaceTokens():
  ''' Get Space Tokens '''
  return  S_OK( ( 'LHCb_USER', 'LHCb-Disk', 'LHCb-Tape' ) )

def getSpaceTokenEndpoints():
  ''' Get Space Token Endpoints '''
  
  return Utils.getCSTree( 'Shares/Disk' )
  
  #return getTypedDictRootedAt(root="", relpath="/Resources/Shares/Disk")

#def getVOMSEndpoints():
#  ''' Get VOMS endpoints '''
#  
#  endpoints = gConfig.getSections( '/Registry/VOMS/Servers/lhcb' )
#  if endpoints[ 'OK' ]:
#    return endpoints[ 'Value' ]
#  return [] 

def getFileCatalogs():
  '''
    Gets all storage elements from /Resources/FileCatalogs
  '''
  
  _basePath = 'Resources/FileCatalogs'
    
  fileCatalogs = gConfig.getSections( _basePath )
  return fileCatalogs 

def getComputingElements():
  '''
    Gets all computing elements from /Resources/Sites/<>/<>/CE
  '''
  _basePath = 'Resources/Sites'
  
  ces = []
  
  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]
  
  for domainName in domainNames:
    domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
    if not domainSites[ 'OK' ]:
      return domainSites
    domainSites = domainSites[ 'Value' ]
    
    for site in domainSites:
      siteCEs = gConfig.getSections( '%s/%s/%s/CEs' % ( _basePath, domainName, site ) )
      if not siteCEs[ 'OK' ]:
        #return siteCEs
        gLogger.error( siteCEs[ 'Message' ] )
        continue
      siteCEs = siteCEs[ 'Value' ]
      ces.extend( siteCEs )  

  # Remove duplicated ( just in case )
  ces = list( set ( ces ) )
    
  return S_OK( ces ) 

def getQueues():
  '''
    Gets all computing elements from /Resources/Sites/<>/<>/CE/Queues
  '''
  _basePath = 'Resources/Sites'
  
  queues = []
  
  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]
  
  for domainName in domainNames:
    domainSites = gConfig.getSections( '%s/%s' % ( _basePath, domainName ) )
    if not domainSites[ 'OK' ]:
      return domainSites
    domainSites = domainSites[ 'Value' ]
    
    for site in domainSites:
      siteCEs = gConfig.getSections( '%s/%s/%s/CEs' % ( _basePath, domainName, site ) )
      if not siteCEs[ 'OK' ]:
        #return siteCEs
        gLogger.error( siteCEs[ 'Message' ] )
        continue
      siteCEs = siteCEs[ 'Value' ]
      
      for siteCE in siteCEs:
        siteQueue = gConfig.getSections( '%s/%s/%s/CEs/%s/Queues' % ( _basePath, domainName, site, siteCE ) )
        if not siteQueue[ 'OK' ]:
          #return siteQueue
          gLogger.error( siteQueue[ 'Message' ] )
          continue
        siteQueue = siteQueue[ 'Value' ]
        
        queues.extend( siteQueue )  

  # Remove duplicated ( just in case )
  queues = list( set ( queues ) )
    
  return S_OK( queues ) 

## /Registry ###################################################################

def getRegistryUsers():
  '''
    Gets all users from /Registry/Users
  '''

  _basePath = 'Registry/Users' 

  registryUsers = {}

  userNames = gConfig.getSections( _basePath )  
  if not userNames[ 'OK' ]:
    return userNames
  userNames = userNames[ 'Value' ]
  
  for userName in userNames:
    
    # returns { 'Email' : x, 'DN': y, 'CA' : z }
    userDetails = gConfig.getOptionsDict( '%s/%s' % ( _basePath, userName ) )
    if not userDetails[ 'OK' ]:   
      return userDetails
    
    registryUsers[ userName ] = userDetails[ 'Value' ]
    
  return S_OK( registryUsers )   

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF