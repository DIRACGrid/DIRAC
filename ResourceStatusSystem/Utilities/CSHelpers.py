# $HeadURL:  $
''' CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.  
'''

from DIRAC                                                 import gConfig, gLogger, S_OK

__RCSID__ = '$Id:  $'

def warmUp():
  '''
    gConfig has its own dark side, it needs some warm up phase.  
  '''
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()

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

def getStorageElements():
  '''
    Gets all storage elements from /Resources/StorageElements
  '''
  
  _basePath = 'Resources/StorageElements'
    
  seNames = gConfig.getSections( _basePath )
  return seNames 

def getFTS():
  '''
    Gets all storage elements from /Resources/FTSEndpoints
  '''
  
  _basePath = 'Resources/FTSEndpoints'
    
  ftsEndpoints = gConfig.getOptions( _basePath )
  return ftsEndpoints 

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