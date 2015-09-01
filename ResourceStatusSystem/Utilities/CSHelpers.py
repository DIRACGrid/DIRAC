''' CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.
'''

from DIRAC                                       import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName
from DIRAC.ResourceStatusSystem.Utilities        import Utils
from DIRAC.Resources.Storage.StorageElement      import StorageElement

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

def getStorageElementsHosts( seNames = None ):

  seHosts = []

  if seNames is None:
    seNames = getStorageElements()
    if not seNames[ 'OK' ]:
      return seNames
    seNames = seNames[ 'Value' ]

  for seName in seNames:

    seHost = getSEHost( seName )
    if not seHost['OK']:
      return seHost
    if seHost['Value']:
      seHosts.append( seHost )

  return S_OK( list( set( seHosts ) ) )

def _getSEParameters( seName ):
  se = StorageElement( seName )
  seParameters = se.getStorageParameters( 'SRM2' )
  return seParameters

def getSEToken( seName ):
  ''' Get StorageElement token
  '''

  seParameters = _getSEParameters( seName )
  if not seParameters['OK']:
    return seParameters

  return S_OK( seParameters['Value']['SpaceToken'] )

def getSEHost( seName ):
  ''' Get StorageElement host name
  '''

  seParameters = _getSEParameters( seName )
  if not seParameters['OK']:
    return seParameters

  return S_OK( seParameters['Value']['Host'] )

def getStorageElementEndpoint( seName ):
  """ Get endpoint as combination of host, port, wsurl
  """
  seParameters = _getSEParameters( seName )
  if not seParameters['OK']:
    return seParameters
  host = seParameters['Value']['Host']
  port = seParameters['Value']['Port']
  wsurl = seParameters['Value']['WSUrl']

  # MAYBE wusrl is not defined
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

def getFTS():
  '''
    Gets all storage elements from /Resources/FTSEndpoints
  '''

  ftsEndpoints = []

  fts2 = getFTS2()

  if not fts2['OK']:
    return fts2

  ftsEndpoints += fts2['Value']

  fts3 = getFTS3()

  if not fts3['OK']:
    return fts3

  ftsEndpoints += fts3['Value']



  return S_OK( ftsEndpoints )


def getFTS2():
  '''
    Gets all storage elements from /Resources/FTSEndpoints
  '''

  _basePath = 'Resources/FTSEndpoints/FTS2'

  ftsEndpoints = gConfig.getOptions( _basePath )
  ftsEndpointDefaultLocation = gConfig.getValue( '/Resources/FTSEndpoints/Default/FTSEndpoint', '' )
  if ftsEndpoints['OK'] and ftsEndpointDefaultLocation:
    ftsEndpoints['Value'].append( ftsEndpointDefaultLocation )

  return ftsEndpoints

def getFTS3():
  '''
    Gets all storage elements from /Resources/FTSEndpoints
  '''

  _basePath = 'Resources/FTSEndpoints/FTS3'

  ftsEndpoints = gConfig.getOptions( _basePath )
  return ftsEndpoints

def getSpaceTokenEndpoints():
  ''' Get Space Token Endpoints '''

  return Utils.getCSTree( 'Shares/Disk' )

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
        # return siteCEs
        gLogger.error( siteCEs[ 'Message' ] )
        continue
      siteCEs = siteCEs[ 'Value' ]
      ces.extend( siteCEs )

  # Remove duplicated ( just in case )
  ces = list( set ( ces ) )

  return S_OK( ces )

# #
# Quick functions implemented for Andrew

def getSiteComputingElements( siteName ):
  '''
    Gets all computing elements from /Resources/Sites/<>/<siteName>/CE
  '''

  _basePath = 'Resources/Sites'

  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]

  for domainName in domainNames:
    ces = gConfig.getValue( '%s/%s/%s/CE' % ( _basePath, domainName, siteName ), '' )
    if ces:
      return ces.split( ', ' )

  return []

def getSiteStorageElements( siteName ):
  '''
    Gets all computing elements from /Resources/Sites/<>/<siteName>/SE
  '''

  _basePath = 'Resources/Sites'

  domainNames = gConfig.getSections( _basePath )
  if not domainNames[ 'OK' ]:
    return domainNames
  domainNames = domainNames[ 'Value' ]

  for domainName in domainNames:
    ses = gConfig.getValue( '%s/%s/%s/SE' % ( _basePath, domainName, siteName ), '' )
    if ses:
      return ses.split( ', ' )

  return []

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
        # return siteCEs
        gLogger.error( siteCEs[ 'Message' ] )
        continue
      siteCEs = siteCEs[ 'Value' ]

      for siteCE in siteCEs:
        siteQueue = gConfig.getSections( '%s/%s/%s/CEs/%s/Queues' % ( _basePath, domainName, site, siteCE ) )
        if not siteQueue[ 'OK' ]:
          # return siteQueue
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
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
