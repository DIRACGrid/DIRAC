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

def getGOCSites( diracSites = None ):
  
  #FIXME: THIS SHOULD GO INTO Resources HELPER
  
  if diracSites is None:
    diracSites = Resources.getSites()
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


def getStorageElementsHosts( seNames = None ):
  
  seHosts = []
  
  resources = Resources.Resources()
  
  if seNames is None:
    seNames = resources.getEligibleStorageElements()
    if not seNames[ 'OK' ]:
      return seNames
    seNames = seNames[ 'Value' ]
  
  for seName in seNames:
    
    result = getSEProtocolOption( seName, 'Host' )
    if result['OK']:
      seHosts.append( result['Value'] )
      
  return S_OK( list( set( seHosts ) ) )

def getSEProtocolOption( se, optionName ):
  """ 
    Get option of the Storage Element access protocol
  """
  resources = Resources.Resources()
  result = resources.getAccessProtocols( se )
  if not result['OK']:
    return S_ERROR( "Acces Protocol for SE %s not found: %s" % ( se, result['Message'] ) )
  
  try:
    ap = result['Value'][0]
  except IndexError:
    return S_ERROR( 'No AccessProtocol associated to %s' % se  )
  
  return resources.getAccessProtocolOption( ap, optionName )

def getStorageElementEndpoint( storageElement ):
  
  resources = Resources.Resources()
  result = resources.getAccessProtocols( storageElement )
  if not result['OK']:
    return result
  # FIXME: There can be several access protocols for the same SE !
  try:
    ap = result['Value'][0]
  except IndexError:
    return S_ERROR( 'No AccessProtocol associated to %s' % storageElement  )
  
  result = resources.getAccessProtocolOptionsDict( ap )
  #result = resources.getAccessProtocols( storageElement )
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
  
  resources = Resources.Resources()  
  
  if storageElements is None:
    storageElements = resources.getEligibleStorageElements()
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

def getSpaceTokenEndpoints():
  ''' Get Space Token Endpoints '''
  
  return Utils.getCSTree( 'Shares/Disk' )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF