import os
import base64
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain, g_X509ChainType
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import Locations, CS


def getProxyInfo( proxy = False, disableVOMS = False ):
  """
  Returns a dict with all the proxy info
  * values that will be there always
   'chain' : chain object containing the proxy
   'subject' : subject of the proxy
   'issuer' : issuer of the proxy
   'isProxy' : bool
   'isLimitedProxy' : bool
   'validDN' : Valid DN in DIRAC
   'validGroup' : Valid Group in DIRAC
   'secondsLeft' : Seconds left
  * values that can be there
   'path' : path to the file,
   'group' : DIRAC group
   'groupProperties' : Properties that apply to the DIRAC Group
   'username' : DIRAC username
   'identity' : DN that generated the proxy
   'hostname' : DIRAC host nickname
   'VOMS'
  """
  #Discover proxy location
  proxyLocation = False
  if type( proxy ) == g_X509ChainType:
    chain = proxy
  else:
    if not proxy:
      proxyLocation = Locations.getProxyLocation()
    elif type( proxy ) in ( types.StringType, types.UnicodeType ):
      proxyLocation = proxy
    if not proxyLocation:
      return S_ERROR( "Can't find a valid proxy" )
    chain = X509Chain()
    retVal = chain.loadProxyFromFile( proxyLocation )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load %s: %s " % ( proxyLocation, retVal[ 'Message' ] ) )

  retVal = chain.getCredentials()
  if not retVal[ 'OK' ]:
    return retVal

  infoDict = retVal[ 'Value' ]
  infoDict[ 'chain' ] = chain
  if proxyLocation:
    infoDict[ 'path' ] = proxyLocation

  if not disableVOMS and chain.isVOMS()['Value']:
    retVal = VOMS().getVOMSAttributes( chain )
    if retVal[ 'OK' ]:
      infoDict[ 'VOMS' ] = retVal[ 'Value' ]

  return S_OK( infoDict )

def getProxyInfoAsString( proxyLoc = False, disableVOMS = False ):
  retVal = getProxyInfo( proxyLoc, disableVOMS )
  if not retVal[ 'OK' ]:
    return retVal
  infoDict = retVal[ 'Value' ]
  return S_OK( formatProxyInfoAsString( infoDict ) )

def formatProxyInfoAsString( infoDict ):
  leftAlign = 13
  contentList = []
  for field in ( 'subject', 'issuer', 'identity', ( 'secondsLeft', 'time left' ),
                 ( 'group', 'DIRAC group' ), 'path', 'username', ( 'VOMS', 'VOMS fqan' ) ):
    if type( field ) == types.StringType:
      dispField = field
    else:
      dispField = field[1]
      field = field[0]
    if not field in infoDict:
      continue
    if field == 'secondsLeft':
      secs = infoDict[ field ]
      hours = int( secs /  3600 )
      secs -= hours * 3600
      mins = int( secs / 60 )
      secs -= mins * 60
      value = "%02d:%02d:%02d" % ( hours, mins, secs )
    else:
      value = infoDict[ field ]
    contentList.append( "%s: %s" % ( dispField.ljust( leftAlign ), value ) )
  return "\n".join( contentList )


def getProxyStepsInfo( chain ):
  infoList = []
  nC = chain.getNumCertsInChain()['Value']
  for i in range( nC ):
    cert = chain.getCertInChain( i )['Value']
    stepInfo = {}
    stepInfo[ 'subject' ] = cert.getSubjectDN()['Value']
    stepInfo[ 'issuer' ] = cert.getIssuerDN()['Value']
    stepInfo[ 'serial' ] = cert.getSerialNumber()['Value']
    stepInfo[ 'not before' ] = cert.getNotBeforeDate()['Value']
    stepInfo[ 'not after' ] = cert.getNotAfterDate()['Value']
    dG = cert.getDIRACGroup( ignoreDefault = True )['Value']
    if dG:
      stepInfo[ 'group' ] = dG
    if cert.hasVOMSExtensions()[ 'Value' ]:
      stepInfo[ 'VOMS ext' ] = True
    infoList.append( stepInfo )
  return S_OK( infoList )

def formatProxyStepsInfoAsString( infoList ):
  contentsList = []
  for i in range( len( infoList ) ):
    contentsList.append( " + Step %s" % i )
    stepInfo = infoList[i]
    for key in ( 'subject', 'issuer', 'serial', 'not after', 'not before', 'group', 'VOMS ext' ):
      if key in stepInfo:
        value = stepInfo[ key ]
        if key == 'serial':
          value = base64.b16encode( value )
        contentsList.append( "  %s : %s" % ( key.ljust(10).capitalize(), value ) )
  return "\n".join( contentsList )