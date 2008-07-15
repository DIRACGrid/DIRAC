
import os
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import CS

def getShifterProxy( type, file = False ):
  """
  This method returns a shifter's proxy
    - type : ProductionManager / DataManager...
  """
  if file:
    try:
      os.makedirs( os.dirname( file ) )
    except:
      pass
  shifterSection = "/Operations/Shifter/%s" % type
  userName = gConfig.getValue( '%s/User' % shifterSection, '' )
  if not userName:
    return S_ERROR( "No shifter defined in %s/User" % shifterSection )
  result = CS.getDNForUsername( userName )
  if not result[ 'OK' ]:
    return result
  userDN = result[ 'Value' ][0]
  userGroup = gConfig.getValue( '%s/Group' % shifterSection, 'lhcb_prod' )
  gLogger.info( "Getting proxy for shifter %s@%s (%s)" % ( userName, userGroup, userDN ) )
  result = gProxyManager.downloadVOMSProxy( userDN, userGroup )
  if not result[ 'OK' ]:
    return result
  chain = result[ 'Value' ]
  result = gProxyManager.dumpProxyToFile( chain, destinationFile = file )
  if not result[ 'OK' ]:
    return result
  fileName = result[ 'Value' ]
  return S_OK( { 'DN' : userDN,
                 'username' : userName,
                 'group' : userGroup,
                 'chain' : result[ 'Value' ],
                 'proxyFile' : fileName } )

def setupShifterProxyInEnv( type, file = False ):
  """
  Return the shifter's proxy and set it up as the default
  proxy via changing the environment
  This method returns a shifter's proxy
    - type : ProductionManager / DataManager...
  """
  result = getShifterProxy( type, file )
  if not result[ 'OK' ]:
    return result
  proxyDict = result[ 'Value' ]
  os.environ[ 'X509_USER_PROXY' ] = proxyDict[ 'proxyFile' ]
  return result