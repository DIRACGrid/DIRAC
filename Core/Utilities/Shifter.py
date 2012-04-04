########################################################################
# $HeadURL$
########################################################################
""" Handling the donwload of the shifter Proxy
"""
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security import CS

def getShifterProxy( shifterType, fileName = False ):
  """
  This method returns a shifter's proxy
    - shifterType : ProductionManager / DataManager...
  """
  if fileName:
    try:
      os.makedirs( os.path.dirname( fileName ) )
    except OSError:
      pass
  shifterSection = "Shifter/%s" % shifterType
  userName = Operations().getValue( '%s/User' % shifterSection, '' )
  if not userName:
    return S_ERROR( "No shifter defined in %s/User" % shifterSection )
  result = CS.getDNForUsername( userName )
  if not result[ 'OK' ]:
    return result
  userDN = result[ 'Value' ][0]
  userGroup = Operations().getValue( '%s/Group' % shifterSection, CS.getDefaultUserGroup() )
  vomsAttr = CS.getVOMSAttributeForGroup( userGroup )
  if vomsAttr:
    gLogger.info( "Getting VOMS [%s] proxy for shifter %s@%s (%s)" % ( vomsAttr, userName,
                                                                       userGroup, userDN ) )
    result = gProxyManager.downloadVOMSProxy( userDN, userGroup, requiredTimeLeft = 4 * 43200 )
  else:
    gLogger.info( "Getting proxy for shifter %s@%s (%s)" % ( userName, userGroup, userDN ) )
    result = gProxyManager.downloadProxy( userDN, userGroup, requiredTimeLeft = 4 * 43200 )
  if not result[ 'OK' ]:
    return result
  chain = result[ 'Value' ]
  result = gProxyManager.dumpProxyToFile( chain, destinationFile = fileName )
  if not result[ 'OK' ]:
    return result
  fileName = result[ 'Value' ]
  return S_OK( { 'DN' : userDN,
                 'username' : userName,
                 'group' : userGroup,
                 'chain' : chain,
                 'proxyFile' : fileName } )

def setupShifterProxyInEnv( shifterType, fileName = False ):
  """
  Return the shifter's proxy and set it up as the default
  proxy via changing the environment
  This method returns a shifter's proxy
    - shifterType : ProductionManager / DataManager...
  """
  result = getShifterProxy( shifterType, fileName )
  if not result[ 'OK' ]:
    return result
  proxyDict = result[ 'Value' ]
  os.environ[ 'X509_USER_PROXY' ] = proxyDict[ 'proxyFile' ]
  return result
