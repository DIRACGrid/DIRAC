########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/ProxyManagerHandler.py,v 1.1 2008/06/25 20:00:51 acasajus Exp $
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id: ProxyManagerHandler.py,v 1.1 2008/06/25 20:00:51 acasajus Exp $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.Core.Security import Properties

gProxyDB = False


def initializeProxyManagerHandler( serviceInfo ):
  global gProxyDB

  serviceCS = serviceInfo [ 'serviceSectionPath' ]
  requireVoms = gConfig.getValue( "%s/requireVOMS" % serviceCS, "yes" ).lower() in ( "yes", "y", "1" )
  useMyProxy = gConfig.getValue( "%s/UseMyProxy" % serviceCS, "yes" ).lower() in ( "yes", "y", "1" )
  MyProxyServer = gConfig.getValue( "%s/MyProxyServer" % serviceCS, "myproxy.cern.ch" )
  gLogger.info( "VOMS: %s\nMyProxy: %s\n MyProxy Server: %s" % ( requireVoms, useMyProxy, MyProxyServer ) )
  try:
    gProxyDB = ProxyDB( requireVoms = requireVoms,
                        useMyProxy = useMyProxy,
                         MyProxyServer = MyProxyServer )
  except:
    return S_ERROR( "Can't initialize ProxyDB" )
  return S_OK()

class ProxyManagerHandler( RequestHandler ):

  types_requestDelegation = []
  def export_requestDelegation( self ):
    """ Request a delegation. Send a delegation request to client
    """
    credDict = self.getRemoteCredentials()
    return gProxyDB.generateDelegationRequest( credDict[ 'x509Chain' ], credDict[ 'DN' ], credDict[ 'group' ] )

  types_completeDelegation = [ ( types.IntType, types.LongType ), types.StringType ]
  def export_completeDelegation( self, requestId, pemChain ):
    """ Upload result of delegation
    """
    credDict = self.getRemoteCredentials()
    return gProxyDB.completeDelegation( requestId, credDict[ 'DN' ], credDict[ 'group' ], pemChain )

  types_getRegisteredUsers = []
  def export_getRegisteredUsers( self, validSecondsRequired = 0 ):
    """
    Get the list of users who have a valid proxy in the system
      - validSecondsRequired is an optional argument to specify the required
          seconds the proxy is valid for
    """
    return gProxyDB.getUsers( validSecondsRequired )

  types_getDelegatedProxy = [ types.StringType, types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_getDelegatedProxy( self, userDN, userGroup, requestPem, requiredLifetime ):
    """
    Get the list of users who have a valid proxy in the system
      - validSecondsRequired is an optional argument to specify the required
          seconds the proxy is valid for
      * Properties :
        FullDelegation <- permits full delegation of proxies
    """
    credDict = self.getRemoteCredentials()
    forceLimited = True
    if Properties.FULL_DELEGATION in credDict[ 'properties' ]:
      forceLimited = False
    retVal = gProxyDB.getProxy( userDN, userGroup, requiredLifeTime = requiredLifetime )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( retVal[ 'Value' ] )
