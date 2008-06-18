########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/Attic/ProxyManagerHandler.py,v 1.1 2008/06/18 19:59:06 acasajus Exp $
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id: ProxyManagerHandler.py,v 1.1 2008/06/18 19:59:06 acasajus Exp $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB
from DIRAC.Core.Security import Properties

# This is a global instance of the JobDB class
gProxyDB = False

def initializeProxyManagerHandler( serviceInfo ):
  global gProxyDB

  gProxyDB = ProxyRepositoryDB()
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
    #Remove this when we get to prod
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
