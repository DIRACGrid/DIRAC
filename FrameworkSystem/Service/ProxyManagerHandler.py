########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/ProxyManagerHandler.py,v 1.4 2008/07/01 19:31:47 acasajus Exp $
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id: ProxyManagerHandler.py,v 1.4 2008/07/01 19:31:47 acasajus Exp $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.Core.Security import Properties, CS
from DIRAC.Core.Security.VOMS import VOMS

gProxyDB = False


def initializeProxyManagerHandler( serviceInfo ):
  global gProxyDB

  serviceCS = serviceInfo [ 'serviceSectionPath' ]
  requireVoms = gConfig.getValue( "%s/requireVOMS" % serviceCS, "yes" ).lower() in ( "yes", "y", "1" )
  useMyProxy = gConfig.getValue( "%s/UseMyProxy" % serviceCS, "yes" ).lower() in ( "yes", "y", "1" )
  MyProxyServer = gConfig.getValue( "/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch" )
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
    userDN = credDict[ 'DN' ]
    userGroup = credDict[ 'group' ]
    retVal = gProxyDB.getRemainingTime( userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    csOption = "%s/SkipUploadLifeTime" % self.serviceInfoDict[ 'serviceSectionPath' ]
    #If we have a proxy longer than 12h it's not needed
    if remainingSecs > gConfig.getValue( csOption, 43200 ):
      return S_OK()
    return gProxyDB.generateDelegationRequest( credDict[ 'x509Chain' ], userDN, userGroup )

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

  types_getProxy = [ types.StringType, types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_getProxy( self, userDN, userGroup, requestPem, requiredLifetime ):
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

  types_getVOMSProxy = [ types.StringType, types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_getVOMSProxy( self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute = False ):
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
    retVal = gProxyDB.getVOMSProxy( userDN,
                                    userGroup,
                                    requiredLifeTime = requiredLifetime,
                                    requestedVOMSAttr = vomsAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( retVal[ 'Value' ] )

  types_setPersistency = [ types.StringType, types.StringType, types.BooleanType ]
  def export_setPersistency( self, userDN, userGroup, persistentFlag ):
    """
    Set the persistency for a given dn/group
    """
    return gProxyDB.setPersistencyFlag( userDN, userGroup, persistentFlag )

  types_getContents = []
  def export_getContents( self, condDict, start = 0, limit = 0 ):
    """
    Retrieve the contents of the DB
    """
    if type( condDict ) != types.DictType:
      return S_ERROR( "Type mismatch in first parameter" )
    if type( start ) not in ( types.IntType, types.LongType ):
      return S_ERROR( "Type mismatch in second parameter" )
    if type( limit ) not in ( types.IntType, types.LongType ):
      return S_ERROR( "Type mismatch in third parameter" )
    credDict = self.getRemoteCredentials()
    if not Properties.PROXY_MANAGEMENT in credDict[ 'properties' ]:
      result = CS.getDNForUsername( credDict[ 'username' ] )
      if not result[ 'OK' ]:
        return S_ERROR( "You are not a valid user!" )
      condDict[ 'UserDN' ] = result[ 'Value' ]
    return gProxyDB.getProxiesFor( condDict, start, limit )