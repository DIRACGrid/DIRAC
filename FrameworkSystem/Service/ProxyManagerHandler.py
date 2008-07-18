########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/ProxyManagerHandler.py,v 1.9 2008/07/18 11:06:15 acasajus Exp $
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id: ProxyManagerHandler.py,v 1.9 2008/07/18 11:06:15 acasajus Exp $"

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
  gLogger.info( "VOMS: %s\nMyProxy: %s\n MyProxy Server: %s" % ( requireVoms, useMyProxy, MyProxyServer ) )
  try:
    gProxyDB = ProxyDB( requireVoms = requireVoms,
                        useMyProxy = useMyProxy )
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
    retVal = gProxyDB.completeDelegation( requestId, credDict[ 'DN' ], credDict[ 'group' ], pemChain )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  types_getRegisteredUsers = []
  def export_getRegisteredUsers( self, validSecondsRequired = 0 ):
    """
    Get the list of users who have a valid proxy in the system
      - validSecondsRequired is an optional argument to specify the required
          seconds the proxy is valid for
    """
    credDict = self.getRemoteCredentials()
    if not Properties.PROXY_MANAGEMENT in credDict[ 'properties' ]:
      gProxyDB.getUsers( validSecondsRequired, dnMask = [ credDict[ 'DN' ] ] )
    return gProxyDB.getUsers( validSecondsRequired )

  types_getProxy = [ types.StringType, types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_getProxy( self, userDN, userGroup, requestPem, requiredLifetime ):
    """
    Get a proxy for a userDN/userGroup
      - requestPem : PEM encoded request object for delegation
      - requiredLifetime: Argument for length of proxy
      * Properties :
        FullDelegation <- permits full delegation of proxies
        LimitedDelegation <- permits downloading only limited proxies
        PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()

    if Properties.FULL_DELEGATION in credDict[ 'properties' ]:
      forceLimited = False
    else:
      forceLimited = True
      if Properties.PRIVATE_LIMITED_DELEGATION in credDict[ 'properties' ]:
        if credDict[ 'DN' ] != userDN:
          return S_ERROR( "You are not allowed to download any proxy" )

    retVal = gProxyDB.getProxy( userDN, userGroup, requiredLifeTime = requiredLifetime )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    if not retVal[ 'OK' ]:
      return retVal
    gProxyDB.logAction( "download proxy", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return S_OK( retVal[ 'Value' ] )

  types_getVOMSProxy = [ types.StringType, types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_getVOMSProxy( self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute = False ):
    """
    Get a proxy for a userDN/userGroup
      - requestPem : PEM encoded request object for delegation
      - requiredLifetime: Argument for length of proxy
      - vomsAttribute : VOMS attr to add to the proxy
      * Properties :
        FullDelegation <- permits full delegation of proxies
        LimitedDelegation <- permits downloading only limited proxies
        PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()

    if Properties.FULL_DELEGATION in credDict[ 'properties' ]:
      forceLimited = False
    else:
      forceLimited = True
      if Properties.PRIVATE_LIMITED_DELEGATION in credDict[ 'properties' ]:
        if credDict[ 'DN' ] != userDN:
          return S_ERROR( "You are not allowed to download any proxy" )

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
    gProxyDB.logAction( "download voms proxy", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return S_OK( retVal[ 'Value' ] )

  types_setPersistency = [ types.StringType, types.StringType, types.BooleanType ]
  def export_setPersistency( self, userDN, userGroup, persistentFlag ):
    """
    Set the persistency for a given dn/group
    """
    retVal = gProxyDB.setPersistencyFlag( userDN, userGroup, persistentFlag )
    if not retVal[ 'OK' ]:
      return retVal
    gProxyDB.logAction( "set persistency to %s" % bool( persistentFlag ),
                                                  credDict[ 'DN' ],
                                                  credDict[ 'group' ],
                                                  userDN,
                                                  userGroup )
    return S_OK()

  types_deleteProxyBundle = [ ( types.ListType, types.TupleType ) ]
  def export_deleteProxyBundle( self, idList ):
    """
    delete a list of id's
    """
    errorInDelete = []
    deleted = 0
    for id in idList:
      if len( id ) != 2:
        errorInDelete.append( "%s doesn't have two fields" % str(id) )
      retVal = self.export_deleteProxy( id[0], id[1] )
      if not retVal[ 'OK' ]:
        errorInDelete.append( "%s : %s" %( str(id), retVal[ 'Message' ] ) )
      else:
        deleted += 1
    if errorInDelete:
      return S_ERROR( "Could not delete some proxies: %s" % ",".join( errorInDelete ) )
    return S_OK( deleted )

  types_deleteProxy = [ ( types.ListType, types.TupleType ) ]
  def export_deleteProxy( self, userDN, userGroup ):
    """
    Delete a proxy from the DB
    """
    credDict = self.getRemoteCredentials()
    if not Properties.PROXY_MANAGEMENT in credDict[ 'properties' ]:
      if userDN != credDict[ 'DN' ]:
        return S_ERROR( "You aren't allowed! Bad boy!" )
    retVal =  gProxyDB.deleteProxy( userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    gProxyDB.logAction( "delete proxy", credDict[ 'DN' ], credDict[ 'group' ],
                                        userDN, userGroup )
    return S_OK()

  types_getContents = [ types.DictType, ( types.ListType, types.TupleType ),
                       ( types.IntType, types.LongType ), ( types.IntType, types.LongType ) ]
  def export_getContents( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    credDict = self.getRemoteCredentials()
    if not Properties.PROXY_MANAGEMENT in credDict[ 'properties' ]:
      result = CS.getDNForUsername( credDict[ 'username' ] )
      if not result[ 'OK' ]:
        return S_ERROR( "You are not a valid user!" )
      selDict[ 'UserDN' ] = result[ 'Value' ]
    return gProxyDB.getProxiesContent( selDict, sortDict, start, limit )

  types_getLogContents = [ types.DictType, ( types.ListType, types.TupleType ),
                       ( types.IntType, types.LongType ), ( types.IntType, types.LongType ) ]
  def export_getLogContents( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    return gProxyDB.getLogsContent( selDict, sortDict, start, limit )