########################################################################
# $HeadURL$
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id$"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.Core.Security import Properties, CS
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

gProxyDB = False


def initializeProxyManagerHandler( serviceInfo ):
  global gProxyDB

  serviceCS = serviceInfo [ 'serviceSectionPath' ]
  requireVoms = gConfig.getValue( "%s/RequireVOMS" % serviceCS, "no" ).lower() in ( "yes", "y", "1" )
  useMyProxy = gConfig.getValue( "%s/UseMyProxy" % serviceCS, "no" ).lower() in ( "yes", "y", "1" )
  try:
    gProxyDB = ProxyDB( requireVoms = requireVoms,
                        useMyProxy = useMyProxy )
  except:
    return S_ERROR( "Can't initialize ProxyDB" )
  gThreadScheduler.addPeriodicTask( 900, gProxyDB.purgeExpiredTokens, elapsedTime = 900 )
  gThreadScheduler.addPeriodicTask( 900, gProxyDB.purgeExpiredRequests, elapsedTime = 900 )
  gThreadScheduler.addPeriodicTask( 3600, gProxyDB.purgeLogs )
  gThreadScheduler.addPeriodicTask( 3600, gProxyDB.purgeExpiredProxies )
  gLogger.info( "VOMS: %s\nMyProxy: %s\n MyProxy Server: %s" % ( requireVoms, useMyProxy, gProxyDB.getMyProxyServer() ) )
  return S_OK()

class ProxyManagerHandler( RequestHandler ):

  __maxExtraLifeFactor = 1.5

  def __generateUserProxiesInfo( self ):
    proxiesInfo = {}
    credDict = self.getRemoteCredentials()
    result = Registry.getDNForUsername( credDict[ 'username' ] )
    if not result[ 'OK' ]:
      return retDict
    selDict = { 'UserDN' : result[ 'Value' ] }
    result = gProxyDB.getProxiesContent( selDict, {}, 0, 0 )
    if not result[ 'OK']:
      return retDict
    contents = result[ 'Value' ]
    userDNIndex = contents[ 'ParameterNames' ].index( "UserDN" )
    userGroupIndex = contents[ 'ParameterNames' ].index( "UserGroup" )
    expirationIndex = contents[ 'ParameterNames' ].index( "ExpirationTime" )
    for record in contents[ 'Records' ]:
      userDN = record[ userDNIndex ]
      if userDN not in proxiesInfo:
        proxiesInfo[ userDN ] = {}
      userGroup = record[ userGroupIndex ]
      proxiesInfo[ userDN ][ userGroup ] = record[ expirationIndex  ]
    return proxiesInfo

  def __addKnownUserProxiesInfo( self, retDict ):
    """ Given a S_OK/S_ERR add a proxies entry with info of all the proxies a user has uploaded
    """
    retDict[ 'proxies' ] = self.__generateUserProxiesInfo()
    return retDict

  auth_getUserProxiesInfo = [ 'authenticated' ]
  types_getUserProxiesInfo = []
  def export_getUserProxiesInfo( self ):
    """ Get the info about the user proxies in the system
    """
    return S_OK( self.__generateUserProxiesInfo() )

  types_requestDelegationUpload = [ ( types.IntType, types.LongType ), ( types.StringType, types.BooleanType ) ]
  def export_requestDelegationUpload( self, requestedUploadTime, userGroup ):
    """ Request a delegation. Send a delegation request to client
    """
    credDict = self.getRemoteCredentials()
    userDN = credDict[ 'DN' ]
    userName = credDict[ 'username' ]
    if not userGroup:
      userGroup = credDict[ 'group' ]
    retVal = Registry.getGroupsForUser( credDict[ 'username' ] )
    if not retVal[ 'OK' ]:
      return retVal
    groupsAvailable = retVal[ 'Value' ]
    if userGroup not in groupsAvailable:
      return S_ERROR( "%s is not a valid group for user %s" % ( userGroup, userName ) )
    clientChain = credDict[ 'x509Chain' ]
    clientSecs = clientChain.getIssuerCert()[ 'Value' ].getRemainingSecs()[ 'Value' ]
    requestedUploadTime = min( requestedUploadTime, clientSecs )
    retVal = gProxyDB.getRemainingTime( userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    #If we have a proxy longer than the one uploading it's not needed
    #ten minute margin to compensate just in case
    if remainingSecs >= requestedUploadTime - 600:
      gLogger.info( "Upload request not necessary by %s:%s" % ( userName, userGroup ) )
      return self.__addKnownUserProxiesInfo( S_OK() )
    result = gProxyDB.generateDelegationRequest( credDict[ 'x509Chain' ], userDN )
    if result[ 'OK' ]:
      gLogger.info( "Upload request by %s:%s given id %s" % ( userName, userGroup, result['Value']['id'] ) )
    else:
      gLogger.error( "Upload request failed", "by %s:%s : %s" % ( userName, userGroup, result['Message'] ) )
    return result

  types_completeDelegationUpload = [ ( types.IntType, types.LongType ), types.StringType ]
  def export_completeDelegationUpload( self, requestId, pemChain ):
    """ Upload result of delegation
    """
    credDict = self.getRemoteCredentials()
    userId = "%s:%s" % ( credDict[ 'username' ], credDict[ 'group' ] )
    retVal = gProxyDB.completeDelegation( requestId, credDict[ 'DN' ], pemChain )
    if not retVal[ 'OK' ]:
      gLogger.error( "Upload proxy failed", "id: %s user: %s message: %s" % ( requestId, userId, retVal[ 'Message' ] ) )
      return self.__addKnownUserProxiesInfo( retVal )
    gLogger.info( "Upload %s by %s completed" % ( requestId, userId ) )
    return self.__addKnownUserProxiesInfo( S_OK() )

  types_getRegisteredUsers = []
  def export_getRegisteredUsers( self, validSecondsRequired = 0 ):
    """
    Get the list of users who have a valid proxy in the system
      - validSecondsRequired is an optional argument to specify the required
          seconds the proxy is valid for
    """
    credDict = self.getRemoteCredentials()
    if not Properties.PROXY_MANAGEMENT in credDict[ 'properties' ]:
      return gProxyDB.getUsers( validSecondsRequired, userMask = [ credDict[ 'username' ] ] )
    return gProxyDB.getUsers( validSecondsRequired )

  def __checkProperties( self, requestedUserDN, requestedUserGroup ):
    """
    Check the properties and return if they can only download limited proxies if authorized
    """
    credDict = self.getRemoteCredentials()
    if Properties.FULL_DELEGATION in credDict[ 'properties' ]:
      return S_OK( False )
    if Properties.LIMITED_DELEGATION in credDict[ 'properties' ]:
      return S_OK( True )
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict[ 'properties' ]:
      if credDict[ 'DN' ] != requestedUserDN:
        return S_ERROR( "You are not allowed to download any proxy" )
      if Properties.PRIVATE_LIMITED_DELEGATION in Registry.getPropertiesForGroup( requestedUserGroup ):
        return S_ERROR( "You can't download proxies for that group" )
      return S_OK( True )
    #Not authorized!
    return S_ERROR( "You can't get proxies! Bad boy!" )

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

    result = self.__checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    forceLimited = result[ 'Value' ]

    gProxyDB.logAction( "download proxy", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return self.__getProxy( userDN, userGroup, requestPem, requiredLifetime, forceLimited )

  def __getProxy( self, userDN, userGroup, requestPem, requiredLifetime, forceLimited ):
    """
    Internal to get a proxy
    """
    retVal = gProxyDB.getProxy( userDN, userGroup, requiredLifeTime = requiredLifetime )
    if not retVal[ 'OK' ]:
      return retVal
    chain, secsLeft = retVal[ 'Value' ]
    #If possible we return a proxy 1.5 longer than requested
    requiredLifetime = min( secsLeft, requiredLifetime * self.__maxExtraLifeFactor )
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    if not retVal[ 'OK' ]:
      return retVal
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

    result = self.__checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    forceLimited = result[ 'Value' ]

    gProxyDB.logAction( "download voms proxy", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return self.__getVOMSProxy( userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited )

  def __getVOMSProxy( self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited ):
    retVal = gProxyDB.getVOMSProxy( userDN,
                                    userGroup,
                                    requiredLifeTime = requiredLifetime,
                                    requestedVOMSAttr = vomsAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    chain, secsLeft = retVal[ 'Value' ]
    #If possible we return a proxy 1.5 longer than requested
    requiredLifetime = min( secsLeft, requiredLifetime * self.__maxExtraLifeFactor )
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    if not retVal[ 'OK' ]:
      return retVal
    credDict = self.getRemoteCredentials()
    return S_OK( retVal[ 'Value' ] )

  types_setPersistency = [ types.StringType, types.StringType, types.BooleanType ]
  def export_setPersistency( self, userDN, userGroup, persistentFlag ):
    """
    Set the persistency for a given dn/group
    """
    retVal = gProxyDB.setPersistencyFlag( userDN, userGroup, persistentFlag )
    if not retVal[ 'OK' ]:
      return retVal
    credDict = self.getRemoteCredentials()
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
        errorInDelete.append( "%s doesn't have two fields" % str( id ) )
      retVal = self.export_deleteProxy( id[0], id[1] )
      if not retVal[ 'OK' ]:
        errorInDelete.append( "%s : %s" % ( str( id ), retVal[ 'Message' ] ) )
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
    retVal = gProxyDB.deleteProxy( userDN, userGroup )
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
      selDict[ 'UserName' ] = credDict[ 'username' ]
    return gProxyDB.getProxiesContent( selDict, sortDict, start, limit )

  types_getLogContents = [ types.DictType, ( types.ListType, types.TupleType ),
                       ( types.IntType, types.LongType ), ( types.IntType, types.LongType ) ]
  def export_getLogContents( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    return gProxyDB.getLogsContent( selDict, sortDict, start, limit )


  types_generateToken = [ types.StringType, types.StringType, ( types.IntType, types.LongType ) ]
  def export_generateToken( self, requesterDN, requesterGroup, tokenUses ):
    """
    Generate tokens for proxy retrieval
    """
    credDict = self.getRemoteCredentials()
    gProxyDB.logAction( "generate tokens", credDict[ 'DN' ], credDict[ 'group' ], requesterDN, requesterGroup )
    return gProxyDB.generateToken( requesterDN, requesterGroup, numUses = tokenUses )

  types_getProxyWithToken = [ types.StringType, types.StringType,
                              types.StringType, ( types.IntType, types.LongType ),
                              types.StringType ]
  def export_getProxyWithToken( self, userDN, userGroup, requestPem, requiredLifetime, token ):
    """
    Get a proxy for a userDN/userGroup
      - requestPem : PEM encoded request object for delegation
      - requiredLifetime: Argument for length of proxy
      - token : Valid token to get a proxy
      * Properties :
        FullDelegation <- permits full delegation of proxies
        LimitedDelegation <- permits downloading only limited proxies
        PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()
    result = gProxyDB.useToken( token, credDict[ 'DN' ], credDict[ 'group' ] )
    gLogger.info( "Trying to use token %s by %s:%s" % ( token, credDict[ 'DN' ], credDict[ 'group' ] ) )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      return S_ERROR( "Proxy token is invalid" )
    gProxyDB.logAction( "used token", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )

    result = self.__checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    gProxyDB.logAction( "download proxy with token", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return self.__getProxy( userDN, userGroup, requestPem, requiredLifetime, True )

  types_getVOMSProxyWithToken = [ types.StringType, types.StringType,
                                  types.StringType, ( types.IntType, types.LongType ),
                                  types.StringType ]
  def export_getVOMSProxyWithToken( self, userDN, userGroup, requestPem, requiredLifetime, token, vomsAttribute = False ):
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
    result = gProxyDB.useToken( token, credDict[ 'DN' ], credDict[ 'group' ] )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      return S_ERROR( "Proxy token is invalid" )
    gProxyDB.logAction( "used token", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )

    result = self.__checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    gProxyDB.logAction( "download voms proxy with token", credDict[ 'DN' ], credDict[ 'group' ], userDN, userGroup )
    return self.__getVOMSProxy( userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, True )
