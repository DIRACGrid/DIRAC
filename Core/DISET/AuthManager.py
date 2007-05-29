from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig

class AuthManager:

  def __init__( self, authSection ):
    self.authSection = authSection

  def authQuery( self, methodQuery, credDict ):
    #Check if query comes though a gateway/web server
    if self.authIsForwardingCredentials( credDict ):
      self.authUnpackForwardedCredentials( credDict )
      return self.authQuery( methodQuery, credDict )
    if 'DN' in credDict:
      #Get the username
      if not self.authGetUsername( credDict ):
        gLogger.debug( "Query no authorized, user has no valid credentials" )
        return False
    #Check everyone is authorized
    authGroups = self.authGetGroupsForMethod( methodQuery )
    if "any" in authGroups or "all" in authGroups:
      return True
    #Check user is authenticated
    if not 'DN' in credDict:
      gLogger.debug( "User has no credentials" )
      return False
    #Check authorized groups
    if not credDict[ 'group' ] in authGroups and not "authenticated" in authGroups:
      gLogger.debug( "Group is not authorized" )
      return False
    return True

  def authGetGroupsForMethod( self, method ):
    authGroups = gConfig.getValue( "%s/%s" % ( self.authSection, method ), [] )
    if not authGroups:
      authGroups = gConfig.getValue( "%s/Default" % self.authSection, [] )
    return authGroups

  def authIsForwardingCredentials( self, credDict ):
    trustedHostsList = gConfig.getValue( "/DIRAC/Security/TrustedHosts", [] )
    return credDict[ 'DN' ] in trustedHostsList and \
            type( credDict[ 'group' ] ) == types.TupleType

  def authUnpackForwardedCredentials( self, credDict ):
    credDict[ 'DN' ] = credDict[ 'group' ][0]
    credDict[ 'group' ] = credDict[ 'group' ][1]

  def authGetUsername( self, credDict ):
    if not "DN" in credDict:
      return True
    usersInGroup = gConfig.getValue( "/Groups/%s/users" % credDict[ 'group' ], [] )
    if not usersInGroup:
      return False
    userName = self.authFindDNInUsers( credDict[ 'DN' ], usersInGroup )
    if userName:
      credDict[ 'username' ] = userName
      return True
    return False

  def authFindDNInUsers( self, DN, users ):
    for user in users:
      if DN == gConfig.getValue( "/Users/%s/DN" % user, "" ):
        return user
    return False