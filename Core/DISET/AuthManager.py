from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger

class AuthManager:

  def __init__( self, authSection ):
    self.authSection = authSection

  def authQuery( self, methodQuery, credDict ):
    #Check if query comes though a gateway/web server
    if self.forwardedCredentials( credDict ):
      self.unpackForwardedCredentials( credDict )
      return self.authQuery( methodQuery, credDict )
    if 'DN' in credDict:
      #Get the username
      if not self.getUsername( credDict ):
        gLogger.debug( "Query no authorized, user has no valid credentials" )
        return False
    #Check everyone is authorized
    authGroups = self.getValidGroupsForMethod( methodQuery )
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

  def getValidGroupsForMethod( self, method ):
    authGroups = gConfig.getValue( "%s/%s" % ( self.authSection, method ), [] )
    if not authGroups:
      authGroups = gConfig.getValue( "%s/Default" % self.authSection, [] )
    return authGroups

  def forwardedCredentials( self, credDict ):
    trustedHostsList = gConfig.getValue( "/DIRAC/Security/TrustedHosts", [] )
    return type( credDict[ 'group' ] ) == types.TupleType and \
            'DN' in credDict and \
            credDict[ 'DN' ] in trustedHostsList

  def unpackForwardedCredentials( self, credDict ):
    credDict[ 'DN' ] = credDict[ 'group' ][0]
    credDict[ 'group' ] = credDict[ 'group' ][1]

  def getUsername( self, credDict ):
    if not "DN" in credDict:
      return True
    usersInGroup = gConfig.getValue( "/Groups/%s/users" % credDict[ 'group' ], [] )
    if not usersInGroup:
      return False
    userName = self.findUsername( credDict[ 'DN' ], usersInGroup )
    if userName:
      credDict[ 'username' ] = userName
      return True
    return False

  def findUsername( self, DN, users = False ):
    if not users:
      retVal = gConfig.getSections( "/Users" )
      if retVal[ 'OK' ]:
        users = retVal[ 'Value' ]
      else:
        users = []
    for user in users:
      if DN == gConfig.getValue( "/Users/%s/DN" % user, "" ):
        return user
    return False

  def getGroupsForUsername( self, username ):
    userGroups = []
    retVal = gConfig.getSections( "/Groups" )
    if retVal[ 'OK' ]:
      groups = retVal[ 'Value' ]
    else:
      groups = []
    for group in groups:
      if username in gConfig.getValue( "/Groups/%s/users" % group, [] ):
        userGroups.append( group )
    return userGroups