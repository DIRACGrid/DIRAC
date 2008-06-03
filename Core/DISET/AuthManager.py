# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/AuthManager.py,v 1.15 2008/06/03 14:32:16 acasajus Exp $
__RCSID__ = "$Id: AuthManager.py,v 1.15 2008/06/03 14:32:16 acasajus Exp $"

import types
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger

class AuthManager:

  __authLogger = gLogger.getSubLogger( "Authorization" )
  __hostsGroup = "hosts"

  def __init__( self, authSection ):
    """
    Constructor

    @type authSection: string
    @param authSection: Section containing the authorization rules
    """
    self.authSection = authSection

  def authQuery( self, methodQuery, credDict ):
    """
    Check if the query is authorized for a credentials dictionary

    @type  methodQuery: string
    @param methodQuery: Method to test
    @type  credDict: dictionary
    @param credDict: dictionary containing credentials for test. The dictionary can contain the DN
                        and selected group.
    @return: Boolean result of test
    """
    #Check if query comes though a gateway/web server
    if self.forwardedCredentials( credDict ):
      self.__authLogger.warn( "Query comes from a gateway" )
      self.unpackForwardedCredentials( credDict )
      return self.authQuery( methodQuery, credDict )
    #Check for invalid forwarding
    if 'extraCredentials' in credDict:
      #Invalid forwarding?
      if type( credDict[ 'extraCredentials' ] ) not in  ( types.StringType, types.UnicodeType ):
        self.__authLogger.warn( "The credentials seem to be forwarded by a host, but it is not a trusted one" )
        return False
    #Is it a host?
    if 'extraCredentials' in credDict and credDict[ 'extraCredentials' ] == self.__hostsGroup:
      #Get the nickname of the host
      credDict[ 'group' ] = credDict[ 'extraCredentials' ]
    #HACK TO MAINTAIN COMPATIBILITY
    else:
      if 'extraCredentials' in credDict and not 'group' in credDict:
        credDict[ 'group' ]  = credDict[ 'extraCredentials' ]
    #END OF HACK
    #Get the username
    if 'DN' in credDict:
      #For host
      if credDict[ 'group' ] == self.__hostsGroup:
        if not self.getHostNickName( credDict ):
          self.__authLogger.warn( "Host is invalid" )
          return False
      else:
      #For users
        if not self.getUsername( credDict ):
          self.__authLogger.warn( "User is invalid or does not belong to the group it's saying" )
          return False
    #Check everyone is authorized
    authGroups = self.getValidGroupsForMethod( methodQuery )
    if "any" in authGroups or "all" in authGroups:
      return True
    #Check user is authenticated
    if not 'DN' in credDict:
      self.__authLogger.warn( "User has no DN" )
      return False
    #Check authorized groups
    if not credDict[ 'group' ] in authGroups and not "authenticated" in authGroups:
      self.__authLogger.warn( "Peer group is not authorized" )
      return False
    return True

  def getHostNickName( self, credDict ):
    """
    Discover the host nickname associated to the DN.
    The nickname will be included in the credentials dictionary.

    @type  credDict: dictionary
    @param credDict: Credentials to ckeck
    @return: Boolean specifying whether the nickname was found
    """
    if not "DN" in credDict:
      return True
    if not 'group' in credDict:
      return False
    retVal = gConfig.getOptions( "/Hosts/" )
    if not retVal[ 'OK' ]:
      self.__authLogger.warn( "Can't get list of host nicknames, rejecting" )
      return False
    for nickname in retVal[ 'Value' ]:
      hostDN = gConfig.getValue( "/Hosts/%s" % nickname, "" )
      if hostDN == credDict[ 'DN' ]:
        credDict[ 'username' ] = nickname
        return True
    self.__authLogger.warn( "Host DN is unknown %d" % credDict[ 'DN' ] )
    return False

  def getValidGroupsForMethod( self, method ):
    """
    Get all authorized groups for calling a method

    @type  method: string
    @param method: Method to test
    @return: List containing the allowed groups
    """
    authGroups = gConfig.getValue( "%s/%s" % ( self.authSection, method ), [] )
    if not authGroups:
      defaultPath = "%s/Default" % "/".join( method.split( "/" )[:-1] )
      self.__authLogger.warn( "Method %s has no groups defined, trying %s" % ( method, defaultPath ) )
      authGroups = gConfig.getValue( "%s/%s" % ( self.authSection, defaultPath ), [] )
    return authGroups

  def forwardedCredentials( self, credDict ):
    """
    Check whether the credentials are being forwarded by a valid source

    @type  credDict: dictionary
    @param credDict: Credentials to ckeck
    @return: Boolean with the result
    """
    trustedHostsList = gConfig.getValue( "/DIRAC/Security/TrustedHosts", [] )
    return 'extraCredentials' in credDict and type( credDict[ 'extraCredentials' ] ) == types.TupleType and \
            'DN' in credDict and \
            credDict[ 'DN' ] in trustedHostsList

  def unpackForwardedCredentials( self, credDict ):
    """
    Extract the forwarded credentials

    @type  credDict: dictionary
    @param credDict: Credentials to unpack
    """
    credDict[ 'DN' ] = credDict[ 'extraCredentials' ][0]
    credDict[ 'group' ] = credDict[ 'extraCredentials' ][1]
    del( credDict[ 'extraCredentials' ] )


  def getUsername( self, credDict ):
    """
    Discover the username associated to the DN. It will check if the selected group is valid.
    The username will be included in the credentials dictionary.

    @type  credDict: dictionary
    @param credDict: Credentials to ckeck
    @return: Boolean specifying whether the username was found
    """
    if not "DN" in credDict:
      return True
    if not 'group' in credDict:
      credDict[ 'group' ] = gConfig.getValue( '/DIRAC/DefaultGroup', 'lhcb_user' )
    usersInGroup = gConfig.getValue( "/Groups/%s/users" % credDict[ 'group' ], [] )
    if not usersInGroup:
      return False
    userName = self.findUsername( credDict[ 'DN' ], usersInGroup )
    if userName:
      credDict[ 'username' ] = userName
      return True
    return False

  def findUsername( self, DN, users = False ):
    """
    Discover the username associated to the DN.

    @type  DN: string
    @param DN: DN of the username
    @type users : list
    @param users : Optional list of usernames to check. If its not specified all
                    usernames will be checked
    @return: Boolean specifying whether the username was found
    """
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
    """
    Get the groups witch a username is member of.

    @type  username: string
    @param username: Username to check
    @return: List of groups
    """
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
