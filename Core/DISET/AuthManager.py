# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/AuthManager.py,v 1.11 2007/12/19 17:49:57 acasajus Exp $
__RCSID__ = "$Id: AuthManager.py,v 1.11 2007/12/19 17:49:57 acasajus Exp $"

import types
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger


class AuthManager:

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
      gLogger.verbose( "Query comes from a gateway" )
      self.unpackForwardedCredentials( credDict )
      return self.authQuery( methodQuery, credDict )
    else:
      if 'group' in credDict and type( credDict[ 'group' ] ) not in  ( types.StringType, types.UnicodeType ):
        gLogger.warn( "The credentials seem to be forwarded by a host, but it is not a trusted one" )
        return False
    if 'DN' in credDict:
      #Get the username
      if not self.getUsername( credDict ):
        gLogger.verbose( "User is invalid or does not belong to the group it's saying" )
        return False
    #Check everyone is authorized
    authGroups = self.getValidGroupsForMethod( methodQuery )
    if "any" in authGroups or "all" in authGroups:
      return True
    #Check user is authenticated
    if not 'DN' in credDict:
      gLogger.verbose( "User has no DN" )
      return False
    #Check authorized groups
    if not credDict[ 'group' ] in authGroups and not "authenticated" in authGroups:
      gLogger.verbose( "User group is not authorized" )
      return False
    return True

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
      gLogger.verbose( "Method %s has no groups defined, trying %s" % ( method, defaultPath ) )
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
    return 'group' in credDict and type( credDict[ 'group' ] ) == types.TupleType and \
            'DN' in credDict and \
            credDict[ 'DN' ] in trustedHostsList

  def unpackForwardedCredentials( self, credDict ):
    """
    Extract the forwarded credentials

    @type  credDict: dictionary
    @param credDict: Credentials to unpack
    """
    credDict[ 'DN' ] = credDict[ 'group' ][0]
    credDict[ 'group' ] = credDict[ 'group' ][1]

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
      return False
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
