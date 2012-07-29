# $HeadURL$
__RCSID__ = "$Id$"

import types
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Security import CS
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import List

class AuthManager:
  """
    Handle Service Authorization
  """

  __authLogger = gLogger.getSubLogger( "Authorization" )
  KW_HOSTS_GROUP = 'hosts'
  KW_DN = 'DN'
  KW_GROUP = 'group'
  KW_EXTRA_CREDENTIALS = 'extraCredentials'
  KW_PROPERTIES = 'properties'
  KW_USERNAME = 'username'


  def __init__( self, authSection ):
    """
    Constructor

    @type authSection: string
    @param authSection: Section containing the authorization rules
    """
    self.authSection = authSection

  def authQuery( self, methodQuery, credDict, defaultProperties = False ):
    """
    Check if the query is authorized for a credentials dictionary

    @type  methodQuery: string
    @param methodQuery: Method to test
    @type  credDict: dictionary
    @param credDict: dictionary containing credentials for test. The dictionary can contain the DN
                        and selected group.
    @return: Boolean result of test
    """
    userString = ""
    if self.KW_DN in credDict:
      userString += "DN=%s" % credDict[ self.KW_DN ]
    if self.KW_GROUP in credDict:
      userString += " group=%s" % credDict[ self.KW_GROUP ]
    if self.KW_EXTRA_CREDENTIALS in credDict:
      userString += " extraCredentials=%s" % str( credDict[ self.KW_EXTRA_CREDENTIALS ] )
    self.__authLogger.verbose( "Trying to authenticate %s" % userString )
    #Get properties
    requiredProperties = self.getValidPropertiesForMethod( methodQuery, defaultProperties )
    lowerCaseProperties = [ prop.lower() for prop in requiredProperties ]
    allowAll = "any" in lowerCaseProperties or "all" in lowerCaseProperties
    #Set no properties by default
    credDict[ self.KW_PROPERTIES ] = []
    #Check non secure backends
    if self.KW_DN not in credDict or not credDict[ self.KW_DN ]:
      if allowAll:
        self.__authLogger.verbose( "Accepted request from unsecure transport" )
        return True
      else:
        self.__authLogger.verbose( "Explicit property required and query seems to be coming through an unsecure transport" )
        return False
    #Check if query comes though a gateway/web server
    if self.forwardedCredentials( credDict ):
      self.__authLogger.verbose( "Query comes from a gateway" )
      self.unpackForwardedCredentials( credDict )
      return self.authQuery( methodQuery, credDict )
    #Get the properties
    #Check for invalid forwarding
    if self.KW_EXTRA_CREDENTIALS in credDict:
      #Invalid forwarding?
      if type( credDict[ self.KW_EXTRA_CREDENTIALS ] ) not in  ( types.StringType, types.UnicodeType ):
        self.__authLogger.verbose( "The credentials seem to be forwarded by a host, but it is not a trusted one" )
        return False
    #Is it a host?
    if self.KW_EXTRA_CREDENTIALS in credDict and credDict[ self.KW_EXTRA_CREDENTIALS ] == self.KW_HOSTS_GROUP:
      #Get the nickname of the host
      credDict[ self.KW_GROUP ] = credDict[ self.KW_EXTRA_CREDENTIALS ]
    #HACK TO MAINTAIN COMPATIBILITY
    else:
      if self.KW_EXTRA_CREDENTIALS in credDict and not self.KW_GROUP in credDict:
        credDict[ self.KW_GROUP ] = credDict[ self.KW_EXTRA_CREDENTIALS ]
    #END OF HACK
    #Get the username
    if self.KW_DN in credDict and credDict[ self.KW_DN ]:
      if not self.KW_GROUP in credDict:
        result = CS.findDefaultGroupForDN( credDict[ self.KW_DN ] )
        if not result['OK']:
          return False
        credDict[ self.KW_GROUP ] = result['Value']
      if credDict[ self.KW_GROUP ] == self.KW_HOSTS_GROUP:
      #For host
        if not self.getHostNickName( credDict ):
          self.__authLogger.warn( "Host is invalid" )
          if not allowAll:
            return False
          #If all, then set anon credentials
          credDict[ self.KW_USERNAME ] = "anonymous"
          credDict[ self.KW_GROUP ] = "visitor"
      else:
      #For users
        if not self.getUsername( credDict ):
          self.__authLogger.warn( "User is invalid or does not belong to the group it's saying" )
          if not allowAll:
            return False
          #If all, then set anon credentials
          credDict[ self.KW_USERNAME ] = "anonymous"
          credDict[ self.KW_GROUP ] = "visitor"
    #If any or all in the props, allow
    if allowAll:
      return True
    #Check authorized groups
    if "authenticated" in lowerCaseProperties:
      return True
    if not self.matchProperties( credDict, requiredProperties ):
      self.__authLogger.warn( "Client is not authorized\nValid properties: %s\nClient: %s" %
                               ( requiredProperties, credDict ) )
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
    if not self.KW_DN in credDict:
      return True
    if not self.KW_GROUP in credDict:
      return False
    retVal = CS.getHostnameForDN( credDict[ self.KW_DN ] )
    if not retVal[ 'OK' ]:
      gLogger.warn( "Cannot find hostname for DN %s: %s" % ( credDict[ self.KW_DN ], retVal[ 'Message' ] ) )
      return False
    credDict[ self.KW_USERNAME ] = retVal[ 'Value' ]
    credDict[ self.KW_PROPERTIES ] = CS.getPropertiesForHost( credDict[ self.KW_USERNAME ], [] )
    return True

  def getValidPropertiesForMethod( self, method, defaultProperties = False ):
    """
    Get all authorized groups for calling a method

    @type  method: string
    @param method: Method to test
    @return: List containing the allowed groups
    """
    authProps = gConfig.getValue( "%s/%s" % ( self.authSection, method ), [] )
    if authProps:
      return authProps
    if defaultProperties:
      self.__authLogger.verbose( "Using hardcoded properties for method %s : %s" % ( method, defaultProperties ) )
      if type( defaultProperties ) not in ( types.ListType, types.TupleType ):
        return List.fromChar( defaultProperties )
      return defaultProperties
    defaultPath = "%s/Default" % "/".join( method.split( "/" )[:-1] )
    authProps = gConfig.getValue( "%s/%s" % ( self.authSection, defaultPath ), [] )
    if authProps:
      self.__authLogger.verbose( "Method %s has no properties defined using %s" % ( method, defaultPath ) )
      return authProps
    self.__authLogger.verbose( "Method %s has no authorization rules defined. Allowing no properties" % method )
    return []

  def forwardedCredentials( self, credDict ):
    """
    Check whether the credentials are being forwarded by a valid source

    @type  credDict: dictionary
    @param credDict: Credentials to ckeck
    @return: Boolean with the result
    """
    if self.KW_EXTRA_CREDENTIALS in credDict and type( credDict[ self.KW_EXTRA_CREDENTIALS ] ) == types.TupleType:
      if self.KW_DN in credDict:
        retVal = CS.getHostnameForDN( credDict[ self.KW_DN ] )
        if retVal[ 'OK' ]:
          hostname = retVal[ 'Value' ]
          if Properties.TRUSTED_HOST in CS.getPropertiesForHost( hostname, [] ):
            return True
    return False

  def unpackForwardedCredentials( self, credDict ):
    """
    Extract the forwarded credentials

    @type  credDict: dictionary
    @param credDict: Credentials to unpack
    """
    credDict[ self.KW_DN ] = credDict[ self.KW_EXTRA_CREDENTIALS ][0]
    credDict[ self.KW_GROUP ] = credDict[ self.KW_EXTRA_CREDENTIALS ][1]
    del( credDict[ self.KW_EXTRA_CREDENTIALS ] )


  def getUsername( self, credDict ):
    """
    Discover the username associated to the DN. It will check if the selected group is valid.
    The username will be included in the credentials dictionary.

    @type  credDict: dictionary
    @param credDict: Credentials to ckeck
    @return: Boolean specifying whether the username was found
    """
    if not self.KW_DN in credDict:
      return True
    if not self.KW_GROUP in credDict:
      result = CS.findDefaultGroupForDN( credDict[ self.KW_DN ] )
      if not result['OK']:
        return False
      credDict[ self.KW_GROUP ] = result['Value']
    credDict[ self.KW_PROPERTIES ] = CS.getPropertiesForGroup( credDict[ self.KW_GROUP ], [] )
    usersInGroup = CS.getUsersInGroup( credDict[ self.KW_GROUP ], [] )
    if not usersInGroup:
      return False
    retVal = CS.getUsernameForDN( credDict[ self.KW_DN ], usersInGroup )
    if retVal[ 'OK' ]:
      credDict[ self.KW_USERNAME ] = retVal[ 'Value' ]
      return True
    return False

  def matchProperties( self, credDict, validProps, caseSensitive = False ):
    """
    Return True if one or more properties are in the valid list of properties
    @type  props: list
    @param props: List of properties to match
    @type  validProps: list
    @param validProps: List of valid properties
    @return: Boolean specifying whether any property has matched the valid ones
    """
    #HACK: Map lower case properties to properties to make the check in lowercase but return the proper case
    if not caseSensitive:
      validProps = dict( [ ( prop.lower(), prop ) for prop in validProps ] )
    else:
      validProps = dict( [ ( prop, prop ) for prop in validProps ] )
    groupProperties = credDict[ self.KW_PROPERTIES ]
    foundProps = []
    for prop in groupProperties:
      if not caseSensitive:
        prop = prop.lower()
      if prop in validProps:
        foundProps.append( validProps[ prop ] )
    credDict[ self.KW_PROPERTIES ] = foundProps
    return foundProps
