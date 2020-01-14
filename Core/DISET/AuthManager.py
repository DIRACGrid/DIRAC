""" Module that holds DISET Authorization class for services
"""

import six

from DIRAC.Core.Utilities import List
from DIRAC.Core.Security import Properties
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"

KW_ID = 'ID'
KW_DN = 'DN'
KW_GROUP = 'group'
KW_USERNAME = 'username'
KW_HOSTS_GROUP = 'hosts'
KW_PROPERTIES = 'properties'
KW_EXTRA_CREDENTIALS = 'extraCredentials'

def forwardingCredentials(credDict):
  """ Check whether the credentials are being forwarded by a valid source and extract it

      :param dict credDict: Credentials to ckeck
      
      :return: bool
  """
  if isinstance(credDict.get(KW_EXTRA_CREDENTIALS), (tuple, list)):
    retVal = Registry.getHostnameForDN(credDict.get(KW_DN))
    if not retVal['OK']:
      gLogger.debug("The credentials forwarded not by a host:", credDict.get(KW_DN))
      return False
    hostname = retVal['Value']
    if Properties.TRUSTED_HOST not in Registry.getPropertiesForHost(hostname, []):
      gLogger.debug("The credentials forwarded by a %s host, but it is not a trusted one" % hostname)
      return False
    credDict[KW_DN] = credDict[KW_EXTRA_CREDENTIALS][0]
    credDict[KW_GROUP] = credDict[KW_EXTRA_CREDENTIALS][1]
    del credDict[KW_EXTRA_CREDENTIALS]
    return True
  return False

def initializationOfSession(credDict):
  """ Discover the username associated to the authentication session. It will check if the selected group is valid.
      The username will be included in the credentials dictionary. And will discover DN for group if last not set.

      :param dict credDict: Credentials to check

      :return: bool -- specifying whether the username was found
  """
  # Find user
  result = Registry.getUsernameForID(credDict[KW_ID])
  if not result['OK']:
    credDict[KW_USERNAME] = "anonymous"
    credDict[KW_GROUP] = "visitor"
    return False
  credDict[KW_USERNAME] = result['Value']
  return True

def initializationOfCertificate(credDict):
  """ Discover the username associated to the certificate DN. It will check if the selected group is valid.
      The username will be included in the credentials dictionary.

      :param dict credDict: Credentials to check

      :return: bool -- specifying whether the username was found
  """
  # Search host
  result = Registry.getHostnameForDN(credDict[KW_DN])
  if result['OK'] and result['Value']:
    credDict[KW_GROUP] = KW_HOSTS_GROUP
    result = Registry.getHostnameForDN(credDict[KW_DN])
    if not result['OK']:
      gLogger.warn("Cannot find hostname for DN %s: %s" % (credDict[KW_DN], retVal['Message']))
      credDict[KW_USERNAME] = "anonymous"
      credDict[KW_GROUP] = "visitor"
      return False
    credDict[KW_USERNAME] = result['Value']
    return True

  # Search user
  result = Registry.getUsernameForDN(credDict[KW_DN])
  if not result['OK']:
    credDict[KW_USERNAME] = "anonymous"
    credDict[KW_GROUP] = "visitor"
    return False
  credDict[KW_USERNAME] = result['Value']
  return True

def initializationOfGroup(credDict):
  """ Check/get default group

      :param dict credDict: Credentials to check

      :return: bool -- specifying whether the username was found
  """
  # Find/check group
  credDict[KW_PROPERTIES] = []
  if not credDict.get(KW_GROUP):
    result = Registry.findDefaultGroupForUser(credDict[KW_USERNAME])
    if not result['OK']:
      credDict[KW_USERNAME] = "anonymous"
      credDict[KW_GROUP] = "visitor"
      return False
    credDict[KW_GROUP] = result['Value']

  if credDict[KW_GROUP] == KW_HOSTS_GROUP:
    credDict[KW_PROPERTIES] = Registry.getPropertiesForHost(credDict[KW_USERNAME], [])
    return True

  result = Registry.getGroupsForUser(credDict[KW_USERNAME])
  if not result['OK']:
    credDict[KW_USERNAME] = "anonymous"
    credDict[KW_GROUP] = "visitor"
    return False
  if credDict[KW_GROUP] not in result['Value']:
    credDict[KW_GROUP] = "visitor"
    return False

  # Get DN for group
  result = Registry.getDNForUsernameInGroup(credDict[KW_USERNAME], credDict[KW_GROUP])
  if not result['OK']:
    gLogger.error(result['Message'])
    credDict[KW_GROUP] = "visitor"
    return False
  credDict[KW_DN] = result['Value']

  # Fill group properties
  credDict[KW_PROPERTIES] = Registry.getPropertiesForGroup(credDict[KW_GROUP], [])
  return True


class AuthManager(object):
  """ Handle Service Authorization
  """
  __authLogger = gLogger.getSubLogger("Authorization")

  def __init__(self, authSection):
    """ Constructor

        :param str authSection: Section containing the authorization rules
    """
    self.authSection = authSection

  def authQuery(self, methodQuery, credDict, defaultProperties=False):
    """ Check if the query is authorized for a credentials dictionary

        :param str methodQuery: Method to test
        :param dict credDict: dictionary containing credentials for test. The dictionary can contain the DN
               and selected group.
        :param defaultProperties: default properties
        :type defaultProperties: list or tuple

        :return: bool -- result of test
    """
    userString = ""
    if KW_ID in credDict:
      userString += "ID=%s" % credDict[KW_ID]
    if KW_DN in credDict:
      userString += "DN=%s" % credDict[KW_DN]
    if credDict.get(KW_GROUP):
      userString += " group=%s" % credDict[KW_GROUP]
    if KW_EXTRA_CREDENTIALS in credDict:
      userString += " extraCredentials=%s" % str(credDict[KW_EXTRA_CREDENTIALS])
    self.__authLogger.debug("Trying to authenticate %s" % userString)

    # Get properties
    requiredProperties = self.getValidPropertiesForMethod(methodQuery, defaultProperties)
    lowerCaseProperties = [prop.lower() for prop in requiredProperties] or ['any']
    allowAll = "any" in lowerCaseProperties or "all" in lowerCaseProperties

    # Extract valid groups
    validGroups = self.getValidGroups(requiredProperties)

    # Read extra credentials
    if KW_EXTRA_CREDENTIALS in credDict:
      # Is it a host? and HACK TO MAINTAIN COMPATIBILITY
      if credDict.get(KW_EXTRA_CREDENTIALS) == KW_HOSTS_GROUP:
        credDict[KW_GROUP] = credDict[KW_EXTRA_CREDENTIALS]
        del credDict[KW_EXTRA_CREDENTIALS]
      # Check if query comes though a gateway/web server
      elif forwardingCredentials(credDict):
        self.__authLogger.debug("Query comes from a gateway")
        return self.authQuery(methodQuery, credDict, requiredProperties)
      else:
        return False

    # Check authorization
    authorized = True
    # Search user name
    if not credDict.get(KW_USERNAME):
      if credDict.get(KW_DN):
        # With certificate
        authorized = initializationOfCertificate(credDict)
      elif credDict.get(KW_ID):
        # With IdP session
        authorized = initializationOfSession(credDict)
      else:
        credDict[KW_USERNAME] = "anonymous"
        credDict[KW_GROUP] = "visitor"
        authorized = False
    
    # Search group
    if authorized:
      authorized = initializationOfGroup(credDict)

    # Authorize check
    if allowAll or authorized:
      # Properties check
      if not self.matchProperties(credDict, list(set(requiredProperties) - set(['Any', 'any',
                                                                                'All', 'all',
                                                                                'authenticated',
                                                                                'Authenticated']))):
        self.__authLogger.warn("Client is not authorized\nValid properties: %s\nClient: %s" %
                               (requiredProperties, credDict))
        return False
      # Groups check
      elif validGroups and credDict[KW_GROUP] not in validGroups:
        self.__authLogger.warn("Client is not authorized\nValid groups: %s\nClient: %s" %
                               (validGroups, credDict))
        return False
      else:
        if not authorized:
          self.__authLogger.debug("Accepted request from unsecure transport")
        return True
    else:
      self.__authLogger.debug("User is invalid or does not belong to the group it's saying")
    return False

  def getValidPropertiesForMethod(self, method, defaultProperties=False):
    """ Get all authorized groups for calling a method

        :param str method: Method to test
        :param defaultProperties: default properties
        :type defaultProperties: list or tuple
        
        :return: list -- List containing the allowed groups
    """
    authProps = gConfig.getValue("%s/%s" % (self.authSection, method), [])
    if authProps:
      return authProps
    if defaultProperties:
      self.__authLogger.debug("Using hardcoded properties for method %s : %s" % (method, defaultProperties))
      if not isinstance(defaultProperties, (list, tuple)):
        return List.fromChar(defaultProperties)
      return defaultProperties
    defaultPath = "%s/Default" % "/".join(method.split("/")[:-1])
    authProps = gConfig.getValue("%s/%s" % (self.authSection, defaultPath), [])
    if authProps:
      self.__authLogger.debug("Method %s has no properties defined using %s" % (method, defaultPath))
      return authProps
    self.__authLogger.debug("Method %s has no authorization rules defined. Allowing no properties" % method)
    return []

  def getValidGroups(self, rawProperties):
    """ Get valid groups as specified in the method authorization rules

        :param list rawProperties: all method properties

        :return: list -- list of allowed groups
    """
    validGroups = []
    for prop in list(rawProperties):
      if prop.startswith('group:'):
        rawProperties.remove(prop)
        prop = prop.replace('group:', '')
        validGroups.append(prop)
      elif prop.startswith('vo:'):
        rawProperties.remove(prop)
        vo = prop.replace('vo:', '')
        result = Registry.getGroupsForVO(vo)
        if result['OK']:
          validGroups.extend(result['Value'])

    validGroups = list(set(validGroups))
    return validGroups

  def matchProperties(self, credDict, validProps, caseSensitive=False):
    """ Return True if one or more properties are in the valid list of properties

        :param dict credDict: credentials to match
        :param list validProps: List of valid properties
        :param bool caseSensitive: Map lower case properties to properties to make the check in
               lowercase but return the proper case
        
        :return: bool -- specifying whether any property has matched the valid ones
    """
    if not validProps:
      return True
    if not caseSensitive:
      validProps = dict((prop.lower(), prop) for prop in validProps)
    else:
      validProps = dict((prop, prop) for prop in validProps)
    foundProps = []
    for prop in credDict[KW_PROPERTIES]:
      if not caseSensitive:
        prop = prop.lower()
      if prop in validProps:
        foundProps.append(validProps[prop])
    return bool(foundProps)