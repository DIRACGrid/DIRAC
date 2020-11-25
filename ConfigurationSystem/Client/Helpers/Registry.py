""" Helper for /Registry section
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import errno

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

__RCSID__ = "$Id$"

# pylint: disable=missing-docstring

gBaseRegistrySection = "/Registry"


def getUsernameForDN(dn, usersList=None):
  """ Find DIRAC user for DN

      :param str dn: user DN
      :param list usersList: list of possible users

      :return: S_OK(str)/S_ERROR()
  """
  dn = dn.strip()
  if not usersList:
    result = gConfig.getSections("%s/Users" % gBaseRegistrySection)
    if not result['OK']:
      return result
    usersList = result['Value']
  for username in usersList:
    if dn in gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), []):
      return S_OK(username)
  return S_ERROR("No username found for dn %s" % dn)


def getDNForUsername(username):
  """ Get user DN for user

      :param str username: user name

      :return: S_OK(str)/S_ERROR()
  """
  dnList = gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), [])
  return S_OK(dnList) if dnList else S_ERROR("No DN found for user %s" % username)


def getDNForHost(host):
  """ Get host DN

      :param str host: host domain

      :return: S_OK(list)/S_ERROR() -- list of DNs
  """
  dnList = gConfig.getValue("%s/Hosts/%s/DN" % (gBaseRegistrySection, host), [])
  return S_OK(dnList) if dnList else S_ERROR("No DN found for host %s" % host)


def getGroupsForDN(dn):
  """ Get all possible groups for DN

      :param str dn: user DN

      :return: S_OK(list)/S_ERROR() -- contain list of groups
  """
  dn = dn.strip()
  result = getUsernameForDN(dn)
  if not result['OK']:
    return result
  return getGroupsForUser(result['Value'])


def __getGroupsWithAttr(attrName, value):
  """ Get all possible groups with some attribute

      :param str attrName: attribute name
      :param str value: attribute value

      :return: S_OK(list)/S_ERROR() -- contain list of groups
  """
  result = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  if not result['OK']:
    return result
  groupsList = result['Value']
  groups = []
  for group in groupsList:
    if value in gConfig.getValue("%s/Groups/%s/%s" % (gBaseRegistrySection, group, attrName), []):
      groups.append(group)
  groups.sort()
  return S_OK(groups) if groups else S_ERROR("No groups found for %s=%s" % (attrName, value))


def getGroupsForUser(username):
  """ Find groups for user

      :param str username: user name

      :return: S_OK(list)/S_ERROR() -- contain list of groups
  """
  return __getGroupsWithAttr('Users', username)


def getGroupsForVO(vo):
  """ Get groups for VO

      :param str vo: DIRAC VO name

      :return: S_OK(list)/S_ERROR()
  """
  if getVO():  # tries to get default VO in /DIRAC/VirtualOrganization
    return gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  if not vo:
    return S_ERROR("No VO requested")
  return __getGroupsWithAttr('VO', vo)


def getGroupsWithProperty(propName):
  """ Search groups by property

      :param str propName: property name

      :return: S_OK(list)/S_ERROR()
  """
  return __getGroupsWithAttr("Properties", propName)


def getHostnameForDN(dn):
  """ Search host name for host DN

      :param str dn: host DN

      :return: S_OK()/S_ERROR()
  """
  dn = dn.strip()
  result = gConfig.getSections("%s/Hosts" % gBaseRegistrySection)
  if not result['OK']:
    return result
  hostList = result['Value']
  for hostname in hostList:
    if dn in gConfig.getValue("%s/Hosts/%s/DN" % (gBaseRegistrySection, hostname), []):
      return S_OK(hostname)
  return S_ERROR("No hostname found for dn %s" % dn)


def getDefaultUserGroup():
  """ Search general default group

      :return: str
  """
  return gConfig.getValue("/%s/DefaultGroup" % gBaseRegistrySection, "user")


def findDefaultGroupForDN(dn):
  """ Search defaut group for DN

      :param str dn: DN

      :return: S_OK()/S_ERROR()
  """
  dn = dn.strip()
  result = getUsernameForDN(dn)
  if not result['OK']:
    return result
  return findDefaultGroupForUser(result['Value'])


def findDefaultGroupForUser(userName):
  """ Get default group for user

      :param str userName: user name

      :return: S_OK(str)/S_ERROR()
  """
  defGroups = getUserOption(userName, "DefaultGroup", [])
  defGroups += gConfig.getValue("%s/DefaultGroup" % gBaseRegistrySection, ["user"])
  result = getGroupsForUser(userName)
  if not result['OK']:
    return result
  userGroups = result['Value']
  for group in defGroups:
    if group in userGroups:
      return S_OK(group)
  return S_OK(userGroups[0]) if userGroups else S_ERROR("User %s has no groups" % userName)


def getAllUsers():
  """ Get all users

      :return: list
  """
  result = gConfig.getSections("%s/Users" % gBaseRegistrySection)
  return result['Value'] if result['OK'] else []


def getAllGroups():
  """ Get all groups

      :return: list
  """
  result = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  return result['Value'] if result['OK'] else []


def getUsersInGroup(groupName, defaultValue=None):
  """ Find all users for group

      :param str group: group name
      :param defaultValue: default value

      :return: list
  """
  option = "%s/Groups/%s/Users" % (gBaseRegistrySection, groupName)
  return gConfig.getValue(option, [] if defaultValue is None else defaultValue)


def getUsersInVO(vo, defaultValue=None):
  """ Search users in VO

      :param str vo: DIRAC VO name
      :param defaultValue: default value

      :return: list
  """
  result = getGroupsForVO(vo)
  if not result['OK'] or not result['Value']:
    return [] if defaultValue is None else defaultValue
  groups = result['Value']

  userList = []
  for group in groups:
    userList += getUsersInGroup(group)
  return userList


def getDNsInVO(vo):
  """ Get all DNs that have a VO users

      :param str vo: VO name

      :return: list
  """
  DNs = []
  for user in getUsersInVO(vo):
    result = getDNForUsername(user)
    if result['OK']:
      DNs.extend(result['Value'])
  return DNs


def getDNsInGroup(groupName):
  """ Find all DNs  for DIRAC group

      :param str groupName: group name

      :return: list
  """
  DNs = []
  for user in getUsersInGroup(groupName):
    result = getDNForUsername(user)
    if result['OK']:
      DNs.extend(result['Value'])
  return DNs


def getPropertiesForGroup(groupName, defaultValue=None):
  """ Return group properties

      :param str groupName: group name
      :param defaultValue: default value

      :return: defaultValue or list
  """
  option = "%s/Groups/%s/Properties" % (gBaseRegistrySection, groupName)
  return gConfig.getValue(option, [] if defaultValue is None else defaultValue)


def getPropertiesForHost(hostName, defaultValue=None):
  """ Return host properties

      :param str hostName: host name
      :param defaultValue: default value

      :return: defaultValue or list
  """
  option = "%s/Hosts/%s/Properties" % (gBaseRegistrySection, hostName)
  return gConfig.getValue(option, [] if defaultValue is None else defaultValue)


def getPropertiesForEntity(group, name="", dn="", defaultValue=None):
  """ Return some entity properties

      :param str group: group name
      :param str name: entity name
      :param str dn: DN
      :param defaultValue: default value

      :return: defaultValue or list
  """
  if defaultValue is None:
    defaultValue = []
  if group == 'hosts':
    if not name:
      result = getHostnameForDN(dn)
      if not result['OK']:
        return [] if defaultValue is None else defaultValue
      name = result['Value']
    return getPropertiesForHost(name, defaultValue)
  return getPropertiesForGroup(group, defaultValue)


def __matchProps(sProps, rProps):
  """ Match properties

      :param sProps: submitted properties
      :param rProps: required properties

      :return: list -- contain matched properties
  """
  foundProps = []
  for prop in sProps:
    if prop in rProps:
      foundProps.append(prop)
  return foundProps


def groupHasProperties(groupName, propList):
  """ Match required properties with group properties

      :param str groupName: group name
      :param list propList: required properties

      :return: list -- contain matched properties
  """

  if isinstance(propList, six.string_types):
    propList = [propList]
  return __matchProps(propList, getPropertiesForGroup(groupName))


def hostHasProperties(hostName, propList):
  """ Match required properties with host properties

      :param str hostName: host name
      :param list propList: required properties

      :return: list -- contain matched properties
  """
  if isinstance(propList, six.string_types):
    propList = [propList]
  return __matchProps(propList, getPropertiesForHost(hostName))


def getUserOption(userName, optName, defaultValue=""):
  """ Get user option

      :param str userName: user name
      :param str optName: option name
      :param defaultValue: default value

      :return: defaultValue or str
  """
  return gConfig.getValue("%s/Users/%s/%s" % (gBaseRegistrySection, userName, optName), defaultValue)


def getGroupOption(groupName, optName, defaultValue=""):
  """ Get group option

      :param str groupName: group name
      :param str optName: option name
      :param defaultValue: default value

      :return: defaultValue or str
  """
  return gConfig.getValue("%s/Groups/%s/%s" % (gBaseRegistrySection, groupName, optName), defaultValue)


def getHostOption(hostName, optName, defaultValue=""):
  """ Get host option

      :param str hostName: host name
      :param str optName: option name
      :param defaultValue: default value

      :return: defaultValue or str
  """
  return gConfig.getValue("%s/Hosts/%s/%s" % (gBaseRegistrySection, hostName, optName), defaultValue)


def getHosts():
  """ Get all hosts

      :return: S_OK()/S_ERROR()
  """
  return gConfig.getSections('%s/Hosts' % gBaseRegistrySection)


def getVOOption(voName, optName, defaultValue=""):
  """ Get VO option

      :param str voName: DIRAC VO name
      :param str optName: option name
      :param defaultValue: default value

      :return: defaultValue or str
  """
  return gConfig.getValue("%s/VO/%s/%s" % (gBaseRegistrySection, voName, optName), defaultValue)


def getBannedIPs():
  """ Get banned IPs

      :return: list
  """
  return gConfig.getValue("%s/BannedIPs" % gBaseRegistrySection, [])


def getVOForGroup(group):
  """ Search VO name for group

      :param str group: group name

      :return: str
  """
  return getVO() or gConfig.getValue("%s/Groups/%s/VO" % (gBaseRegistrySection, group), "")


def getDefaultVOMSAttribute():
  """ Get default VOMS attribute

      :return: str
  """
  return gConfig.getValue("%s/DefaultVOMSAttribute" % gBaseRegistrySection, "")


def getVOMSAttributeForGroup(group):
  """ Search VOMS attribute for group

      :param str group: group name

      :return: str
  """
  return gConfig.getValue("%s/Groups/%s/VOMSRole" % (gBaseRegistrySection, group), getDefaultVOMSAttribute())


def getDefaultVOMSVO():
  """ Get default VOMS VO

      :return: str
  """
  return gConfig.getValue("%s/DefaultVOMSVO" % gBaseRegistrySection, "") or getVO()


def getVOMSVOForGroup(group):
  """ Search VOMS VO for group

      :param str group: group name

      :return: str
  """
  vomsVO = gConfig.getValue("%s/Groups/%s/VOMSVO" % (gBaseRegistrySection, group), getDefaultVOMSVO())
  if not vomsVO:
    vo = getVOForGroup(group)
    vomsVO = getVOOption(vo, 'VOMSName', '')
  return vomsVO


def getGroupsWithVOMSAttribute(vomsAttr):
  """ Search groups with VOMS attribute

      :param str vomsAttr: VOMS attribute

      :return: list
  """
  groups = []
  for group in gConfig.getSections("%s/Groups" % (gBaseRegistrySection)).get('Value', []):
    if vomsAttr == gConfig.getValue("%s/Groups/%s/VOMSRole" % (gBaseRegistrySection, group), ""):
      groups.append(group)
  return groups


def getVOs():
  """ Get all the configured VOs

      :return: S_OK(list)/S_ERROR()
  """
  voName = getVO()
  return S_OK([voName]) if voName else gConfig.getSections('%s/VO' % gBaseRegistrySection)


def getVOMSServerInfo(requestedVO=''):
  """ Get information on VOMS servers for the given VO or for all of them

      :param str requestedVO: requested VO

      :return: S_OK()/S_ERROR()
  """
  vomsDict = {}

  result = getVOs()
  if result['OK']:
    voNames = result['Value']
    for vo in voNames:
      if requestedVO and vo != requestedVO:
        continue
      vomsName = getVOOption(vo, 'VOMSName', '')
      if not vomsName:
        continue
      vomsDict.setdefault(vo, {})
      vomsDict[vo]['VOMSName'] = getVOOption(vo, 'VOMSName', '')
      result = gConfig.getSections('%s/VO/%s/VOMSServers' % (gBaseRegistrySection, vo))
      if result['OK']:
        serverList = result['Value']
        vomsDict[vo].setdefault("Servers", {})
        for server in serverList:
          vomsDict[vo]['Servers'].setdefault(server, {})
          DN = gConfig.getValue('%s/VO/%s/VOMSServers/%s/DN' % (gBaseRegistrySection, vo, server), '')
          CA = gConfig.getValue('%s/VO/%s/VOMSServers/%s/CA' % (gBaseRegistrySection, vo, server), '')
          port = gConfig.getValue('%s/VO/%s/VOMSServers/%s/Port' % (gBaseRegistrySection, vo, server), 0)
          vomsDict[vo]['Servers'][server]['DN'] = DN
          vomsDict[vo]['Servers'][server]['CA'] = CA
          vomsDict[vo]['Servers'][server]['Port'] = port

  return S_OK(vomsDict)


def getVOMSRoleGroupMapping(vo=''):
  """ Get mapping of the VOMS role to the DIRAC group

      :param str vo: perform the operation for the given VO

      :return: S_OK(dict)/S_ERROR() -- dictionary have standard structure with two mappings:
               VOMS-DIRAC { <VOMS_Role>: [<DIRAC_Group>] },
               DIRAC-VOMS { <DIRAC_Group>: <VOMS_Role> }
               and a list of DIRAC groups without mapping
  """
  result = getGroupsForVO(vo)
  if not result['OK']:
    return result

  groupList = result['Value']

  vomsGroupDict = {}
  groupVomsDict = {}
  noVOMSGroupList = []
  noVOMSSyncGroupList = []

  for group in groupList:
    vomsRole = getGroupOption(group, 'VOMSRole')
    if vomsRole:
      vomsGroupDict.setdefault(vomsRole, [])
      vomsGroupDict[vomsRole].append(group)
      groupVomsDict[group] = vomsRole
      syncVOMS = getGroupOption(group, 'AutoSyncVOMS', True)
      if not syncVOMS:
        noVOMSSyncGroupList.append(group)

  for group in groupList:
    if group not in groupVomsDict:
      noVOMSGroupList.append(group)

  return S_OK({"VOMSDIRAC": vomsGroupDict,
               "DIRACVOMS": groupVomsDict,
               "NoVOMS": noVOMSGroupList,
               "NoSyncVOMS": noVOMSSyncGroupList})


def getUsernameForID(ID, usersList=None):
  """ Get DIRAC user name by ID

      :param str ID: user ID
      :param list usersList: list of DIRAC user names

      :return: S_OK(str)/S_ERROR()
  """
  if not usersList:
    result = gConfig.getSections("%s/Users" % gBaseRegistrySection)
    if not result['OK']:
      return result
    usersList = result['Value']
  for username in usersList:
    if ID in gConfig.getValue("%s/Users/%s/ID" % (gBaseRegistrySection, username), []):
      return S_OK(username)
  return S_ERROR("No username found for ID %s" % ID)


def getCAForUsername(username):
  """ Get CA option by user name

      :param str username: user name

      :return: S_OK(str)/S_ERROR()
  """
  dnList = gConfig.getValue("%s/Users/%s/CA" % (gBaseRegistrySection, username), [])
  return S_OK(dnList) if dnList else S_ERROR("No CA found for user %s" % username)


def getDNProperty(userDN, value, defaultValue=None):
  """ Get property from DNProperties section by user DN

      :param str userDN: user DN
      :param str value: option that need to get
      :param defaultValue: default value

      :return: S_OK()/S_ERROR() -- str or list that contain option value
  """
  result = getUsernameForDN(userDN)
  if not result['OK']:
    return result
  pathDNProperties = "%s/Users/%s/DNProperties" % (gBaseRegistrySection, result['Value'])
  result = gConfig.getSections(pathDNProperties)
  if result['OK']:
    for section in result['Value']:
      if userDN == gConfig.getValue("%s/%s/DN" % (pathDNProperties, section)):
        return S_OK(gConfig.getValue("%s/%s/%s" % (pathDNProperties, section, value), defaultValue))
  return S_OK(defaultValue)


def getProxyProvidersForDN(userDN):
  """ Get proxy providers by user DN

      :param str userDN: user DN

      :return: S_OK(list)/S_ERROR()
  """
  return getDNProperty(userDN, 'ProxyProviders', [])


def getDNFromProxyProviderForUserID(proxyProvider, userID):
  """ Get groups by user DN in DNProperties

      :param str proxyProvider: proxy provider name
      :param str userID: user identificator

      :return: S_OK(str)/S_ERROR()
  """
  # Get user name
  result = getUsernameForID(userID)
  if not result['OK']:
    return result
  # Get DNs from user
  result = getDNForUsername(result['Value'])
  if not result['OK']:
    return result
  for DN in result['Value']:
    result = getProxyProvidersForDN(DN)
    if not result['OK']:
      return result
    if proxyProvider in result['Value']:
      return S_OK(DN)
  return S_ERROR(errno.ENODATA,
                 "No DN found for %s proxy provider for user ID %s" % (proxyProvider, userID))


def isDownloadableGroup(groupName):
  """ Get permission to download proxy with group in a argument

      :params str groupName: DIRAC group

      :return: boolean
  """
  if getGroupOption(groupName, 'DownloadableProxy') in [False, 'False', 'false', 'no']:
    return False
  return True


def getUserDict(username):
  """ Get full information from user section

      :param str username: DIRAC user name

      :return: S_OK()/S_ERROR()
  """
  resDict = {}
  relPath = '%s/Users/%s/' % (gBaseRegistrySection, username)
  result = gConfig.getConfigurationTree(relPath)
  if not result['OK']:
    return result
  for key, value in result['Value'].items():
    if value:
      resDict[key.replace(relPath, '')] = value
  return S_OK(resDict)


def getEmailsForGroup(groupName):
  """ Get email list of users in group

      :param str groupName: DIRAC group name

      :return: list(list) -- inner list contains emails for a user
  """
  emails = []
  for username in getUsersInGroup(groupName):
    email = getUserOption(username, 'Email', [])
    emails.append(email)
  return emails
