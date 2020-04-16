""" Helper for /Registry section
"""
import six
import errno

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

__RCSID__ = "$Id$"

# pylint: disable=missing-docstring

gBaseRegistrySection = "/Registry"


def getUsernameForDN(dn, usersList=False):
  if not usersList:
    retVal = gConfig.getSections("%s/Users" % gBaseRegistrySection)
    if not retVal['OK']:
      return retVal
    usersList = retVal['Value']
  for username in usersList:
    if dn in gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), []):
      return S_OK(username)
  return S_ERROR("No username found for dn %s" % dn)


def getDNForUsername(username):
  dnList = gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), [])
  if dnList:
    return S_OK(dnList)
  return S_ERROR("No DN found for user %s" % username)


def getDNForHost(host):
  dnList = gConfig.getValue("%s/Hosts/%s/DN" % (gBaseRegistrySection, host), [])
  if dnList:
    return S_OK(dnList)
  return S_ERROR("No DN found for host %s" % host)


def getGroupsForDN(dn):
  retVal = getUsernameForDN(dn)
  if not retVal['OK']:
    return retVal
  return getGroupsForUser(retVal['Value'])


def __getGroupsWithAttr(attrName, value):
  retVal = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  if not retVal['OK']:
    return retVal
  groupsList = retVal['Value']
  groups = []
  for group in groupsList:
    if value in gConfig.getValue("%s/Groups/%s/%s" % (gBaseRegistrySection, group, attrName), []):
      groups.append(group)
  if not groups:
    return S_ERROR("No groups found for %s=%s" % (attrName, value))
  groups.sort()
  return S_OK(groups)


def getGroupsForUser(username):
  return __getGroupsWithAttr('Users', username)


def getGroupsForVO(vo):
  if getVO():
    return gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  return __getGroupsWithAttr('VO', vo)


def getGroupsWithProperty(propName):
  return __getGroupsWithAttr("Properties", propName)


def getHostnameForDN(dn):
  retVal = gConfig.getSections("%s/Hosts" % gBaseRegistrySection)
  if not retVal['OK']:
    return retVal
  hostList = retVal['Value']
  for hostname in hostList:
    if dn in gConfig.getValue("%s/Hosts/%s/DN" % (gBaseRegistrySection, hostname), []):
      return S_OK(hostname)
  return S_ERROR("No hostname found for dn %s" % dn)


def getDefaultUserGroup():
  return gConfig.getValue("/%s/DefaultGroup" % gBaseRegistrySection, "user")


def findDefaultGroupForDN(dn):
  result = getUsernameForDN(dn)
  if not result['OK']:
    return result
  return findDefaultGroupForUser(result['Value'])


def findDefaultGroupForUser(userName):
  userDefGroups = getUserOption(userName, "DefaultGroup", [])
  defGroups = userDefGroups + gConfig.getValue("%s/DefaultGroup" % gBaseRegistrySection, ["user"])
  result = getGroupsForUser(userName)
  if not result['OK']:
    return result
  userGroups = result['Value']
  for group in defGroups:
    if group in userGroups:
      return S_OK(group)
  if userGroups:
    return S_OK(userGroups[0])
  return S_ERROR("User %s has no groups" % userName)


def getAllUsers():
  retVal = gConfig.getSections("%s/Users" % gBaseRegistrySection)
  if not retVal['OK']:
    return []
  return retVal['Value']


def getAllGroups():
  retVal = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
  if not retVal['OK']:
    return []
  return retVal['Value']


def getUsersInGroup(groupName, defaultValue=None):
  if defaultValue is None:
    defaultValue = []
  option = "%s/Groups/%s/Users" % (gBaseRegistrySection, groupName)
  return gConfig.getValue(option, defaultValue)


def getUsersInVO(vo, defaultValue=None):
  if defaultValue is None:
    defaultValue = []
  result = getGroupsForVO(vo)
  if not result['OK']:
    return defaultValue
  groups = result['Value']
  if not groups:
    return defaultValue

  userList = []
  for group in groups:
    userList += getUsersInGroup(group)
  return userList


def getDNsInVO(vo):
  DNs = []
  for user in getUsersInVO(vo):
    result = getDNForUsername(user)
    if result['OK']:
      DNs.extend(result['Value'])
  return DNs


def getDNsInGroup(groupName):
  DNs = []
  for user in getUsersInGroup(groupName):
    result = getDNForUsername(user)
    if result['OK']:
      DNs.extend(result['Value'])
  return DNs


def getPropertiesForGroup(groupName, defaultValue=None):
  if defaultValue is None:
    defaultValue = []
  option = "%s/Groups/%s/Properties" % (gBaseRegistrySection, groupName)
  return gConfig.getValue(option, defaultValue)


def getPropertiesForHost(hostName, defaultValue=None):
  if defaultValue is None:
    defaultValue = []
  option = "%s/Hosts/%s/Properties" % (gBaseRegistrySection, hostName)
  return gConfig.getValue(option, defaultValue)


def getPropertiesForEntity(group, name="", dn="", defaultValue=None):
  if defaultValue is None:
    defaultValue = []
  if group == 'hosts':
    if not name:
      result = getHostnameForDN(dn)
      if not result['OK']:
        return defaultValue
      name = result['Value']
    return getPropertiesForHost(name, defaultValue)
  else:
    return getPropertiesForGroup(group, defaultValue)


def __matchProps(sProps, rProps):
  foundProps = []
  for prop in sProps:
    if prop in rProps:
      foundProps.append(prop)
  return foundProps


def groupHasProperties(groupName, propList):
  if isinstance(propList, six.string_types):
    propList = [propList]
  return __matchProps(propList, getPropertiesForGroup(groupName))


def hostHasProperties(hostName, propList):
  if isinstance(propList, six.string_types):
    propList = [propList]
  return __matchProps(propList, getPropertiesForHost(hostName))


def getUserOption(userName, optName, defaultValue=""):
  return gConfig.getValue("%s/Users/%s/%s" % (gBaseRegistrySection, userName, optName), defaultValue)


def getGroupOption(groupName, optName, defaultValue=""):
  return gConfig.getValue("%s/Groups/%s/%s" % (gBaseRegistrySection, groupName, optName), defaultValue)


def getHostOption(hostName, optName, defaultValue=""):
  return gConfig.getValue("%s/Hosts/%s/%s" % (gBaseRegistrySection, hostName, optName), defaultValue)


def getHosts():
  return gConfig.getSections('%s/Hosts' % gBaseRegistrySection)


def getVOOption(voName, optName, defaultValue=""):
  return gConfig.getValue("%s/VO/%s/%s" % (gBaseRegistrySection, voName, optName), defaultValue)


def getBannedIPs():
  return gConfig.getValue("%s/BannedIPs" % gBaseRegistrySection, [])


def getVOForGroup(group):
  voName = getVO()
  if voName:
    return voName
  return gConfig.getValue("%s/Groups/%s/VO" % (gBaseRegistrySection, group), "")


def getDefaultVOMSAttribute():
  return gConfig.getValue("%s/DefaultVOMSAttribute" % gBaseRegistrySection, "")


def getVOMSAttributeForGroup(group):
  return gConfig.getValue("%s/Groups/%s/VOMSRole" % (gBaseRegistrySection, group), getDefaultVOMSAttribute())


def getDefaultVOMSVO():
  vomsVO = gConfig.getValue("%s/DefaultVOMSVO" % gBaseRegistrySection, "")
  if vomsVO:
    return vomsVO
  return getVO()


def getVOMSVOForGroup(group):
  vomsVO = gConfig.getValue("%s/Groups/%s/VOMSVO" % (gBaseRegistrySection, group), getDefaultVOMSVO())
  if not vomsVO:
    vo = getVOForGroup(group)
    vomsVO = getVOOption(vo, 'VOMSName', '')
  return vomsVO


def getGroupsWithVOMSAttribute(vomsAttr):
  retVal = gConfig.getSections("%s/Groups" % (gBaseRegistrySection))
  if not retVal['OK']:
    return []
  groups = []
  for group in retVal['Value']:
    if vomsAttr == gConfig.getValue("%s/Groups/%s/VOMSRole" % (gBaseRegistrySection, group), ""):
      groups.append(group)
  return groups


def getVOs():
  """ Get all the configured VOs
  """
  voName = getVO()
  if voName:
    return S_OK([voName])
  return gConfig.getSections('%s/VO' % gBaseRegistrySection)


def getVOMSServerInfo(requestedVO=''):
  """ Get information on VOMS servers for the given VO or for all of them
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
  :return: standard structure with two mappings: VOMS-DIRAC { <VOMS_Role>: [<DIRAC_Group>] }
           and DIRAC-VOMS { <DIRAC_Group>: <VOMS_Role> } and a list of DIRAC groups without mapping
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

      :param basestring ID: user ID
      :param list usersList: list of DIRAC user names

      :return: S_OK(basestring)/S_ERROR()
  """
  if not usersList:
    retVal = gConfig.getSections("%s/Users" % gBaseRegistrySection)
    if not retVal['OK']:
      return retVal
    usersList = retVal['Value']
  for username in usersList:
    if ID in gConfig.getValue("%s/Users/%s/ID" % (gBaseRegistrySection, username), []):
      return S_OK(username)
  return S_ERROR("No username found for ID %s" % ID)


def getCAForUsername(username):
  """ Get CA option by user name

      :param basestring username: user name

      :return: S_OK(basestring)/S_ERROR()
  """
  dnList = gConfig.getValue("%s/Users/%s/CA" % (gBaseRegistrySection, username), [])
  if dnList:
    return S_OK(dnList)
  return S_ERROR("No CA found for user %s" % username)


def getDNProperty(userDN, value):
  """ Get property from DNProperties section by user DN

      :param basestring userDN: user DN
      :param basestring value: option that need to get

      :return: S_OK(basestring,list)/S_ERROR() -- basestring or list that contain option value
  """
  result = getUsernameForDN(userDN)
  if not result['OK']:
    return result
  pathDNProperties = "%s/Users/%s/DNProperties" % (gBaseRegistrySection, result['Value'])
  result = gConfig.getSections(pathDNProperties)
  if not result['OK']:
    return result
  for section in result['Value']:
    if userDN == gConfig.getValue("%s/%s/DN" % (pathDNProperties, section):
      return S_OK(gConfig.getValue("%s/%s/%s" % (pathDNProperties, section, value)))

  return S_ERROR('No properties found for %s' % userDN)


def getProxyProvidersForDN(userDN):
  """ Get proxy providers by user DN

      :param basestring userDN: user DN

      :return: S_OK(list)/S_ERROR()
  """
  result = getDNProperty(userDN, 'ProxyProviders')
  if not result['OK']:
    return result
  ppList = result['Value'] or []
  return S_OK(ppList if isinstance(ppList, list) else ppList.split())


def getDNFromProxyProviderForUserID(proxyProvider, userID):
  """ Get groups by user DN in DNProperties

      :param basestring proxyProvider: proxy provider name
      :param basestring userID: user identificator

      :return: S_OK(basestring)/S_ERROR()
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

      :params basestring groupName: DIRAC group

      :return: boolean
  """
  if getGroupOption(groupName, 'DownloadableProxy') in [False, 'False', 'false', 'no']:
    return False
  return True


def getUserDict(username):
  """ Get full information from user section

      :param basestring username: DIRAC user name

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

      :param basestring groupName: DIRAC group name

      :return: list(list) -- inner list contains emails for a user
  """
  emails = []
  for username in getUsersInGroup(groupName, defaultValue=[]):
    email = gConfig.getValue("%s/Users/%s/Email" % (gBaseRegistrySection, username), [])
    emails.append(email)
  return emails
