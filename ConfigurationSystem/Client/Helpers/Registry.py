""" Helper for /Registry section
"""
import six
import errno

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO
from DIRAC.FrameworkSystem.Client.ProxyManagerData import gProxyManagerData
from OAuthDIRAC.FrameworkSystem.Client.OAuthManagerData import gOAuthManagerData

__RCSID__ = "$Id$"

# pylint: disable=missing-docstring

gBaseRegistrySection = "/Registry"


def getVOMSInfo(vo=None, dn=None):
  """ Get cached information from VOMS API
  
      :param list dn: requested DN

      :return: S_OK(dict)/S_ERROR()
  """
  return gProxyManagerData.getActualVOMSesDNs(voList=[vo] if vo else vo, dnList=[dn] if dn else dn)


def getUsernameForDN(dn, usersList=None):
  """ Find DIRAC user for DN

      :param str dn: user DN
      :param list usersList: list of possible users

      :return: S_OK(str)/S_ERROR()
  """
  if not usersList:
    result = gConfig.getSections("%s/Users" % gBaseRegistrySection)
    if not result['OK']:
      return result
    usersList = result['Value']
  for username in usersList:
    if dn in gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), []):
      return S_OK(username)

  # Get users profiles from session manager cache
  result = gOAuthManagerData.getIDsForDN(dn)
  if result['OK']:
    for uid in result['Value']:
      result = getUsernameForID(uid)
      if result['OK']:
        return result

  return S_ERROR("No username found for dn %s" % dn)


def getDNsForUsernameFromSC(username):
  """ Find DNs for DIRAC user from CS

      :param str username: DIRAC user

      :return: list -- contain DNs
  """
  return gConfig.getValue("%s/Users/%s/DN" % (gBaseRegistrySection, username), [])


def getDNForHost(host):
  """ Get host DN

      :param str host: host domain

      :return: S_OK(list)/S_ERROR() -- list of DNs
  """
  dnList = gConfig.getValue("%s/Hosts/%s/DN" % (gBaseRegistrySection, host), [])
  return S_OK(dnList) if dnList else S_ERROR("No DN found for host %s" % host)


def getGroupsForDN(dn, groupsList=None):
  """ Get all possible groups for DN

      :param str dn: user DN
      :param list groupsList: group list where need to search

      :return: S_OK(list)/S_ERROR() -- contain list of groups
  """
  groups = []
  if not groupsList:
    result = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
    if not result['OK']:
      return result
    groupsList = result['Value']

  result = getUsernameForDN(dn)
  if not result['OK']:
    return result
  user = result['Value']

  # Get VOMS information cache
  result = getVOMSInfo(dn=dn)
  if not result['OK']:
    return result
  vomsData = result['Value']

  result = getVOsWithVOMS()
  if not result['OK']:
    return result
  vomsVOs = result['Value']

  for group in groupsList:
    if user in getGroupOption(group, 'Users', []):
      vo = getGroupOption(group, 'VO')
      # Is VOMS VO?
      if vo in vomsVOs and vomsData.get(vo) and vomsData[vo]['OK'] and vomsData[vo]['Value']:
        voData = vomsData[vo]['Value']
        role = getGroupOption(group, 'VOMSRole')
        if not role or role in voData[dn]['VOMSRoles']:
          groups.append(group)
      else:
        # If it's not VOMS VO or cannot get information from VOMS
        groups.append(group)

  groups.sort()
  return S_OK(list(set(groups))) if groups else S_ERROR('No groups found for %s' % dn)


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


def getGroupsForUser(username, groupsList=None):
  """ Find groups for user or if set reseachedGroup check it for user

      :param str username: user name
      :param list groupsList: groups

      :return: S_OK(list or bool)/S_ERROR() -- contain list of groups or status group for user
  """
  if not groupsList:
    retVal = gConfig.getSections("%s/Groups" % gBaseRegistrySection)
    if not retVal['OK']:
      return retVal
    groupsList = retVal['Value']

  groups = []
  for group in groupsList:
    if username in getGroupOption(group, 'Users', []):
      groups.append(group)

  groups.sort()
  return S_OK(list(set(groups))) if groups else S_ERROR('No groups found for %s user' % username)


def getGroupsForVO(vo):
  """ Get groups for VO

      :param str vo: DIRAC VO name

      :return: S_OK(list)/S_ERROR()
  """
  if getVO():
    return gConfig.getSections("%s/Groups" % gBaseRegistrySection)
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


def getUsersInGroup(group, defaultValue=None):
  """ Find all users for group

      :param str group: group name
      :param defaultValue: default value

      :return: list
  """
  users = getGroupOption(group, 'Users', [])
  users.sort()
  return list(set(users)) or [] if defaultValue is None else defaultValue


def getUsersInVO(vo, defaultValue=None):
  """ Search users in VO

      :param str vo: DIRAC VO name
      :param defaultValue: default value

      :return: list
  """
  users = []
  result = getGroupsForVO(vo)
  if result['OK'] and result['Value']:
    for group in result['Value']:
      users += getUsersInGroup(group)

  users.sort()
  return list(set(users)) or [] if defaultValue is None else defaultValue


def getDNsInGroup(group, checkStatus=False):
  """ Find user DNs for DIRAC group

      :param str group: group name
      :param bool checkStatus: don't add suspended DNs

      :return: list
  """
  vomsData = {}
  vo = getGroupOption(group, 'VO')
  
  # Get VOMS information for VO, if it's VOMS VO
  result = getVOsWithVOMS()
  if not result['OK']:
    return result
  if vo in result['Value']:
    result = getVOMSInfo(vo=vo)
    if not result['OK']:
      return result
    vomsData = result['Value']

  DNs = []
  for username in getGroupOption(group, 'Users', []):
    result = getDNsForUsername(username)
    if not result['OK']:
      return result
    userDNs = result['Value']
    if vomsData.get(vo) and vomsData[vo]['OK']:
      voData = vomsData[vo]['Value']
      role = getGroupOption(group, 'VOMSRole')
      for dn in userDNs:
        if dn in voData:
          if not role or role in voData[dn]['ActuelRoles' if checkStatus else 'VOMSRoles']:
            DNs.append(dn)
    else:
      DNs += userDNs

  return list(set(DNs))


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


def getGroupsWithVOMSAttribute(vomsAttr, groupsList=None):
  """ Search groups with VOMS attribute

      :param str vomsAttr: VOMS attribute
      :param list groupsList: groups where need to search

      :return: list
  """
  groups = []
  for group in groupsList or getAllGroups():
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


def getDNProperty(dn, prop, defaultValue=None, username=None):
  """ Get user DN property

      :param str dn: user DN
      :param str prop: property name
      :param defaultValue: default value
      :param str username: username

      :return: S_OK()/S_ERROR()
  """
  if not username:
    result = getUsernameForDN(dn)
    if not result['OK']:
      return result
    username = result['Value']

  root = "%s/Users/%s/DNProperties" % (gBaseRegistrySection, username)
  result = gConfig.getSections(root)
  if not result['OK']:
    return result
  for section in result['Value']:
    if dn == gConfig.getValue("%s/%s/DN" % (root, section)):
      return S_OK(gConfig.getValue("%s/%s/%s" % (root, section, prop), defaultValue))
  return S_OK(defaultValue)


def isDownloadableGroup(groupName):
  """ Get permission to download proxy with group in a argument

      :params str groupName: DIRAC group

      :return: boolean
  """
  if getGroupOption(groupName, 'DownloadableProxy') in [False, 'False', 'false', 'no']:
    return False
  return True


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


def getIDsForUsername(username):
  """ Return IDs for DIRAC user

      :param str username: DIRAC user

      :return: list -- contain IDs
  """
  return gConfig.getValue("%s/Users/%s/ID" % (gBaseRegistrySection, username), [])


def getVOsWithVOMS(voList=None):
  """ Get all the configured VOMS VOs

      :param list voList: VOs where to look

      :return: S_OK(list)/S_ERROR()
  """
  vos = []
  if not voList:
    result = getVOs()
    if not result['OK']:
      return result
    voList = result['Value']
  for vo in voList:
    if getVOOption(vo, 'VOMSName'):
      vos.append(vo)
  return S_OK(vos)


def getDNsForUsername(username):
  """ Find all DNs for DIRAC user

      :param str username: DIRAC user

      :return: S_OK(list)/S_ERROR() -- contain DNs
  """
  userDNs = getDNsForUsernameFromSC(username)
  for uid in getIDsForUsername(username):
    result = gOAuthManagerData.getDNsForID(uid)
    if result['OK']:
      userDNs += result['Value']
  return S_OK(list(set(userDNs)))

def getDNForUsernameInGroup(username, group, checkStatus=False):
  """ Get user DN for user in group

      :param str username: user name
      :param str group: group name
      :param bool checkStatus: don't add suspended DNs

      :return: S_OK(str)/S_ERROR()
  """
  result = getDNsForUsernameInGroup(username, group, checkStatus)
  return S_OK(result['Value'][0]) if result['OK'] else result

def getDNsForUsernameInGroup(username, group, checkStatus=False):
  """ Get user DN for user in group

      :param str username: user name
      :param str group: group name
      :param bool checkStatus: don't add suspended DNs

      :return: S_OK(str)/S_ERROR()
  """
  if username not in getGroupOption(group, 'Users', []):
    return S_ERROR('%s group not have %s user.' % (group, username))

  result = getVOsWithVOMS()
  if not result['OK']:
    return result
  vomsVOs = result['Value']

  DNs = []
  result = getDNsForUsername(username)
  if not result['OK']:
    return result
  userDNs = result['Value']

  vo = getGroupOption(group, 'VO')
  if vo in vomsVOs:
    result = getVOMSInfo(vo=vo)
    if not result['OK']:
      return result
    vomsData = result['Value']
    if vomsData.get(vo) and vomsData[vo]['OK']:
      voData = vomsData[vo]['Value']
      role = getGroupOption(group, 'VOMSRole')
      for dn in userDNs:
        if dn in voData:
          if not checkStatus or not voData[dn]['Suspended']:
            if not role or role in voData[dn]['ActuelRoles' if checkStatus else 'VOMSRoles']:
              DNs.append(dn)
    else:
      DNs += userDNs
  else:
    DNs += userDNs
  
  if DNs:
    return S_OK(list(set(DNs)))
  return S_ERROR('For %s@%s not found DN%s.' % (username, group, ' or it suspended' if checkStatus else ''))


def findSomeDNToUseForGroupsThatNotNeedDN(username):
  """ This method is HACK for groups that not need DN from user, like as dirac_user, dirac_admin
      In this cause we will search first DN in CS or any DN that we can to find

      :param str username: user name

      :return: S_OK(str)/S_ERROR()
  """
  defDNs = getDNsForUsernameFromSC(username)
  if not defDNs:
    result = getDNsForUsername(username)
    return S_OK(result['Value'][0]) if result['OK'] else result
  return S_OK(defDNs[0])
