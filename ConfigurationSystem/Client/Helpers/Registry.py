# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

gBaseSecuritySection = "/Registry"

def getUsernameForDN( dn, usersList = False ):
  if not usersList:
    retVal = gConfig.getSections( "%s/Users" % gBaseSecuritySection )
    if not retVal[ 'OK' ]:
      return retVal
    usersList = retVal[ 'Value' ]
  for username in usersList:
    if dn in gConfig.getValue( "%s/Users/%s/DN" % ( gBaseSecuritySection, username ), [] ):
      return S_OK( username )
  return S_ERROR( "No username found for dn %s" % dn )

def getDNForUsername( username ):
  dnList = gConfig.getValue( "%s/Users/%s/DN" % ( gBaseSecuritySection, username ), [] )
  if dnList:
    return S_OK( dnList )
  return S_ERROR( "No DN found for user %s" % username )

def getGroupsForUser( username ):
  retVal = gConfig.getSections( "%s/Groups" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return retVal
  groupsList = retVal[ 'Value' ]
  userGroups = []
  for group in groupsList:
    if username in gConfig.getValue( "%s/Groups/%s/Users" % ( gBaseSecuritySection, group ), [] ):
      userGroups.append( group )
  if not userGroups:
    return S_ERROR( "No groups found for user %s" % username )
  userGroups.sort()
  return S_OK( userGroups )

def getGroupsForDN( dn ):
  retVal = getUsernameForDN( dn )
  if not retVal[ 'OK' ]:
    return retVal
  return getGroupsForUser( retVal[ 'Value' ] )

def getHostnameForDN( dn ):
  retVal = gConfig.getSections( "%s/Hosts" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return retVal
  hostList = retVal[ 'Value' ]
  for hostname in hostList:
    if dn in gConfig.getValue( "%s/Hosts/%s/DN" % ( gBaseSecuritySection, hostname ), [] ):
      return S_OK( hostname )
  return S_ERROR( "No hostname found for dn %s" % dn )

def getDefaultUserGroup():
  return gConfig.getValue( "/%s/DefaultGroup" % gBaseSecuritySection, "user" )

def findDefaultGroupForDN( dn ):
  result = getUsernameForDN( dn )
  if not result[ 'OK' ]:
    return result
  return findDefaultGroupForUser( result[ 'Value' ] )

def findDefaultGroupForUser( userName ):
  defGroups = gConfig.getValue( "/%s/DefaultGroup" % gBaseSecuritySection, [ "user" ] )
  result = getGroupsForUser( userName )
  if not result[ 'OK' ]:
    return result
  userGroups = result[ 'Value' ]
  for group in defGroups:
    if group in userGroups:
      return S_OK( group )
  return S_OK( False )

def getAllUsers():
  retVal = gConfig.getSections( "%s/Users" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return []
  return retVal[ 'Value' ]

def getUsersInGroup( groupName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Groups/%s/Users" % ( gBaseSecuritySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForGroup( groupName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Groups/%s/Properties" % ( gBaseSecuritySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForHost( hostName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Hosts/%s/Properties" % ( gBaseSecuritySection, hostName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForEntity( group, name = "", dn = "", defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  if group == 'hosts':
    if not name:
      result = getHostnameForDN( dn )
      if not result[ 'OK' ]:
        return defaultValue
      name = result[ 'Value' ]
    return getPropertiesForHost( name, defaultValue )
  else:
    return getPropertiesForGroup( group, defaultValue )

def getBannedIPs():
  return gConfig.getValue( "%s/BannedIPs" % gBaseSecuritySection, [] )

def getVOForGroup( group ):
  voName = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
  if voName:
    return voName
  return gConfig.getValue( "%s/Groups/%s/VO" % ( gBaseSecuritySection, group ), "" )

def getDefaultVOMSAttribute():
  return gConfig.getValue( "%s/DefaultVOMSAttribute" % gBaseSecuritySection, "" )

def getVOMSAttributeForGroup( group ):
  return gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( gBaseSecuritySection, group ), getDefaultVOMSAttribute() )

def getDefaultVOMSVO():
  vomsVO = gConfig.getValue( "%s/DefaultVOMSVO" % gBaseSecuritySection, "" )
  if vomsVO:
    return vomsVO
  return getVO()

def getVOMSVOForGroup( group ):
  return gConfig.getValue( "%s/Groups/%s/VOMSVO" % ( gBaseSecuritySection, group ), getDefaultVOMSVO() )

def getGroupsWithVOMSAttribute( vomsAttr ):
  retVal = gConfig.getSections( "%s/Groups" % ( gBaseSecuritySection ) )
  if not retVal[ 'OK' ]:
    return []
  groups = []
  for group in retVal[ 'Value' ]:
    if vomsAttr == gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( gBaseSecuritySection, group ), "" ):
      groups.append( group )
  return groups
