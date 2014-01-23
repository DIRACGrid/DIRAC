# $HeadURL$
__RCSID__ = "$Id$"

import types
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

def getGroupsForDN( dn ):
  retVal = getUsernameForDN( dn )
  if not retVal[ 'OK' ]:
    return retVal
  return getGroupsForUser( retVal[ 'Value' ] )

def __getGroupsWithAttr( attrName, value ):
  retVal = gConfig.getSections( "%s/Groups" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return retVal
  groupsList = retVal[ 'Value' ]
  groups = []
  for group in groupsList:
    if value in gConfig.getValue( "%s/Groups/%s/%s" % ( gBaseSecuritySection, group, attrName ), [] ):
      groups.append( group )
  if not groups:
    return S_ERROR( "No groups found for %s=%s" % ( attrName,value ) )
  groups.sort()
  return S_OK( groups )

def getGroupsForUser( username ):
  return __getGroupsWithAttr( 'Users', username )

def getGroupsForVO( vo ):
  if getVO():
    return gConfig.getSections( "%s/Groups" % gBaseSecuritySection )
  return __getGroupsWithAttr( 'VO', vo )

def getGroupsWithProperty( propName ):
  return __getGroupsWithAttr( "Properties", propName )

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
  userDefGroups = getUserOption( userName, "DefaultGroup", [] )
  defGroups = userDefGroups + gConfig.getValue( "%s/DefaultGroup" % gBaseSecuritySection, [ "user" ] )
  result = getGroupsForUser( userName )
  if not result[ 'OK' ]:
    return result
  userGroups = result[ 'Value' ]
  for group in defGroups:
    if group in userGroups:
      return S_OK( group )
  if userGroups:
    return S_OK( userGroups[0] )
  return S_ERROR( "User %s has no groups" % userName )

def getAllUsers():
  retVal = gConfig.getSections( "%s/Users" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return []
  return retVal[ 'Value' ]

def getAllGroups():
  retVal = gConfig.getSections( "%s/Groups" % gBaseSecuritySection )
  if not retVal[ 'OK' ]:
    return []
  return retVal[ 'Value' ]

def getUsersInGroup( groupName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Groups/%s/Users" % ( gBaseSecuritySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getDNsInGroup( groupName ):
  DNs = []
  for user in getUsersInGroup( groupName ):
    result = getDNForUsername( user )
    if result[ 'OK' ]:
      DNs.extend( result[ 'Value' ] )
  return DNs

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

def __matchProps( sProps, rProps ):
  foundProps = []
  for prop in sProps:
    if prop in rProps:
      foundProps.append( prop )
  return foundProps

def groupHasProperties( groupName, propList ):
  if type( propList ) in types.StringTypes:
    propList = [ propList ]
  return __matchProps( propList, getPropertiesForGroup( groupName ) )

def hostHasProperties( hostName, propList ):
  if type( propList ) in types.StringTypes:
    propList = [ propList ]
  return __matchProps( propList, getPropertiesForHost( hostName ) )

def getUserOption( userName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Users/%s/%s" % ( gBaseSecuritySection, userName, optName ), defaultValue )

def getGroupOption( groupName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Groups/%s/%s" % ( gBaseSecuritySection, groupName, optName ), defaultValue )

def getHostOption( hostName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Hosts/%s/%s" % ( gBaseSecuritySection, hostName, optName ), defaultValue )

def getHosts():
  return gConfig.getSections( '%s/Hosts' % gBaseSecuritySection )

def getVOOption( voName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/VO/%s/%s" % ( gBaseSecuritySection, voName, optName ), defaultValue )


def getBannedIPs():
  return gConfig.getValue( "%s/BannedIPs" % gBaseSecuritySection, [] )

def getVOForGroup( group ):
  voName = getVO()
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
  vomsVO = gConfig.getValue( "%s/Groups/%s/VOMSVO" % ( gBaseSecuritySection, group ), getDefaultVOMSVO() )
  if not vomsVO:
    vo = getVOForGroup( group )
    vomsVO = getVOOption( vo, 'VOMSName', '' )
  return vomsVO

def getGroupsWithVOMSAttribute( vomsAttr ):
  retVal = gConfig.getSections( "%s/Groups" % ( gBaseSecuritySection ) )
  if not retVal[ 'OK' ]:
    return []
  groups = []
  for group in retVal[ 'Value' ]:
    if vomsAttr == gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( gBaseSecuritySection, group ), "" ):
      groups.append( group )
  return groups
