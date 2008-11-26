# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Security/CS.py,v 1.13 2008/11/26 15:30:02 acasajus Exp $
__RCSID__ = "$Id: CS.py,v 1.13 2008/11/26 15:30:02 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Config import gConfig

g_BaseSecuritySection = "/Security"

def getUsernameForDN( dn, usersList = False ):
  if not usersList:
    retVal = gConfig.getSections( "%s/Users" % g_BaseSecuritySection )
    if not retVal[ 'OK' ]:
      return retVal
    usersList = retVal[ 'Value' ]
  for username in usersList:
    if dn in gConfig.getValue( "%s/Users/%s/DN" % ( g_BaseSecuritySection, username ), [] ):
      return S_OK( username )
  return S_ERROR( "No username found for dn %s" % dn )

def getDNForUsername( username ):
  dnList = gConfig.getValue( "%s/Users/%s/DN" % ( g_BaseSecuritySection, username ), [] )
  if dnList:
    return S_OK( dnList )
  return S_ERROR( "No DN found for user %s" % username )

def getGroupsForUser( username ):
  retVal = gConfig.getSections( "%s/Groups" % g_BaseSecuritySection )
  if not retVal[ 'OK' ]:
    return retVal
  groupsList = retVal[ 'Value' ]
  userGroups = []
  for group in groupsList:
    if username in gConfig.getValue( "%s/Groups/%s/Users" % ( g_BaseSecuritySection, group ), [] ):
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
  retVal = gConfig.getSections( "%s/Hosts" % g_BaseSecuritySection )
  if not retVal[ 'OK' ]:
    return retVal
  hostList = retVal[ 'Value' ]
  for hostname in hostList:
    if dn in gConfig.getValue( "%s/Hosts/%s/DN" % ( g_BaseSecuritySection, hostname ), [] ):
      return S_OK( hostname )
  return S_ERROR( "No hostname found for dn %s" % dn )

def getDefaultUserGroup():
  return gConfig.getValue( "/%s/DefaultGroup" % g_BaseSecuritySection, "lhcb_user" )

def getUsersInGroup( groupName, defaultValue = [] ):
  option = "%s/Groups/%s/Users" %( g_BaseSecuritySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForGroup( groupName, defaultValue = [] ):
  option = "%s/Groups/%s/Properties" %( g_BaseSecuritySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForHost( hostName, defaultValue = [] ):
  option = "%s/Hosts/%s/Properties" %( g_BaseSecuritySection, hostName )
  return gConfig.getValue( option, defaultValue )

def getBannedIPs():
  return gConfig.getValue( "%s/BannedIPs" % g_BaseSecuritySection, [] )

def getDefaultVOMSAttribute():
  return gConfig.getValue( "%s/DefaultVOMSAttribute" % g_BaseSecuritySection, "/lhcb/Role=user" )

def getVOMSAttributeForGroup( group ):
  return gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( g_BaseSecuritySection, group ), getDefaultVOMSAttribute() )

def getGroupsWithVOMSAttribute( vomsAttr ):
  retVal = gConfig.getSections( "%s/Groups" % ( g_BaseSecuritySection ) )
  if not retVal[ 'OK' ]:
    return []
  groups = []
  for group in retVal[ 'Value' ]:
    if vomsAttr == gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( g_BaseSecuritySection, group ), "" ):
      groups.append( group )
  return groups

def skipCACheck():
  return gConfig.getValue( "/DIRAC/Security/SkipCAChecks", "false" ).lower() in ( "y", "yes", "true" )