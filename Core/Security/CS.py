# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Security/CS.py,v 1.1 2008/06/09 19:28:52 acasajus Exp $
__RCSID__ = "$Id: CS.py,v 1.1 2008/06/09 19:28:52 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Config import gConfig

def getUsernameForDN( dn, usersList = False ):
  if not usersList:
    retVal = gConfig.getSections( "/Users" )
    if not retVal[ 'OK' ]:
      return retVal
    usersList = retVal[ 'Value' ]
  for username in usersList:
    if dn == gConfig.getValue( "/Users/%s/DN" % username, "" ):
      return S_OK( username )
  return S_ERROR( "No username found for dn %s" % dn )

def getGroupsForUser( username ):
  retVal = gConfig.getSections( "/Groups" )
  if not retVal[ 'OK' ]:
    return retVal
  groupsList = retVal[ 'Value' ]
  userGroups = []
  for group in groupsList:
    if username in gConfig.getValue( "/Groups/%s/users" % group, [] ):
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
  retVal = gConfig.getSections( "/Hosts" )
  if not retVal[ 'OK' ]:
    return retVal
  hostList = retVal[ 'Value' ]
  for hostname in hostList:
    if dn == gConfig.getValue( "/Hosts/%s/DN" % hostname, "" ):
      return S_OK( hostname )
  return S_ERROR( "No hostname found for dn %s" % dn )

def getDefaultUserGroup():
  return gConfig.getValue( "/DIRAC/DefaultGroup", "user" )

def getUsersInGroup( groupName, defaultValue = False ):
  if not defaultValue:
    return gConfig.getOption( "/Groups/%s/users" % groupName )
  return gConfig.getValue( "/Groups/%s/users" % groupName, defaultValue )

def getPropertiesInGroup( groupName, defaultValue = False ):
  if not defaultValue:
    return gConfig.getOption( "/Groups/%s/Properties" % groupName )
  return gConfig.getValue( "/Groups/%s/Properties" % groupName, defaultValue )

def getTrustedHostList():
  return gConfig.getValue( "/DIRAC/Security/TrustedHosts", [] )