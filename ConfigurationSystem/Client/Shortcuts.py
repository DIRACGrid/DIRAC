# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/Shortcuts.py,v 1.1 2008/03/19 11:00:01 acasajus Exp $
__RCSID__ = "$Id: Shortcuts.py,v 1.1 2008/03/19 11:00:01 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Config import gConfig

def getUsernameForDN( dn ):
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