########################################################################
# $HeadURL$
########################################################################

""" ProfileManager manages web user profiles
    in the DISET framework
"""

__RCSID__ = "$Id$"

import types
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from DIRAC.FrameworkSystem.DB.UserProfileDB import UserProfileDB
from DIRAC.Core.Security import Properties

gUPDB = False

def initializeUserProfileManagerHandler( serviceInfo ):
  global gUPDB
  try:
    gUPDB = UserProfileDB()
  except Exception, e:
    return S_ERROR( "Can't initialize UserProfileDB: %s" % str( e ) )
  return S_OK()

class UserProfileManagerHandler( RequestHandler ):

  types_retrieveUserProfileVar = [ types.StringType, types.StringType ]
  def export_retrieveUserProfileVar( self, profileName, varName ):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveVar( userName, userGroup,
                              userName, userGroup,
                              profileName, varName )

  types_retrieveProfileVar = [ types.StringType, types.StringType, types.StringType, types.StringType ]
  def export_retrieveProfileVar( self, ownerName, ownerGroup, profileName, varName ):
    """ Get profile data for web for any user according to perms
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveVar( userName, userGroup,
                              ownerName, ownerGroup,
                              profileName, varName )

  types_retrieveUserProfileAllVars = [ types.StringType ]
  def export_retrieveUserProfileAllVars( self, profileName ):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveAllUserVars( userName, userGroup, profileName )

  types_storeUserProfileVar = [ types.StringType, types.StringType, types.StringType, types.DictType ]
  def export_storeUserProfileVar( self, profileName, varName, data, perms ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.storeVar( userName, userGroup, profileName, varName, data, perms )

  types_deleteUserProfileVar = [ types.StringType, types.StringType ]
  def export_deleteUserProfileVar( self, profileName, varName ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.deleteVar( userName, userGroup, profileName, varName )

  types_listAvailableProfileVars = [ types.StringType ]
  def export_listAvailableProfileVars( self, profileName ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.listVars( userName, userGroup, profileName )

  types_setUserProfileVarPermissions = [  types.StringType, types.StringType, types.DictType ]
  def export_setUserProfileVarPermissions( self, profileName, varName, perms ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.setUserVarPerms( userName, userGroup, profileName, varName, perms )

  types_getUserProfileVarPermissions = [  types.StringType, types.StringType ]
  def export_getUserProfileVarPermissions( self, profileName, varName ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveVarPerms( userName, userGroup,
                                   userName, userGroup,
                                   profileName, varName )

  types_storeHashTag = [ types.StringType ]
  def export_storeHashTag( self, tagName ):
    """ Set hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.storeHashTag( userName, userGroup, tagName )

  types_retrieveHashTag = [ types.StringType ]
  def export_retrieveHashTag( self, hashTag ):
    """ Get hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveHashTag( userName, userGroup, hashTag )

  types_retrieveAllHashTags = []
  def export_retrieveAllHashTags( self ):
    """ Get all hash tags
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    print "ASDASDASD"
    return gUPDB.retrieveAllHashTags( userName, userGroup )

  types_deleteProfiles = [ types.ListType ]
  def export_deleteProfiles( self, userList ):
    """
    Delete profiles for a list of users
    """
    credDict = self.getRemoteCredentials()
    requesterUserName = credDict[ 'username' ]
    if Properties.SERVICE_ADMINISTRATOR in credDict[ 'properties' ]:
      admin = True
    else:
      admin = False
    for entry in userList:
      userName = entry
      if admin or userName == requesterUserName:
        result = gUPDB.deleteUserProfile( userName )
        if not result[ 'OK' ]:
          return result
    return S_OK()
