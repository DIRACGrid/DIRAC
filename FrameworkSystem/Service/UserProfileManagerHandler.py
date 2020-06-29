""" ProfileManager manages web user profiles
    in the DISET framework
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.UserProfileDB import UserProfileDB
from DIRAC.Core.Security import Properties

gUPDB = False


def initializeUserProfileManagerHandler(serviceInfo):
  global gUPDB
  try:
    gUPDB = UserProfileDB()
  except Exception as e:
    return S_ERROR("Can't initialize UserProfileDB: %s" % str(e))
  return S_OK()


class UserProfileManagerHandler(RequestHandler):

  types_retrieveProfileVar = [types.StringTypes, types.StringTypes]

  def export_retrieveProfileVar(self, profileName, varName):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveVar(userName, userGroup,
                             userName, userGroup,
                             profileName, varName)

  types_retrieveProfileVarFromUser = [types.StringTypes, types.StringTypes, types.StringTypes, types.StringTypes]

  def export_retrieveProfileVarFromUser(self, ownerName, ownerGroup, profileName, varName):
    """ Get profile data for web for any user according to perms
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveVar(userName, userGroup,
                             ownerName, ownerGroup,
                             profileName, varName)

  types_retrieveProfileAllVars = [types.StringTypes]

  def export_retrieveProfileAllVars(self, profileName):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveAllUserVars(userName, userGroup, profileName)

  types_storeProfileVar = [types.StringTypes, types.StringTypes, types.StringTypes, types.DictType]

  def export_storeProfileVar(self, profileName, varName, data, perms):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.storeVar(userName, userGroup, profileName, varName, data, perms)

  types_deleteProfileVar = [types.StringTypes, types.StringTypes]

  def export_deleteProfileVar(self, profileName, varName):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.deleteVar(userName, userGroup, profileName, varName)

  types_listAvailableProfileVars = [types.StringTypes]

  def export_listAvailableProfileVars(self, profileName, filterDict={}):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.listVars(userName, userGroup, profileName, filterDict)

  types_getUserProfiles = []

  def export_getUserProfiles(self):
    """ Get all profiles for a user
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveUserProfiles(userName, userGroup)

  types_setProfileVarPermissions = [types.StringTypes, types.StringTypes, types.DictType]

  def export_setProfileVarPermissions(self, profileName, varName, perms):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.setUserVarPerms(userName, userGroup, profileName, varName, perms)

  types_getProfileVarPermissions = [types.StringTypes, types.StringTypes]

  def export_getProfileVarPermissions(self, profileName, varName):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveVarPerms(userName, userGroup,
                                  userName, userGroup,
                                  profileName, varName)

  types_storeHashTag = [types.StringTypes]

  def export_storeHashTag(self, tagName):
    """ Set hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.storeHashTag(userName, userGroup, tagName)

  types_retrieveHashTag = [types.StringTypes]

  def export_retrieveHashTag(self, hashTag):
    """ Get hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveHashTag(userName, userGroup, hashTag)

  types_retrieveAllHashTags = []

  def export_retrieveAllHashTags(self):
    """ Get all hash tags
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return gUPDB.retrieveAllHashTags(userName, userGroup)

  types_deleteProfiles = [types.ListType]

  def export_deleteProfiles(self, userList):
    """
    Delete profiles for a list of users
    """
    credDict = self.getRemoteCredentials()
    requesterUserName = credDict['username']
    if Properties.SERVICE_ADMINISTRATOR in credDict['properties']:
      admin = True
    else:
      admin = False
    for entry in userList:
      userName = entry
      if admin or userName == requesterUserName:
        result = gUPDB.deleteUserProfile(userName)
        if not result['OK']:
          return result
    return S_OK()

  types_getUserProfileNames = [types.DictType]

  def export_getUserProfileNames(self, permission):
    """
    it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
    """
    return gUPDB.getUserProfileNames(permission)
