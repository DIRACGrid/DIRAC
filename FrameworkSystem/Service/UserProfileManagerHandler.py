""" ProfileManager manages web user profiles
    in the DISET framework
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import six

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK
from DIRAC.FrameworkSystem.DB.UserProfileDB import UserProfileDB
from DIRAC.Core.Security import Properties


class UserProfileManagerHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ Handler initialization
    """
    cls.gUPDB = UserProfileDB()
    return S_OK()

  types_retrieveProfileVar = [six.string_types, six.string_types]

  def export_retrieveProfileVar(self, profileName, varName):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveVar(userName, userGroup,
				  userName, userGroup,
				  profileName, varName)

  types_retrieveProfileVarFromUser = [six.string_types, six.string_types, six.string_types, six.string_types]

  def export_retrieveProfileVarFromUser(self, ownerName, ownerGroup, profileName, varName):
    """ Get profile data for web for any user according to perms
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveVar(userName, userGroup,
				  ownerName, ownerGroup,
				  profileName, varName)

  types_retrieveProfileAllVars = [six.string_types]

  def export_retrieveProfileAllVars(self, profileName):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveAllUserVars(userName, userGroup, profileName)

  types_storeProfileVar = [six.string_types, six.string_types, six.string_types, dict]

  def export_storeProfileVar(self, profileName, varName, data, perms):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.storeVar(userName, userGroup, profileName, varName, data, perms)

  types_deleteProfileVar = [six.string_types, six.string_types]

  def export_deleteProfileVar(self, profileName, varName):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.deleteVar(userName, userGroup, profileName, varName)

  types_listAvailableProfileVars = [six.string_types]

  def export_listAvailableProfileVars(self, profileName, filterDict={}):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.listVars(userName, userGroup, profileName, filterDict)

  types_getUserProfiles = []

  def export_getUserProfiles(self):
    """ Get all profiles for a user
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveUserProfiles(userName, userGroup)

  types_setProfileVarPermissions = [six.string_types, six.string_types, dict]

  def export_setProfileVarPermissions(self, profileName, varName, perms):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.setUserVarPerms(userName, userGroup, profileName, varName, perms)

  types_getProfileVarPermissions = [six.string_types, six.string_types]

  def export_getProfileVarPermissions(self, profileName, varName):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveVarPerms(userName, userGroup,
				       userName, userGroup,
				       profileName, varName)

  types_storeHashTag = [six.string_types]

  def export_storeHashTag(self, tagName):
    """ Set hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.storeHashTag(userName, userGroup, tagName)

  types_retrieveHashTag = [six.string_types]

  def export_retrieveHashTag(self, hashTag):
    """ Get hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveHashTag(userName, userGroup, hashTag)

  types_retrieveAllHashTags = []

  def export_retrieveAllHashTags(self):
    """ Get all hash tags
    """
    credDict = self.getRemoteCredentials()
    userName = credDict['username']
    userGroup = credDict['group']
    return self.gUPDB.retrieveAllHashTags(userName, userGroup)

  types_deleteProfiles = [list]

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
	result = self.gUPDB.deleteUserProfile(userName)
        if not result['OK']:
          return result
    return S_OK()

  types_getUserProfileNames = [dict]

  def export_getUserProfileNames(self, permission):
    """
    it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
    """
    return self.gUPDB.getUserProfileNames(permission)
