""" ProfileManager manages web user profiles interfacin to UserProfileDB
"""

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK
from DIRAC.FrameworkSystem.DB.UserProfileDB import UserProfileDB
from DIRAC.Core.Security import Properties


class UserProfileManagerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """Handler initialization"""
        cls.upDB = UserProfileDB()
        return S_OK()

    types_retrieveProfileVar = [str, str]

    def export_retrieveProfileVar(self, profileName, varName):
        """Get profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.retrieveVar(userName, userGroup, userName, userGroup, profileName, varName)

    types_retrieveProfileVarFromUser = [str, str, str, str]

    def export_retrieveProfileVarFromUser(self, ownerName, ownerGroup, profileName, varName):
        """Get profile data for web for any user according to perms"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.retrieveVar(userName, userGroup, ownerName, ownerGroup, profileName, varName)

    types_retrieveProfileAllVars = [str]

    def export_retrieveProfileAllVars(self, profileName):
        """Get profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.retrieveAllUserVars(userName, userGroup, profileName)

    types_storeProfileVar = [str, str, str, dict]

    def export_storeProfileVar(self, profileName, varName, data, perms):
        """Set profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.storeVar(userName, userGroup, profileName, varName, data, perms)

    types_deleteProfileVar = [str, str]

    def export_deleteProfileVar(self, profileName, varName):
        """Set profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.deleteVar(userName, userGroup, profileName, varName)

    types_listStatesForWeb = [dict]

    def export_listStatesForWeb(self, permission):
        retVal = self.export_getUserProfileNames(permission)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]

        records = []
        for i in data:
            application = i.replace("Web/application/", "")
            retVal = self.export_listAvailableProfileVars(i)
            if not retVal["OK"]:
                return retVal
            states = retVal["Value"]
            for state in states:
                record = dict(zip(["user", "group", "vo", "name"], state))
                record["app"] = application
                retVal = self.export_getProfileVarPermissions(i, record["name"])
                if not retVal["OK"]:
                    return retVal
                record["permissions"] = retVal["Value"]
                records += [record]

        return S_OK(records)

    types_listAvailableProfileVars = [str]

    def export_listAvailableProfileVars(self, profileName, filterDict={}):
        """Set profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.listVars(userName, userGroup, profileName, filterDict)

    types_getUserProfiles = []

    def export_getUserProfiles(self):
        """Get all profiles for a user"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.retrieveUserProfiles(userName, userGroup)

    types_setProfileVarPermissions = [str, str, dict]

    def export_setProfileVarPermissions(self, profileName, varName, perms):
        """Set profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.setUserVarPerms(userName, userGroup, profileName, varName, perms)

    types_getProfileVarPermissions = [str, str]

    def export_getProfileVarPermissions(self, profileName, varName):
        """Set profile data for web"""
        credDict = self.getRemoteCredentials()
        userName = credDict["username"]
        userGroup = credDict["group"]
        return self.upDB.retrieveVarPerms(userName, userGroup, userName, userGroup, profileName, varName)

    types_deleteProfiles = [list]

    def export_deleteProfiles(self, userList):
        """
        Delete profiles for a list of users
        """
        credDict = self.getRemoteCredentials()
        requesterUserName = credDict["username"]
        if Properties.SERVICE_ADMINISTRATOR in credDict["properties"]:
            admin = True
        else:
            admin = False
        for entry in userList:
            userName = entry
            if admin or userName == requesterUserName:
                result = self.upDB.deleteUserProfile(userName)
                if not result["OK"]:
                    return result
        return S_OK()

    types_getUserProfileNames = [dict]

    def export_getUserProfileNames(self, permission):
        """
        it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
        """
        return self.upDB.getUserProfileNames(permission)
