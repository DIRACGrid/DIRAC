""" DIRAC FileCatalog mix-in class to manage users and groups from the CS Registry
"""
import time
import threading

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.UserGroupManager.UserAndGroupManagerBase import (
    UserAndGroupManagerBase,
)


class UserAndGroupManagerCS(UserAndGroupManagerBase):
    def getUserAndGroupID(self, credDict):
        user = credDict.get("username", "anon")
        group = credDict.get("group", "anon")
        return S_OK((user, group))

    #####################################################################
    #
    #  User related methods
    #
    #####################################################################

    def addUser(self, name):
        return S_OK(name)

    def deleteUser(self, name, force=True):
        return S_OK()

    def getUsers(self):
        res = gConfig.getSections("/Registry/Users")
        if not res["OK"]:
            return res
        userDict = {}
        for user in res["Value"]:
            userDict[user] = user
        return S_OK(userDict)

    def getUserName(self, uid):
        return S_OK(uid)

    def findUser(self, user):
        return S_OK(user)

    #####################################################################
    #
    #  Group related methods
    #
    #####################################################################

    def addGroup(self, gname):
        return S_OK(gname)

    def deleteGroup(self, gname, force=True):
        return S_OK()

    def getGroups(self):
        res = gConfig.getSections("/Registry/Groups")
        if not res["OK"]:
            return res
        groupDict = {}
        for group in res["Value"]:
            groupDict[group] = group
        return S_OK(groupDict)

    def getGroupName(self, gid):
        return S_OK(gid)

    def findGroup(self, group):
        return S_OK(group)
