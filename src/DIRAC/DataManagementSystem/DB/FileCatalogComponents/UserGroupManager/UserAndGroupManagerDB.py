""" DIRAC FileCatalog mix-in class to manage users and groups within the FC database
"""
import time
import threading

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.UserGroupManager.UserAndGroupManagerBase import (
    UserAndGroupManagerBase,
)


class UserAndGroupManagerDB(UserAndGroupManagerBase):
    def getUserAndGroupID(self, credDict):
        """Get a uid, gid tuple for the given Credentials"""
        # Get the user
        s_uid = credDict.get("username", "anon")
        res = self.getUserID(s_uid)
        if not res["OK"]:
            return res
        uid = res["Value"]
        # Get the group (create it if it doesn't exist)
        s_gid = credDict.get("group", "anon")
        res = self.getGroupID(s_gid)
        if not res["OK"]:
            return res
        gid = res["Value"]
        return S_OK((uid, gid))

    #####################################################################
    #
    #  User related methods
    #
    #####################################################################

    def getUserID(self, user):
        """Get ID for a user specified by its name"""
        if isinstance(user, int):
            return S_OK(user)
        if user in self.db.users.keys():
            return S_OK(self.db.users[user])
        return self.__addUser(user)

    def addUser(self, uname):
        """Add a new user with a name 'uname'"""
        return self.getUserID(uname)

    def getUsers(self):
        # self.__refreshUsers()
        return S_OK(self.db.users)

    def findUser(self, user):
        return self.getUserID(user)

    def getUserName(self, uid):
        """Get user name for the given id"""
        if uid in self.db.uids.keys():
            return S_OK(self.db.uids[uid])
        return S_ERROR("User id %d not found" % uid)

    def deleteUser(self, uname, force=True):
        """Delete a user specified by its name"""
        # ToDo: Check first if there are files belonging to the user
        if not force:
            pass
        return self.__removeUser(uname)

    def __addUser(self, uname):
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager AddUser lock created. Waited {waitTime - startTime:.3f} seconds. {uname}")
        if uname in self.db.users.keys():
            uid = self.db.users[uname]
            gLogger.debug(f"UserGroupManager AddUser lock released. Used {time.time() - waitTime:.3f} seconds. {uname}")
            self.lock.release()
            return S_OK(uid)
        res = self.db.insertFields("FC_Users", ["UserName"], [uname])
        if not res["OK"]:
            gLogger.debug(f"UserGroupManager AddUser lock released. Used {time.time() - waitTime:.3f} seconds. {uname}")
            self.lock.release()
            if "Duplicate entry" in res["Message"]:
                result = self._refreshUsers()
                if not result["OK"]:
                    return result
                if uname in self.db.users.keys():
                    uid = self.db.users[uname]
                    return S_OK(uid)
            return res
        uid = res["lastRowId"]
        self.db.uids[uid] = uname
        self.db.users[uname] = uid
        gLogger.debug(f"UserGroupManager AddUser lock released. Used {time.time() - waitTime:.3f} seconds. {uname}")
        self.lock.release()
        return S_OK(uid)

    def __removeUser(self, uname):
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager RemoveUser lock created. Waited {waitTime - startTime:.3f} seconds. {uname}")
        uid = self.db.users.get(uname, "Missing")
        req = f"DELETE FROM FC_Users WHERE UserName='{uname}'"
        res = self.db._update(req)
        if not res["OK"]:
            gLogger.debug(
                f"UserGroupManager RemoveUser lock released. Used {time.time() - waitTime:.3f} seconds. {uname}"
            )
            self.lock.release()
            return res
        if uid != "Missing":
            self.db.users.pop(uname)
            self.db.uids.pop(uid)
        gLogger.debug(f"UserGroupManager RemoveUser lock released. Used {time.time() - waitTime:.3f} seconds. {uname}")
        self.lock.release()
        return S_OK()

    def _refreshUsers(self):
        """Get the current user IDs and names"""
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager RefreshUsers lock created. Waited {waitTime - startTime:.3f} seconds.")
        req = "SELECT UID,UserName from FC_Users"
        res = self.db._query(req)
        if not res["OK"]:
            gLogger.debug(f"UserGroupManager RefreshUsers lock released. Used {time.time() - waitTime:.3f} seconds.")
            self.lock.release()
            return res
        self.db.users = {}
        self.db.uids = {}
        for uid, uname in res["Value"]:
            self.db.users[uname] = uid
            self.db.uids[uid] = uname
        gLogger.debug(f"UserGroupManager RefreshUsers lock released. Used {time.time() - waitTime:.3f} seconds.")
        self.lock.release()
        return S_OK()

    #####################################################################
    #
    #  Group related methods
    #

    def getGroupID(self, group):
        """Get ID for a group specified by its name"""
        if isinstance(group, int):
            return S_OK(group)
        if group in self.db.groups.keys():
            return S_OK(self.db.groups[group])
        return self.__addGroup(group)

    def addGroup(self, gname):
        """Add a new group with a name 'name'"""
        return self.getGroupID(gname)

    def getGroups(self):
        # self.__refreshGroups()
        return S_OK(self.db.groups)

    def findGroup(self, group):
        return self.getGroupID(group)

    def getGroupName(self, gid):
        """Get group name for the given id"""
        if gid in self.db.gids.keys():
            return S_OK(self.db.gids[gid])
        return S_ERROR("Group id %d not found" % gid)

    def deleteGroup(self, gname, force=True):
        """Delete a group specified by its name"""
        if not force:
            # ToDo: Check first if there are files belonging to the group
            pass
        return self.__removeGroup(gname)

    def __addGroup(self, group):
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager AddGroup lock created. Waited {waitTime - startTime:.3f} seconds. {group}")
        if group in self.db.groups.keys():
            gid = self.db.groups[group]
            gLogger.debug(
                f"UserGroupManager AddGroup lock released. Used {time.time() - waitTime:.3f} seconds. {group}"
            )
            self.lock.release()
            return S_OK(gid)
        res = self.db.insertFields("FC_Groups", ["GroupName"], [group])
        if not res["OK"]:
            gLogger.debug(
                f"UserGroupManager AddGroup lock released. Used {time.time() - waitTime:.3f} seconds. {group}"
            )
            self.lock.release()
            if "Duplicate entry" in res["Message"]:
                result = self._refreshGroups()
                if not result["OK"]:
                    return result
                if group in self.db.groups.keys():
                    gid = self.db.groups[group]
                    return S_OK(gid)
            return res
        gid = res["lastRowId"]
        self.db.gids[gid] = group
        self.db.groups[group] = gid
        gLogger.debug(f"UserGroupManager AddGroup lock released. Used {time.time() - waitTime:.3f} seconds. {group}")
        self.lock.release()
        return S_OK(gid)

    def __removeGroup(self, group):
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager RemoveGroup lock created. Waited {waitTime - startTime:.3f} seconds. {group}")
        gid = self.db.groups.get(group, "Missing")
        req = f"DELETE FROM FC_Groups WHERE GroupName='{group}'"
        res = self.db._update(req)
        if not res["OK"]:
            gLogger.debug(
                f"UserGroupManager RemoveGroup lock released. Used {time.time() - waitTime:.3f} seconds. {group}"
            )
            self.lock.release()
            return res
        if gid != "Missing":
            self.db.groups.pop(group)
            self.db.gids.pop(gid)
        gLogger.debug(f"UserGroupManager RemoveGroup lock released. Used {time.time() - waitTime:.3f} seconds. {group}")
        self.lock.release()
        return S_OK()

    def _refreshGroups(self):
        """Get the current group IDs and names"""
        req = "SELECT GID,GroupName from FC_Groups"
        startTime = time.time()
        self.lock.acquire()
        waitTime = time.time()
        gLogger.debug(f"UserGroupManager RefreshGroups lock created. Waited {waitTime - startTime:.3f} seconds.")
        res = self.db._query(req)
        if not res["OK"]:
            gLogger.debug(f"UserGroupManager RefreshGroups lock released. Used {time.time() - waitTime:.3f} seconds.")
            self.lock.release()
            return res
        self.db.groups = {}
        self.db.gids = {}
        for gid, gname in res["Value"]:
            self.db.groups[gname] = gid
            self.db.gids[gid] = gname
        gLogger.debug(f"UserGroupManager RefreshGroups lock released. Used {time.time() - waitTime:.3f} seconds.")
        self.lock.release()
        return S_OK()
