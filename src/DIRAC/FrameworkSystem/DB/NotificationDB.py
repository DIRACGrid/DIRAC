""" NotificationDB class is a front-end to the Notifications database
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB


class NotificationDB(DB):
    def __init__(self, parentLogger=None):
        DB.__init__(self, "NotificationDB", "Framework/NotificationDB", parentLogger=parentLogger)
        result = self.__initializeDB()
        if not result["OK"]:
            self.log.fatal("Cannot initialize DB!", result["Message"])
        self.__notificationQueryFields = ("id", "user", "seen", "message", "timestamp")

    def __initializeDB(self):
        retVal = self._query("show tables")
        if not retVal["OK"]:
            return retVal

        tablesInDB = [t[0] for t in retVal["Value"]]
        tablesToCreate = {}
        if "ntf_AssigneeGroups" not in tablesInDB:
            tablesToCreate["ntf_AssigneeGroups"] = {
                "Fields": {
                    "AssigneeGroup": "VARCHAR(64) NOT NULL",
                    "User": "VARCHAR(64) NOT NULL",
                },
                "Indexes": {"ag": ["AssigneeGroup"]},
            }

        if tablesToCreate:
            return self._createTables(tablesToCreate)
        return S_OK()

    ###
    # Assignee groups management
    ###

    def getUserAsignees(self, assignee):
        # Check if it is a user
        if assignee in Registry.getAllUsers():
            return S_OK([assignee])
        result = self._escapeString(assignee)
        if not result["OK"]:
            return result
        escAG = result["Value"]
        sqlSel = f"SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = {escAG}"
        result = self._query(sqlSel)
        if not result["OK"]:
            return result
        users = [row[0] for row in result["Value"]]
        if not users:
            return S_OK([])
        return S_OK(users)

    def setAssigneeGroup(self, groupName, usersList):
        validUsers = Registry.getAllUsers()
        result = self._escapeString(groupName)
        if not result["OK"]:
            return result
        escGroup = result["Value"]
        sqlSel = f"SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = {escGroup}"
        result = self._query(sqlSel)
        if not result["OK"]:
            return result
        currentUsers = [row[0] for row in result["Value"]]
        usersToDelete = []
        usersToAdd = []
        finalUsersInGroup = len(currentUsers)
        for user in currentUsers:
            if user not in usersList:
                result = self._escapeString(user)
                if not result["OK"]:
                    return result
                usersToDelete.append(result["Value"])
                finalUsersInGroup -= 1
        for user in usersList:
            if user not in validUsers:
                continue
            if user not in currentUsers:
                result = self._escapeString(user)
                if not result["OK"]:
                    return result
                usersToAdd.append(f"( {escGroup}, {result['Value']} )")
                finalUsersInGroup += 1
        if not finalUsersInGroup:
            return S_ERROR("Group must have at least one user!")
        # Delete old users
        if usersToDelete:
            sqlDel = f"DELETE FROM `ntf_AssigneeGroups` WHERE User in ( {','.join(usersToDelete)} )"
            result = self._update(sqlDel)
            if not result["OK"]:
                return result
        # Add new users
        if usersToAdd:
            sqlInsert = f"INSERT INTO `ntf_AssigneeGroups` ( AssigneeGroup, User ) VALUES {','.join(usersToAdd)}"
            result = self._update(sqlInsert)
            if not result["OK"]:
                return result
        return S_OK()

    def deleteAssigneeGroup(self, groupName):
        result = self._escapeString(groupName)
        if not result["OK"]:
            return result
        escGroup = result["Value"]
        sqlSel = f"SELECT AlarmId FROM `ntf_Alarms` WHERE Assignee={escGroup}"
        result = self._query(sqlSel)
        if not result["OK"]:
            return result
        if result["Value"]:
            alarmIds = [row[0] for row in result["Value"]]
            return S_ERROR(f"There are {len(alarmIds)} alarms assigned to this group")
        sqlDel = f"DELETE FROM `ntf_AssigneeGroups` WHERE AssigneeGroup={escGroup}"
        return self._update(sqlDel)

    def getAssigneeGroups(self):
        result = self._query("SELECT AssigneeGroup, User from `ntf_AssigneeGroups` ORDER BY User")
        if not result["OK"]:
            return result
        agDict = {}
        for row in result["Value"]:
            ag = row[0]
            user = row[1]
            if ag not in agDict:
                agDict[ag] = []
            agDict[ag].append(user)
        return S_OK(agDict)

    def getAssigneeGroupsForUser(self, user):
        if user not in Registry.getAllUsers():
            return S_ERROR(f"{user} is an unknown user")
        result = self._escapeString(user)
        if not result["OK"]:
            return result
        user = result["Value"]
        result = self._query(f"SELECT AssigneeGroup from `ntf_AssigneeGroups` WHERE User={user}")
        if not result["OK"]:
            return result
        return S_OK([row[0] for row in result["Value"]])
