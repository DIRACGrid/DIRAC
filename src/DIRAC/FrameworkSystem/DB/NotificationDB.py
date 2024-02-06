""" NotificationDB class is a front-end to the Notifications database
"""
from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.Mail import Mail


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

        if "ntf_Notifications" not in tablesInDB:
            tablesToCreate["ntf_Notifications"] = {
                "Fields": {
                    "Id": "INTEGER UNSIGNED AUTO_INCREMENT NOT NULL",
                    "User": "VARCHAR(64) NOT NULL",
                    "Message": "TEXT NOT NULL",
                    "Seen": "TINYINT(1) NOT NULL DEFAULT 0",
                    "Expiration": "DATETIME",
                    "Timestamp": "DATETIME",
                    "DeferToMail": "TINYINT(1) NOT NULL DEFAULT 1",
                },
                "PrimaryKey": "Id",
            }

        if tablesToCreate:
            return self._createTables(tablesToCreate)
        return S_OK()

    def __sendMailToUser(self, user, subject, message):
        address = gConfig.getValue(f"/Registry/Users/{user}/Email", "")
        if not address:
            self.log.error("User does not have an email registered", user)
            return S_ERROR(f"User {user} does not have an email registered")
        self.log.info(f"Sending mail ({subject}) to user {user} at {address}")
        m = Mail()
        m._subject = f"[DIRAC] {subject}"
        m._message = message
        m._mailAddress = address
        result = m._send()
        if not result["OK"]:
            gLogger.warn(f"Could not send mail with the following message:\n{result['Message']}")

        return result

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

    ###
    # Notifications
    ###

    def addNotificationForUser(self, user, message, lifetime=0, deferToMail=1):
        if user not in Registry.getAllUsers():
            return S_ERROR(f"{user} is an unknown user")
        self.log.info(f"Adding a notification for user {user} (msg is {len(message)} chars)")
        result = self._escapeString(user)
        if not result["OK"]:
            return result
        user = result["Value"]
        result = self._escapeString(message)
        if not result["OK"]:
            return result
        message = result["Value"]
        sqlFields = ["User", "Message", "Timestamp"]
        sqlValues = [user, message, "UTC_TIMESTAMP()"]
        if not deferToMail:
            sqlFields.append("DeferToMail")
            sqlValues.append("0")
        if lifetime:
            sqlFields.append("Expiration")
            sqlValues.append("TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % int(lifetime))
        sqlInsert = f"INSERT INTO `ntf_Notifications` ({','.join(sqlFields)}) VALUES ({','.join(sqlValues)}) "
        result = self._update(sqlInsert)
        if not result["OK"]:
            return result
        return S_OK(result["lastRowId"])

    def removeNotificationsForUser(self, user, msgIds=False):
        if user not in Registry.getAllUsers():
            return S_ERROR(f"{user} is an unknown user")
        result = self._escapeString(user)
        if not result["OK"]:
            return result
        user = result["Value"]
        delSQL = f"DELETE FROM `ntf_Notifications` WHERE User={user}"
        escapedIDs = []
        if msgIds:
            for iD in msgIds:
                result = self._escapeString(str(iD))
                if not result["OK"]:
                    return result
                escapedIDs.append(result["Value"])
            delSQL = f"{delSQL} AND Id in ( {','.join(escapedIDs)} ) "
        return self._update(delSQL)

    def markNotificationsSeen(self, user, seen=True, msgIds=False):
        if user not in Registry.getAllUsers():
            return S_ERROR(f"{user} is an unknown user")
        result = self._escapeString(user)
        if not result["OK"]:
            return result
        user = result["Value"]
        if seen:
            seen = 1
        else:
            seen = 0
        updateSQL = "UPDATE `ntf_Notifications` SET Seen=%d WHERE User=%s" % (seen, user)
        escapedIDs = []
        if msgIds:
            for iD in msgIds:
                result = self._escapeString(str(iD))
                if not result["OK"]:
                    return result
                escapedIDs.append(result["Value"])
            updateSQL = f"{updateSQL} AND Id in ( {','.join(escapedIDs)} ) "
        return self._update(updateSQL)

    def getNotifications(self, condDict={}, sortList=False, start=0, limit=0):
        condSQL = []
        for field in self.__notificationQueryFields:
            if field in condDict:
                fieldValues = []
                for value in condDict[field]:
                    result = self._escapeString(value)
                    if not result["OK"]:
                        return result
                    fieldValues.append(result["Value"])
                condSQL.append(f"{field} in ( {','.join(fieldValues)} )")

        eSortList = []
        for field, order in sortList:
            if order.lower() in ["asc", "desc"]:
                eSortList.append((f"`{field.replace('`', '')}`", order))

        selSQL = f"SELECT {','.join(self.__notificationQueryFields)} FROM `ntf_Notifications`"
        if condSQL:
            selSQL = f"{selSQL} WHERE {' AND '.join(condSQL)}"
        if eSortList:
            selSQL += f" ORDER BY {', '.join([f'{sort[0]} {sort[1]}' for sort in eSortList])}"
        else:
            selSQL += " ORDER BY Id DESC"
        if limit:
            selSQL += " LIMIT %d,%d" % (start, limit)

        result = self._query(selSQL)
        if not result["OK"]:
            return result

        resultDict = {}
        resultDict["ParameterNames"] = self.__notificationQueryFields
        resultDict["Records"] = [list(v) for v in result["Value"]]
        return S_OK(resultDict)

    def purgeExpiredNotifications(self):
        self.log.info("Purging expired notifications")
        delConds = ["(Seen=1 OR DeferToMail=0)", "(TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), Expiration ) < 0 )"]
        delSQL = f"DELETE FROM `ntf_Notifications` WHERE {' AND '.join(delConds)}"
        result = self._update(delSQL)
        if not result["OK"]:
            return result
        self.log.info(f"Purged {result['Value']} notifications")
        deferCond = ["Seen=0", "DeferToMail=1", "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), Expiration ) < 0"]
        selSQL = f"SELECT Id, User, Message FROM `ntf_Notifications` WHERE {' AND '.join(deferCond)}"
        result = self._query(selSQL)
        if not result["OK"]:
            return result
        messages = result["Value"]
        if not messages:
            return S_OK()
        ids = []
        for msg in messages:
            self.__sendMailToUser(msg[1], "Notification defered to mail", msg[2])
            ids.append(str(msg[0]))
        self.log.info(f"Deferred {len(ids)} notifications")
        return self._update(f"DELETE FROM `ntf_Notifications` WHERE Id in ({','.join(ids)})")
