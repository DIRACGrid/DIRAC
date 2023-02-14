""" NotificationDB class is a front-end to the Notifications database
"""
import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DEncode
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class NotificationDB(DB):
    def __init__(self, parentLogger=None):
        DB.__init__(self, "NotificationDB", "Framework/NotificationDB", parentLogger=parentLogger)
        result = self.__initializeDB()
        if not result["OK"]:
            self.log.fatal("Cannot initialize DB!", result["Message"])
        self.__alarmQueryFields = [
            "alarmid",
            "author",
            "creationtime",
            "modtime",
            "subject",
            "status",
            "priority",
            "notifications",
            "body",
            "assignee",
            "alarmkey",
        ]
        self.__alarmLogFields = ["timestamp", "author", "comment", "modifications"]
        self.__notificationQueryFields = ("id", "user", "seen", "message", "timestamp")
        self.__newAlarmMandatoryFields = [
            "author",
            "subject",
            "status",
            "notifications",
            "body",
            "assignee",
            "priority",
        ]
        self.__updateAlarmIdentificationFields = ["id", "alarmKey"]
        self.__updateAlarmMandatoryFields = ["author"]
        self.__updateAlarmAtLeastOneField = ["comment", "modifications"]
        self.__updateAlarmModificableFields = ["status", "assignee", "priority"]
        self.__validAlarmStatus = ["Open", "OnGoing", "Closed", "Testing"]
        self.__validAlarmNotifications = ["Web", "Mail", "SMS"]
        self.__validAlarmPriorities = ["Low", "Medium", "High", "Extreme"]

    def __initializeDB(self):
        retVal = self._query("show tables")
        if not retVal["OK"]:
            return retVal

        tablesInDB = [t[0] for t in retVal["Value"]]
        tablesToCreate = {}
        if "ntf_Alarms" not in tablesInDB:
            tablesToCreate["ntf_Alarms"] = {
                "Fields": {
                    "AlarmId": "INTEGER UNSIGNED AUTO_INCREMENT NOT NULL",
                    "AlarmKey": "VARCHAR(32) NOT NULL",
                    "Author": "VARCHAR(64) NOT NULL",
                    "CreationTime": "DATETIME NOT NULL",
                    "ModTime": "DATETIME NOT NULL",
                    "Subject": "VARCHAR(255) NOT NULL",
                    "Status": "VARCHAR(64) NOT NULL",
                    "Priority": "VARCHAR(32) NOT NULL",
                    "Body": "TEXT",
                    "Assignee": "VARCHAR(64) NOT NULL",
                    "Notifications": "VARCHAR(128) NOT NULL",
                },
                "PrimaryKey": "AlarmId",
                "Indexes": {"Status": ["Status"], "Assignee": ["Assignee"]},
            }
        if "ntf_AssigneeGroups" not in tablesInDB:
            tablesToCreate["ntf_AssigneeGroups"] = {
                "Fields": {
                    "AssigneeGroup": "VARCHAR(64) NOT NULL",
                    "User": "VARCHAR(64) NOT NULL",
                },
                "Indexes": {"ag": ["AssigneeGroup"]},
            }

        if "ntf_AlarmLog" not in tablesInDB:
            tablesToCreate["ntf_AlarmLog"] = {
                "Fields": {
                    "AlarmId": "INTEGER UNSIGNED NOT NULL",
                    "Timestamp": "DATETIME NOT NULL",
                    "Author": "VARCHAR(64) NOT NULL",
                    "Comment": "TEXT",
                    "Modifications": "VARCHAR(255)",
                },
                "Indexes": {"AlarmID": ["AlarmId"]},
            }

        if "ntf_AlarmFollowers" not in tablesInDB:
            tablesToCreate["ntf_AlarmFollowers"] = {
                "Fields": {
                    "AlarmId": "INTEGER UNSIGNED NOT NULL",
                    "User": "VARCHAR(64) NOT NULL",
                    "Mail": "TINYINT(1) DEFAULT 0",
                    "Notification": "TINYINT(1) DEFAULT 1",
                    "SMS": "TINYINT(1) DEFAULT 0",
                },
                "Indexes": {"AlarmID": ["AlarmId"]},
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

    def __checkAlarmField(self, name, value):
        name = name.lower()
        if name == "status":
            if value not in self.__validAlarmStatus:
                return S_ERROR(f"Status {value} is invalid. Valid ones are: {self.__validAlarmStatus}")
        elif name == "priority":
            if value not in self.__validAlarmPriorities:
                return S_ERROR(f"Type {value} is invalid. Valid ones are: {self.__validAlarmPriorities}")
        elif name == "assignee":
            result = self.getUserAsignees(value)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR(f"{value} is not a known assignee")
            return result
        return S_OK()

    def newAlarm(self, alarmDef):
        """Create a new alarm record"""
        followers = []
        for field in self.__newAlarmMandatoryFields:
            if field not in alarmDef:
                return S_ERROR(f"Oops. Missing {field}")
            result = self.__checkAlarmField(field, alarmDef[field])
            if not result["OK"]:
                return result
            if field == "assignee":
                followers = result["Value"]
        author = alarmDef["author"]
        if author not in followers:
            followers.append(author)

        sqlFieldsName = []
        sqlFieldsValue = []
        for field in self.__newAlarmMandatoryFields:
            if field == "notifications":
                notifications = {}
                for nType in self.__validAlarmNotifications:
                    if nType in alarmDef[field]:
                        notifications[nType] = 1
                    else:
                        notifications[nType] = 0
                val = DEncode.encode(notifications)
            else:
                val = alarmDef[field]
            # Add to the list of fields to add
            sqlFieldsName.append(field)
            result = self._escapeString(val)
            if result["OK"]:
                sqlFieldsValue.append(result["Value"])
            else:
                return S_ERROR(f"Failed to escape value {val}")
        sqlFieldsName.extend(["CreationTime", "ModTime"])
        sqlFieldsValue.extend(["UTC_TIMESTAMP()", "UTC_TIMESTAMP()"])

        # Get the defined alarmkey and generate a random one if not defined
        if "alarmKey" in alarmDef:
            result = self._escapeString(alarmDef["alarmKey"])
            if result["OK"]:
                alarmKey = result["Value"]
            else:
                return S_ERROR(f"Failed to escape value {val} for key AlarmKey")
            gLogger.info(f"Checking there are no alarms with key {alarmKey}")
            result = self._query(f"SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmKey={alarmKey}")
            if not result["OK"]:
                return result
            if result["Value"]:
                return S_ERROR(f"Oops, alarm with id {result['Value'][0][0]} has the same alarm key!")
        else:
            alarmKey = str(time.time())[-31:]
        sqlFieldsName.append("AlarmKey")
        sqlFieldsValue.append(alarmKey)

        sqlInsert = "INSERT INTO `ntf_Alarms` ({}) VALUES ({})".format(
            ",".join(sqlFieldsName), ",".join(sqlFieldsValue)
        )

        result = self._update(sqlInsert)
        if not result["OK"]:
            return result
        alarmId = result["lastRowId"]
        for follower in followers:
            result = self.modifyFollowerForAlarm(alarmId, follower, notifications)
            if not result["OK"]:
                varMsg = f"\nFollower: {follower}\nAlarm: {alarmId}\nError: {result['Message']}"
                self.log.error("Couldn't set follower for alarm", varMsg)
        self.__notifyAlarm(alarmId)
        return S_OK(alarmId)

    def deleteAlarmsByAlarmKey(self, alarmKeyList):
        alarmsIdList = []
        for alarmKey in alarmKeyList:
            result = self.__getAlarmIdFromKey(alarmKey)
            if not result["OK"]:
                return result
            alarmId = result["Value"]
            alarmsIdList.append(alarmId)
        self.log.info(f"Trying to delete alarms with:\n alamKey {alarmKeyList}\n  alarmId {alarmsIdList}")
        return self.deleteAlarmsByAlarmId(alarmsIdList)

    def deleteAlarmsByAlarmId(self, alarmIdList):
        self.log.info(f"Trying to delete alarms with ids {alarmIdList}")
        try:
            alarmId = int(alarmIdList)
            alarmIdList = [alarmId]
        except Exception:
            pass

        try:
            alarmIdList = [int(alarmId) for alarmId in alarmIdList]
        except Exception:
            self.log.error("At least one alarmId is not a number", str(alarmIdList))
            return S_ERROR(f"At least one alarmId is not a number: {str(alarmIdList)}")

        tablesToCheck = ("ntf_AlarmLog", "ntf_AlarmFollowers", "ntf_Alarms")
        alamsSQLList = ",".join(["%d" % alarmId for alarmId in alarmIdList])
        for tableName in tablesToCheck:
            delSql = f"DELETE FROM `{tableName}` WHERE AlarmId in ( {alamsSQLList} )"
            result = self._update(delSql)
            if not result["OK"]:
                self.log.error("Could not delete alarm", f"from table {tableName}: {result['Message']}")
        return S_OK()

    def __processUpdateAlarmModifications(self, modifications):
        if not isinstance(modifications, dict):
            return S_ERROR("Modifications must be a dictionary")
        updateFields = []
        followers = []
        for field in modifications:
            if field not in self.__updateAlarmModificableFields:
                return S_ERROR(f"{field} is not a valid modificable field")
            value = modifications[field]
            result = self.__checkAlarmField(field, value)
            if not result["OK"]:
                return result
            if field == "assignee":
                followers = result["Value"]
            result = self._escapeString(modifications[field])
            if not result["OK"]:
                return result
            updateFields.append(f"{field}={result['Value']}")
        return S_OK((", ".join(updateFields), DEncode.encode(modifications), followers))

    def __getAlarmIdFromKey(self, alarmKey):
        result = self._escapeString(alarmKey)
        if not result["OK"]:
            return S_ERROR(f"Cannot escape alarmKey {alarmKey}")
        alarmKey = result["Value"]
        sqlQuery = f"SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmKey={alarmKey}"
        result = self._query(sqlQuery)
        if result["OK"]:
            result["Value"] = result["Value"][0][0]
        return result

    def updateAlarm(self, updateReq):
        # Discover alarm identification
        idOK = False
        for field in self.__updateAlarmIdentificationFields:
            if field in updateReq:
                idOK = True
        if not idOK:
            return S_ERROR(
                f"Need at least one field to identify which alarm to update! {self.__updateAlarmIdentificationFields}"
            )
        if "alarmKey" in updateReq:
            alarmKey = updateReq["alarmKey"]
            result = self.__getAlarmIdFromKey(alarmKey)
            if not result["OK"]:
                self.log.error("Could not get alarm id for key", f" {alarmKey}: {result['Value']}")
                return result
            updateReq["id"] = result["Value"]
            self.log.info(f"Retrieving alarm key {alarmKey} maps to id {updateReq['id']}")
        # Check fields
        for field in self.__updateAlarmMandatoryFields:
            if field not in updateReq:
                return S_ERROR(f"Oops. Missing {field}")
        validReq = False
        for field in self.__updateAlarmAtLeastOneField:
            if field in updateReq:
                validReq = True
        if not validReq:
            return S_OK(f"Requirement needs at least one of {' '.join(self.__updateAlarmAtLeastOneField)}")
        author = updateReq["author"]
        followers = [author]
        if author not in Registry.getAllUsers():
            return S_ERROR(f"{author} is not a known user")
        result = self._escapeString(author)
        if not result["OK"]:
            return result
        author = result["Value"]
        try:
            alarmId = int(updateReq["id"])
        except Exception:
            return S_ERROR("Oops, Alarm id is not valid!")
        result = self._query("SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmId=%d" % alarmId)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Alarm {alarmId} does not exist!")
        sqlFields = ["AlarmId", "Author", "Timestamp"]
        sqlValues = ["%d" % alarmId, author, "UTC_TIMESTAMP()"]
        rawComment = ""
        if "comment" in updateReq:
            rawComment = updateReq["comment"]
            result = self._escapeString(rawComment)
            if not result["OK"]:
                return result
            sqlFields.append("Comment")
            sqlValues.append(result["Value"])
        modifications = False
        if "modifications" in updateReq:
            modifications = updateReq["modifications"]
            result = self.__processUpdateAlarmModifications(modifications)
            if not result["OK"]:
                return result
            alarmModsSQL, encodedMods, newFollowers = result["Value"]
            sqlFields.append("Modifications")
            result = self._escapeString(encodedMods)
            if not result["OK"]:
                return result
            sqlValues.append(result["Value"])
            if newFollowers:
                followers.extend(newFollowers)
        logSQL = f"INSERT INTO `ntf_AlarmLog` ({','.join(sqlFields)}) VALUES ({','.join(sqlValues)})"
        result = self._update(logSQL)
        if not result["OK"]:
            return result
        modSQL = "ModTime=UTC_TIMESTAMP()"
        if modifications:
            modSQL = f"{modSQL}, {alarmModsSQL}"
        updateSQL = "UPDATE `ntf_Alarms` SET %s WHERE AlarmId=%d" % (modSQL, alarmId)
        result = self._update(updateSQL)
        if not result["OK"]:
            return result
        # Get notifications config
        sqlQuery = f"SELECT Notifications FROM `ntf_Alarms` WHERE AlarmId={alarmId}"
        result = self._query(sqlQuery)
        if not result["OK"] or not result["Value"]:
            self.log.error("Could not retrieve default notifications for alarm", f"{alarmId}")
            return S_OK(alarmId)
        notificationsDict = DEncode.decode(result["Value"][0][0])[0]
        for v in self.__validAlarmNotifications:
            if v not in notificationsDict:
                notificationsDict[v] = 0
        for follower in followers:
            result = self.modifyFollowerForAlarm(alarmId, follower, notificationsDict, overwrite=False)
            if not result["OK"]:
                varMsg = f"\nFollower: {follower}\nAlarm: {alarmId}\nError: {result['Message']}"
                self.log.error("Couldn't set follower for alarm", varMsg)
        return self.__notifyAlarm(alarmId)

    def __notifyAlarm(self, alarmId):
        result = self.getSubscribersForAlarm(alarmId)
        if not result["OK"]:
            return result
        subscribers = result["Value"]
        needLongText = False
        if subscribers["mail"]:
            needLongText = True
        result = self.getAlarmInfo(alarmId)
        if not result["OK"]:
            return result
        alarmInfo = result["Value"]
        result = self.getAlarmLog(alarmId)
        if not result["OK"]:
            return result
        alarmLog = result["Value"]
        if subscribers["notification"]:
            msg = self.__generateAlarmInfoMessage(alarmInfo)
            logMsg = self.__generateAlarmLogMessage(alarmLog, True)
            if logMsg:
                msg = f"{msg}\n\n{'*' * 30}\nLast modification:\n{logMsg}"
            for user in subscribers["notification"]:
                self.addNotificationForUser(user, msg, 86400, deferToMail=True)
        if subscribers["mail"]:
            msg = self.__generateAlarmInfoMessage(alarmInfo)
            logMsg = self.__generateAlarmLogMessage(alarmLog)
            if logMsg:
                msg = f"{msg}\n\n{'*' * 30}\nAlarm Log:\n{logMsg}"
                subject = f"Update on alarm {alarmId}"
            else:
                subject = f"New alarm {alarmId}"
            for user in subscribers["mail"]:
                self.__sendMailToUser(user, subject, msg)
        if subscribers["sms"]:
            # TODO
            pass
        return S_OK()

    def __generateAlarmLogMessage(self, alarmLog, showOnlyLast=False):
        if len(alarmLog["Records"]) == 0:
            return ""
        records = alarmLog["Records"]
        if showOnlyLast:
            logToShow = [-1]
        else:
            logToShow = list(range(len(records) - 1, -1, -1))
        finalMessage = []
        for iD in logToShow:
            rec = records[iD]
            data = {}
            for i in range(len(alarmLog["ParameterNames"])):
                if rec[i]:
                    data[alarmLog["ParameterNames"][i]] = rec[i]
            # [ 'timestamp', 'author', 'comment', 'modifications' ]
            msg = [f" Entry by : {data['author']}"]
            msg.append(f" On       : {data['timestamp'].strftime('%Y/%m/%d %H:%M:%S')}")
            if "modifications" in data:
                mods = data["modifications"]
                keys = sorted(mods)
                msg.append(" Modificaitons:")
                for key in keys:
                    msg.append(f"   {key} -> {mods[key]}")
            if "comment" in data:
                msg.append(f" Comment:\n\n{data['comment']}")
            finalMessage.append("\n".join(msg))
        return "\n\n===============\n".join(finalMessage)

    def __generateAlarmInfoMessage(self, alarmInfo):
        # [ 'alarmid', 'author', 'creationtime', 'modtime', 'subject', 'status', 'type', 'body', 'assignee' ]
        msg = " Alarm %6d\n" % alarmInfo["alarmid"]
        msg += f"   Author            : {alarmInfo['author']}\n"
        msg += f"   Subject           : {alarmInfo['subject']}\n"
        msg += f"   Status            : {alarmInfo['status']}\n"
        msg += f"   Priority          : {alarmInfo['priority']}\n"
        msg += f"   Assignee          : {alarmInfo['assignee']}\n"
        msg += f"   Creation date     : {alarmInfo['creationtime'].strftime('%Y/%m/%d %H:%M:%S')} UTC\n"
        msg += f"   Last modificaiton : {alarmInfo['modtime'].strftime('%Y/%m/%d %H:%M:%S')} UTC\n"
        msg += f"   Body:\n\n{alarmInfo['body']}"
        return msg

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

    def getAlarms(self, condDict={}, sortList=False, start=0, limit=0, modifiedAfter=None):
        condSQL = []
        for field in self.__alarmQueryFields:
            if field in condDict:
                fieldValues = []
                rawValue = condDict[field]
                if field == "assignee":
                    expandedValue = []
                    for user in rawValue:
                        result = self.getAssigneeGroupsForUser(user)
                        if not result["OK"]:
                            return result
                        for ag in result["Value"]:
                            if ag not in expandedValue:
                                expandedValue.append(ag)
                    rawValue = expandedValue
                for value in rawValue:
                    result = self._escapeString(value)
                    if not result["OK"]:
                        return result
                    fieldValues.append(result["Value"])
                condSQL.append(f"{field} in ( {','.join(fieldValues)} )")

        selSQL = f"SELECT {','.join(self.__alarmQueryFields)} FROM `ntf_Alarms`"
        if modifiedAfter:
            condSQL.append(f"ModTime >= {modifiedAfter.strftime('%Y-%m-%d %H:%M:%S')}")
        if condSQL:
            selSQL = f"{selSQL} WHERE {' AND '.join(condSQL)}"
        if sortList:
            selSQL += f" ORDER BY {', '.join([f'{sort[0]} {sort[1]}' for sort in sortList])}"
        if limit:
            selSQL += " LIMIT %d,%d" % (start, limit)

        result = self._query(selSQL)
        if not result["OK"]:
            return result

        resultDict = {}
        resultDict["ParameterNames"] = self.__alarmQueryFields
        resultDict["Records"] = [list(v) for v in result["Value"]]
        return S_OK(resultDict)

    def getAlarmInfo(self, alarmId):
        result = self.getAlarms({"alarmId": alarmId})
        if not result["OK"]:
            return result
        alarmInfo = {}
        data = result["Value"]
        if len(data["Records"]) == 0:
            return S_OK({})
        for i in range(len(data["ParameterNames"])):
            alarmInfo[data["ParameterNames"][i]] = data["Records"][0][i]
        return S_OK(alarmInfo)

    def getAlarmLog(self, alarmId):
        try:
            alarmId = int(alarmId)
        except Exception:
            return S_ERROR("Alarm id must be a non decimal number")
        sqlSel = "SELECT %s FROM `ntf_AlarmLog` WHERE AlarmId=%d ORDER BY Timestamp ASC" % (
            ",".join(self.__alarmLogFields),
            alarmId,
        )
        result = self._query(sqlSel)
        if not result["OK"]:
            return result
        decodedRows = []
        for row in result["Value"]:
            decodedRows.append(list(row))
            if not row[3]:
                decodedRows.append(list(row))
                continue
            dec = DEncode.decode(row[3])
            decodedRows[-1][3] = dec[0]

        resultDict = {}
        resultDict["ParameterNames"] = self.__alarmLogFields
        resultDict["Records"] = decodedRows
        return S_OK(resultDict)

    ###
    # Followers management
    ###

    def modifyFollowerForAlarm(self, alarmId, user, notificationsDict, overwrite=True):
        rawUser = user
        if rawUser not in Registry.getAllUsers():
            return S_OK()
        result = self._escapeString(user)
        if not result["OK"]:
            return result
        user = result["Value"]
        subscriber = False
        for k in notificationsDict:
            if notificationsDict[k]:
                subscriber = True
                break
        selSQL = "SELECT Notification, Mail, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d AND User=%s" % (
            alarmId,
            user,
        )
        result = self._query(selSQL)
        if not result["OK"]:
            return result
        if not result["Value"]:
            if not subscriber:
                return S_OK()
            sqlValues = ["%d" % alarmId, user]
            for k in self.__validAlarmNotifications:
                if notificationsDict[k]:
                    sqlValues.append("1")
                else:
                    sqlValues.append("0")
            inSQL = (
                "INSERT INTO `ntf_AlarmFollowers` ( AlarmId, User, Notification, Mail, SMS ) VALUES (%s)"
                % ",".join(sqlValues)
            )
            return self._update(inSQL)
        sqlCond = "AlarmId=%d AND User=%s" % (alarmId, user)
        # Need to delete
        if not subscriber:
            return self._update(f"DELETE FROM `ntf_AlarmFollowers` WHERE {sqlCond}")
        if not overwrite:
            return S_OK()
        # Need to update
        modSQL = []
        for k in self.__validAlarmNotifications:
            if notificationsDict[k]:
                modSQL.append(f"{k}=1")
            else:
                modSQL.append(f"{k}=0")
        return self._update(f"UPDATE `ntf_AlarmFollowers` SET {modSQL} WHERE {sqlCond}")

    def getSubscribersForAlarm(self, alarmId):
        selSQL = "SELECT User, Mail, Notification, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d" % alarmId
        result = self._query(selSQL)
        if not result["OK"]:
            return result
        fw = result["Value"]
        followWays = {"mail": [], "notification": [], "sms": []}
        followers = []
        for user, mail, Notification, SMS in fw:
            if user in followers:
                continue
            followers.append(user)
            if mail:
                followWays["mail"].append(user)
            if Notification:
                followWays["notification"].append(user)
            if SMS:
                followWays["sms"].append(user)
        return S_OK(followWays)

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
