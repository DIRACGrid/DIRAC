""" NotificationDB class is a front-end to the Notifications database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DEncode
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class NotificationDB(DB):

  def __init__(self):
    DB.__init__(self, 'NotificationDB', 'Framework/NotificationDB')
    result = self.__initializeDB()
    if not result['OK']:
      self.log.fatal("Cannot initialize DB!", result['Message'])
    self.__alarmQueryFields = ['alarmid', 'author', 'creationtime', 'modtime', 'subject',
                               'status', 'priority', 'notifications', 'body', 'assignee', 'alarmkey']
    self.__alarmLogFields = ['timestamp', 'author', 'comment', 'modifications']
    self.__notificationQueryFields = ('id', 'user', 'seen', 'message', 'timestamp')
    self.__newAlarmMandatoryFields = ['author', 'subject', 'status', 'notifications', 'body', 'assignee', 'priority']
    self.__updateAlarmIdentificationFields = ['id', 'alarmKey']
    self.__updateAlarmMandatoryFields = ['author']
    self.__updateAlarmAtLeastOneField = ['comment', 'modifications']
    self.__updateAlarmModificableFields = ['status', 'assignee', 'priority']
    self.__validAlarmStatus = ['Open', 'OnGoing', 'Closed', 'Testing']
    self.__validAlarmNotifications = ['Web', 'Mail', 'SMS']
    self.__validAlarmPriorities = ['Low', 'Medium', 'High', 'Extreme']

  def __initializeDB(self):
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesToCreate = {}
    if 'ntf_Alarms' not in tablesInDB:
      tablesToCreate['ntf_Alarms'] = {'Fields': {'AlarmId': 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                 'AlarmKey': 'VARCHAR(32) NOT NULL',
                                                 'Author': 'VARCHAR(64) NOT NULL',
                                                 'CreationTime': 'DATETIME NOT NULL',
                                                 'ModTime': 'DATETIME NOT NULL',
                                                 'Subject': 'VARCHAR(255) NOT NULL',
                                                 'Status': 'VARCHAR(64) NOT NULL',
                                                 'Priority': 'VARCHAR(32) NOT NULL',
                                                 'Body': 'BLOB',
                                                 'Assignee': 'VARCHAR(64) NOT NULL',
                                                 'Notifications': 'VARCHAR(128) NOT NULL'
                                                 },
                                      'PrimaryKey': 'AlarmId',
                                      'Indexes': {'Status': ['Status'],
                                                    'Assignee': ['Assignee']}
                                      }
    if 'ntf_AssigneeGroups' not in tablesInDB:
      tablesToCreate['ntf_AssigneeGroups'] = {'Fields': {'AssigneeGroup': 'VARCHAR(64) NOT NULL',
                                                         'User': 'VARCHAR(64) NOT NULL',
                                                         },
                                              'Indexes': {'ag': ['AssigneeGroup']}
                                              }

    if 'ntf_AlarmLog' not in tablesInDB:
      tablesToCreate['ntf_AlarmLog'] = {'Fields': {'AlarmId': 'INTEGER UNSIGNED NOT NULL',
                                                   'Timestamp': 'DATETIME NOT NULL',
                                                   'Author': 'VARCHAR(64) NOT NULL',
                                                   'Comment': 'BLOB',
                                                   'Modifications': 'VARCHAR(255)',
                                                   },
                                        'Indexes': {'AlarmID': ['AlarmId']}
                                        }

    if 'ntf_AlarmFollowers' not in tablesInDB:
      tablesToCreate['ntf_AlarmFollowers'] = {'Fields': {'AlarmId': 'INTEGER UNSIGNED NOT NULL',
                                                         'User': 'VARCHAR(64) NOT NULL',
                                                         'Mail': 'TINYINT(1) DEFAULT 0',
                                                         'Notification': 'TINYINT(1) DEFAULT 1',
                                                         'SMS': 'TINYINT(1) DEFAULT 0',
                                                         },
                                              'Indexes': {'AlarmID': ['AlarmId']}
                                              }

    if 'ntf_Notifications' not in tablesInDB:
      tablesToCreate['ntf_Notifications'] = {'Fields': {'Id': 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                        'User': 'VARCHAR(64) NOT NULL',
                                                        'Message': 'BLOB NOT NULL',
                                                        'Seen': 'TINYINT(1) NOT NULL DEFAULT 0',
                                                        'Expiration': 'DATETIME',
                                                        'Timestamp': 'DATETIME',
                                                        'DeferToMail': 'TINYINT(1) NOT NULL DEFAULT 1',
                                                        },
                                             'PrimaryKey': 'Id',
                                             }

    if tablesToCreate:
      return self._createTables(tablesToCreate)
    return S_OK()

  def __checkAlarmField(self, name, value):
    name = name.lower()
    if name == 'status':
      if value not in self.__validAlarmStatus:
        return S_ERROR("Status %s is invalid. Valid ones are: %s" % (value, self.__validAlarmStatus))
    elif name == 'priority':
      if value not in self.__validAlarmPriorities:
        return S_ERROR("Type %s is invalid. Valid ones are: %s" % (value, self.__validAlarmPriorities))
    elif name == 'assignee':
      result = self.getUserAsignees(value)
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR("%s is not a known assignee" % value)
      return result
    return S_OK()

  def newAlarm(self, alarmDef):
    """ Create a new alarm record
    """
    followers = []
    for field in self.__newAlarmMandatoryFields:
      if field not in alarmDef:
        return S_ERROR("Oops. Missing %s" % field)
      result = self.__checkAlarmField(field, alarmDef[field])
      if not result['OK']:
        return result
      if field == 'assignee':
        followers = result['Value']
    author = alarmDef['author']
    if author not in followers:
      followers.append(author)

    sqlFieldsName = []
    sqlFieldsValue = []
    for field in self.__newAlarmMandatoryFields:
      if field == 'notifications':
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
      if result['OK']:
        sqlFieldsValue.append(result['Value'])
      else:
        return S_ERROR('Failed to escape value %s' % val)
    sqlFieldsName.extend(['CreationTime', 'ModTime'])
    sqlFieldsValue.extend(['UTC_TIMESTAMP()', 'UTC_TIMESTAMP()'])

    # Get the defined alarmkey and generate a random one if not defined
    if 'alarmKey' in alarmDef:
      result = self._escapeString(alarmDef['alarmKey'])
      if result['OK']:
        alarmKey = result['Value']
      else:
        return S_ERROR('Failed to escape value %s for key AlarmKey' % val)
      gLogger.info("Checking there are no alarms with key %s" % alarmKey)
      result = self._query("SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmKey=%s" % alarmKey)
      if not result['OK']:
        return result
      if result['Value']:
        return S_ERROR("Oops, alarm with id %s has the same alarm key!" % result['Value'][0][0])
    else:
      alarmKey = str(time.time())[-31:]
    sqlFieldsName.append('AlarmKey')
    sqlFieldsValue.append(alarmKey)

    sqlInsert = "INSERT INTO `ntf_Alarms` (%s) VALUES (%s)" % (",".join(sqlFieldsName),
                                                               ",".join(sqlFieldsValue))

    result = self._update(sqlInsert)
    if not result['OK']:
      return result
    alarmId = result['lastRowId']
    for follower in followers:
      result = self.modifyFollowerForAlarm(alarmId, follower, notifications)
      if not result['OK']:
        varMsg = "\nFollower: %s\nAlarm: %s\nError: %s" % (follower, alarmId, result['Message'])
        self.log.error("Couldn't set follower for alarm", varMsg)
    self.__notifyAlarm(alarmId)
    return S_OK(alarmId)

  def deleteAlarmsByAlarmKey(self, alarmKeyList):
    alarmsIdList = []
    for alarmKey in alarmKeyList:
      result = self.__getAlarmIdFromKey(alarmKey)
      if not result['OK']:
        return result
      alarmId = result['Value']
      alarmsIdList.append(alarmId)
    self.log.info("Trying to delete alarms with:\n alamKey %s\n  alarmId %s" % (alarmKeyList, alarmsIdList))
    return self.deleteAlarmsByAlarmId(alarmsIdList)

  def deleteAlarmsByAlarmId(self, alarmIdList):
    self.log.info("Trying to delete alarms with ids %s" % alarmIdList)
    try:
      alarmId = int(alarmIdList)
      alarmIdList = [alarmId]
    except BaseException:
      pass

    try:
      alarmIdList = [int(alarmId) for alarmId in alarmIdList]
    except BaseException:
      self.log.error("At least one alarmId is not a number", str(alarmIdList))
      return S_ERROR("At least one alarmId is not a number: %s" % str(alarmIdList))

    tablesToCheck = ("ntf_AlarmLog", "ntf_AlarmFollowers", "ntf_Alarms")
    alamsSQLList = ",".join(["%d" % alarmId for alarmId in alarmIdList])
    for tableName in tablesToCheck:
      delSql = "DELETE FROM `%s` WHERE AlarmId in ( %s )" % (tableName, alamsSQLList)
      result = self._update(delSql)
      if not result['OK']:
        self.log.error("Could not delete alarm", "from table %s: %s" % (tableName, result['Message']))
    return S_OK()

  def __processUpdateAlarmModifications(self, modifications):
    if not isinstance(modifications, dict):
      return S_ERROR("Modifications must be a dictionary")
    updateFields = []
    followers = []
    for field in modifications:
      if field not in self.__updateAlarmModificableFields:
        return S_ERROR("%s is not a valid modificable field" % field)
      value = modifications[field]
      result = self.__checkAlarmField(field, value)
      if not result['OK']:
        return result
      if field == 'assignee':
        followers = result['Value']
      result = self._escapeString(modifications[field])
      if not result['OK']:
        return result
      updateFields.append("%s=%s" % (field, result['Value']))
    return S_OK((", ".join(updateFields), DEncode.encode(modifications), followers))

  def __getAlarmIdFromKey(self, alarmKey):
    result = self._escapeString(alarmKey)
    if not result['OK']:
      return S_ERROR("Cannot escape alarmKey %s" % alarmKey)
    alarmKey = result['Value']
    sqlQuery = "SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmKey=%s" % alarmKey
    result = self._query(sqlQuery)
    if result['OK']:
      result['Value'] = result['Value'][0][0]
    return result

  def updateAlarm(self, updateReq):
    # Discover alarm identification
    idOK = False
    for field in self.__updateAlarmIdentificationFields:
      if field in updateReq:
        idOK = True
    if not idOK:
      return S_ERROR(
          "Need at least one field to identify which alarm to update! %s" %
          self.__updateAlarmIdentificationFields)
    if 'alarmKey' in updateReq:
      alarmKey = updateReq['alarmKey']
      result = self.__getAlarmIdFromKey(alarmKey)
      if not result['OK']:
        self.log.error("Could not get alarm id for key", " %s: %s" % (alarmKey, result['Value']))
        return result
      updateReq['id'] = result['Value']
      self.log.info("Retrieving alarm key %s maps to id %s" % (alarmKey, updateReq['id']))
    # Check fields
    for field in self.__updateAlarmMandatoryFields:
      if field not in updateReq:
        return S_ERROR("Oops. Missing %s" % field)
    validReq = False
    for field in self.__updateAlarmAtLeastOneField:
      if field in updateReq:
        validReq = True
    if not validReq:
      return S_OK("Requirement needs at least one of %s" % " ".join(self.__updateAlarmAtLeastOneField))
    author = updateReq['author']
    followers = [author]
    if author not in Registry.getAllUsers():
      return S_ERROR("%s is not a known user" % author)
    result = self._escapeString(author)
    if not result['OK']:
      return result
    author = result['Value']
    try:
      alarmId = int(updateReq['id'])
    except BaseException:
      return S_ERROR("Oops, Alarm id is not valid!")
    result = self._query("SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmId=%d" % alarmId)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR("Alarm %s does not exist!" % alarmId)
    sqlFields = ['AlarmId', 'Author', 'Timestamp']
    sqlValues = ["%d" % alarmId, author, 'UTC_TIMESTAMP()']
    rawComment = ""
    if 'comment' in updateReq:
      rawComment = updateReq['comment']
      result = self._escapeString(rawComment)
      if not result['OK']:
        return result
      sqlFields.append("Comment")
      sqlValues.append(result['Value'])
    modifications = False
    if 'modifications' in updateReq:
      modifications = updateReq['modifications']
      result = self.__processUpdateAlarmModifications(modifications)
      if not result['OK']:
        return result
      alarmModsSQL, encodedMods, newFollowers = result['Value']
      sqlFields.append("Modifications")
      result = self._escapeString(encodedMods)
      if not result['OK']:
        return result
      sqlValues.append(result['Value'])
      if newFollowers:
        followers.extend(newFollowers)
    logSQL = "INSERT INTO `ntf_AlarmLog` (%s) VALUES (%s)" % (",".join(sqlFields), ",".join(sqlValues))
    result = self._update(logSQL)
    if not result['OK']:
      return result
    modSQL = "ModTime=UTC_TIMESTAMP()"
    if modifications:
      modSQL = "%s, %s" % (modSQL, alarmModsSQL)
    updateSQL = "UPDATE `ntf_Alarms` SET %s WHERE AlarmId=%d" % (modSQL, alarmId)
    result = self._update(updateSQL)
    if not result['OK']:
      return result
    # Get notifications config
    sqlQuery = "SELECT Notifications FROM `ntf_Alarms` WHERE AlarmId=%s" % alarmId
    result = self._query(sqlQuery)
    if not result['OK'] or not result['Value']:
      self.log.error("Could not retrieve default notifications for alarm", "%s" % alarmId)
      return S_OK(alarmId)
    notificationsDict = DEncode.decode(result['Value'][0][0])[0]
    for v in self.__validAlarmNotifications:
      if v not in notificationsDict:
        notificationsDict[v] = 0
    for follower in followers:
      result = self.modifyFollowerForAlarm(alarmId, follower, notificationsDict, overwrite=False)
      if not result['OK']:
        varMsg = "\nFollower: %s\nAlarm: %s\nError: %s" % (follower, alarmId, result['Message'])
        self.log.error("Couldn't set follower for alarm", varMsg)
    return self.__notifyAlarm(alarmId)

  def __notifyAlarm(self, alarmId):
    result = self.getSubscribersForAlarm(alarmId)
    if not result['OK']:
      return result
    subscribers = result['Value']
    needLongText = False
    if subscribers['mail']:
      needLongText = True
    result = self.getAlarmInfo(alarmId)
    if not result['OK']:
      return result
    alarmInfo = result['Value']
    result = self.getAlarmLog(alarmId)
    if not result['OK']:
      return result
    alarmLog = result['Value']
    if subscribers['notification']:
      msg = self.__generateAlarmInfoMessage(alarmInfo)
      logMsg = self.__generateAlarmLogMessage(alarmLog, True)
      if logMsg:
        msg = "%s\n\n%s\nLast modification:\n%s" % (msg, "*" * 30, logMsg)
      for user in subscribers['notification']:
        self.addNotificationForUser(user, msg, 86400, deferToMail=True)
    if subscribers['mail']:
      msg = self.__generateAlarmInfoMessage(alarmInfo)
      logMsg = self.__generateAlarmLogMessage(alarmLog)
      if logMsg:
        msg = "%s\n\n%s\nAlarm Log:\n%s" % (msg, "*" * 30, logMsg)
        subject = "Update on alarm %s" % alarmId
      else:
        subject = "New alarm %s" % alarmId
      for user in subscribers['mail']:
        self.__sendMailToUser(user, subject, msg)
    if subscribers['sms']:
      # TODO
      pass
    return S_OK()

  def __generateAlarmLogMessage(self, alarmLog, showOnlyLast=False):
    if len(alarmLog['Records']) == 0:
      return ""
    records = alarmLog['Records']
    if showOnlyLast:
      logToShow = [-1]
    else:
      logToShow = list(range(len(records) - 1, -1, -1))
    finalMessage = []
    for iD in logToShow:
      rec = records[iD]
      data = {}
      for i in range(len(alarmLog['ParameterNames'])):
        if rec[i]:
          data[alarmLog['ParameterNames'][i]] = rec[i]
      #[ 'timestamp', 'author', 'comment', 'modifications' ]
      msg = [" Entry by : %s" % data['author']]
      msg.append(" On       : %s" % data['timestamp'].strftime("%Y/%m/%d %H:%M:%S"))
      if 'modifications' in data:
        mods = data['modifications']
        keys = sorted(mods)
        msg.append(" Modificaitons:")
        for key in keys:
          msg.append("   %s -> %s" % (key, mods[key]))
      if 'comment' in data:
        msg.append(" Comment:\n\n%s" % data['comment'])
      finalMessage.append("\n".join(msg))
    return "\n\n===============\n".join(finalMessage)

  def __generateAlarmInfoMessage(self, alarmInfo):
    #[ 'alarmid', 'author', 'creationtime', 'modtime', 'subject', 'status', 'type', 'body', 'assignee' ]
    msg = " Alarm %6d\n" % alarmInfo['alarmid']
    msg += "   Author            : %s\n" % alarmInfo['author']
    msg += "   Subject           : %s\n" % alarmInfo['subject']
    msg += "   Status            : %s\n" % alarmInfo['status']
    msg += "   Priority          : %s\n" % alarmInfo['priority']
    msg += "   Assignee          : %s\n" % alarmInfo['assignee']
    msg += "   Creation date     : %s UTC\n" % alarmInfo['creationtime'].strftime("%Y/%m/%d %H:%M:%S")
    msg += "   Last modificaiton : %s UTC\n" % alarmInfo['modtime'].strftime("%Y/%m/%d %H:%M:%S")
    msg += "   Body:\n\n%s" % alarmInfo['body']
    return msg

  def __sendMailToUser(self, user, subject, message):
    address = gConfig.getValue("/Registry/Users/%s/Email" % user, "")
    if not address:
      self.log.error("User does not have an email registered", user)
      return S_ERROR("User %s does not have an email registered" % user)
    self.log.info("Sending mail (%s) to user %s at %s" % (subject, user, address))
    m = Mail()
    m._subject = "[DIRAC] %s" % subject
    m._message = message
    m._mailAddress = address
    result = m._send()
    if not result['OK']:
      gLogger.warn('Could not send mail with the following message:\n%s' % result['Message'])

    return result

  def getAlarms(self, condDict={}, sortList=False, start=0, limit=0, modifiedAfter=None):

    condSQL = []
    for field in self.__alarmQueryFields:
      if field in condDict:
        fieldValues = []
        rawValue = condDict[field]
        if field == 'assignee':
          expandedValue = []
          for user in rawValue:
            result = self.getAssigneeGroupsForUser(user)
            if not result['OK']:
              return result
            for ag in result['Value']:
              if ag not in expandedValue:
                expandedValue.append(ag)
          rawValue = expandedValue
        for value in rawValue:
          result = self._escapeString(value)
          if not result['OK']:
            return result
          fieldValues.append(result['Value'])
        condSQL.append("%s in ( %s )" % (field, ",".join(fieldValues)))

    selSQL = "SELECT %s FROM `ntf_Alarms`" % ",".join(self.__alarmQueryFields)
    if modifiedAfter:
      condSQL.append("ModTime >= %s" % modifiedAfter.strftime("%Y-%m-%d %H:%M:%S"))
    if condSQL:
      selSQL = "%s WHERE %s" % (selSQL, " AND ".join(condSQL))
    if sortList:
      selSQL += " ORDER BY %s" % ", ".join(["%s %s" % (sort[0], sort[1]) for sort in sortList])
    if limit:
      selSQL += " LIMIT %d,%d" % (start, limit)

    result = self._query(selSQL)
    if not result['OK']:
      return result

    resultDict = {}
    resultDict['ParameterNames'] = self.__alarmQueryFields
    resultDict['Records'] = [list(v) for v in result['Value']]
    return S_OK(resultDict)

  def getAlarmInfo(self, alarmId):
    result = self.getAlarms({'alarmId': alarmId})
    if not result['OK']:
      return result
    alarmInfo = {}
    data = result['Value']
    if len(data['Records']) == 0:
      return S_OK({})
    for i in range(len(data['ParameterNames'])):
      alarmInfo[data['ParameterNames'][i]] = data['Records'][0][i]
    return S_OK(alarmInfo)

  def getAlarmLog(self, alarmId):
    try:
      alarmId = int(alarmId)
    except BaseException:
      return S_ERROR("Alarm id must be a non decimal number")
    sqlSel = "SELECT %s FROM `ntf_AlarmLog` WHERE AlarmId=%d ORDER BY Timestamp ASC" % (",".join(self.__alarmLogFields),
                                                                                        alarmId)
    result = self._query(sqlSel)
    if not result['OK']:
      return result
    decodedRows = []
    for row in result['Value']:
      decodedRows.append(list(row))
      if not row[3]:
        decodedRows.append(list(row))
        continue
      dec = DEncode.decode(row[3])
      decodedRows[-1][3] = dec[0]

    resultDict = {}
    resultDict['ParameterNames'] = self.__alarmLogFields
    resultDict['Records'] = decodedRows
    return S_OK(resultDict)

###
# Followers management
###

  def modifyFollowerForAlarm(self, alarmId, user, notificationsDict, overwrite=True):
    rawUser = user
    if rawUser not in Registry.getAllUsers():
      return S_OK()
    result = self._escapeString(user)
    if not result['OK']:
      return result
    user = result['Value']
    subscriber = False
    for k in notificationsDict:
      if notificationsDict[k]:
        subscriber = True
        break
    selSQL = "SELECT Notification, Mail, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d AND User=%s" % (alarmId, user)
    result = self._query(selSQL)
    if not result['OK']:
      return result
    if not result['Value']:
      if not subscriber:
        return S_OK()
      sqlValues = ["%d" % alarmId, user]
      for k in self.__validAlarmNotifications:
        if notificationsDict[k]:
          sqlValues.append("1")
        else:
          sqlValues.append("0")
      inSQL = "INSERT INTO `ntf_AlarmFollowers` ( AlarmId, User, Notification, Mail, SMS ) VALUES (%s)" % ",".join(
          sqlValues)
      return self._update(inSQL)
    sqlCond = "AlarmId=%d AND User=%s" % (alarmId, user)
    # Need to delete
    if not subscriber:
      return self._update("DELETE FROM `ntf_AlarmFollowers` WHERE %s" % sqlCond)
    if not overwrite:
      return S_OK()
    # Need to update
    modSQL = []
    for k in self.__validAlarmNotifications:
      if notificationsDict[k]:
        modSQL.append("%s=1" % k)
      else:
        modSQL.append("%s=0" % k)
    return self._update("UPDATE `ntf_AlarmFollowers` SET %s WHERE %s" % (modSQL, sqlCond))

  def getSubscribersForAlarm(self, alarmId):
    selSQL = "SELECT User, Mail, Notification, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d" % alarmId
    result = self._query(selSQL)
    if not result['OK']:
      return result
    fw = result['Value']
    followWays = {'mail': [], 'notification': [], 'sms': []}
    followers = []
    for user, mail, Notification, SMS in fw:
      if user in followers:
        continue
      followers.append(user)
      if mail:
        followWays['mail'].append(user)
      if Notification:
        followWays['notification'].append(user)
      if SMS:
        followWays['sms'].append(user)
    return S_OK(followWays)

###
# Assignee groups management
###

  def getUserAsignees(self, assignee):
    # Check if it is a user
    if assignee in Registry.getAllUsers():
      return S_OK([assignee])
    result = self._escapeString(assignee)
    if not result['OK']:
      return result
    escAG = result['Value']
    sqlSel = "SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = %s" % escAG
    result = self._query(sqlSel)
    if not result['OK']:
      return result
    users = [row[0] for row in result['Value']]
    if not users:
      return S_OK([])
    return S_OK(users)

  def setAssigneeGroup(self, groupName, usersList):
    validUsers = Registry.getAllUsers()
    result = self._escapeString(groupName)
    if not result['OK']:
      return result
    escGroup = result['Value']
    sqlSel = "SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = %s" % escGroup
    result = self._query(sqlSel)
    if not result['OK']:
      return result
    currentUsers = [row[0] for row in result['Value']]
    usersToDelete = []
    usersToAdd = []
    finalUsersInGroup = len(currentUsers)
    for user in currentUsers:
      if user not in usersList:
        result = self._escapeString(user)
        if not result['OK']:
          return result
        usersToDelete.append(result['Value'])
        finalUsersInGroup -= 1
    for user in usersList:
      if user not in validUsers:
        continue
      if user not in currentUsers:
        result = self._escapeString(user)
        if not result['OK']:
          return result
        usersToAdd.append("( %s, %s )" % (escGroup, result['Value']))
        finalUsersInGroup += 1
    if not finalUsersInGroup:
      return S_ERROR("Group must have at least one user!")
    # Delete old users
    if usersToDelete:
      sqlDel = "DELETE FROM `ntf_AssigneeGroups` WHERE User in ( %s )" % ",".join(usersToDelete)
      result = self._update(sqlDel)
      if not result['OK']:
        return result
    # Add new users
    if usersToAdd:
      sqlInsert = "INSERT INTO `ntf_AssigneeGroups` ( AssigneeGroup, User ) VALUES %s" % ",".join(usersToAdd)
      result = self._update(sqlInsert)
      if not result['OK']:
        return result
    return S_OK()

  def deleteAssigneeGroup(self, groupName):
    result = self._escapeString(groupName)
    if not result['OK']:
      return result
    escGroup = result['Value']
    sqlSel = "SELECT AlarmId FROM `ntf_Alarms` WHERE Assignee=%s" % escGroup
    result = self._query(sqlSel)
    if not result['OK']:
      return result
    if result['Value']:
      alarmIds = [row[0] for row in result['Value']]
      return S_ERROR("There are %s alarms assigned to this group" % len(alarmIds))
    sqlDel = "DELETE FROM `ntf_AssigneeGroups` WHERE AssigneeGroup=%s" % escGroup
    return self._update(sqlDel)

  def getAssigneeGroups(self):
    result = self._query("SELECT AssigneeGroup, User from `ntf_AssigneeGroups` ORDER BY User")
    if not result['OK']:
      return result
    agDict = {}
    for row in result['Value']:
      ag = row[0]
      user = row[1]
      if ag not in agDict:
        agDict[ag] = []
      agDict[ag].append(user)
    return S_OK(agDict)

  def getAssigneeGroupsForUser(self, user):
    if user not in Registry.getAllUsers():
      return S_ERROR("%s is an unknown user" % user)
    result = self._escapeString(user)
    if not result['OK']:
      return result
    user = result['Value']
    result = self._query("SELECT AssigneeGroup from `ntf_AssigneeGroups` WHERE User=%s" % user)
    if not result['OK']:
      return result
    return S_OK([row[0] for row in result['Value']])

###
# Notifications
###

  def addNotificationForUser(self, user, message, lifetime=0, deferToMail=1):
    if user not in Registry.getAllUsers():
      return S_ERROR("%s is an unknown user" % user)
    self.log.info("Adding a notification for user %s (msg is %s chars)" % (user, len(message)))
    result = self._escapeString(user)
    if not result['OK']:
      return result
    user = result['Value']
    result = self._escapeString(message)
    if not result['OK']:
      return result
    message = result['Value']
    sqlFields = ['User', 'Message', 'Timestamp']
    sqlValues = [user, message, 'UTC_TIMESTAMP()']
    if not deferToMail:
      sqlFields.append("DeferToMail")
      sqlValues.append("0")
    if lifetime:
      sqlFields.append("Expiration")
      sqlValues.append("TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % int(lifetime))
    sqlInsert = "INSERT INTO `ntf_Notifications` (%s) VALUES (%s) " % (",".join(sqlFields),
                                                                       ",".join(sqlValues))
    result = self._update(sqlInsert)
    if not result['OK']:
      return result
    return S_OK(result['lastRowId'])

  def removeNotificationsForUser(self, user, msgIds=False):
    if user not in Registry.getAllUsers():
      return S_ERROR("%s is an unknown user" % user)
    result = self._escapeString(user)
    if not result['OK']:
      return result
    user = result['Value']
    delSQL = "DELETE FROM `ntf_Notifications` WHERE User=%s" % user
    escapedIDs = []
    if msgIds:
      for iD in msgIds:
        result = self._escapeString(str(iD))
        if not result['OK']:
          return result
        escapedIDs.append(result['Value'])
      delSQL = "%s AND Id in ( %s ) " % (delSQL, ",".join(escapedIDs))
    return self._update(delSQL)

  def markNotificationsSeen(self, user, seen=True, msgIds=False):
    if user not in Registry.getAllUsers():
      return S_ERROR("%s is an unknown user" % user)
    result = self._escapeString(user)
    if not result['OK']:
      return result
    user = result['Value']
    if seen:
      seen = 1
    else:
      seen = 0
    updateSQL = "UPDATE `ntf_Notifications` SET Seen=%d WHERE User=%s" % (seen, user)
    escapedIDs = []
    if msgIds:
      for iD in msgIds:
        result = self._escapeString(str(iD))
        if not result['OK']:
          return result
        escapedIDs.append(result['Value'])
      updateSQL = "%s AND Id in ( %s ) " % (updateSQL, ",".join(escapedIDs))
    return self._update(updateSQL)

  def getNotifications(self, condDict={}, sortList=False, start=0, limit=0):

    condSQL = []
    for field in self.__notificationQueryFields:
      if field in condDict:
        fieldValues = []
        for value in condDict[field]:
          result = self._escapeString(value)
          if not result['OK']:
            return result
          fieldValues.append(result['Value'])
        condSQL.append("%s in ( %s )" % (field, ",".join(fieldValues)))

    eSortList = []
    for field, order in sortList:
      if order.lower() in ['asc', 'desc']:
        eSortList.append(('`%s`' % field.replace('`', ''), order))

    selSQL = "SELECT %s FROM `ntf_Notifications`" % ",".join(self.__notificationQueryFields)
    if condSQL:
      selSQL = "%s WHERE %s" % (selSQL, " AND ".join(condSQL))
    if eSortList:
      selSQL += " ORDER BY %s" % ", ".join(["%s %s" % (sort[0], sort[1]) for sort in eSortList])
    else:
      selSQL += " ORDER BY Id DESC"
    if limit:
      selSQL += " LIMIT %d,%d" % (start, limit)

    result = self._query(selSQL)
    if not result['OK']:
      return result

    resultDict = {}
    resultDict['ParameterNames'] = self.__notificationQueryFields
    resultDict['Records'] = [list(v) for v in result['Value']]
    return S_OK(resultDict)

  def purgeExpiredNotifications(self):
    self.log.info("Purging expired notifications")
    delConds = ['(Seen=1 OR DeferToMail=0)', '(TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), Expiration ) < 0 )']
    delSQL = "DELETE FROM `ntf_Notifications` WHERE %s" % " AND ".join(delConds)
    result = self._update(delSQL)
    if not result['OK']:
      return result
    self.log.info("Purged %s notifications" % result['Value'])
    deferCond = ['Seen=0', 'DeferToMail=1', 'TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), Expiration ) < 0']
    selSQL = "SELECT Id, User, Message FROM `ntf_Notifications` WHERE %s" % " AND ".join(deferCond)
    result = self._query(selSQL)
    if not result['OK']:
      return result
    messages = result['Value']
    if not messages:
      return S_OK()
    ids = []
    for msg in messages:
      self.__sendMailToUser(msg[1], 'Notification defered to mail', msg[2])
      ids.append(str(msg[0]))
    self.log.info("Deferred %s notifications" % len(ids))
    return self._update("DELETE FROM `ntf_Notifications` WHERE Id in (%s)" % ",".join(ids))
