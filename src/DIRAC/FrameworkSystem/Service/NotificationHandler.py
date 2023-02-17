""" The Notification service provides a toolkit to contact people via email
    (eventually SMS etc.) to trigger some actions.

    The original motivation for this is due to some sites restricting the
    sending of email but it is useful for e.g. crash reports to get to their
    destination.

    Another use-case is for users to request an email notification for the
    completion of their jobs.  When output data files are uploaded to the
    Grid, an email could be sent by default with the metadata of the file.

    It can also be used to set alarms to be promptly forwarded to those
    subscribing to them.
"""
from DIRAC import gConfig, S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.DB.NotificationDB import NotificationDB
from DIRAC.Core.Utilities.DictCache import DictCache


class NotificationHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """Handler initialization"""
        cls.notDB = NotificationDB()
        cls.mailCache = DictCache()
        gThreadScheduler.addPeriodicTask(3600, cls.notDB.purgeExpiredNotifications)
        gThreadScheduler.addPeriodicTask(3600, cls.mailCache.purgeExpired())
        return S_OK()

    def initialize(self):
        credDict = self.getRemoteCredentials()
        self.clientDN = credDict["DN"]
        self.clientGroup = credDict["group"]
        self.clientProperties = credDict["properties"]
        self.client = credDict["username"]

    ###########################################################################
    types_sendMail = [str, str, str, str]

    def export_sendMail(self, address, subject, body, fromAddress):
        """Send an email with supplied body to the specified address using the Mail utility.

        :param str address: recipient addresses
        :param str subject: subject of letter
        :param str body: body of letter
        :param str fromAddress: sender address, if "", will be used default from CS

        :return: S_OK(str)/S_ERROR() -- str is status message
        """
        self.log.verbose(f"Received signal to send the following mail to {address}:\nSubject = {subject}\n{body}")
        if self.mailCache.exists(hash(address + subject + body)):
            return S_OK("Email with the same content already sent today to current addresses, come back tomorrow")
        eMail = Mail()
        notificationSection = PathFinder.getServiceSection("Framework/Notification")
        csSection = notificationSection + "/SMTP"
        eMail._smtpHost = gConfig.getValue(f"{csSection}/Host")
        eMail._smtpPort = gConfig.getValue(f"{csSection}/Port")
        eMail._smtpLogin = gConfig.getValue(f"{csSection}/Login")
        eMail._smtpPasswd = gConfig.getValue(f"{csSection}/Password")
        eMail._smtpPtcl = gConfig.getValue(f"{csSection}/Protocol")
        eMail._subject = subject
        eMail._message = body
        eMail._mailAddress = address
        if fromAddress:
            eMail._fromAddress = fromAddress
        eMail._fromAddress = gConfig.getValue(f"{csSection}/FromAddress") or eMail._fromAddress
        result = eMail._send()
        if not result["OK"]:
            self.log.warn(f"Could not send mail with the following message:\n{result['Message']}")
        else:
            self.mailCache.add(hash(address + subject + body), 3600 * 24)
            self.log.info(f"Mail sent successfully to {address} with subject {subject}")
            self.log.debug(result["Value"])

        return result

    ###########################################################################
    types_sendSMS = [str, str, str]

    def export_sendSMS(self, userName, body, fromAddress):
        """Send an SMS with supplied body to the specified DIRAC user using the Mail utility via an SMS switch.

        :param str userName: user name
        :param str body: message
        :param str fromAddress: sender address

        :return: S_OK()/S_ERROR()
        """
        self.log.verbose(f"Received signal to send the following SMS to {userName}:\n{body}")
        mobile = gConfig.getValue(f"/Registry/Users/{userName}/Mobile", "")
        if not mobile:
            return S_ERROR(f"No registered mobile number for {userName}")

        csSection = PathFinder.getServiceSection("Framework/Notification")
        smsSwitch = gConfig.getValue(f"{csSection}/SMSSwitch", "")
        if not smsSwitch:
            return S_ERROR(f"No SMS switch is defined in CS path {csSection}/SMSSwitch")

        address = f"{mobile}@{smsSwitch}"
        subject = "DIRAC SMS"
        eMail = Mail()
        eMail._subject = subject
        eMail._message = body
        eMail._mailAddress = address
        if fromAddress:
            eMail._fromAddress = fromAddress
        result = eMail._send()
        if not result["OK"]:
            self.log.warn(f"Could not send SMS to {userName} with the following message:\n{result['Message']}")
        else:
            self.log.info(f"SMS sent successfully to {userName} ")
            self.log.debug(result["Value"])

        return result

    ###########################################################################
    # ALARMS
    ###########################################################################

    types_newAlarm = [dict]

    def export_newAlarm(self, alarmDefinition):
        """Set a new alarm in the Notification database"""
        credDict = self.getRemoteCredentials()
        if "username" not in credDict:
            return S_ERROR("OOps. You don't have a username! This shouldn't happen :P")
        alarmDefinition["author"] = credDict["username"]
        return self.notDB.newAlarm(alarmDefinition)

    types_updateAlarm = [dict]

    def export_updateAlarm(self, updateDefinition):
        """update an existing alarm in the Notification database"""
        credDict = self.getRemoteCredentials()
        if "username" not in credDict:
            return S_ERROR("OOps. You don't have a username! This shouldn't happen :P")
        updateDefinition["author"] = credDict["username"]
        return self.notDB.updateAlarm(updateDefinition)

    types_getAlarmInfo = [int]

    @classmethod
    def export_getAlarmInfo(cls, alarmId):
        """Get the extended info of an alarm"""
        result = cls.notDB.getAlarmInfo(alarmId)
        if not result["OK"]:
            return result
        alarmInfo = result["Value"]
        result = cls.notDB.getAlarmLog(alarmId)
        if not result["OK"]:
            return result
        return S_OK({"info": alarmInfo, "log": result["Value"]})

    types_getAlarms = [dict, list, int, int]

    @classmethod
    def export_getAlarms(cls, selectDict, sortList, startItem, maxItems):
        """Select existing alarms suitable for the Web monitoring"""
        return cls.notDB.getAlarms(selectDict, sortList, startItem, maxItems)

    types_deleteAlarmsByAlarmId = [(list, int)]

    @classmethod
    def export_deleteAlarmsByAlarmId(cls, alarmsIdList):
        """Delete alarms by alarmId"""
        return cls.notDB.deleteAlarmsByAlarmId(alarmsIdList)

    types_deleteAlarmsByAlarmKey = [(str, list)]

    @classmethod
    def export_deleteAlarmsByAlarmKey(cls, alarmsKeyList):
        """Delete alarms by alarmId"""
        return cls.notDB.deleteAlarmsByAlarmKey(alarmsKeyList)

    ###########################################################################
    # MANANGE ASSIGNEE GROUPS
    ###########################################################################

    types_setAssigneeGroup = [str, list]

    @classmethod
    def export_setAssigneeGroup(cls, groupName, userList):
        """Create a group of users to be used as an assignee for an alarm"""
        return cls.notDB.setAssigneeGroup(groupName, userList)

    types_getUsersInAssigneeGroup = [str]

    @classmethod
    def export_getUsersInAssigneeGroup(cls, groupName):
        """Get users in assignee group"""
        return cls.notDB.getUserAsignees(groupName)

    types_deleteAssigneeGroup = [str]

    @classmethod
    def export_deleteAssigneeGroup(cls, groupName):
        """Delete an assignee group"""
        return cls.notDB.deleteAssigneeGroup(groupName)

    types_getAssigneeGroups = []

    @classmethod
    def export_getAssigneeGroups(cls):
        """Get all assignee groups and the users that belong to them"""
        return cls.notDB.getAssigneeGroups()

    types_getAssigneeGroupsForUser = [str]

    def export_getAssigneeGroupsForUser(self, user):
        """Get all assignee groups and the users that belong to them"""
        credDict = self.getRemoteCredentials()
        if Properties.ALARMS_MANAGEMENT not in credDict["properties"]:
            user = credDict["username"]
        return self.notDB.getAssigneeGroupsForUser(user)

    ###########################################################################
    # MANAGE NOTIFICATIONS
    ###########################################################################

    types_addNotificationForUser = [str, str]

    def export_addNotificationForUser(self, user, message, lifetime=604800, deferToMail=True):
        """Create a group of users to be used as an assignee for an alarm"""
        try:
            lifetime = int(lifetime)
        except Exception:
            return S_ERROR("Message lifetime has to be a non decimal number")
        return self.notDB.addNotificationForUser(user, message, lifetime, deferToMail)

    types_removeNotificationsForUser = [str, list]

    def export_removeNotificationsForUser(self, user, notIds):
        """Get users in assignee group"""
        credDict = self.getRemoteCredentials()
        if Properties.ALARMS_MANAGEMENT not in credDict["properties"]:
            user = credDict["username"]
        return self.notDB.removeNotificationsForUser(user, notIds)

    types_markNotificationsAsRead = [str, list]

    def export_markNotificationsAsRead(self, user, notIds):
        """Delete an assignee group"""
        credDict = self.getRemoteCredentials()
        if Properties.ALARMS_MANAGEMENT not in credDict["properties"]:
            user = credDict["username"]
        return self.notDB.markNotificationsSeen(user, True, notIds)

    types_markNotificationsAsNotRead = [str, list]

    def export_markNotificationsAsNotRead(self, user, notIds):
        """Delete an assignee group"""
        credDict = self.getRemoteCredentials()
        if Properties.ALARMS_MANAGEMENT not in credDict["properties"]:
            user = credDict["username"]
        return self.notDB.markNotificationsSeen(user, False, notIds)

    types_getNotifications = [dict, list, int, int]

    def export_getNotifications(self, selectDict, sortList, startItem, maxItems):
        """Get all assignee groups and the users that belong to them"""
        credDict = self.getRemoteCredentials()
        if Properties.ALARMS_MANAGEMENT not in credDict["properties"]:
            selectDict["user"] = [credDict["username"]]
        return self.notDB.getNotifications(selectDict, sortList, startItem, maxItems)
