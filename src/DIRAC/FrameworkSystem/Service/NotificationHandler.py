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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from DIRAC import gConfig, S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.DB.NotificationDB import NotificationDB
from DIRAC.Core.Utilities.DictCache import DictCache

__RCSID__ = "$Id$"


class NotificationHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ Handler initialization
    """
    cls.notDB = NotificationDB()
    cls.mailCache = DictCache()
    gThreadScheduler.addPeriodicTask(3600, cls.notDB.purgeExpiredNotifications)
    gThreadScheduler.addPeriodicTask(3600, cls.mailCache.purgeExpired())
    return S_OK()

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.clientDN = credDict['DN']
    self.clientGroup = credDict['group']
    self.clientProperties = credDict['properties']
    self.client = credDict['username']

  ###########################################################################
  types_sendMail = [six.string_types, six.string_types, six.string_types, six.string_types]

  def export_sendMail(self, address, subject, body, fromAddress):
    """ Send an email with supplied body to the specified address using the Mail utility.

        :param six.string_types address: recipient addresses
        :param six.string_types subject: subject of letter
        :param six.string_types body: body of letter
        :param six.string_types fromAddress: sender address, if None, will be used default from CS

        :return: S_OK(six.string_types)/S_ERROR() -- six.string_types is status message
    """
    self.log.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' % (address, subject, body))
    if self.mailCache.exists(hash(address + subject + body)):
      return S_OK('Email with the same content already sent today to current addresses, come back tomorrow')
    eMail = Mail()
    notificationSection = PathFinder.getServiceSection("Framework/Notification")
    csSection = notificationSection + '/SMTP'
    eMail._smtpHost = gConfig.getValue('%s/Host' % csSection)
    eMail._smtpPort = gConfig.getValue('%s/Port' % csSection)
    eMail._smtpLogin = gConfig.getValue('%s/Login' % csSection)
    eMail._smtpPasswd = gConfig.getValue('%s/Password' % csSection)
    eMail._smtpPtcl = gConfig.getValue('%s/Protocol' % csSection)
    eMail._subject = subject
    eMail._message = body
    eMail._mailAddress = address
    if not fromAddress == 'None':
      eMail._fromAddress = fromAddress
    eMail._fromAddress = gConfig.getValue('%s/FromAddress' % csSection) or eMail._fromAddress
    result = eMail._send()
    if not result['OK']:
      self.log.warn('Could not send mail with the following message:\n%s' % result['Message'])
    else:
      self.mailCache.add(hash(address + subject + body), 3600 * 24)
      self.log.info('Mail sent successfully to %s with subject %s' % (address, subject))
      self.log.debug(result['Value'])

    return result

  ###########################################################################
  types_sendSMS = [six.string_types, six.string_types, six.string_types]

  def export_sendSMS(self, userName, body, fromAddress):
    """ Send an SMS with supplied body to the specified DIRAC user using the Mail utility via an SMS switch.

        :param six.string_types userName: user name
        :param six.string_types body: message
        :param six.string_types fromAddress: sender address

        :return: S_OK()/S_ERROR()
    """
    self.log.verbose('Received signal to send the following SMS to %s:\n%s' % (userName, body))
    mobile = gConfig.getValue('/Registry/Users/%s/Mobile' % userName, '')
    if not mobile:
      return S_ERROR('No registered mobile number for %s' % userName)

    csSection = PathFinder.getServiceSection('Framework/Notification')
    smsSwitch = gConfig.getValue('%s/SMSSwitch' % csSection, '')
    if not smsSwitch:
      return S_ERROR('No SMS switch is defined in CS path %s/SMSSwitch' % csSection)

    address = '%s@%s' % (mobile, smsSwitch)
    subject = 'DIRAC SMS'
    eMail = Mail()
    eMail._subject = subject
    eMail._message = body
    eMail._mailAddress = address
    if not fromAddress == 'None':
      eMail._fromAddress = fromAddress
    result = eMail._send()
    if not result['OK']:
      self.log.warn('Could not send SMS to %s with the following message:\n%s' % (userName, result['Message']))
    else:
      self.log.info('SMS sent successfully to %s ' % (userName))
      self.log.debug(result['Value'])

    return result

  ###########################################################################
  # ALARMS
  ###########################################################################

  types_newAlarm = [dict]

  def export_newAlarm(self, alarmDefinition):
    """ Set a new alarm in the Notification database
    """
    credDict = self.getRemoteCredentials()
    if 'username' not in credDict:
      return S_ERROR("OOps. You don't have a username! This shouldn't happen :P")
    alarmDefinition['author'] = credDict['username']
    return self.notDB.newAlarm(alarmDefinition)

  types_updateAlarm = [dict]

  def export_updateAlarm(self, updateDefinition):
    """ update an existing alarm in the Notification database
    """
    credDict = self.getRemoteCredentials()
    if 'username' not in credDict:
      return S_ERROR("OOps. You don't have a username! This shouldn't happen :P")
    updateDefinition['author'] = credDict['username']
    return self.notDB.updateAlarm(updateDefinition)

  types_getAlarmInfo = [six.integer_types]

  @classmethod
  def export_getAlarmInfo(cls, alarmId):
    """ Get the extended info of an alarm
    """
    result = cls.notDB.getAlarmInfo(alarmId)
    if not result['OK']:
      return result
    alarmInfo = result['Value']
    result = cls.notDB.getAlarmLog(alarmId)
    if not result['OK']:
      return result
    return S_OK({'info': alarmInfo, 'log': result['Value']})

  types_getAlarms = [dict, list, int, int]

  @classmethod
  def export_getAlarms(cls, selectDict, sortList, startItem, maxItems):
    """ Select existing alarms suitable for the Web monitoring
    """
    return cls.notDB.getAlarms(selectDict, sortList, startItem, maxItems)

  types_deleteAlarmsByAlarmId = [(list, int)]

  @classmethod
  def export_deleteAlarmsByAlarmId(cls, alarmsIdList):
    """ Delete alarms by alarmId
    """
    return cls.notDB.deleteAlarmsByAlarmId(alarmsIdList)

  types_deleteAlarmsByAlarmKey = [(six.string_types, list)]

  @classmethod
  def export_deleteAlarmsByAlarmKey(cls, alarmsKeyList):
    """ Delete alarms by alarmId
    """
    return cls.notDB.deleteAlarmsByAlarmKey(alarmsKeyList)

  ###########################################################################
  # MANANGE ASSIGNEE GROUPS
  ###########################################################################

  types_setAssigneeGroup = [six.string_types, list]

  @classmethod
  def export_setAssigneeGroup(cls, groupName, userList):
    """ Create a group of users to be used as an assignee for an alarm
    """
    return cls.notDB.setAssigneeGroup(groupName, userList)

  types_getUsersInAssigneeGroup = [six.string_types]

  @classmethod
  def export_getUsersInAssigneeGroup(cls, groupName):
    """ Get users in assignee group
    """
    return cls.notDB.getUserAsignees(groupName)

  types_deleteAssigneeGroup = [six.string_types]

  @classmethod
  def export_deleteAssigneeGroup(cls, groupName):
    """ Delete an assignee group
    """
    return cls.notDB.deleteAssigneeGroup(groupName)

  types_getAssigneeGroups = []

  @classmethod
  def export_getAssigneeGroups(cls):
    """ Get all assignee groups and the users that belong to them
    """
    return cls.notDB.getAssigneeGroups()

  types_getAssigneeGroupsForUser = [six.string_types]

  def export_getAssigneeGroupsForUser(self, user):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return self.notDB.getAssigneeGroupsForUser(user)

  ###########################################################################
  # MANAGE NOTIFICATIONS
  ###########################################################################

  types_addNotificationForUser = [six.string_types, six.string_types]

  def export_addNotificationForUser(self, user, message, lifetime=604800, deferToMail=True):
    """ Create a group of users to be used as an assignee for an alarm
    """
    try:
      lifetime = int(lifetime)
    except Exception:
      return S_ERROR("Message lifetime has to be a non decimal number")
    return self.notDB.addNotificationForUser(user, message, lifetime, deferToMail)

  types_removeNotificationsForUser = [six.string_types, list]

  def export_removeNotificationsForUser(self, user, notIds):
    """ Get users in assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return self.notDB.removeNotificationsForUser(user, notIds)

  types_markNotificationsAsRead = [six.string_types, list]

  def export_markNotificationsAsRead(self, user, notIds):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return self.notDB.markNotificationsSeen(user, True, notIds)

  types_markNotificationsAsNotRead = [six.string_types, list]

  def export_markNotificationsAsNotRead(self, user, notIds):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return self.notDB.markNotificationsSeen(user, False, notIds)

  types_getNotifications = [dict, list, int, int]

  def export_getNotifications(self, selectDict, sortList, startItem, maxItems):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      selectDict['user'] = [credDict['username']]
    return self.notDB.getNotifications(selectDict, sortList, startItem, maxItems)
