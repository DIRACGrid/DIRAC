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
from DIRAC import gConfig, gLogger, S_OK, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.DB.NotificationDB import NotificationDB
from DIRAC.Core.Utilities.DictCache import DictCache

__RCSID__ = "$Id$"

gNotDB = None
gMailCache = DictCache()


class NotificationHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ Handler initialization
    """
    global gNotDB
    gNotDB = NotificationDB()
    gThreadScheduler.addPeriodicTask(3600, gNotDB.purgeExpiredNotifications)
    gThreadScheduler.addPeriodicTask(3600, gMailCache.purgeExpired())
    return S_OK()

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.clientDN = credDict['DN']
    self.clientGroup = credDict['group']
    self.clientProperties = credDict['properties']
    self.client = credDict['username']

  ###########################################################################
  types_sendMail = [basestring, basestring, basestring, basestring, bool]

  def export_sendMail(self, address, subject, body, fromAddress, avoidSpam=False):
    """ Send an email with supplied body to the specified address using the Mail utility.

        :param basestring address: recipient addresses
        :param basestring subject: subject of letter
        :param basestring body: body of letter
        :param basestring fromAddress: sender address, if None, will be used default from CS
        :param bool avoidSpam: Deprecated

        :return: S_OK(basestring)/S_ERROR() -- basestring is status message
    """
    gLogger.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' % (address, subject, body))
    if gMailCache.exists(hash(address + subject + body)):
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
      gLogger.warn('Could not send mail with the following message:\n%s' % result['Message'])
    else:
      gMailCache.add(hash(address + subject + body), 3600 * 24)
      gLogger.info('Mail sent successfully to %s with subject %s' % (address, subject))
      gLogger.debug(result['Value'])

    return result

  ###########################################################################
  types_sendSMS = [basestring, basestring, basestring]

  def export_sendSMS(self, userName, body, fromAddress):
    """ Send an SMS with supplied body to the specified DIRAC user using the Mail utility via an SMS switch.

        :param basestring userName: user name
        :param basestring body: message
        :param basestring fromAddress: sender address

        :return: S_OK()/S_ERROR()
    """
    gLogger.verbose('Received signal to send the following SMS to %s:\n%s' % (userName, body))
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
      gLogger.warn('Could not send SMS to %s with the following message:\n%s' % (userName, result['Message']))
    else:
      gLogger.info('SMS sent successfully to %s ' % (userName))
      gLogger.debug(result['Value'])

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
    return gNotDB.newAlarm(alarmDefinition)

  types_updateAlarm = [dict]

  def export_updateAlarm(self, updateDefinition):
    """ update an existing alarm in the Notification database
    """
    credDict = self.getRemoteCredentials()
    if 'username' not in credDict:
      return S_ERROR("OOps. You don't have a username! This shouldn't happen :P")
    updateDefinition['author'] = credDict['username']
    return gNotDB.updateAlarm(updateDefinition)

  types_getAlarmInfo = [six.integer_types]

  def export_getAlarmInfo(self, alarmId):
    """ Get the extended info of an alarm
    """
    result = gNotDB.getAlarmInfo(alarmId)
    if not result['OK']:
      return result
    alarmInfo = result['Value']
    result = gNotDB.getAlarmLog(alarmId)
    if not result['OK']:
      return result
    return S_OK({'info': alarmInfo, 'log': result['Value']})

  types_getAlarms = [dict, list, int, int]

  def export_getAlarms(self, selectDict, sortList, startItem, maxItems):
    """ Select existing alarms suitable for the Web monitoring
    """
    return gNotDB.getAlarms(selectDict, sortList, startItem, maxItems)

  types_deleteAlarmsByAlarmId = [(list, int)]

  def export_deleteAlarmsByAlarmId(self, alarmsIdList):
    """ Delete alarms by alarmId
    """
    return gNotDB.deleteAlarmsByAlarmId(alarmsIdList)

  types_deleteAlarmsByAlarmKey = [(basestring, list)]

  def export_deleteAlarmsByAlarmKey(self, alarmsKeyList):
    """ Delete alarms by alarmId
    """
    return gNotDB.deleteAlarmsByAlarmKey(alarmsKeyList)

  ###########################################################################
  # MANANGE ASSIGNEE GROUPS
  ###########################################################################

  types_setAssigneeGroup = [basestring, list]

  def export_setAssigneeGroup(self, groupName, userList):
    """ Create a group of users to be used as an assignee for an alarm
    """
    return gNotDB.setAssigneeGroup(groupName, userList)

  types_getUsersInAssigneeGroup = [basestring]

  def export_getUsersInAssigneeGroup(self, groupName):
    """ Get users in assignee group
    """
    return gNotDB.getUserAsignees(groupName)

  types_deleteAssigneeGroup = [basestring]

  def export_deleteAssigneeGroup(self, groupName):
    """ Delete an assignee group
    """
    return gNotDB.deleteAssigneeGroup(groupName)

  types_getAssigneeGroups = []

  def export_getAssigneeGroups(self):
    """ Get all assignee groups and the users that belong to them
    """
    return gNotDB.getAssigneeGroups()

  types_getAssigneeGroupsForUser = [basestring]

  def export_getAssigneeGroupsForUser(self, user):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return gNotDB.getAssigneeGroupsForUser(user)

  ###########################################################################
  # MANAGE NOTIFICATIONS
  ###########################################################################

  types_addNotificationForUser = [basestring, basestring]

  def export_addNotificationForUser(self, user, message, lifetime=604800, deferToMail=True):
    """ Create a group of users to be used as an assignee for an alarm
    """
    try:
      lifetime = int(lifetime)
    except BaseException:
      return S_ERROR("Message lifetime has to be a non decimal number")
    return gNotDB.addNotificationForUser(user, message, lifetime, deferToMail)

  types_removeNotificationsForUser = [basestring, list]

  def export_removeNotificationsForUser(self, user, notIds):
    """ Get users in assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return gNotDB.removeNotificationsForUser(user, notIds)

  types_markNotificationsAsRead = [basestring, list]

  def export_markNotificationsAsRead(self, user, notIds):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return gNotDB.markNotificationsSeen(user, True, notIds)

  types_markNotificationsAsNotRead = [basestring, list]

  def export_markNotificationsAsNotRead(self, user, notIds):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      user = credDict['username']
    return gNotDB.markNotificationsSeen(user, False, notIds)

  types_getNotifications = [dict, list, int, int]

  def export_getNotifications(self, selectDict, sortList, startItem, maxItems):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict['properties']:
      selectDict['user'] = [credDict['username']]
    return gNotDB.getNotifications(selectDict, sortList, startItem, maxItems)
