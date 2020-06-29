""" DIRAC Notification Client class encapsulates the methods exposed
    by the Notification service.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.Mail import Mail


@createClient('Framework/Notification')
class NotificationClient(Client):

  def __init__(self, **kwargs):
    """ Notification Client constructor
    """
    super(NotificationClient, self).__init__(**kwargs)

    self.log = gLogger.getSubLogger('NotificationClient')
    self.setServer('Framework/Notification')

  def sendMail(self, addresses, subject, body,
               fromAddress=None, localAttempt=True, html=False, avoidSpam=False):
    """ Send an e-mail with subject and body to the specified address. Try to send
        from local area before central service by default.
    """
    self.log.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' % (addresses,
                                                                                              subject,
                                                                                              body))
    result = S_ERROR()

    addresses = [addresses] if isinstance(addresses, six.string_types) else list(addresses)
    for address in addresses:

      if localAttempt:
        try:
          m = Mail()
          m._subject = subject
          m._message = body
          m._mailAddress = address
          m._html = html
          if fromAddress:
            m._fromAddress = fromAddress
          result = m._send()
        except Exception as x:
          self.log.warn('Sending mail failed with exception:\n%s' % (str(x)))

        if result['OK']:
          self.log.verbose('Mail sent successfully from local host to %s with subject %s' % (address, subject))
          self.log.debug(result['Value'])
          return result

        self.log.warn(
            'Could not send mail with the following message:\n%s\n will attempt to send via NotificationService' %
            result['Message'])

      result = self._getRPC().sendMail(address, subject, body, str(fromAddress), avoidSpam)
      if not result['OK']:
        self.log.error('Could not send mail via central Notification service', result['Message'])
        return result
      else:
        self.log.verbose(result['Value'])

    return result

  def sendSMS(self, userName, body, fromAddress=None):
    """ Send an SMS with body to the specified DIRAC user name.
    """
    self.log.verbose('Received signal to send the following SMS to %s:\n%s' % (userName, body))
    result = self._getRPC().sendSMS(userName, body, str(fromAddress))
    if not result['OK']:
      self.log.error('Could not send SMS via central Notification service', result['Message'])
    else:
      self.log.verbose(result['Value'])

    return result

  ###########################################################################
  # ALARMS
  ###########################################################################

  def newAlarm(self, subject, status, notifications, assignee, body, priority, alarmKey=""):
    if not isinstance(notifications, (list, tuple)):
      return S_ERROR(
          "Notifications parameter has to be a list or a tuple with a combination of [ 'Web', 'Mail', 'SMS' ]")
    alarmDef = {'subject': subject, 'status': status,
                'notifications': notifications, 'assignee': assignee,
                'priority': priority, 'body': body}
    if alarmKey:
      alarmDef['alarmKey'] = alarmKey
    return self._getRPC().newAlarm(alarmDef)

  def updateAlarm(self, id=-1, alarmKey="", comment=False, modDict={}):
    if id == -1 and not alarmKey:
      return S_ERROR("Need either alarm id or key to update an alarm!")
    updateReq = {'comment': comment, 'modifications': modDict}
    if id != -1:
      updateReq['id'] = id
    if alarmKey:
      updateReq['alarmKey'] = alarmKey
    return self._getRPC().updateAlarm(updateReq)

  ###########################################################################
  # MANAGE NOTIFICATIONS
  ###########################################################################

  def addNotificationForUser(self, user, message, lifetime=604800, deferToMail=True):
    try:
      lifetime = int(lifetime)
    except BaseException:
      return S_ERROR("Message lifetime has to be a non decimal number")
    return self._getRPC().addNotificationForUser(user, message, lifetime, deferToMail)
