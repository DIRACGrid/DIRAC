"""  ErrorMessageMonitor gets new errors that have been injected into the
     SystemLoggingDB and reports them by mail to the person(s) in charge
     of checking that they conform with DIRAC style. Reviewer option
     contains the list of users to be notified.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUserOption
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

AGENT_NAME = 'Logging/ErrorMessageMonitor'


class ErrorMessageMonitor(AgentModule):

  def initialize(self):

    self.systemLoggingDB = SystemLoggingDB()

    self.notification = NotificationClient()

    userList = self.am_getOption("Reviewer", [])

    self.log.debug("Users to be notified:", ', '.join(userList))

    mailList = []
    for user in userList:
      mail = getUserOption(user, 'Email', '')
      if not mail:
        self.log.warn("Could not get user's mail", user)
      else:
        mailList.append(mail)

    if not mailList:
      mailList = Operations().getValue('EMail/Logging', [])

    if not len(mailList):
      errString = "There are no valid users in the mailing list"
      varString = "[" + ','.join(userList) + "]"
      self.log.warn(errString, varString)

    self.log.info("List of mails to be notified", ','.join(mailList))

    self._mailAddress = mailList
    self._subject = 'New error messages were entered in the SystemLoggingDB'
    return S_OK()

  def execute(self):
    """ The main agent execution method
    """
    condDict = {'ReviewedMessage': 0}
    result = self.systemLoggingDB.getCounters('FixedTextMessages', ['ReviewedMessage'], condDict)
    if not result['OK']:
      return result

    if not result['Value']:
      self.log.info('No messages need review')
      return S_OK('No messages need review')
    returnFields = ['FixedTextID', 'FixedTextString', 'SystemName', 'SubSystemName']
    result = self.systemLoggingDB._queryDB(showFieldList=returnFields,
                                           groupColumn='FixedTextString,FixedTextID,SystemName',
                                           condDict=condDict)
    if not result['OK']:
      self.log.error('Failed to obtain the non reviewed Strings',
                     result['Message'])
      return S_OK()
    messageList = result['Value']

    if messageList == 'None' or not messageList:
      self.log.error('The DB query returned an empty result')
      return S_OK()

    mailBody = 'These new messages have arrived to the Logging Service\n'
    for message in messageList:
      mailBody = mailBody + "String: '" + message[1] + "'\tSystem: '" \
          + message[2] + "'\tSubsystem: '" + message[3] + "'\n"

    if self._mailAddress:
      result = self.notification.sendMail(self._mailAddress, self._subject, mailBody)
      if not result['OK']:
        self.log.warn("The mail could not be sent", result['Message'])
        return S_OK()

    messageIDs = [message[0] for message in messageList]
    condDict = {'FixedTextID': messageIDs}
    result = self.systemLoggingDB.updateFields('FixedTextMessages', ['ReviewedMessage'], [1], condDict=condDict)
    if not result['OK']:
      self.log.error('Could not update message Status', result['ERROR'])
      return S_OK()
    self.log.verbose('Updated message Status for:', str(messageList))

    self.log.info("The messages have been sent for review",
                  "There are %s new descriptions" % len(messageList))
    return S_OK("%s Messages have been sent for review" % len(messageList))
