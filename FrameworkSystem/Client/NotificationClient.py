########################################################################
# $Id: NotificationClient.py,v 1.3 2009/10/20 15:56:10 acasajus Exp $
########################################################################

""" DIRAC Notification Client class encapsulates the methods exposed
    by the Notification service.
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC import gLogger, S_OK, S_ERROR

import os

class NotificationClient:

  #############################################################################
  def __init__( self, rpcFunctor = False ):
    """ Notification Client constructor
    """
    self.log = gLogger.getSubLogger('NotificationClient')
    if rpcFunctor:
      self.__rpcFunctor = rpcFunctor
    else:
      self.__rpcFunctor = RPCClient
      
  def __getRPCClient( self ):
    return self.__rpcFunctor( "Framework/Notification" )

  #############################################################################
  def sendMail(self,address,subject,body,fromAddress=None,localAttempt=True):
    """ Send an e-mail with subject and body to the specified address. Try to send
        from local area before central service by default.
    """
    self.log.verbose('Received signal to send the following mail to %s:\nSubject = %s\n%s' %(address,subject,body))
    result = S_ERROR()
    if localAttempt:
      try:
        m = Mail()
        m._subject = subject
        m._message = body
        m._mailAddress = address
        if fromAddress:
          m._fromAddress = fromAddress
        result = m._send()
      except Exception,x:
        self.log.warn('Sending mail failed with exception:\n%s' %(str(x)))

      if result['OK']:
        self.log.verbose('Mail sent successfully from local host to %s with subject %s' %(address,subject))
        self.log.debug(result['Value'])
        return result

      self.log.warn('Could not send mail with the following message:\n%s\n will attempt to send via NotificationService' %result['Message'])

    notify = RPCClient('Framework/Notification',useCertificates=False,timeout=120)
    result = notify.sendMail(address,subject,body,str(fromAddress))
    if not result['OK']:
      self.log.error('Could not send mail via central Notification service',result['Message'])
    else:
      self.log.verbose(result['Value'])

    return result

  #############################################################################
  def sendSMS(self,userName,body,fromAddress=None):
    """ Send an SMS with body to the specified DIRAC user name.
    """
    self.log.verbose('Received signal to send the following SMS to %s:\n%s' %(userName,body))
    notify = RPCClient('Framework/Notification',useCertificates=False,timeout=120)
    result = notify.sendSMS(userName,body,str(fromAddress))
    if not result['OK']:
      self.log.error('Could not send SMS via central Notification service',result['Message'])
    else:
      self.log.verbose(result['Value'])

    return result

  ###########################################################################
  # ALARMS
  ###########################################################################
  
  def newAlarm( self, subject, status, type, assignee, body ):
    rpcClient = self.__getRPCClient()
    return rpcClient.newAlarm( { 'subject' : subject, 'status' : status, 
                                 'type' : type, 'assignee' : assignee, 
                                 'body' : body } )
    
  def updateAlarm( self, id, comment = False, modDict = {} ):
    rpcClient = self.__getRPCClient()
    return rpcClient.updateAlarm( { 'id' : id, 'comment' : comment, 
                                 'modifications' : modDict } )
    
  def getAlarms( self, selectDict, sortList, startItem, maxItems ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAlarms( selectDict, sortList, startItem, maxItems )
  
  def getAlarmInfo( self, alarmId ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAlarmInfo( alarmId )
  
  ###########################################################################
  # MANANGE ASSIGNEE GROUPS
  ###########################################################################
  
  def setAssigneeGroup( self, groupName, userList ):
    rpcClient = self.__getRPCClient()
    return rpcClient.setAssigneeGroup( groupName, userList )
  
  def getUsersInAssigneeGroup( self, groupName ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getUsersInAssigneeGroup( groupName )
  
  def deleteAssigneeGroup( self, groupName ):
    rpcClient = self.__getRPCClient()
    return rpcClient.deleteAssigneeGroup( groupName )
  
  def getAssigneeGroups( self):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAssigneeGroups()
    
  def getAssigneeGroupsForUser( self, user):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAssigneeGroupsForUser( user )
  
  ###########################################################################
  # MANAGE NOTIFICATIONS
  ###########################################################################
  
  def addNotificationForUser( self, user, message, lifetime = 604800, deferToMail = True ):
    rpcClient = self.__getRPCClient()
    try:
      lifetime = int( lifetime )
    except:
      return S_ERROR( "Message lifetime has to be a non decimal number" )
    return rpcClient.addNotificationForUser( user, message, lifetime, deferToMail ) 
  
  def removeNotificationsForUser( self, user ):
    rpcClient = self.__getRPCClient()
    return rpcClient.removeNotificationsForUser( user )
  
  def markNotificationsAsRead( self, user, notIds = [] ):
    rpcClient = self.__getRPCClient()
    return rpcClient.markNotificationsAsRead( user, notIds )
  
  def getNotifications( self, selectDict, sortList, startItem, maxItems ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getNotifications( selectDict, sortList, startItem, maxItems )
  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#