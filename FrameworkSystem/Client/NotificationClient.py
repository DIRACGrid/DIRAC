########################################################################
# $Id$
########################################################################

""" DIRAC Notification Client class encapsulates the methods exposed
    by the Notification service.
"""

import types

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC import gLogger, S_ERROR

class NotificationClient:

  #############################################################################
  def __init__( self, rpcFunctor = False ):
    """ Notification Client constructor
    """
    self.log = gLogger.getSubLogger( 'NotificationClient' )
    if rpcFunctor:
      self.__rpcFunctor = rpcFunctor
    else:
      self.__rpcFunctor = RPCClient

  def __getRPCClient( self, **kwargs ):
    return self.__rpcFunctor( "Framework/Notification", **kwargs )

  #############################################################################
  def sendMail( self, address, subject, body, fromAddress = None, localAttempt = True ):
    """ Send an e-mail with subject and body to the specified address. Try to send
        from local area before central service by default.
    """
    self.log.verbose( 'Received signal to send the following mail to %s:\nSubject = %s\n%s' % ( address, subject, body ) )
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
      except Exception, x:
        self.log.warn( 'Sending mail failed with exception:\n%s' % ( str( x ) ) )

      if result['OK']:
        self.log.verbose( 'Mail sent successfully from local host to %s with subject %s' % ( address, subject ) )
        self.log.debug( result['Value'] )
        return result

      self.log.warn( 'Could not send mail with the following message:\n%s\n will attempt to send via NotificationService' % result['Message'] )

    notify = self.__getRPCClient( timeout = 120 )
    result = notify.sendMail( address, subject, body, str( fromAddress ) )
    if not result['OK']:
      self.log.error( 'Could not send mail via central Notification service', result['Message'] )
    else:
      self.log.verbose( result['Value'] )

    return result

  #############################################################################
  def sendSMS( self, userName, body, fromAddress = None ):
    """ Send an SMS with body to the specified DIRAC user name.
    """
    self.log.verbose( 'Received signal to send the following SMS to %s:\n%s' % ( userName, body ) )
    notify = RPCClient( 'Framework/Notification', timeout = 120 )
    result = notify.sendSMS( userName, body, str( fromAddress ) )
    if not result['OK']:
      self.log.error( 'Could not send SMS via central Notification service', result['Message'] )
    else:
      self.log.verbose( result['Value'] )

    return result

  ###########################################################################
  # ALARMS
  ###########################################################################

  def newAlarm( self, subject, status, notifications, assignee, body, priority, alarmKey = "" ):
    rpcClient = self.__getRPCClient()
    if type( notifications ) not in ( types.ListType, types.TupleType ):
      return S_ERROR( "Notifications parameter has to be a list or a tuple with a combination of [ 'Web', 'Mail', 'SMS' ]" )
    alarmDef = { 'subject' : subject, 'status' : status,
                 'notifications' : notifications, 'assignee' : assignee,
                 'priority' : priority, 'body' : body }
    if alarmKey:
      alarmDef[ 'alarmKey' ] = alarmKey
    return rpcClient.newAlarm( alarmDef )

  def updateAlarm( self, id = -1, alarmKey = "", comment = False, modDict = {} ):
    rpcClient = self.__getRPCClient()
    if id == -1 and not alarmKey:
      return S_ERROR( "Need either alarm id or key to update an alarm!" )
    updateReq = { 'comment' : comment, 'modifications' : modDict }
    if id != -1:
      updateReq[ 'id' ] = id
    if alarmKey:
      updateReq[ 'alarmKey' ] = alarmKey
    return rpcClient.updateAlarm( updateReq )

  def getAlarms( self, selectDict, sortList, startItem, maxItems ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAlarms( selectDict, sortList, startItem, maxItems )

  def getAlarmInfo( self, alarmId ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAlarmInfo( alarmId )

  def deleteAlarmsById( self, alarmIdList ):
    rpcClient = self.__getRPCClient()
    return rpcClient.deleteAlarmsByAlarmId( alarmIdList )

  def deleteAlarmsByKey( self, alarmKeyList ):
    rpcClient = self.__getRPCClient()
    return rpcClient.deleteAlarmsByAlarmKey( alarmKeyList )

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

  def getAssigneeGroups( self ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getAssigneeGroups()

  def getAssigneeGroupsForUser( self, user ):
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

  def removeNotificationsForUser( self, user, notIds ):
    rpcClient = self.__getRPCClient()
    return rpcClient.removeNotificationsForUser( user, notIds )

  def markNotificationsAsRead( self, user, notIds = [] ):
    rpcClient = self.__getRPCClient()
    return rpcClient.markNotificationsAsRead( user, notIds )

  def markNotificationsAsNotRead( self, user, notIds = [] ):
    rpcClient = self.__getRPCClient()
    return rpcClient.markNotificationsAsNotRead( user, notIds )

  def getNotifications( self, selectDict, sortList, startItem, maxItems ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getNotifications( selectDict, sortList, startItem, maxItems )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
