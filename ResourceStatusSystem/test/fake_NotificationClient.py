################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

class NotificationClient:
  def __init__( self, rpcFunctor = False ):   
    pass
  
  def __getRPCClient( self ):
    pass

  def sendMail( self, address, subject, body, fromAddress = None, localAttempt = True ):
    pass

  def sendSMS( self, userName, body, fromAddress = None ):
    pass

  def newAlarm( self, subject, status, notifications, assignee, body, priority, alarmKey = "" ):
    pass

  def updateAlarm( self, id = -1, alarmKey = "", comment = False, modDict = {} ):
    pass

  def getAlarms( self, selectDict, sortList, startItem, maxItems ):
    pass

  def getAlarmInfo( self, alarmId ):
    pass

  def deleteAlarmsById( self, alarmIdList ):
    pass

  def deleteAlarmsByKey( self, alarmKeyList ):
    pass

  def setAssigneeGroup( self, groupName, userList ):
    pass

  def getUsersInAssigneeGroup( self, groupName ):
    pass

  def deleteAssigneeGroup( self, groupName ):
    pass

  def getAssigneeGroups( self ):
    pass

  def getAssigneeGroupsForUser( self, user ):
    pass

  def addNotificationForUser( self, user, message, lifetime = 604800, deferToMail = True ):
    pass

  def removeNotificationsForUser( self, user, notIds ):
    pass

  def markNotificationsAsRead( self, user, notIds = [] ):
    pass

  def markNotificationsAsNotRead( self, user, notIds = [] ):
    pass

  def getNotifications( self, selectDict, sortList, startItem, maxItems ):
    pass

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF