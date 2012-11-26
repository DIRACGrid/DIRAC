########################################################################
# $Id$
########################################################################

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

__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Mail import Mail
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.NotificationDB import NotificationDB
from DIRAC.Core.Security import Properties

gNotDB = None

def initializeNotificationHandler( serviceInfo ):

  global gNotDB
  gNotDB = NotificationDB()
  gThreadScheduler.addPeriodicTask( 3600, gNotDB.purgeExpiredNotifications() )
  return S_OK()

class NotificationHandler( RequestHandler ):

  def initialize( self ):
    credDict = self.getRemoteCredentials()
    self.clientDN = credDict['DN']
    self.clientGroup = credDict['group']
    self.clientProperties = credDict[ 'properties' ]
    self.client = credDict[ 'username' ]

  ###########################################################################
  types_sendMail = [StringType, StringType, StringType, StringType]
  def export_sendMail( self, address, subject, body, fromAddress ):
    """ Send an email with supplied body to the specified address using the Mail utility.
    """
    gLogger.verbose( 'Received signal to send the following mail to %s:\nSubject = %s\n%s' % ( address, subject, body ) )
    m = Mail()
    m._subject = subject
    m._message = body
    m._mailAddress = address
    if not fromAddress == 'None':
      m._fromAddress = fromAddress
    result = m._send()
    if not result['OK']:
      gLogger.warn( 'Could not send mail with the following message:\n%s' % result['Message'] )
    else:
      gLogger.info( 'Mail sent successfully to %s with subject %s' % ( address, subject ) )
      gLogger.debug( result['Value'] )

    return result

  ###########################################################################
  types_sendSMS = [StringType, StringType, StringType]
  def export_sendSMS( self, userName, body, fromAddress ):
    """ Send an SMS with supplied body to the specified DIRAC user using the Mail utility via an SMS switch.
    """
    gLogger.verbose( 'Received signal to send the following SMS to %s:\n%s' % ( userName, body ) )
    mobile = gConfig.getValue( '/Registry/Users/%s/Mobile' % userName, '' )
    if not mobile:
      return S_ERROR( 'No registered mobile number for %s' % userName )

    csSection = PathFinder.getServiceSection( 'Framework/Notification' )
    smsSwitch = gConfig.getValue( '%s/SMSSwitch' % csSection, '' )
    if not smsSwitch:
      return S_ERROR( 'No SMS switch is defined in CS path %s/SMSSwitch' % csSection )

    address = '%s@%s' % ( mobile, smsSwitch )
    subject = 'DIRAC SMS'
    m = Mail()
    m._subject = subject
    m._message = body
    m._mailAddress = address
    if not fromAddress == 'None':
      m._fromAddress = fromAddress
    result = m._send()
    if not result['OK']:
      gLogger.warn( 'Could not send SMS to %s with the following message:\n%s' % ( userName, result['Message'] ) )
    else:
      gLogger.info( 'SMS sent successfully to %s ' % ( userName ) )
      gLogger.debug( result['Value'] )

    return result

  ###########################################################################
  # ALARMS
  ###########################################################################

  types_newAlarm = [ DictType ]
  def export_newAlarm( self, alarmDefinition ):
    """ Set a new alarm in the Notification database
    """
    credDict = self.getRemoteCredentials()
    if 'username' not in credDict:
      return S_ERROR( "OOps. You don't have a username! This shouldn't happen :P" )
    alarmDefinition[ 'author' ] = credDict[ 'username' ]
    return gNotDB.newAlarm( alarmDefinition )

  types_updateAlarm = [ DictType ]
  def export_updateAlarm( self, updateDefinition ):
    """ update an existing alarm in the Notification database
    """
    credDict = self.getRemoteCredentials()
    if 'username' not in credDict:
      return S_ERROR( "OOps. You don't have a username! This shouldn't happen :P" )
    updateDefinition[ 'author' ] = credDict[ 'username' ]
    return gNotDB.updateAlarm( updateDefinition )

  types_getAlarmInfo = [ ( IntType, LongType ) ]
  def export_getAlarmInfo( self, alarmId ):
    """ Get the extended info of an alarm
    """
    result = gNotDB.getAlarmInfo( alarmId )
    if not result[ 'OK' ]:
      return result
    alarmInfo = result[ 'Value' ]
    result = gNotDB.getAlarmLog( alarmId )
    if not result[ 'OK' ]:
      return result
    return S_OK( { 'info' : alarmInfo, 'log' : result[ 'Value' ] } )

  types_getAlarms = [DictType, ListType, IntType, IntType]
  def export_getAlarms( self, selectDict, sortList, startItem, maxItems ):
    """ Select existing alarms suitable for the Web monitoring
    """
    return gNotDB.getAlarms( selectDict, sortList, startItem, maxItems )

  types_deleteAlarmsByAlarmId = [ ( ListType, IntType ) ]
  def export_deleteAlarmsByAlarmId( self, alarmsIdList ):
    """ Delete alarms by alarmId
    """
    return gNotDB.deleteAlarmsByAlarmId( alarmsIdList )

  types_deleteAlarmsByAlarmKey = [ ( ListType, StringType ) ]
  def export_deleteAlarmsByAlarmKey( self, alarmsKeyList ):
    """ Delete alarms by alarmId
    """
    return gNotDB.deleteAlarmsByAlarmKey( alarmsKeyList )


  ###########################################################################
  # MANANGE ASSIGNEE GROUPS
  ###########################################################################

  types_setAssigneeGroup = [ StringType, ListType ]
  def export_setAssigneeGroup( self, groupName, userList ):
    """ Create a group of users to be used as an assignee for an alarm
    """
    return gNotDB.setAssigneeGroup( groupName, userList )

  types_getUsersInAssigneeGroup = [ StringType ]
  def export_getUsersInAssigneeGroup( self, groupName ):
    """ Get users in assignee group
    """
    return gNotDB.getUserAsignees( groupName )

  types_deleteAssigneeGroup = [ StringType ]
  def export_deleteAssigneeGroup( self, groupName ):
    """ Delete an assignee group
    """
    return gNotDB.deleteAssigneeGroup( groupName )

  types_getAssigneeGroups = []
  def export_getAssigneeGroups( self ):
    """ Get all assignee groups and the users that belong to them
    """
    return gNotDB.getAssigneeGroups()

  types_getAssigneeGroupsForUser = [ StringType ]
  def export_getAssigneeGroupsForUser( self, user ):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict[ 'properties' ]:
      user = credDict[ 'username' ]
    return gNotDB.getAssigneeGroupsForUser( user )

  ###########################################################################
  # MANAGE NOTIFICATIONS
  ###########################################################################

  types_addNotificationForUser = [ StringType, StringType ]
  def export_addNotificationForUser( self, user, message, lifetime = 604800, deferToMail = True ):
    """ Create a group of users to be used as an assignee for an alarm
    """
    try:
      lifetime = int( lifetime )
    except:
      return S_ERROR( "Message lifetime has to be a non decimal number" )
    return gNotDB.addNotificationForUser( user, message, lifetime, deferToMail )

  types_removeNotificationsForUser = [ StringType, ListType ]
  def export_removeNotificationsForUser( self, user, notIds ):
    """ Get users in assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict[ 'properties' ]:
      user = credDict[ 'username' ]
    return gNotDB.removeNotificationsForUser( user, notIds )

  types_markNotificationsAsRead = [ StringType, ListType ]
  def export_markNotificationsAsRead( self, user, notIds ):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict[ 'properties' ]:
      user = credDict[ 'username' ]
    return gNotDB.markNotificationsSeen( user, True, notIds )

  types_markNotificationsAsNotRead = [ StringType, ListType ]
  def export_markNotificationsAsNotRead( self, user, notIds ):
    """ Delete an assignee group
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict[ 'properties' ]:
      user = credDict[ 'username' ]
    return gNotDB.markNotificationsSeen( user, False, notIds )

  types_getNotifications = [ DictType, ListType, IntType, IntType ]
  def export_getNotifications( self, selectDict, sortList, startItem, maxItems ):
    """ Get all assignee groups and the users that belong to them
    """
    credDict = self.getRemoteCredentials()
    if Properties.ALARMS_MANAGEMENT not in credDict[ 'properties' ]:
      selectDict[ 'user' ] = [ credDict[ 'username' ] ]
    return gNotDB.getNotifications( selectDict, sortList, startItem, maxItems )
