########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/NotificationDB.py,v 1.3 2009/10/16 16:25:51 acasajus Exp $
########################################################################
""" NotificationDB class is a front-end to the Notifications database
"""

__RCSID__ = "$Id: NotificationDB.py,v 1.3 2009/10/16 16:25:51 acasajus Exp $"

import time
import types
import threading
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security import CS

class NotificationDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'NotificationDB', 'Framework/NotificationDB', maxQueueSize )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      self.log.fatal( "Cannot initialize DB!", result[ 'Message' ] )
    self.__alarmQueryFields = [ 'alarmid', 'creator', 'creationtime', 'modtime', 'subject', 
                                'status', 'type', 'body', 'assignee' ]
    self.__notificationQueryFields = ( 'id', 'user', 'seen', 'message' )
    self.__newAlarmMandatoryFields = [ 'creator', 'subject', 'status', 'type', 'body', 'assignee' ]
    self.__updateAlarmMandatoryFields = [ 'id', 'author' ]
    self.__updateAlarmAtLeastOneField = [ 'comment', 'modifications' ]
    self.__updateAlarmModificableFields = [ 'status', 'type', 'assignee' ]
    self.__validAlarmStatus = [ 'Open', 'OnGoing', 'Closed', 'Testing' ]
    self.__validAlarmTypes = [ 'Action' ]
    
  def __initializeDB(self):
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesToCreate = {}
    if 'ntf_Alarms' not in tablesInDB:
      tablesToCreate[ 'ntf_Alarms' ] = { 'Fields' : { 'AlarmId' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                         'Creator' : 'VARCHAR(64) NOT NULL',
                                                         'CreationTime' : 'DATETIME NOT NULL',
                                                         'ModTime' : 'DATETIME NOT NULL',
                                                         'Subject' : 'VARCHAR(255) NOT NULL',
                                                         'Status' : 'VARCHAR(64) NOT NULL',
                                                         'Type' : 'VARCHAR(32) NOT NULL',
                                                         'Body' : 'BLOB',
                                                         'Assignee' : 'VARCHAR(64) NOT NULL'
                                                        },
                                            'PrimaryKey' : 'AlarmId',
                                            'Indexes' : { 'Status' : [ 'Status' ], 
                                                          'Assignee' : [ 'Assignee' ] }
                                          }
    if 'ntf_AssigneeGroups' not in tablesInDB:
      tablesToCreate[ 'ntf_AssigneeGroups' ] = { 'Fields' : { 'AssigneeGroup' : 'VARCHAR(64) NOT NULL',
                                                         'User' : 'VARCHAR(64) NOT NULL',
                                                        },
                                            'Indexes' : { 'ag' : [ 'AssigneeGroup' ] }
                                          }
      
    if 'ntf_AlarmLog' not in tablesInDB:
      tablesToCreate[ 'ntf_AlarmLog' ] = { 'Fields' : { 'AlarmId' : 'INTEGER UNSIGNED NOT NULL',
                                                            'Timestamp' : 'DATETIME NOT NULL',
                                                            'Author' : 'VARCHAR(64) NOT NULL',
                                                            'Comment' : 'BLOB',
                                                            'Modifications' : 'VARCHAR(256)',
                                                        },
                                            'Indexes' : { 'AlarmID' : [ 'AlarmId' ] }
                                          }
      
    if 'ntf_AlarmFollowers' not in tablesInDB:
      tablesToCreate[ 'ntf_AlarmFollowers' ] = { 'Fields' : { 'AlarmId'   : 'INTEGER UNSIGNED NOT NULL',
                                                                  'User'  : 'VARCHAR(64) NOT NULL',
                                                                  'Mail'  : 'TINYINT(1) DEFAULT 0',
                                                                  'Notification' : 'TINYINT(1) DEFAULT 1',
                                                                  'SMS'   : 'TINYINT(1) DEFAULT 0',
                                                        },
                                            'Indexes' : { 'AlarmID' : [ 'AlarmId' ] }
                                          }
      
    if 'ntf_Notifications' not in tablesInDB:
      tablesToCreate[ 'ntf_Notifications' ] = { 'Fields' : { 'Id'   : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                                 'User'  : 'VARCHAR(64) NOT NULL',
                                                                 'Message'  : 'BLOB NOT NULL',
                                                                 'Seen' : 'TINYINT(1) NOT NULL DEFAULT 0',
                                                                 'Expiration' : 'DATETIME',
                                                                 'DeferToMail' : 'TINYINT(1) NOT NULL DEFAULT 1',
                                                        },
                                                    'PrimaryKey' : 'Id',
                                                  }
      
    if tablesToCreate:
      return self._createTables( tablesToCreate )
    return S_OK()
    
  def __checkAlarmField( self, name, value ):
    name = name.lower()
    if name == 'status':
      if value not in self.__validAlarmStatus:
        return S_ERROR( "Status %s is invalid" % alarmDef[ 'status' ] )
    elif name == 'type':
      if value not in self.__validAlarmTypes:
        return S_ERROR( "Type %s is invalid" % alarmDef[ 'type' ] )
    elif name == 'assignee':
      result = self.getUserAsignees( value )
      if not result[ 'OK' ]:
        return result
      if not result[ 'Value' ]:
        return S_ERROR( "%s is not a known assignee" % alarmDef[ 'assignee' ] )
      return result
    return S_OK()

  def newAlarm( self, alarmDef ):
    """ Create a new alarm record
    """
    followers = ""
    for field in self.__newAlarmMandatoryFields:
      if field not in alarmDef:
        return S_ERROR( "Oops. Missing %s" % field )
      result = self.__checkAlarmField( field, alarmDef[ field ] )
      if not result[ 'OK' ]:
        return result
      if field == 'assignee':
        followers = result[ 'Value' ]
    creator = alarmDef[ 'creator' ]
    if creator not in followers:
      followers.append( creator )
      
    sqlFieldsName = []
    sqlFieldsValue = []
    for field in self.__newAlarmMandatoryFields:
      sqlFieldsName.append( field )
      val = alarmDef[ field ]
      result = self._escapeString( val )
      if result['OK']:
        sqlFieldsValue.append( result['Value'] )
      else:
        return S_ERROR('Failed to escape value %s' % val )
    sqlFieldsName.extend( [ 'CreationTime', 'ModTime' ] )
    sqlFieldsValue.extend( [ 'UTC_TIMESTAMP()', 'UTC_TIMESTAMP()' ] )
      
    
    sqlInsert = "INSERT INTO `ntf_Alarms` (%s) VALUES (%s)" % ( ",".join( sqlFieldsName ), 
                                                                    ",".join( sqlFieldsValue ) )
    
    result = self._update( sqlInsert )
    if not result['OK']:
      return result
    alarmId = result[ 'lastRowId' ]
    for follower in followers:
      result = self.modifyFollowerForAlarm( alarmId, follower, 1, 0, 0)
      if not result[ 'OK' ]:
        varMsg = "\nFollower: %s\nAlarm: %s\nError: %s" % ( follower, alarmId, result['Message'] )
        self.log.error( "Couldn't set follower for alarm", varMsg )
    self.__notifyAlarm( alarmId )
    return S_OK( alarmId )
  
  def __processUpdateAlarmModifications( self, modifications ):
    if type( modifications ) != types.DictType:
      return S_ERROR( "Modifications must be a dictionary" )
    updateFields = []
    followers = []
    for field in modifications:
      if field not in self.__updateAlarmModificableFields:
        return S_ERROR( "%s is not a valid modificable field" % field )
      value = modifications[ field ]
      result = self.__checkAlarmField( field , value )
      if not result[ 'OK' ]:
        return result
      if field == 'assignee':
        followers = result[ 'Value' ]
      result = self._escapeString( modifications[ field ] )
      if not result[ 'OK' ]:
        return result
      updateFields.append( "%s=%s" % ( field, result[ 'Value' ] ) )
    return S_OK( ( ", ".join( updateFields ), DEncode.encode( modifications ), followers ) )
        
  def updateAlarm( self, updateReq ):
    for field in self.__updateAlarmMandatoryFields:
      if field not in updateReq:
        return S_ERROR( "Oops. Missing %s" % field )
    validReq = False
    for field in self.__updateAlarmAtLeastOneField:
      if field in updateReq:
        validReq = True
    if not validReq:
      return S_OK( "Requirement needs at least one of %s" % " ".join( self.__updateAlarmAtLeastOneField ) )
    author = updateReq[ 'author' ]
    followers = [ author ]
    if author not in CS.getAllUsers():
      return S_ERROR( "%s is not a known user" % author )
    result = self._escapeString( author )
    if not result[ 'OK' ]:
      return result
    author = result[ 'Value' ]
    try:
      alarmId  = int( updateReq[ 'id' ] )
    except:
      return S_ERROR( "Oops, Alarm id is not valid! (bad boy...)" )
    result = self._query( "SELECT AlarmId FROM `ntf_Alarms` WHERE AlarmId=%d" % alarmId )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      return S_ERROR( "Alarm %s does not exist!" % alarmId )
    sqlFields = [ 'AlarmId', 'Author', 'Timestamp' ]
    sqlValues = [ "%d" % alarmId, author, 'UTC_TIMESTAMP()' ]
    rawComment = ""
    if 'comment' in updateReq:
      rawComment = updateReq[ 'comment' ]
      result = self._escapeString( rawComment )
      if not result[ 'OK' ]:
        return result
      sqlFields.append( "Comment" )
      sqlValues.append( result[ 'Value' ] )
    modifications = False
    if 'modifications' in updateReq:
      modifications = updateReq[ 'modifications' ]
      result = self.__processUpdateAlarmModifications( modifications )
      if not result[ 'OK' ]:
        return result
      alarmModsSQL, encodedMods, newFollowers = result[ 'Value' ]
      sqlFields.append( "Modifications" )
      result = self._escapeString( encodedMods )
      if not result[ 'OK' ]:
        return result
      sqlValues.append( result[ 'Value' ] )
      if newFollowers:
        followers.extend( newFollowers )
    logSQL = "INSERT INTO `ntf_AlarmLog` (%s) VALUES (%s)" % ( ",".join( sqlFields ), ",".join( sqlValues ) )
    result = self._update( logSQL )
    if not result[ 'OK' ]:
      return result
    modSQL = "ModTime=UTC_TIMESTAMP()"
    if modifications:
      modSQL = "%s, %s" % ( modSQL, alarmModsSQL )
    updateSQL = "UPDATE `ntf_Alarms` SET %s WHERE AlarmId=%d" % ( modSQL, alarmId )
    result = self._update( updateSQL )
    if not result[ 'OK' ]:
      return result
    for follower in followers:
      result = self.modifyFollowerForAlarm( alarmId, follower, 1, 0, 0, overwrite = False )
      if not result[ 'OK' ]:
        varMsg = "\nFollower: %s\nAlarm: %s\nError: %s" % ( follower, alarmId, result['Message'] )
        self.log.error( "Couldn't set follower for alarm", varMsg )
    return self.__notifyAlarm( alarmId )
  
  def __notifyAlarm( self, alarmId ):
    result = self.getSubscribersForAlarm( alarmId )
    if not result[ 'OK' ]:
      return result
    subscribers = result[ 'Value' ]
    #TODO: HERE
    print subscribers
    return S_OK()

  def getAlarms( self, condDict = {}, modifiedAfter = False, sortList = False, start = 0, limit = 0 ):
    
    condSQL = []
    for field in self.__alarmQueryFields:
      if field in condDict:
        fieldValues = []
        for value in condDict[ field ]:
          result = self._escapeString( value )
          if not result[ 'OK' ]:
            return result
          fieldValues.append( result[ 'Value' ] )
        condSQL.append( "%s in ( %s )" % ( field, ",".join( fieldValues ) ) )
    
    print condSQL
    
    selSQL = "SELECT %s FROM `ntf_Alarms`" % ",".join( self.__alarmQueryFields )
    if modifiedAfter:
      condSQL.append( "ModTime >= %s" % modifiedAfter.strftime( "%Y-%m-%d %H:%M:%S" ) )
    if condSQL:
      selSQL = "%s WHERE %s" % ( selSQL, " AND ".join( condSQL ) )
    if sortList:
      selSQL += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    if limit:
      selSQL += " LIMIT %d,%d" % ( start, limit )

    result = self._query( selSQL )
    if not result['OK']:
      return result

    resultDict = {}
    resultDict['ParameterNames'] = self.__alarmQueryFields
    resultDict['Records'] = [ list(v) for v in result['Value'] ]
    return S_OK( resultDict )

###
# Followers management
###    

  def modifyFollowerForAlarm( self, alarmId, user, mail, notification, sms, overwrite = True ):
    rawUser = user
    if rawUser not in CS.getAllUsers():
      return S_OK()
    result = self._escapeString( user )
    if not result[ 'OK' ]:
      return result
    user = result[ 'Value' ]
    subscriber = mail or notification or sms
    selSQL = "SELECT Mail, Notification, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d AND User=%s" % ( alarmId, user )
    result = self._query( selSQL )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      if not subscriber:
        return S_OK()
      sqlValues = [ "%d" % alarmId, user ]
      for v in ( mail, notification, sms ):
        if v:
          sqlValues.append( "1" )
        else:
          sqlValues.append( "0" )
      inSQL = "INSERT INTO `ntf_AlarmFollowers` ( AlarmId, User, Mail, Notification, SMS ) VALUES (%s)" % ",".join( sqlValues )
      return self._update( inSQL )
    sqlCond = "AlarmId=%d AND User=%s" % ( alarmId, user )
    #Need to delete
    if not subscriber:
      return self._update( "DELETE FROM `ntf_AlarmFollowers` WHERE %s" % sqlCond)
    if not overwrite:
      return S_OK()
    #Need to update
    modSQL = []
    for k, v in ( ( 'Mail', mail ), ( 'Notification', notification ), ( 'SMS', sms ) ):
      if v:
        modSQL.append( "%s=1" % k )
      else:
        modSQL.append( "%s=0" % k )
    return self._update( "UPDATE `ntf_AlarmFollowers` SET %s WHERE %s" % ( modSQL, sqlCond ) )

  def getSubscribersForAlarm( self, alarmId ):
    selSQL = "SELECT User, Mail, Notification, SMS FROM `ntf_AlarmFollowers` WHERE AlarmId=%d" % alarmId
    result = self._query( selSQL )
    if not result[ 'OK' ]:
      return result
    fw = result[ 'Value' ]
    followWays = { 'mail' : [], 'notification' : [], 'sms' : [] }
    followers = []
    for user, mail, Notification, SMS in fw:
      if user in followers:
        continue
      followers.append( user )
      if mail:
        followWays[ 'mail' ].append( user )
      if Notification:
        followWays[ 'notification' ].append( user )
      if SMS:
        followWays[ 'sms' ].append( user )
    return S_OK( followWays )
    
      
###
# Assignee groups management
###
  
  def getUserAsignees( self, assignee ):
    #Check if it is a user
    if assignee in CS.getAllUsers():
      return S_OK( [ assignee ] )
    result = self._escapeString( assignee )
    if not result[ 'OK' ]:
      return result
    escAG = result[ 'Value' ]
    sqlSel = "SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = %s" % escAG
    result = self._query( sqlSel )
    if not result[ 'OK' ]:
      return result
    users = [ row[0] for row in result[ 'Value' ] ]
    if not users:
      return S_OK( [] )
    return S_OK( users )
    
  def setAssigneeGroup( self, groupName, usersList ):
    validUsers = CS.getAllUsers()
    result = self._escapeString( groupName )
    if not result[ 'OK' ]:
      return result
    escGroup = result[ 'Value' ]
    sqlSel = "SELECT User FROM `ntf_AssigneeGroups` WHERE AssigneeGroup = %s" % escGroup
    result = self._query( sqlSel )
    if not result[ 'OK' ]:
      return result
    currentUsers = [ row[0] for row in result[ 'Value' ] ]
    usersToDelete = []
    usersToAdd = []
    finalUsersInGroup = len( currentUsers )
    for user in currentUsers:
      if user not in usersList:
        result = self._escapeString( user )
        if not result[ 'OK' ]:
          return result
        usersToDelete.append( result[ 'Value' ] )
        finalUsersInGroup -= 1
    for user in usersList:
      if user not in validUsers:
        continue
      if user not in currentUsers:
        result = self._escapeString( user )
        if not result[ 'OK' ]:
          return result
        usersToAdd.append( "( %s, %s )" % ( escGroup, result[ 'Value' ] ) )
        finalUsersInGroup += 1
    if not finalUsersInGroup:
      return S_ERROR( "Group must have at least one user!" )
    #Delete old users
    if usersToDelete:
      sqlDel = "DELETE FROM `ntf_AssigneeGroups` WHERE User in ( %s )" % ",".join( usersToDelete )
      result = self._update( sqlDel )
      if not result[ 'OK' ]:
        return result
    #Add new users
    if usersToAdd:
      sqlInsert = "INSERT INTO `ntf_AssigneeGroups` ( AssigneeGroup, User ) VALUES %s" % ",".join( usersToAdd )
      result = self._update( sqlInsert )
      if not result[ 'OK' ]:
        return result
    return S_OK()
  
  def deleteAssigneeGroup( self, groupName ):
    result = self._escapeString( groupName )
    if not result[ 'OK' ]:
      return result
    escGroup = result[ 'Value' ]
    sqlSel = "SELECT AlarmId FROM `ntf_Alarms` WHERE Assignee=%s" % escGroup 
    result = self._query( sqlSel )
    if not result[ 'OK' ]:
      return result
    if result[ 'Value' ]:
      alarmIds = [ row[0] for row in result[ 'Value' ] ]
      return S_ERROR( "There are %s alarms assigned to this group" % len( alarmIds ) )
    sqlDel = "DELETE FROM `ntf_AssigneeGroups` WHERE AssigneeGroup=%s" % escGroup
    return self._update( sqlDel )
  
  def getAssigneeGroups( self ):
    result = self._query( "SELECT AssigneeGroup, User from `ntf_AssigneeGroups` ORDER BY User" )
    if not result[ 'OK' ]:
      return result
    agDict = {}
    for row in result[ 'Value' ]:
      ag = row[0]
      user = row[1]
      if ag not in agDict:
        agDict[ ag ] = []
      agDict[ ag ].append( user )
    return S_OK( agDict )
  
###
# Notifications
###
  
  def addNotificationForUser( self, user, message, lifetime = 0, deferToMail = 1 ):
    if user not in CS.getAllUsers():
      return S_ERROR( "%s is an unknown user" % user )
    result = self._escapeString( user )
    if not result[ 'OK' ]:
      return result
    user = result[ 'Value' ]
    result = self._escapeString( message )
    if not result[ 'OK' ]:
      return result
    message = result[ 'Value' ]
    sqlFields = [ 'User', 'Message' ]
    sqlValues = [ user, message ]
    if not deferToMail:
      sqlFields.append( "DeferToMail" )
      sqlValues.append( "0" )
    if lifetime:
      sqlFields.append( "Expiration" )
      sqlValues.append( "TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % int( lifetime ) )
    result = self._update( "INSERT INTO `ntf_Notifications` (%s) VALUES (%s) " % ( ",".join(sqlFields), 
                                                                                   ",".join(sqlValues) ) )
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'lastRowId' ] )
  
  def removeNotificationsForUser( self, user, msgIds = False ):
    if user not in CS.getAllUsers():
      return S_ERROR( "%s is an unknown user" % user )
    result = self._escapeString( user )
    if not result[ 'OK' ]:
      return result
    user = result[ 'Value' ]
    delSQL = "DELETE FROM `ntf_Notifications` WHERE User=%s" % user
    escapedIDs = []
    if msgIds:
      for id in msgIds:
        result = self._escapeString( str( id ) )
        if not result[ 'OK' ]:
          return result
        escapedIDs.append( result[ 'Value' ] )
      delSQL = "%s AND Id in ( %s ) " % ( delSQL, ",".join( escapedIDs ) ) 
    return self._update( delSQL )
  
  def markNotificationsAsRead( self, user, msgIds = False ):
    if user not in CS.getAllUsers():
      return S_ERROR( "%s is an unknown user" % user )
    result = self._escapeString( user )
    if not result[ 'OK' ]:
      return result
    user = result[ 'Value' ]
    updateSQL = "UPDATE `ntf_Notifications` SET Read=1 WHERE User=%s" % user
    escapedIDs = []
    if msgIds:
      for id in msgIds:
        result = self._escapeString( str( id ) )
        if not result[ 'OK' ]:
          return result
        escapedIDs.append( result[ 'Value' ] )
      updateSQL = "%s AND Id in ( %s ) " % ( delSQL, ",".join( escapedIDs ) ) 
    return self._update( updateSQL )
  
  def getNotifications( self, condDict = {}, sortList = False, start = 0, limit = 0 ):
    
    condSQL = []
    for field in self.__notificationQueryFields:
      if field in condDict:
        fieldValues = []
        for value in condDict[ field ]:
          result = self._escapeString( value )
          if not result[ 'OK' ]:
            return result
          fieldValues.append( result[ 'Value' ] )
        condSQL.append( "%s in ( %s )" % ( field, ",".join( fieldValues ) ) )
    
    selSQL = "SELECT %s FROM `ntf_Notifications`" % ",".join( self.__notificationQueryFields )
    if condSQL:
      selSQL = "%s WHERE %s" % ( selSQL, " AND ".join( condSQL ) )
    if sortList:
      selSQL += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    else:
      selSQL += " ORDER BY Id DESC"
    if limit:
      selSQL += " LIMIT %d,%d" % ( start, limit )

    print selSQL

    result = self._query( selSQL )
    if not result['OK']:
      return result

    resultDict = {}
    resultDict['ParameterNames'] = self.__notificationQueryFields
    resultDict['Records'] = [ list(v) for v in result['Value'] ]
    return S_OK( resultDict )
    
            
       
      
    
    
    
    
    
    
