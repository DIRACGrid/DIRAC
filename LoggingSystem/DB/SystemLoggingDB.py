""" SystemLoggingDB class is a front-end to the Message Logging Database.
    The following methods are provided

    insertMsgIntoDB()
    getMsgByDate()
    getMsgByMainMsg()
    getMsgs()
"""    

import re, os, sys, string
import time
import threading

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, dateTime, hour, date, week, day
from DIRAC.LoggingSystem.private.LogLevels import LogLevels

###########################################################
class SystemLoggingDB(DB):

  def __init__(self, maxQueueSize=10):
    """ Standard Constructor
    """
    DB.__init__(self,'SystemLoggingDB','Logging/SystemLoggingDB',maxQueueSize)

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''

    if condDict != None:
      conjonction = "WHERE ("
      for attrName, attrValue in condDict.items():
        if isinstance( attrValue, str ):
          attrValue = [ attrValue ]
        
        for attrVal in attrValue: 
          condition = ' %s %s %s=\'%s\'' % ( condition,
                                             conjonction,
                                             str(attrName),
                                             str(attrVal)  )
          conjonction = " OR "

          
        conjonction = ") AND ("

      condition = ' %s )' % ( condition )
      conjonction = " AND "
    else:
      conjonction = "WHERE"

    if older:
      condition = ' %s %s MsgTime <\'%s\'' % ( condition,
                                                 conjonction,
                                                 str(older) )
      conjonction = "AND"

    if newer:
      condition = ' %s %s MsgTime >\'%s\'' % ( condition,
                                                 conjonction,
                                                 str(newer) )

    return condition

  def __UniqVal( self, VarList ):
    IntList=list(set(VarList))

    if IntList.count('VartxtString'):
      IntList.remove('VartxtString')
      IntList=list(set(IntList.append('MsgTime')))

    if IntList.count('OwnerGroup'):
      IntList.remove('OwnerGroup')
      IntList=list(set(IntList.append('OwnerDN')))

    return IntList

      
  def __buildTableList( self, showVarList ):
    """ build the SQL list of tables needed for the query
        from the list of variables provided
    """
    TableDict={'MsgTime':'DateStamps', 'FixtxtString':'FixtxtmsgTable',
                 'SystemName':'System', 'SubSystemName':'SubSystem',
                 'FrameName':'Frame', 'LogLevelName':'LogLevels',
                 'OwnerDN':'UserDNs', 'ClientIPNumberString':'ClientIPs'}

    conjunction=' NATURAL JOIN '

    TableList=TableDict.values()
    TableList.remove( 'DateStamps' )
    TableList.insert( 0, 'DateStamps' )
    tablestring=conjunction.join(TableList)

    if not len(showVarList):
      VarList=self.__UniqVal(showVarList)

      tablestring=''

      if VarList.count('MsgTime'):
        VarList.drop('MsgTime')
        tablestring='DateStamps'
        if not len(VarList):
          tablestring='%s%s' %  ( tablestring, conjunction )

      for var in VarList:
        tablestring='%s %s' % ( tablestring, TableDict[var] )
        if not len(VarList):
          tablestring='%s%s' %  ( tablestring, conjunction )

    return tablestring

  def __queryDB( self, showVarList=None, condDict={}, older=None, newer=None ):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and querys the SystemLoggingDB.
        If no list is provided the default is to use all the meaninful
        variables of the DB
    """
    cond = self.__buildCondition( condDict, older, newer )

    if showVarList == None:
      showVarList = ['MsgTime', 'LogLevelName', 'FixtxtString',
                     'VartxtString', 'SystemName', 'FrameName',
                     'SubSystemName', 'OwnerDN', 'OwnerGroup',
                     'ClientIPNumberString']
            
    cmd = 'SELECT %s FROM %s %s' % (','.join(showVarList),
                                    self.__buildTableList(showVarList), cond)

    return self._query(cmd)


  def insertMsgIntoDB( self, Msg, UserDN, usergroup, RemoteAdd ):
    """ This function inserts the Logging message into the DB
    """
    msgName = Msg.getName()
    msgSubSysName = Msg.getSubSystemName()
    msgFrameInfo = Msg.getFrameInfo()
    msgFix = Msg.getFixedMessage()
    msgVar = Msg.getVariableMessage()
    if msgVar=='':
      msgVar = "'No variable text'"
    msgLevel = LogLevels().getLevelValue( Msg.getLevel() )
    msgDate = Time.toString( Msg.getTime() )

    msgList = [ "STR_TO_DATE('%s',GET_FORMAT(DATETIME,'ISO'))" % msgDate, msgVar ]
    
    errstr = 'Could not insert the data into %s table'
    
    userDNvalues = ( UserDN, usergroup )
    qry = 'SELECT UserDNID FROM UserDNs WHERE OwnerDN="%s" AND OwnerGroup="%s"' % userDNvalues
    cmd = 'INSERT INTO UserDNs ( OwnerDN, OwnerGroup ) VALUES ( "%s", "%s" )' % userDNvalues
    result = self.DBCommit( qry, cmd, errstr % 'UserDNs' )
    if not result['OK']:
      return result
    else:
      msgList.append(result['Value'])

    qry = 'SELECT ClientIPNumberID FROM ClientIPs WHERE ClientIPNumberString="%s"' % RemoteAdd
    cmd = 'INSERT INTO ClientIPs ( ClientIPNumberString ) VALUES ( "%s" )' % RemoteAdd
    result = self.DBCommit( qry, cmd, errstr % 'ClientIPs' )
    if not result['OK']:
      return result
    else:
      msgList.append(result['Value'])

    msgList.append(msgLevel)
    
    qry = 'SELECT FixtxtID FROM FixtxtmsgTable WHERE FixtxtString="%s"' % msgFix
    cmd = 'INSERT INTO FixtxtmsgTable ( FixtxtString ) VALUES ( "%s" )' % msgFix
    result = self.DBCommit( qry, cmd, errstr % 'FixtxtmsgTable' )
    if not result['OK']:
      return result
    else:
      msgList.append(result['Value'])


    if not msgName:
      msgName = 'No system'
    qry = 'SELECT SystemID FROM System WHERE SystemName="%s"' % msgName
    cmd = 'INSERT INTO System ( SystemName ) VALUES ( "%s" )' % msgName
    result = self.DBCommit( qry, cmd, errstr % 'System' )
    if not result['OK']:
      return result
    else:
      msgList.append(result['Value'])

    if not msgSubSysName:
      msgSubSysName = 'No subsystem'
    qry = 'SELECT SubSystemID FROM SubSystem WHERE SubSystemName="%s"' % msgSubSysName
    cmd = 'INSERT INTO SubSystem ( SubSystemName ) VALUES ( "%s" )' % msgSubSysName
    result = self.DBCommit( qry, cmd, errstr % 'SubSystem' )
    if not result['OK']:
      return result
    else:
      msgList.append(result['Value'])


    if not msgFrameInfo:
      msgFrameInfo = 'No frame'
    qry = 'SELECT FrameID FROM Frame WHERE FrameName="%s"' % msgFrameInfo
    cmd = 'INSERT INTO Frame ( FrameName ) VALUES ( "%s" )' % msgFrameInfo
    result = self.DBCommit( qry, cmd, errstr % 'Frame' )
    if result['OK']:
      msgList.append(result['Value'])
    else:
      return result

    cmd = 'INSERT INTO DateStamps VALUES (%s)' % ', '.join( str(x) for x in msgList )
    result = self._update(cmd)
    if result['OK']:
      msgList.append(result['Value'])
    else:
      return result


  def DBCommit( self, qry, cmd, err ):
    """  This is an auxiliary function to insert values on a
         satellite Table if they do not exist and returns
         the unique KEY associated to the given set of values
    """
    result= self._query( qry )
    if not result['OK']:
      return S_ERROR('Unable to query the database')
    elif result['Value']==():
      result = self._update(cmd)
      if not result['OK']:
        return S_ERROR( err )

      result = self._query( qry )
      if not result['OK']:
        return S_ERROR( 'Unable to query the database' )

    return S_OK( int( result['Value'][0][0] ) )

  def getMsgByDate(self, initDate = None, endDate = None):
    """ query the database for all the messages between two given dates.
        If no date is provided the date range comprises the last week
        If no endDate is provided the date range is given by the week
        that follows initdate
    """
    if initDate == None and endDate == None:
      endDate = dateTime()
      initDate = endDate - 1 * week
    elif endDate == None:
      if dateTime() > initDate + week: 
        endDate = initDate + week
      elif dateTime() < initDate:
        return S_ERROR ( "initDate can not be set in the future" )
      else:
        endDate = dateTime
    elif initDate == None:
      if endDate > dateTime() + week:
        return S_ERROR ("endDate is too far into the future")
      initDate = endDate - 1 * week

    if endDate > dateTime():
      endDate = dateTime()

    return self.__queryDB( newer = initDate, older = endDate )

  def getMsgByMainTxt( self, Msgtxt, firstdate = None, lastdate = None ):
    return self.__queryDB( condDict = {'FixtxtString': Msgtxt},
                          older = lastdate, newer = firstdate )

  def getMsgs(self, conds , firstdate = None, lastdate = None ):
    return self.__queryDB( condDict = conds, older = lastdate,
                          newer = firstdate )


