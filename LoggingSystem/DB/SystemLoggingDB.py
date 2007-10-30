""" SystemLoggingDB class is a front-end to the Message Logging Database.
    The following methods are provided

    insertMessageIntoDB()
    getMessagesByDate()
    getMessagesByFixedText()
    getMessages()
"""    

import re, os, sys, string
import time
import threading

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config   import gConfig
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, dateTime, hour, date, week, day, fromString, toString
from DIRAC.LoggingSystem.private.LogLevels import LogLevels

###########################################################
class SystemLoggingDB(DB):

  def __init__(self, maxQueueSize=10):
    """ Standard Constructor
    """
    DB.__init__( self, 'SystemLoggingDB', 'Logging/SystemLoggingDB',
                 maxQueueSize)

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
          condition = " %s %s %s='%s'" % ( condition,
                                           conjonction,
                                           str( attrName ),
                                           str( attrVal )  )
          conjonction = " OR "

        conjonction = ") AND ("

      condition = ' %s )' % ( condition )
      conjonction = " AND "
    else:
      conjonction = "WHERE"

    if older:
      condition = " %s %s MessageTime <'%s'" % ( condition,
                                                  conjonction,
                                                  str( older ) )
      conjonction = "AND"

    if newer:
      condition = " %s %s MessageTime >'%s'" % ( condition,
                                                 conjonction,
                                                 str( newer ) )

    return condition

  def __removeVariables( self, fieldList ):
    """ auxiliar function of __buildTableList.  
    """
    internalList = list( set( fieldList ) )

    if internalList.count( 'VariableText' ):
      internalList.remove( 'VariableText' )
      internalList = list( set( intList.append( 'MessageTime' ) ) )

    if internalList.count( 'LogLevel' ):
      internalList.remove( 'LogLevel' )
      internalList = list( set( intList.append( 'MessageTime' ) ) )

    if internalList.count( 'OwnerGroup' ):
      internalList.remove( 'OwnerGroup' )
      internalList = list( set( intList.append( 'OwnerDN' ) ) )

    return internalList

      
  def __buildTableList( self, showFieldList ):
    """ build the SQL list of tables needed for the query
        from the list of variables provided
    """
    tableDict = { 'MessageTime':'MessageRepository',
                  'FixedTextString':'FixedTextMessages',
                  'SystemName':'Systems', 'SubSystemName':'SubSystems',
                  'OwnerDN':'UserDNs',  
                  'ClientIPNumberString':'ClientIPs', 'SiteName':'Sites'}

    conjunction = ' NATURAL JOIN '

    tableList = tableDict.values()
    tableList.remove( 'MessageRepository' )
    tableList.insert( 0, 'MessageRepository' )
    tableString = conjunction.join( tableList )

    if not len(showFieldList):
      fieldList=self.__removeVariables(showFieldList)

      if fieldList.count( 'MessageTime' ):
        fieldList.remove( 'MessageTime' )
        
      tableString = 'MessageRepository'
      if not len(fieldList):
          tableString = '%s%s' %  ( tableString, conjunction )

      for field in fieldList:
        tableString = '%s %s' % ( tableString, TableDict[field] )
        if not len( fieldList ):
          tableString = '%s%s' %  ( tableString, conjunction )

    return tableString

  def __queryDB( self, showFieldList=None, condDict=None, older=None, newer=None ):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and queries the SystemLoggingDB.
        If no list is provided the default is to use all the meaninful
        variables of the DB
    """
    cond = self.__buildCondition( condDict, older, newer )

    if showFieldList == None:
      showFieldList = ['MessageTime', 'LogLevel', 'FixedTextString',
                     'VariableText', 'SystemName', 
                     'SubSystemName', 'OwnerDN', 'OwnerGroup',
                     'ClientIPNumberString','SiteName']
            
    cmd = 'SELECT %s FROM %s %s' % (','.join(showFieldList),
                                    self.__buildTableList(showFieldList), cond)

    return self._query(cmd)

  def __DBCommit( self, tableName, outFields, inFields, inValues ):
    """  This is an auxiliary function to insert values on a
         satellite Table if they do not exist and returns
         the unique KEY associated to the given set of values
    """
    result = self._getFields( tableName, outFields, inFields, inValues )
    if not result['OK']:
      return S_ERROR('Unable to query the database')
    elif result['Value']==():
      result = self._insert( tableName, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Could not insert the data into %s table' % tableName )

      result = self._getFields( tableName, outFields, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Unable to query the database' )

    return S_OK( int( result['Value'][0][0] ) )
      
  def insertMessageIntoDB( self, Message, UserDN, usergroup, remoteAddress ):
    """ This function inserts the Logging message into the DB
    """
    messageName = Message.getName()
    messageSubSysName = Message.getSubSystemName()
    messageLevel = Message.getLevel() 
    messageDate = Time.toString( Message.getTime() )
    messageDate = messageDate[:messageDate.find('.')]
    messageSite = Message.getSite()

    result = self._escapeString( Message.getFixedMessage() )
    if result['OK']:
      messageFixedText = result['Value']
    else:
      return result
    
    result = self._escapeString( Message.getVariableMessage() )
    if result['OK']:
      messageVariableText = result['Value']
      if messageVariableText=='':
        messageVariableText = "'No variable text'"
    else:
      return result
    
    fieldsList = [ 'MessageTime', 'VariableText' ]
    messageList = [ messageDate, messageVariableText ]

    inValues = [ UserDN, usergroup ]
    inFields = [ 'OwnerDN', 'OwnerGroup' ]
    outFields = [ 'UserDNID' ]
    result = self.__DBCommit( 'UserDNs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )
      
    inValues = [ remoteAddress ]
    inFields = [ 'ClientIPNumberString' ]
    outFields = [ 'ClientIPNumberID' ]
    result = self.__DBCommit( 'ClientIPs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )

    if not messageSite:
      messageSite = 'Unknown'
    inFields = [ 'SiteName' ]
    inValues = [ messageSite ]
    outFields = [ 'SiteID' ]
    result = self.__DBCommit( 'Sites', outFields, inFields, inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )

    messageList.append(messageLevel)
    fieldsList.append( 'LogLevel' )
    
    inFields = [ 'FixedTextString' ]
    inValues = [ messageFixedText ]
    outFields = [ 'FixedTextID' ]
    result = self.__DBCommit( 'FixedTextMessages', outFields, inFields,
                              inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )

    if not messageName:
      messageName = 'Unknown'
    inFields = [ 'SystemName' ]
    inValues = [ messageName ]
    outFields = [ 'SystemID' ]
    result = self.__DBCommit( 'Systems', outFields, inFields, inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )

    if not messageSubSysName:
      messageSubSysName = 'Unknown'
    inFields = [ 'SubSystemName' ]
    inValues = [ messageSubSysName ]
    outFields = [ 'SubSystemID' ]
    result = self.__DBCommit( 'SubSystems', outFields, inFields, inValues)
    if not result['OK']:
      return result
    else:
      messageList.append(result['Value'])
      fieldsList.extend( outFields )

    return self._insert( 'MessageRepository', fieldsList, messageList )


  def getMessagesByDate(self, initialDate = None, endDate = None):
    """ Query the database for all the messages between two given dates.
        If no date is provided then the records returned are those generated
        during the last 24 hours.
    """
    from DIRAC.Core.Utilities import dateTime, day

    if not (initialDate or endDate):
      initialDate= dateTime() - 1 * day 
      
    return self.__queryDB( newer = initialDate, older = endDate )

  def getMessagesByFixedText( self, texts, initialDate = None, endDate = None ):
    """ Query the database for all messages whose fixed part match 'texts'
        that were generated between initialDate and endDate
    """
    return self.__queryDB( condDict = {'FixedTextString': texts},
                             older = endDate, newer = initialDate )
    
  def getMessagesBySite(self, site, initialDate = None, endDate = None ):
    """ Query the database for all messages generated by 'site' that were
        generated between initialDate and endDate     
    """
    return self.__queryDB( condDict = { 'SiteName':  site},
                             older = endDate, newer = initialDate )
 
  def getMessages(self, conds , initialDate = None, endDate = None ):
    """ Query the database for all messages satisfying 'conds' that were 
        generated between initialDate and endDate
    """
    return self.__queryDB( condDict = conds, older = endDate,
                             newer = initialDate )

