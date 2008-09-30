# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/DB/SystemLoggingDB.py,v 1.19 2008/09/30 15:24:30 mseco Exp $
__RCSID__ = "$Id: SystemLoggingDB.py,v 1.19 2008/09/30 15:24:30 mseco Exp $"
""" SystemLoggingDB class is a front-end to the Message Logging Database.
    The following methods are provided

    insertMessageIntoSystemLoggingDB()
    getMessagesByDate()
    getMessagesByFixedText()
    getMessages()
"""    

import re, os, sys, string
import time
import threading
from types import ListType, StringTypes, NoneType

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, gConfig, S_OK, S_ERROR
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
    conjonction = ''

    if condDict:
      for attrName, attrValue in condDict.items():
        preCondition = ''
        conjonction = ''
        if type( attrValue ) in StringTypes:
          attrValue = [ attrValue ]
        elif  type( attrValue) is NoneType:
          continue
        elif not type( attrValue ) is ListType:
          errorString = 'The values of conditions should be strings or lists'
          errorDesc = 'The type provided was: %s' % type ( attrValue )
          gLogger.warn( errorString, errorDesc )
          return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )
        
        for attrVal in attrValue:
          preCondition = "%s%s %s='%s'" % ( preCondition, conjonction,
                                            str( attrName ), str( attrVal ) )
          conjonction = " OR"

        if condition:
          condition += " AND" 
        condition += ' (%s )' % preCondition

      conjonction = " AND"
      
    if older:
      condition = "%s%s MessageTime<'%s'" % ( condition,  conjonction,
                                              str( older ) )
      conjonction = " AND"

    if newer:
      condition = "%s%s MessageTime>'%s'" % ( condition, conjonction,
                                              str( newer ) )

    if condition:
      gLogger.debug( '__buildcondition:',
                     'condition string = "%s"' % condition )
      condition = " WHERE%s" % condition
      
    return S_OK(condition)

  def _buildConditionTest(self, condDict, olderDate = None, newerDate = None ):
    """ a wrapper to the private function __buildCondition so test programs
        can access it
    """
    return self.__buildCondition( condDict, older = olderDate,
                                  newer = newerDate )

  def __uniq( self, array ):
    """http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52560
    """

    arrayLength = len( array )
    if arrayLength == 0:
        return []

    sortDictionary = {}

    for key in array:
      sortDictionary[ key ] = 1
    return sortDictionary.keys()

      
  def __buildTableList( self, showFieldList ):
    """ build the SQL list of tables needed for the query
        from the list of variables provided
    """
    import re
    iDpattern = re.compile( r'ID' )   
    tableDict = { 'MessageTime':'MessageRepository',
                  'VariableText':'MessageRepository',
                  'LogLevel':'MessageRepository',
                  'FixedTextString':'FixedTextMessages',
                  'ReviewedMessage':'FixedTextMessages',
                  'SystemName':'Systems', 'SubSystemName':'SubSystems',
                  'OwnerDN':'UserDNs', 'OwnerGroup':'UserDNs',
                  'ClientIPNumberString':'ClientIPs',
                  'ClientFQDN':'ClientIPs', 'SiteName':'Sites'}
    tableDictKeys = tableDict.keys()
    tableList = []

    conjunction = ' NATURAL JOIN '

    gLogger.debug( '__buildTableList:', 'showFieldList = %s' % showFieldList )

    if len(showFieldList):
      for field in showFieldList:
        if not iDpattern.search( field ) and ( field in tableDictKeys ):
          tableList.append( tableDict[field] )

      tableList = self.__uniq(tableList)
      if len( tableList ) == 1:
        tableString = tableList[0]
      else:
        if tableList.count('MessageRepository'):
          tableList.remove('MessageRepository')
        tableString = 'MessageRepository'

        tableString = '%s%s%s' % ( tableString, conjunction,
                                   conjunction.join( tableList ))

    else:
      tableString = conjunction.join( self.__uniq( tableDict.values() ) )

    gLogger.debug( '__buildTableList:', 'tableString = "%s"' % tableString )
    
    return tableString
  
  def _queryDB( self, showFieldList=None, condDict=None, older=None,
                 newer=None, count=False, groupColumn=None, orderFields=None ):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and queries the SystemLoggingDB.
        If no list is provided the default is to use all the meaninful
        variables of the DB
    """
    grouping=''
    ordering=''
    result = self.__buildCondition( condDict, older, newer )
    if not result['OK']: return result
    condition = result['Value']

    if not showFieldList:
      showFieldList = ['MessageTime', 'LogLevel', 'FixedTextString',
                     'VariableText', 'SystemName', 
                     'SubSystemName', 'OwnerDN', 'OwnerGroup',
                     'ClientIPNumberString','SiteName']
    elif type( showFieldList ) in StringTypes:
      showFieldList = [ showFieldList ]
    elif not type( showFieldList ) is ListType:
      errorString = 'The showFieldList variable should be a string or a list of strings'
      errorDesc = 'The type provided was: %s' % type ( attrValue )
      gLogger.warn( errorString, errorDesc )
      return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )
       
    tableList = self.__buildTableList(showFieldList)

    if count: 
      if groupColumn:
        grouping='GROUP BY %s' % groupColumn
        showFieldList.append( 'count(*) as recordCount' )
      else:
        showFieldList = [ 'count(*) as recordCount' ]

    sortingFields = []
    if orderFields:
      for field in orderFields:
        if type( field ) == ListType:
          sortingFields.append( ' '.join(field) )
        else:
          sortingFields.append( field ) 
    	ordering='ORDER BY %s' %  ', '.join( sortingFields )
    	
    cmd = 'SELECT %s FROM %s %s %s %s' % (','.join(showFieldList),
                                    tableList, condition, grouping, ordering)

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
      
  def _insertMessageIntoSystemLoggingDB( self, message, site, nodeFQDN, 
                                         userDN, userGroup, remoteAddress ):
    """ This function inserts the Log message into the DB
    """
    messageDate = Time.toString( message.getTime() )
    messageDate = messageDate[:messageDate.find('.')]
    messageName = message.getName()
    messageSubSystemName = message.getSubSystemName()
    
    fieldsList = [ 'MessageTime', 'VariableText' ]
    messageList = [ messageDate, message.getVariableMessage() ]

    inValues = [ userDN, userGroup ]
    inFields = [ 'OwnerDN', 'OwnerGroup' ]
    outFields = [ 'UserDNID' ]
    result = self.__DBCommit( 'UserDNs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend( outFields )
      
    inValues = [ remoteAddress, nodeFQDN ]
    inFields = [ 'ClientIPNumberString', 'ClientFQDN' ]
    outFields = [ 'ClientIPNumberID' ]
    result = self.__DBCommit( 'ClientIPs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend( outFields )

    if not site:
      site = 'Unknown'
    inFields = [ 'SiteName' ]
    inValues = [ site ]
    outFields = [ 'SiteID' ]
    result = self.__DBCommit( 'Sites', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend( outFields )

    messageList.append(message.getLevel())
    fieldsList.append( 'LogLevel' )
    
    inFields = [ 'FixedTextString' ]
    inValues = [ message.getFixedMessage() ]
    outFields = [ 'FixedTextID' ]
    result = self.__DBCommit( 'FixedTextMessages', outFields, inFields,
                              inValues)
    if not result['OK']:
      return result
    messageList.append( result['Value'] )
    fieldsList.extend( outFields )

    if not messageName:
      messageName = 'Unknown'
    inFields = [ 'SystemName' ]
    inValues = [ messageName ]
    outFields = [ 'SystemID' ]
    result = self.__DBCommit( 'Systems', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend( outFields )

    if not messageSubSystemName:
      messageSubSystemName = 'Unknown'
    inFields = [ 'SubSystemName' ]
    inValues = [ messageSubSystemName ]
    outFields = [ 'SubSystemID' ]
    result = self.__DBCommit( 'SubSystems', outFields, inFields, inValues )
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend( outFields )

    return self._insert( 'MessageRepository', fieldsList, messageList )

  def _insertDataIntoAgentTable(self, agentName, data):
    """Insert the persistent data needed by the agents running on top of
       the SystemLoggingDB.
    """
    outFields = ['AgentID']
    inFields = [ 'AgentName' ]
    inValues = [ agentName ]
    result = self._getFields('AgentPersitentData', outFields, inFields, inValues)
    if not result ['OK']:
      return result
    elif result['Value'] == ():
      inFields = [ 'AgentName', 'AgentData' ]
      inValues = [ agentName, data]
      result=self._insert( 'AgentPersitentData', inFields, inValues )
      if not result['OK']:
        return result
    escapeData = self._escapeString( data )
    cmd = "UPDATE LOW PRIORITY AgentPersitentData SET AgentData='%s' WHERE AgentID=%s" % \
          ( agentName, result['Value'] )
    return self._update(update)
