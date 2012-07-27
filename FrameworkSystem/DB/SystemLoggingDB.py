# $HeadURL$
__RCSID__ = "$Id$"
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
from DIRAC.FrameworkSystem.private.logging.LogLevels import LogLevels

DEBUG = 0

if DEBUG:
  debugFile = open( "SystemLoggingDB.debug.log", "w" )

###########################################################
class SystemLoggingDB( DB ):

  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'SystemLoggingDB', 'Framework/SystemLoggingDB',
                 maxQueueSize )

  def _query( self, cmd, conn = False ):
    start = time.time()
    ret = DB._query( self, cmd, conn )
    if DEBUG:
      print >> debugFile, time.time() - start, cmd.replace( '\n', '' )
      debugFile.flush()
    return ret

  def _update( self, cmd, conn = False ):
    start = time.time()
    ret = DB._update( self, cmd, conn )
    if DEBUG:
      print >> debugFile, time.time() - start, cmd.replace( '\n', '' )
      debugFile.flush()
    return ret

  def __buildCondition( self, condDict, older = None, newer = None ):
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
        elif  type( attrValue ) is NoneType:
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
      condition = "%s%s MessageTime<'%s'" % ( condition, conjonction,
                                              str( older ) )
      conjonction = " AND"

    if newer:
      condition = "%s%s MessageTime>'%s'" % ( condition, conjonction,
                                              str( newer ) )

    if condition:
      gLogger.debug( '__buildcondition:',
                     'condition string = "%s"' % condition )
      condition = " WHERE%s" % condition

    return S_OK( condition )

  def _buildConditionTest( self, condDict, olderDate = None, newerDate = None ):
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
    idPattern = re.compile( r'ID' )

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
    if len( showFieldList ):
      for field in showFieldList:
        if not idPattern.search( field ) and ( field in tableDictKeys ):
          tableList.append( tableDict[field] )

      #if re.search( 'MessageTime', ','.join( showFieldList) ):
      #  tableList.append('MessageRepository')
      tableList = self.__uniq( tableList )

      tableString = ''
      try:
        tableList.pop( tableList.index( 'MessageRepository' ) )
        tableString = 'MessageRepository'
      except:
        pass

      if tableList.count( 'Sites' ) and tableList.count( 'MessageRepository' ) and not \
        tableList.count( 'ClientIPs' ):
        tableList.append( 'ClientIPs' )
      if tableList.count( 'MessageRepository' ) and tableList.count( 'SubSystems' ) \
        and not tableList.count( 'FixedTextMessages' ) and not tableList.count( 'Systems' ):
        tableList.append( 'FixedTextMessages' )
        tableList.append( 'Systems' )
      if tableList.count( 'MessageRepository' ) and tableList.count( 'Systems' ) \
        and not tableList.count( 'FixedTextMessages' ):
        tableList.append( 'FixedTextMessages' )
      if tableList.count( 'FixedTextMessages' ) and tableList.count( 'SubSystems' ) \
        and not tableList.count( 'Systems' ):
        tableList.append( 'Systems' )
      if tableList.count( 'MessageRepository' ) or ( tableList.count( 'FixedTextMessages' ) \
        + tableList.count( 'ClientIPs' ) + tableList.count( 'UserDNs' ) > 1 ) :
        tableString = 'MessageRepository'

      if tableString and len( tableList ):
        tableString = '%s%s' % ( tableString, conjunction )
      tableString = '%s%s' % ( tableString,
                                 conjunction.join( tableList ) )

    else:
      tableString = conjunction.join( self.__uniq( tableDict.values() ) )

    gLogger.debug( '__buildTableList:', 'tableString = "%s"' % tableString )

    return tableString

  def _queryDB( self, showFieldList = None, condDict = None, older = None,
                 newer = None, count = False, groupColumn = None, orderFields = None ):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and queries the SystemLoggingDB.
        If no list is provided the default is to use all the meaningful
        variables of the DB
    """
    grouping = ''
    ordering = ''
    result = self.__buildCondition( condDict, older, newer )
    if not result['OK']: return result
    condition = result['Value']

    if not showFieldList:
      showFieldList = ['MessageTime', 'LogLevel', 'FixedTextString',
                     'VariableText', 'SystemName',
                     'SubSystemName', 'OwnerDN', 'OwnerGroup',
                     'ClientIPNumberString', 'SiteName']
    elif type( showFieldList ) in StringTypes:
      showFieldList = [ showFieldList ]
    elif not type( showFieldList ) is ListType:
      errorString = 'The showFieldList variable should be a string or a list of strings'
      errorDesc = 'The type provided was: %s' % type ( showFieldList )
      gLogger.warn( errorString, errorDesc )
      return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )

    tableList = self.__buildTableList( showFieldList )

    if groupColumn:
      grouping = 'GROUP BY %s' % groupColumn

    if count:
      if groupColumn:
        showFieldList.append( 'count(*) as recordCount' )
      else:
        showFieldList = [ 'count(*) as recordCount' ]

    sortingFields = []
    if orderFields:
      for field in orderFields:
        if type( field ) == ListType:
          sortingFields.append( ' '.join( field ) )
        else:
          sortingFields.append( field )
      ordering = 'ORDER BY %s' % ', '.join( sortingFields )

    cmd = 'SELECT %s FROM %s %s %s %s' % ( ','.join( showFieldList ),
                                    tableList, condition, grouping, ordering )

    gLogger.debug( "Query------->", cmd )

    return self._query( cmd )

  def __DBCommit( self, tableName, outFields, inFields, inValues ):
    """  This is an auxiliary function to insert values on a
         satellite Table if they do not exist and returns
         the unique KEY associated to the given set of values
    """

    #tableDict = { 'MessageRepository':'MessageTime',
    #              'MessageRepository':'VariableText',
    #              'MessageRepository':'LogLevel',
    #              'FixedTextMessages':'FixedTextString',
    #              'FixedTextMessages':'ReviewedMessage',
    #              'Systems':'SystemName', 
    #              'SubSystems':'SubSystemName',
    #              'UserDNs':'OwnerDN', 
    #              'UserDNs':'OwnerGroup',
    #              'ClientIPs':'ClientIPNumberString',
    #              'ClientIPs':'ClientFQDN', 
    #              'Sites':'SiteName'}

    cmd = "SHOW COLUMNS FROM " + tableName + " WHERE Field in ( '" \
          + "', '".join( inFields ) + "' )"
    result = self._query( cmd )
    gLogger.verbose( result )
    if ( not result['OK'] ) or result['Value'] == ():
      gLogger.debug( result['Message'] )
      return S_ERROR( 'Could not get description of the %s table' % tableName )
    for description in result['Value']:
      if re.search( 'varchar', description[1] ):
        indexInteger = inFields.index( description[0] )
        valueLength = len( inValues[ indexInteger ] )
        fieldLength = int( re.search( r'varchar\((\d*)\)',
                           description[1] ).groups()[0] )
        if fieldLength < valueLength:
          inValues[ indexInteger ] = inValues[ indexInteger ][ :fieldLength ]

    result = self._getFields( tableName, outFields, inFields, inValues )
    if not result['OK']:
      return S_ERROR( 'Unable to query the database' )
    elif result['Value'] == ():
      result = self._insert( tableName, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Could not insert the data into %s table' % tableName )

      result = self._getFields( tableName, outFields, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Unable to query the database' )
      if result['Value'] == ():
        # The inserted value is larger than the field size and can not be matched back
        for i in range( len( inFields ) ):
          gLogger.error( 'Could not insert the data into %s table' % tableName, '%s = %s' % ( inFields[i], inValues[i] ) )
        return S_ERROR( 'Could not insert the data into %s table' % tableName )

    return S_OK( int( result['Value'][0][0] ) )

  def _insertMessageIntoSystemLoggingDB( self, message, site, nodeFQDN,
                                         userDN, userGroup, remoteAddress ):
    """ This function inserts the Log message into the DB
    """
    messageDate = Time.toString( message.getTime() )
    messageDate = messageDate[:messageDate.find( '.' )]
    messageName = message.getName()
    messageSubSystemName = message.getSubSystemName()

    fieldsList = [ 'MessageTime', 'VariableText' ]
    messageList = [ messageDate, message.getVariableMessage() ]

    inValues = [ userDN, userGroup ]
    inFields = [ 'OwnerDN', 'OwnerGroup' ]
    outFields = [ 'UserDNID' ]
    result = self.__DBCommit( 'UserDNs', outFields, inFields, inValues )
    if not result['OK']:
      return result
    messageList.append( result['Value'] )
    fieldsList.extend( outFields )

    if not site:
      site = 'Unknown'
    inFields = [ 'SiteName']
    inValues = [ site ]
    outFields = [ 'SiteID' ]
    result = self.__DBCommit( 'Sites', outFields, inFields, inValues )
    if not result['OK']:
      return result
    siteIDKey = result['Value']

    inFields = [ 'ClientIPNumberString' , 'ClientFQDN', 'SiteID' ]
    inValues = [ remoteAddress, nodeFQDN, siteIDKey ]
    outFields = [ 'ClientIPNumberID' ]
    result = self.__DBCommit( 'ClientIPs', outFields, inFields, inValues )
    if not result['OK']:
      return result
    messageList.append( result['Value'] )
    fieldsList.extend( outFields )


    messageList.append( message.getLevel() )
    fieldsList.append( 'LogLevel' )


    if not messageSubSystemName:
      messageSubSystemName = 'Unknown'
    inFields = [ 'SubSystemName' ]
    inValues = [ messageSubSystemName ]
    outFields = [ 'SubSystemID' ]
    result = self.__DBCommit( 'SubSystems', outFields, inFields, inValues )
    if not result['OK']:
      return result
    subSystemsKey = result['Value']

    if not messageName:
      messageName = 'Unknown'
    inFields = [ 'SystemName', 'SubSystemID' ]
    inValues = [ messageName, subSystemsKey ]
    outFields = [ 'SystemID'  ]
    result = self.__DBCommit( 'Systems', outFields, inFields, inValues )
    if not result['OK']:
      return result
    SystemIDKey = result['Value']


    inFields = [ 'FixedTextString' , 'SystemID' ]
    inValues = [ message.getFixedMessage(), SystemIDKey ]
    outFields = [ 'FixedTextID' ]
    result = self.__DBCommit( 'FixedTextMessages', outFields, inFields,
                              inValues )
    if not result['OK']:
      return result
    messageList.append( result['Value'] )
    fieldsList.extend( outFields )

    return self._insert( 'MessageRepository', fieldsList, messageList )

  def _insertDataIntoAgentTable( self, agentName, data ):
    """Insert the persistent data needed by the agents running on top of
       the SystemLoggingDB.
    """
    result = self._escapeString( data )
    if not result['OK']:
      return result
    escapedData = result['Value']

    outFields = ['AgentID']
    inFields = [ 'AgentName' ]
    inValues = [ agentName ]

    result = self._getFields( 'AgentPersistentData', outFields, inFields, inValues )
    if not result ['OK']:
      return result
    elif result['Value'] == ():
      inFields = [ 'AgentName', 'AgentData' ]
      inValues = [ agentName, escapedData]
      result = self._insert( 'AgentPersistentData', inFields, inValues )
      if not result['OK']:
        return result
    cmd = "UPDATE LOW_PRIORITY AgentPersistentData SET AgentData='%s' WHERE AgentID='%s'" % \
          ( escapedData, result['Value'][0][0] )
    return self._update( cmd )

  def _getDataFromAgentTable( self, agentName ):
    outFields = [ 'AgentData' ]
    inFields = [ 'AgentName' ]
    inValues = [ agentName ]

    return self._getFields( 'AgentPersistentData', outFields, inFields, inValues )
