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
from DIRAC.Core.Utilities import Time, dateTime, hour, date, week, day, fromString, toString, List
from DIRAC.FrameworkSystem.private.logging.LogLevels import LogLevels

DEBUG = 1


###########################################################
class SystemLoggingDB( DB ):
  """ .. class:: SystemLoggingDB 

  Python interface to SystemLoggingDB.

CREATE  TABLE IF NOT EXISTS `UserDNs` (
  `UserDNID` INT NOT NULL AUTO_INCREMENT ,
  `OwnerDN` VARCHAR(255) NOT NULL DEFAULT 'unknown' ,
  `OwnerGroup` VARCHAR(128) NOT NULL DEFAULT 'nogroup' ,
  PRIMARY KEY (`UserDNID`) ) ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `Sites` (
  `SiteID` INT NOT NULL AUTO_INCREMENT ,
  `SiteName` VARCHAR(64) NOT NULL DEFAULT 'Unknown' ,
  PRIMARY KEY (`SiteID`) ) ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `ClientIPs` (
  `ClientIPNumberID` INT NOT NULL AUTO_INCREMENT ,
  `ClientIPNumberString` VARCHAR(15) NOT NULL DEFAULT '0.0.0.0' ,
  `ClientFQDN` VARCHAR(128) NOT NULL DEFAULT 'unknown' ,
  `SiteID` INT NOT NULL ,
  PRIMARY KEY (`ClientIPNumberID`, `SiteID`) ,
  INDEX `SiteID` (`SiteID` ASC) ,
    FOREIGN KEY (`SiteID` ) REFERENCES Sites(SiteID) ON UPDATE CASCADE ON DELETE CASCADE ) ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `SubSystems` (
  `SubSystemID` INT NOT NULL AUTO_INCREMENT ,
  `SubSystemName` VARCHAR(128) NOT NULL DEFAULT 'Unknown' ,
  `SystemID` INT NOT NULL AUTO_INCREMENT ,
  PRIMARY KEY (`SubSystemID`) ) ENGINE=InnoDB;
    FOREIGN KEY (`SystemID`) REFERENCES Systems(SystemID) ON UPDATE CASCADE ON DELETE CASCADE) 

CREATE  TABLE IF NOT EXISTS `Systems` (
  `SystemID` INT NOT NULL AUTO_INCREMENT ,
  `SystemName` VARCHAR(128) NOT NULL DEFAULT 'Unknown' ,
  PRIMARY KEY (`SystemID` ) , )
    ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `FixedTextMessages` (
  `FixedTextID` INT NOT NULL AUTO_INCREMENT ,
  `FixedTextString` VARCHAR(255) NOT NULL DEFAULT 'Unknown' ,
  `ReviewedMessage` TINYINT(1) NOT NULL DEFAULT FALSE ,
  `SystemID` INT NOT NULL ,
  PRIMARY KEY (`FixedTextID`, `SystemID`) ,
  INDEX `SystemID` (`SystemID` ASC) ,
    FOREIGN KEY (`SystemID` ) REFERENCES Systems(`SystemID`) ON UPDATE CASCADE ON DELETE CASCADE) ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `MessageRepository` (
  `MessageID` INT NOT NULL AUTO_INCREMENT ,
  `MessageTime` DATETIME NOT NULL ,
  `VariableText` VARCHAR(255) NOT NULL ,
  `UserDNID` INT NOT NULL ,
  `ClientIPNumberID` INT NOT NULL ,
  `LogLevel` VARCHAR(6) NOT NULL ,
  `FixedTextID` INT NOT NULL ,
  PRIMARY KEY (`MessageID`) ,
  INDEX `TimeStampsIDX` (`MessageTime` ASC) ,
  INDEX `FixTextIDX` (`FixedTextID` ASC) ,
  INDEX `UserIDX` (`UserDNID` ASC) ,
  INDEX `IPsIDX` (`ClientIPNumberID` ASC) ,
    FOREIGN KEY (`UserDNID` ) REFERENCES UserDNs(`UserDNID` ) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (`ClientIPNumberID` ) REFERENCES ClientIPs(`ClientIPNumberID` ) ON UPDATE CASCADE ON DELETE CASCADE, 
    FOREIGN KEY (`FixedTextID` ) REFERENCES FixedTextMessages (`FixedTextID` ) ON UPDATE CASCADE ON DELETE CASCADE )
    ENGINE=InnoDB;

CREATE  TABLE IF NOT EXISTS `AgentPersistentData` (
  `AgentID` INT NOT NULL AUTO_INCREMENT ,
  `AgentName` VARCHAR(64) NOT NULL DEFAULT 'unkwown' ,
  `AgentData` VARCHAR(512) NULL DEFAULT NULL ,
  PRIMARY KEY (`AgentID`) ) ENGINE=InnoDB;

"""
  tableNames = [ 'AgentPersistentData',
                 'MessageRepository',
                   'FixedTextMessages',
                     'SubSystems', 'Systems',
                   'ClientIPs', 'Sites',
                   'UserDNs' ]
  tableDict = { 'UserDNs': {
                            'Fields': { 'UserDNID': 'INT NOT NULL AUTO_INCREMENT',
                                        'OwnerDN': "VARCHAR(255) NOT NULL DEFAULT 'unknown'",
                                        'OwnerGroup': "VARCHAR(128) NOT NULL DEFAULT 'nogroup'" },
                            'PrimaryKey': 'UserDNID',
                            'Engine': 'InnoDB',
                            },
                'Sites' : { 'Fields': { 'SiteID': 'INT NOT NULL AUTO_INCREMENT',
                                        'SiteName': "VARCHAR(64) NOT NULL DEFAULT 'Unknown'" },
                            'PrimaryKey': 'SiteID',
                            'Engine': 'InnoDB',
                            },
                'ClientIPs':{ 'Fields': { 'ClientIPNumberID': 'INT NOT NULL AUTO_INCREMENT',
                                          'ClientIPNumberString': "VARCHAR(15) NOT NULL DEFAULT '0.0.0.0'",
                                          'ClientFQDN': "VARCHAR(128) NOT NULL DEFAULT 'unknown'",
                                          'SiteID': 'INT NOT NULL' },
                              'PrimaryKey': [ 'ClientIPNumberID', 'SiteID' ],
                              'ForeignKeys': {'SiteID': 'Sites.SiteID' },
                              'Engine': 'InnoDB',
                              },
                'Systems': {
                             'Fields': { 'SystemID': 'INT NOT NULL AUTO_INCREMENT',
                                         'SystemName': "VARCHAR(128) NOT NULL DEFAULT 'Unknown'" },
                             'PrimaryKey': 'SystemID',
                             'Engine': 'InnoDB',
                             },
                'SubSystems': {
                                'Fields': { 'SubSystemID': 'INT NOT NULL AUTO_INCREMENT',
                                            'SubSystemName': "VARCHAR(128) NOT NULL DEFAULT 'Unknown'",
                                            'SystemID': 'INT NOT NULL', },
                                'PrimaryKey': ['SubSystemID', 'SystemID'],
                                'ForeignKeys': {'SystemID': 'Systems.SystemID' },
                                'Engine': 'InnoDB',
                                },
                'FixedTextMessages': {
                                      'Fields': { 'FixedTextID': 'INT NOT NULL AUTO_INCREMENT',
                                                  'FixedTextString': "VARCHAR( 255 ) NOT NULL DEFAULT 'Unknown'",
                                                  'ReviewedMessage': 'TINYINT( 1 ) NOT NULL DEFAULT FALSE',
                                                  'SubSystemID': 'INT NOT NULL', },
                                      'PrimaryKey': 'FixedTextID',
                                      'Indexes': {'SubSystemID': ['SubSystemID']},
                                      'ForeignKeys': {'SubSystemID': 'SubSystems.SubSystemID' },
                                      'Engine': 'InnoDB',
                                      },
                'MessageRepository': {
                                       'Fields': { 'MessageID': 'INT NOT NULL AUTO_INCREMENT',
                                                   'MessageTime': 'DATETIME NOT NULL',
                                                   'VariableText': 'VARCHAR(255) NOT NULL',
                                                   'UserDNID': 'INT NOT NULL',
                                                   'ClientIPNumberID': 'INT NOT NULL',
                                                   'LogLevel': 'INT NOT NULL',
                                                   'FixedTextID': 'INT NOT NULL', },
                                       'PrimaryKey': 'MessageID',
                                       'Indexes': { 'TimeStampsIDX': ['MessageTime'],
                                                    'FixTextIDX': ['FixedTextID'],
                                                    'UserIDX': ['UserDNID'],
                                                    'IPsIDX': ['ClientIPNumberID'], },
                                       'ForeignKeys': { 'UserDNID': 'UserDNs.UserDNID',
                                                        'ClientIPNumberID': 'ClientIPs.ClientIPNumberID',
                                                        'FixedTextID': 'FixedTextMessages.FixedTextID', },
                                       'Engine': 'InnoDB',
                                },
                'AgentPersistentData': {
                                        'Fields': { 'AgentID': 'INT NOT NULL AUTO_INCREMENT',
                                                    'AgentName': "VARCHAR( 64 ) NOT NULL DEFAULT 'unkwown'",
                                                    'AgentData': 'VARCHAR( 512 ) NULL DEFAULT NULL' },
                                        'PrimaryKey': 'AgentID',
                                        'Engine': 'InnoDB',
                                        },
               }


  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'SystemLoggingDB', 'Framework/SystemLoggingDB',
                 maxQueueSize, debug = DEBUG )

  def _checkTable( self ):
    """ Make sure the tables are created
    """
    return self._createTables( self.tableDict, force = False )


  def _buildConditionTest( self, condDict, olderDate = None, newerDate = None ):
    """ a wrapper to the private function __buildCondition so test programs
        can access it
    """
    return self.buildCondition( condDict, older = olderDate,
                                  newer = newerDate, timeStamp = 'MessageTime' )

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
      tableList = List.uniqueElements( tableList )

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
      tableString = conjunction.join( List.uniqueElements( tableDict.values() ) )

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
    try:
      condition = self.buildCondition( condDict = condDict, older = older, newer = newer , timeStamp = 'MessageTime' )
    except Exception, x:
      return S_ERROR ( str( x ) )

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
      print result['Message']
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


    if not messageName:
      messageName = 'Unknown'
    inFields = [ 'SystemName' ]
    inValues = [ messageName ]
    outFields = [ 'SystemID'  ]
    result = self.__DBCommit( 'Systems', outFields, inFields, inValues )
    if not result['OK']:
      return result
    SystemIDKey = result['Value']

    if not messageSubSystemName:
      messageSubSystemName = 'Unknown'
    inFields = [ 'SubSystemName', 'SystemID' ]
    inValues = [ messageSubSystemName, SystemIDKey  ]
    outFields = [ 'SubSystemID' ]
    result = self.__DBCommit( 'SubSystems', outFields, inFields, inValues )
    if not result['OK']:
      return result
    subSystemsKey = result['Value']


    inFields = [ 'FixedTextString' , 'SubSystemID' ]
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

def testSystemLoggingDB():
  """ Some test cases
  """

  # building up some fake CS values
  gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
  gConfig.setOptionValue( '/DIRAC/Setups/Test/Framework', 'Test' )

  host = '127.0.0.1'
  user = 'Dirac'
  pwd = 'Dirac'
  db = 'AccountingDB'

  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/Host', host )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/DBName', db )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/User', user )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/Password', pwd )

  from DIRAC.FrameworkSystem.private.logging.Message import tupleToMessage

  systemName = 'TestSystem'
  subSystemName = 'TestSubSystem'
  level = 10
  time = Time.toString()
  msgTest = 'Hello'
  variableText = time
  frameInfo = ""
  message = tupleToMessage( ( systemName, level, time, msgTest, variableText, frameInfo, subSystemName ) )
  site = 'somewehere'
  nodeFQDN = '127.0.0.1'
  userDN = 'Yo'
  userGroup = 'Us'
  remoteAddress = 'elsewhere'

  records = 10

  db = SystemLoggingDB()
  assert db._connect()['OK']

  try:
    if False:
      for tableName in db.tableNames:
        result = db._update( 'DROP TABLE `%s`' % tableName )
        assert result['OK']

    gLogger.info( '\n Creating Table\n' )
    # Make sure it is there and it has been created for this test
    result = db._checkTable()
    assert result['OK']

    result = db._checkTable()
    assert not result['OK']
    assert result['Message'] == 'The requested table already exist'

    gLogger.info( '\n Inserting some records\n' )
    for k in range( records ):
      result = db._insertMessageIntoSystemLoggingDB( message, site, nodeFQDN,
                                           userDN, userGroup, remoteAddress )
      assert result['OK']
      assert result['lastRowId'] == k + 1
      assert result['Value'] == 1


    result = db._queryDB( showFieldList = [ 'SiteName' ] )
    assert result['OK']
    assert result['Value'][0][0] == site

    result = db._queryDB( showFieldList = [ 'SystemName' ] )
    assert result['OK']
    assert result['Value'][0][0] == systemName

    result = db._queryDB( showFieldList = [ 'SubSystemName' ] )
    assert result['OK']
    assert result['Value'][0][0] == subSystemName

    result = db._queryDB( showFieldList = [ 'OwnerGroup' ] )
    assert result['OK']
    assert result['Value'][0][0] == userGroup

    result = db._queryDB( showFieldList = [ 'FixedTextString' ] )
    assert result['OK']
    assert result['Value'][0][0] == msgTest

    result = db._queryDB( showFieldList = [ 'VariableText', 'SiteName' ], count = True, groupColumn = 'VariableText' )
    assert result['OK']
    assert result['Value'][0][1] == site
    assert result['Value'][0][2] == records


    print result




    gLogger.info( '\n Removing Table\n' )
    for tableName in db.tableNames:
      result = db._update( 'DROP TABLE `%s`' % tableName )
      assert result['OK']

    gLogger.info( '\n OK\n' )


  except AssertionError:
    print 'ERROR ',
    if not result['OK']:
      print result['Message']
    else:
      print result


    sys.exit( 1 )


if __name__ == '__main__':
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  testSystemLoggingDB()

