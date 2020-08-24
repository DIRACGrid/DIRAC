""" SystemLoggingDB class is a front-end to the Message Logging Database.
    The following methods are provided

    insertMessage()
    getMessagesByDate()
    getMessagesByFixedText()
    getMessages()
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import re

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, List

__RCSID__ = "$Id$"

DEBUG = 0

###########################################################


class SystemLoggingDB(DB):
  """ .. class:: SystemLoggingDB

  Python interface to SystemLoggingDB.

  .. code-block:: sql

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
      `ClientIPNumberString` VARCHAR(45) NOT NULL DEFAULT '0.0.0.0' ,
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

  def __init__(self):
    """ Standard Constructor
    """
    DB.__init__(self, 'SystemLoggingDB', 'Framework/SystemLoggingDB', debug=DEBUG)
    self.tableDict = {'UserDNs': {'Fields': {'UserDNID': 'INT NOT NULL AUTO_INCREMENT',
                                             'OwnerDN': "VARCHAR(255) NOT NULL DEFAULT 'unknown'",
                                             'OwnerGroup': "VARCHAR(128) NOT NULL DEFAULT 'nogroup'"},
                                  'PrimaryKey': 'UserDNID',
                                  'UniqueIndexes': {'Owner': ['OwnerDN', 'OwnerGroup']},
                                  'Engine': 'InnoDB',
                                  },
                      'Sites': {'Fields': {'SiteID': 'INT NOT NULL AUTO_INCREMENT',
                                           'SiteName': "VARCHAR(64) NOT NULL DEFAULT 'Unknown'"},
                                'PrimaryKey': 'SiteID',
                                'UniqueIndexes': {'Site': ['SiteName']},
                                'Engine': 'InnoDB',
                                },
                      'ClientIPs': {'Fields': {'ClientIPNumberID': 'INT NOT NULL AUTO_INCREMENT',
                                               'ClientIPNumberString': "VARCHAR(45) NOT NULL DEFAULT '0.0.0.0'",
                                               'ClientFQDN': "VARCHAR(128) NOT NULL DEFAULT 'unknown'",
                                               'SiteID': 'INT NOT NULL'},
                                    'PrimaryKey': ['ClientIPNumberID', 'SiteID'],
                                    'ForeignKeys': {'SiteID': 'Sites.SiteID'},
                                    'UniqueIndexes': {'Client': ['ClientIPNumberString', 'ClientFQDN', 'SiteID']},
                                    'Engine': 'InnoDB',
                                    },
                      'Systems': {'Fields': {'SystemID': 'INT NOT NULL AUTO_INCREMENT',
                                             'SystemName': "VARCHAR(128) NOT NULL DEFAULT 'Unknown'"},
                                  'PrimaryKey': 'SystemID',
                                  'UniqueIndexes': {'System': ['SystemName']},
                                  'Engine': 'InnoDB',
                                  },
                      'SubSystems': {'Fields': {'SubSystemID': 'INT NOT NULL AUTO_INCREMENT',
                                                'SubSystemName': "VARCHAR(128) NOT NULL DEFAULT 'Unknown'",
                                                'SystemID': 'INT NOT NULL', },
                                     'PrimaryKey': ['SubSystemID', 'SystemID'],
                                     'ForeignKeys': {'SystemID': 'Systems.SystemID'},
                                     'UniqueIndexes': {'SubSystem': ['SubSystemName', 'SystemID']},
                                     'Engine': 'InnoDB',
                                     },
                      'FixedTextMessages': {'Fields': {'FixedTextID': 'INT NOT NULL AUTO_INCREMENT',
                                                       'FixedTextString': "VARCHAR( 767 ) NOT NULL DEFAULT 'Unknown'",
                                                       'ReviewedMessage': 'TINYINT( 1 ) NOT NULL DEFAULT FALSE',
                                                       'SubSystemID': 'INT NOT NULL', },
                                            'PrimaryKey': 'FixedTextID',
                                            'UniqueIndexes': {'FixedText': ['FixedTextString', 'SubSystemID']},
                                            'ForeignKeys': {'SubSystemID': 'SubSystems.SubSystemID'},
                                            'Engine': 'InnoDB',
                                            },
                      'MessageRepository': {'Fields': {'MessageID': 'INT NOT NULL AUTO_INCREMENT',
                                                       'MessageTime': 'DATETIME NOT NULL',
                                                       'VariableText': 'VARCHAR(255) NOT NULL',
                                                       'UserDNID': 'INT NOT NULL',
                                                       'ClientIPNumberID': 'INT NOT NULL',
                                                       'LogLevel': 'VARCHAR(15) NOT NULL',
                                                       'FixedTextID': 'INT NOT NULL', },
                                            'PrimaryKey': 'MessageID',
                                            'Indexes': {'TimeStampsIDX': ['MessageTime'],
                                                        'FixTextIDX': ['FixedTextID'],
                                                        'UserIDX': ['UserDNID'],
                                                        'IPsIDX': ['ClientIPNumberID'], },
                                            'ForeignKeys': {'UserDNID': 'UserDNs.UserDNID',
                                                            'ClientIPNumberID': 'ClientIPs.ClientIPNumberID',
                                                            'FixedTextID': 'FixedTextMessages.FixedTextID',
                                                            },
                                            'Engine': 'InnoDB',
                                            },
                      'AgentPersistentData': {'Fields': {'AgentID': 'INT NOT NULL AUTO_INCREMENT',
                                                         'AgentName': "VARCHAR( 64 ) NOT NULL DEFAULT 'unkwown'",
                                                         'AgentData': 'VARCHAR( 512 ) NULL DEFAULT NULL'
                                                         },
                                              'PrimaryKey': 'AgentID',
                                              'Engine': 'InnoDB',
                                              },
                      }

    result = self._checkTable()
    if not result['OK']:
      gLogger.error('Failed to check/create the database tables', result['Message'])

  def _checkTable(self):
    """ Make sure the tables are created
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal
    self.log.debug("Tables already created: %s" % str(retVal['Value']))

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesToCreate = {}
    for tableName, tableDef in self.tableDict.items():
      if tableName not in tablesInDB:
        tablesToCreate[tableName] = tableDef

    self.log.debug("Tables that will be created: %s" % str(tablesToCreate))
    return self._createTables(tablesToCreate, force=False)

  def __buildTableList(self, showFieldList):
    """ build the SQL list of tables needed for the query
        from the list of variables provided
    """
    idPattern = re.compile(r'ID')

    tableDict = {'MessageTime': 'MessageRepository',
                 'VariableText': 'MessageRepository',
                 'LogLevel': 'MessageRepository',
                 'FixedTextString': 'FixedTextMessages',
                 'ReviewedMessage': 'FixedTextMessages',
                 'SystemName': 'Systems', 'SubSystemName': 'SubSystems',
                 'OwnerDN': 'UserDNs', 'OwnerGroup': 'UserDNs',
                 'ClientIPNumberString': 'ClientIPs',
                 'ClientFQDN': 'ClientIPs', 'SiteName': 'Sites'}
    tableDictKeys = list(tableDict)
    tableList = []

    conjunction = ' NATURAL JOIN '

    self.log.debug('__buildTableList:', 'showFieldList = %s' % showFieldList)
    if len(showFieldList):
      for field in showFieldList:
        if not idPattern.search(field) and (field in tableDictKeys):
          tableList.append(tableDict[field])

      # if re.search( 'MessageTime', ','.join( showFieldList) ):
      #  tableList.append('MessageRepository')
      tableList = List.uniqueElements(tableList)

      tableString = ''
      try:
        tableList.pop(tableList.index('MessageRepository'))
        tableString = 'MessageRepository'
      except ValueError:
        pass

      if tableList.count('Sites') and tableList.count('MessageRepository') and not \
              tableList.count('ClientIPs'):
        tableList.append('ClientIPs')
      if tableList.count('MessageRepository') and tableList.count('SubSystems') \
              and not tableList.count('FixedTextMessages') and not tableList.count('Systems'):
        tableList.append('FixedTextMessages')
        tableList.append('Systems')
      if tableList.count('MessageRepository') and tableList.count('Systems') \
              and not tableList.count('FixedTextMessages'):
        tableList.append('FixedTextMessages')
      if tableList.count('FixedTextMessages') and tableList.count('SubSystems') \
              and not tableList.count('Systems'):
        tableList.append('Systems')
      if tableList.count('MessageRepository') or (tableList.count('FixedTextMessages')
                                                  + tableList.count('ClientIPs') + tableList.count('UserDNs') > 1):
        tableString = 'MessageRepository'

      if tableString and len(tableList):
        tableString = '%s%s' % (tableString, conjunction)
      tableString = '%s%s' % (tableString,
                              conjunction.join(tableList))

    else:
      tableString = conjunction.join(List.uniqueElements(tableDict.values()))

    self.log.debug('__buildTableList:', 'tableString = "%s"' % tableString)
    return tableString

  def _queryDB(self, showFieldList=None, condDict=None, older=None,
               newer=None, count=False, groupColumn=None, orderFields=None):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and queries the SystemLoggingDB.
        If no list is provided the default is to use all the meaningful
        variables of the DB
    """
    grouping = ''
    ordering = ''
    try:
      condition = self.buildCondition(condDict=condDict, older=older, newer=newer, timeStamp='MessageTime')
    except Exception as x:
      return S_ERROR(str(x))

    if not showFieldList:
      showFieldList = ['MessageTime', 'LogLevel', 'FixedTextString',
                       'VariableText', 'SystemName',
                       'SubSystemName', 'OwnerDN', 'OwnerGroup',
                       'ClientIPNumberString', 'SiteName']
    elif isinstance(showFieldList, six.string_types):
      showFieldList = [showFieldList]
    elif not isinstance(showFieldList, list):
      errorString = 'The showFieldList variable should be a string or a list of strings'
      errorDesc = 'The type provided was: %s' % type(showFieldList)
      self.log.warn(errorString, errorDesc)
      return S_ERROR('%s: %s' % (errorString, errorDesc))

    tableList = self.__buildTableList(showFieldList)

    if groupColumn:
      grouping = 'GROUP BY %s' % groupColumn

    if count:
      if groupColumn:
        showFieldList.append('count(*) as recordCount')
      else:
        showFieldList = ['count(*) as recordCount']

    sortingFields = []
    if orderFields:
      for field in orderFields:
        if isinstance(field, list):
          sortingFields.append(' '.join(field))
        else:
          sortingFields.append(field)
      ordering = 'ORDER BY %s' % ', '.join(sortingFields)

    cmd = 'SELECT %s FROM %s %s %s %s' % (','.join(showFieldList),
                                          tableList, condition, grouping, ordering)

    return self._query(cmd)

  def __insertIntoAuxiliaryTable(self, tableName, outFields, inFields, inValues):
    """  This is an auxiliary function to insert values on a
         satellite Table if they do not exist and returns
         the unique KEY associated to the given set of values
    """

    # tableDict = { 'MessageRepository':'MessageTime',
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

    # Check if the record is already there and get the rowID
    condDict = {}
    condDict.update([(inFields[k], inValues[k]) for k in range(len(inFields))])
    result = self.getFields(tableName, outFields, condDict=condDict)
    if not result['OK']:
      self.log.error('__insertIntoAuxiliaryTable failed to query DB', result['Message'])
      return S_ERROR()
    if len(result['Value']) > 0:
      return S_OK(int(result['Value'][0][0]))

    result = self.insertFields(tableName, inFields, inValues)
    rowID = 0
    if not result['OK'] and 'Duplicate entry' not in result['Message']:
      self.log.error('__insertIntoAuxiliaryTable failed to insert data into DB', result['Message'])
      return S_ERROR('Could not insert the data into %s table' % tableName)
    elif not result['OK']:
      self.log.verbose('__insertIntoAuxiliaryTable duplicated record')
    elif result['Value'] == 0:
      self.log.error('__insertIntoAuxiliaryTable failed to insert data into DB')
    else:
      rowID = result['lastRowId']
      self.log.verbose('__insertIntoAuxiliaryTable new entry added', rowID)
    # check the inserted values
    condDict = {}
    condDict.update([(inFields[k], inValues[k]) for k in range(len(inFields))])
    result = self.getFields(tableName, outFields + inFields, condDict=condDict)
    if not result['OK']:
      self.log.error('__insertIntoAuxiliaryTable failed to query DB', result['Message'])
      return S_ERROR()
    if len(result['Value']) == 0:
      error = 'Could not retrieve inserted values'
      if rowID:
        condDict = {outFields[0]: rowID}
        self.deleteEntries(tableName, condDict)
      self.log.error(error)
      return S_ERROR(error)

    outValues = result['Value'][0][:len(outFields)]
    insertedValues = result['Value'][0][len(outFields):]
    error = ''
    for i, item in enumerate(inValues):
      if item != insertedValues[i]:
        error = 'Inserted Value does not match %s: "%s" != "%s"' % (inFields[i], item, insertedValues[i])
        break

    if error:
      self.log.error(error)
      return S_ERROR('Failed while check of inserted Values')

    return S_OK(int(outValues[0]))

  def insertMessage(self, message, site, nodeFQDN, userDN, userGroup, remoteAddress):
    """ This function inserts the Log message into the DB
    """
    messageDate = Time.toString(message.getTime())
    messageDate = messageDate[:messageDate.find('.')]
    messageName = message.getName()
    messageSubSystemName = message.getSubSystemName()

    fieldsList = [ 'MessageTime', 'VariableText' ]
    messageList = [messageDate, message.getVariableMessage()[:255]]

    inValues = [userDN, userGroup]
    inFields = ['OwnerDN', 'OwnerGroup']
    outFields = ['UserDNID']
    result = self.__insertIntoAuxiliaryTable('UserDNs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend(outFields)

    if not site:
      site = 'Unknown'
    inFields = ['SiteName']
    inValues = [site]
    outFields = ['SiteID']
    result = self.__insertIntoAuxiliaryTable('Sites', outFields, inFields, inValues)
    if not result['OK']:
      return result
    siteIDKey = result['Value']

    inFields = ['ClientIPNumberString', 'ClientFQDN', 'SiteID']
    inValues = [remoteAddress, nodeFQDN, siteIDKey]
    outFields = ['ClientIPNumberID']
    result = self.__insertIntoAuxiliaryTable('ClientIPs', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend(outFields)

    messageList.append(message.getLevel())
    fieldsList.append('LogLevel')

    if not messageName:
      messageName = 'Unknown'
    inFields = ['SystemName']
    inValues = [messageName]
    outFields = ['SystemID']
    result = self.__insertIntoAuxiliaryTable('Systems', outFields, inFields, inValues)
    if not result['OK']:
      return result
    systemIDKey = result['Value']

    if not messageSubSystemName:
      messageSubSystemName = 'Unknown'
    inFields = ['SubSystemName', 'SystemID']
    inValues = [messageSubSystemName, systemIDKey]
    outFields = ['SubSystemID']
    result = self.__insertIntoAuxiliaryTable('SubSystems', outFields, inFields, inValues)
    if not result['OK']:
      return result
    subSystemIDKey = result['Value']

    inFields = ['FixedTextString', 'SubSystemID']
    inValues = [message.getFixedMessage(), subSystemIDKey]
    outFields = ['FixedTextID']
    result = self.__insertIntoAuxiliaryTable('FixedTextMessages', outFields, inFields, inValues)
    if not result['OK']:
      return result
    messageList.append(result['Value'])
    fieldsList.extend(outFields)

    return self.insertFields('MessageRepository', fieldsList, messageList)

  def _insertDataIntoAgentTable(self, agentName, data):
    """Insert the persistent data needed by the agents running on top of
       the SystemLoggingDB.
    """
    result = self._escapeString(data)
    if not result['OK']:
      return result
    escapedData = result['Value']

    outFields = ['AgentID']
    condDict = {'AgentName': agentName}
    inFields = ['AgentName']
    inValues = [agentName]

    result = self.getFields('AgentPersistentData', outFields, condDict)
    if not result['OK']:
      return result
    elif result['Value'] == ():
      inFields = ['AgentName', 'AgentData']
      inValues = [agentName, escapedData]
      result = self.insertFields('AgentPersistentData', inFields, inValues)
      if not result['OK']:
        return result
    cmd = "UPDATE LOW_PRIORITY AgentPersistentData SET AgentData='%s' WHERE AgentID='%s'" % \
          (escapedData, result['Value'][0][0])
    return self._update(cmd)

  def _getDataFromAgentTable(self, agentName):
    """ Get persistent data needed by SystemLogging Agents
    """
    outFields = ['AgentData']
    condDict = {'AgentName': agentName}

    return self.getFields('AgentPersistentData', outFields, condDict)
