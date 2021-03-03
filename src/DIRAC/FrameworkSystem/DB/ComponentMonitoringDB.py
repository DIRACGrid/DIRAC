""" ComponentMonitoring class is a front-end to the Component monitoring Database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import random

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, List, Network

__RCSID__ = "$Id$"


class ComponentMonitoringDB(DB):

  def __init__(self):
    """ c'tor

        Initialize the DB
    """
    DB.__init__(self, 'ComponentMonitoringDB', 'Framework/ComponentMonitoringDB')
    random.seed()
    retVal = self.__initializeDB()
    if not retVal['OK']:
      raise Exception("Can't create tables: %s" % retVal['Message'])
    self.__optionalFields = ('startTime', 'cycles', 'version', 'queries',
                             'DIRACVersion', 'description', 'platform')
    self.__mainFields = ("Id", "Setup", "Type", "ComponentName", "Host", "Port",
                         "StartTime", "LastHeartbeat", "cycles", "queries", "LoggingState")
    self.__versionFields = ('VersionTimestamp', 'Version', 'DIRACVersion', 'Platform', 'Description')

  def getOptionalFields(self):
    return self.__optionalFields

  def __getTableName(self, name):
    return "compmon_%s" % name

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesD = {}

    tN = self.__getTableName("Components")
    if tN not in tablesInDB:
      tablesD[tN] = {'Fields': {'Id': 'INTEGER AUTO_INCREMENT NOT NULL',
                                'ComponentName': 'VARCHAR(255) NOT NULL',
                                'Setup': 'VARCHAR(255) NOT NULL',
                                'Type': 'ENUM ( "service", "agent" ) NOT NULL',
                                'Host': 'VARCHAR(255) NOT NULL',
                                'Port': 'INTEGER DEFAULT 0',
                                'LastHeartbeat': 'DATETIME NOT NULL',
                                'StartTime': 'DATETIME NOT NULL',
                                'LoggingState': 'VARCHAR(64) DEFAULT "unknown"',
                                'Cycles': 'INTEGER',
                                'Queries': 'INTEGER'
                                },
                     'PrimaryKey': 'Id',
                     'Indexes': {'ComponentIndex': ['ComponentName', 'Setup', 'Host', 'Port'],
                                 'TypeIndex': ['Type'],
                                 }
                     }

    tN = self.__getTableName("VersionHistory")
    if tN not in tablesInDB:
      tablesD[tN] = {'Fields': {'CompId': 'INTEGER NOT NULL',
                                'VersionTimestamp': 'DATETIME NOT NULL',
                                'Version': 'VARCHAR(255)',
                                'DIRACVersion': 'VARCHAR(255) NOT NULL',
                                'Platform': 'VARCHAR(255) NOT NULL',
                                'Description': 'BLOB',
                                },
                     'Indexes': {'Component': ['CompId']}
                     }

    return self._createTables(tablesD)

  def __datetime2str(self, dt):
    """
    This method converts the datetime type to a string type.
    """
    if isinstance(dt, six.string_types):
      return dt
    return "%s-%s-%s %s:%s:%s" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

  def __registerIfNotThere(self, compDict):
    """
    Registers the component if it's not there
    """
    sqlCond = []
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName("Components")
    for field in ('componentName', 'setup', 'type', 'host', 'port'):
      if field not in compDict:
        if field == 'port':
          continue
        return S_ERROR("Missing %s field in the component dict" % field)
      value = compDict[field]
      field = field.capitalize()
      sqlInsertFields.append(field)
      sqlInsertValues.append("'%s'" % value)
      sqlCond.append("%s = '%s'" % (field, value))
    compLogName = ":".join(sqlInsertValues).replace("'", "")
    self.log.info("Trying to register %s" % compLogName)
    result = self._query("SELECT id FROM `%s` WHERE %s" % (tableName, " AND ".join(sqlCond)))
    if not result['OK']:
      self.log.error("Cannot register component", "%s: %s" % (compLogName, result['Message']))
      return result
    if len(result['Value']):
      compId = result['Value'][0][0]
      self.log.info("%s has compId %s" % (compLogName, compId))
      return S_OK(compId)
    # It's not there, we just need to insert it
    sqlInsertFields.append("LastHeartbeat")
    sqlInsertValues.append("UTC_TIMESTAMP()")
    if 'startTime' in compDict:
      sqlInsertFields.append("StartTime")
      val = compDict['startTime']
      if isinstance(val, Time._allDateTypes):
        val = self.__datetime2str(val)
      sqlInsertValues.append("'%s'" % val)
    for field in ('cycles', 'queries'):
      if field not in compDict:
        compDict[field] = 0
      value = compDict[field]
      field = field.capitalize()
      sqlInsertFields.append(field)
      sqlInsertValues.append(str(value))
    self.log.info("Registering component %s" % compLogName)
    result = self._update("INSERT INTO `%s` ( %s ) VALUES ( %s )" % (tableName,
                                                                     ", ".join(sqlInsertFields),
                                                                     ", ".join(sqlInsertValues)))
    if not result['OK']:
      return result
    compId = result['lastRowId']
    self.log.info("%s has compId %s" % (compLogName, compId))
    return S_OK(compId)

  def __updateVersionHistoryIfNeeded(self, compId, compDict):
    """
    Updates the version history given the condition dictionary and component id.
    """
    sqlCond = ["CompId=%s" % compId]
    sqlInsertFields = []
    sqlInsertValues = []
    tableName = self.__getTableName("VersionHistory")
    for field in ('version', 'DIRACVersion', 'platform'):
      if field not in compDict:
        return S_ERROR("Missing %s field in the component dict" % field)
      value = compDict[field]
      field = field.capitalize()
      sqlInsertFields.append(field)
      sqlInsertValues.append("'%s'" % value)
      sqlCond.append("%s = '%s'" % (field, value))
    result = self._query("SELECT CompId FROM `%s` WHERE %s" % (tableName, " AND ".join(sqlCond)))
    if not result['OK']:
      return result
    if len(result['Value']):
      return S_OK(compId)
    # It's not there, we just need to insert it
    sqlInsertFields.append('CompId')
    sqlInsertValues.append(str(compId))
    sqlInsertFields.append('VersionTimestamp')
    sqlInsertValues.append('UTC_TIMESTAMP()')
    if 'description' in compDict:
      sqlInsertFields.append("Description")
      result = self._escapeString(compDict['description'])
      if not result['OK']:
        return result
      sqlInsertValues.append(result['Value'])
    result = self._update("INSERT INTO `%s` ( %s ) VALUES ( %s )" % (tableName,
                                                                     ", ".join(sqlInsertFields),
                                                                     ", ".join(sqlInsertValues)))
    if not result['OK']:
      return result
    return S_OK(compId)

  def registerComponent(self, compDict, shallow=False):
    """
    Register a new component in the DB given a component dictionary and returns a component id.
    And if it's already registered it returns the corresponding component id.
    """
    result = self.__registerIfNotThere(compDict)
    if not result['OK']:
      return result
    compId = result['Value']
    if shallow:
      return S_OK(compId)
    # Check if something has changed in the version history
    result = self.__updateVersionHistoryIfNeeded(compId, compDict)
    if not result['OK']:
      return result
    return S_OK(compId)

  def heartbeat(self, compDict):
    """
    Updates the heartbeat
    """
    if 'compId' not in compDict:
      result = self.__registerIfNotThere(compDict)
      if not result['OK']:
        return result
      compId = result['Value']
      compDict['compId'] = compId
    sqlUpdateFields = ['LastHeartbeat=UTC_TIMESTAMP()']
    for field in ('cycles', 'queries'):
      value = 0
      if field in compDict:
        value = compDict[field]
      sqlUpdateFields.append("%s=%s" % (field.capitalize(), value))
    if 'startTime' in compDict:
      sqlUpdateFields.append("StartTime='%s'" % self.__datetime2str(compDict['startTime']))
    return self._update("UPDATE `%s` SET %s WHERE Id=%s" % (self.__getTableName("Components"),
                                                            ", ".join(sqlUpdateFields),
                                                            compDict['compId']))

  def __getComponents(self, condDict):
    """
    Loads the components from the DB.

    :type sourceDict: dictionary
    :param sourceDict: The dictionary containing source information.
    :return: S_OK with the components / the error message.
    """
    compTable = self.__getTableName("Components")
    mainFields = ", ".join(self.__mainFields)
    versionTable = self.__getTableName("VersionHistory")
    versionFields = ", ".join(self.__versionFields)
    sqlWhere = []
    for field in condDict:
      val = condDict[field]
      if isinstance(val, six.string_types):
        sqlWhere.append("%s='%s'" % (field, val))
      elif isinstance(val, six.integer_types + (float,)):
        sqlWhere.append("%s='%s'" % (field, val))
      else:
        sqlWhere.append("( %s )" % " OR ".join(["%s='%s'" % (field, v) for v in val]))
    if sqlWhere:
      sqlWhere = "WHERE %s" % " AND ".join(sqlWhere)
    else:
      sqlWhere = ""
    result = self._query("SELECT %s FROM `%s` %s" % (mainFields, compTable, sqlWhere))
    if not result['OK']:
      return result
    records = []
    dbData = result['Value']
    for record in dbData:
      rD = {}
      for i in range(len(self.__mainFields)):
        rD[self.__mainFields[i]] = record[i]
      result = self._query(
          "SELECT %s FROM `%s` WHERE CompId=%s ORDER BY VersionTimestamp DESC LIMIT 1" %
          (versionFields, versionTable, rD['Id']))
      if not result['OK']:
        return result
      if len(result['Value']) > 0:
        versionRec = result['Value'][0]
        for i in range(len(self.__versionFields)):
          rD[self.__versionFields[i]] = versionRec[i]
      del(rD['Id'])
      records.append(rD)
    return S_OK(StatusSet(records))

  def __checkCondition(self, condDict, field, value):
    """
    It is used to check if a field is present in the condition dictionary or not with the corresponding value.

    :type condDict: dictionary
    :param condDict: The dictionary containing the conditions.
    :type field: string
    :param field: The field.
    :type value: string
    :param field: The value.
    :return: True / False
    """
    if field not in condDict:
      return True
    condVal = condDict[field]
    if isinstance(condVal, (list, tuple)):
      return value in condVal
    return value == condVal

  def __getComponentDefinitionFromCS(self, system, setup, instance, cType, component):
    """
    Gets the basic component details from the configuration file.

    :type system: string
    :param system: The system name.
    :type setup: string
    :param setup: The setup site.
    :type instance: string
    :param instance: The instance.
    :type cType: string
    :param cType: The component type.
    :type component: string
    :param component: The component name.
    :return: a component dictionary.
    """
    componentName = "%s/%s" % (system, component)
    compDict = {'ComponentName': componentName,
                'Type': cType,
                'Setup': setup
                }
    componentSection = "/Systems/%s/%s/%s/%s" % (system, instance,
                                                 "%ss" % cType.capitalize(), component)
    compStatus = gConfig.getValue("%s/Status" % componentSection, 'Active')
    if compStatus.lower() in ("inactive", ):
      compDict['Status'] = compStatus.lower().capitalize()
    if cType == 'service':
      result = gConfig.getOption("%s/Port" % componentSection)
      if not result['OK']:
        compDict['Status'] = 'Error'
        compDict['Message'] = "Component seems to be defined wrong in CS: %s" % result['Message']
        return compDict
      try:
        compDict['Port'] = int(result['Value'])
      except BaseException:
        compDict['Status'] = 'Error'
        compDict['Message'] = "Port for component doesn't seem to be a number"
        return compDict
    return compDict

  def __componentMatchesCondition(self, compDict, requiredComponents, conditionDict={}):
    """
    This method uses __checkCondition method to check if the (key, field) inside component dictionary
    are already present in condition dictionary or not.
    """
    for key in compDict:
      if not self.__checkCondition(conditionDict, key, compDict[key]):
        return False
    return True

  def getComponentsStatus(self, conditionDict={}):
    """
    Get the status of the defined components in the CS compared to the ones that are known in the DB

    :type condDict: dictionary
    :param condDict: The dictionary containing the conditions.
    :return: S_OK with the requires results.
    """
    result = self.__getComponents(conditionDict)
    if not result['OK']:
      return result
    statusSet = result['Value']
    requiredComponents = {}
    result = gConfig.getSections("/DIRAC/Setups")
    if not result['OK']:
      return result
    for setup in result['Value']:
      if not self.__checkCondition(conditionDict, "Setup", setup):
        continue
      # Iterate through systems
      result = gConfig.getOptionsDict("/DIRAC/Setups/%s" % setup)
      if not result['OK']:
        return result
      systems = result['Value']
      for system in systems:
        instance = systems[system]
        # Check defined agents and serviecs
        for cType in ('agent', 'service'):
          # Get entries for the instance of a system
          result = gConfig.getSections("/Systems/%s/%s/%s" % (system, instance, "%ss" % cType.capitalize()))
          if not result['OK']:
            self.log.warn(
                "Opps, sytem seems to be defined wrong\n", "System %s at %s: %s" %
                (system, instance, result['Message']))
            continue
          components = result['Value']
          for component in components:
            componentName = "%s/%s" % (system, component)
            compDict = self.__getComponentDefinitionFromCS(system, setup, instance, cType, component)
            if self.__componentMatchesCondition(compDict, requiredComponents, conditionDict):
              statusSet.addUniqueToSet(requiredComponents, compDict)
        # Walk the URLs
        result = gConfig.getOptionsDict("/Systems/%s/%s/URLs" % (system, instance))
        if not result['OK']:
          self.log.warn("There doesn't to be defined the URLs section for %s in %s instance" % (system, instance))
        else:
          serviceURLs = result['Value']
          for service in serviceURLs:
            for url in List.fromChar(serviceURLs[service]):
              loc = url[url.find("://") + 3:]
              iS = loc.find("/")
              componentName = loc[iS + 1:]
              loc = loc[:iS]
              hostname, port = loc.split(":")
              compDict = {'ComponentName': componentName,
                          'Type': 'service',
                          'Setup': setup,
                          'Host': hostname,
                          'Port': int(port)
                          }
              if self.__componentMatchesCondition(compDict, requiredComponents, conditionDict):
                statusSet.addUniqueToSet(requiredComponents, compDict)
    # WALK THE DICT
    statusSet.setComponentsAsRequired(requiredComponents)
    return S_OK((statusSet.getRequiredComponents(),
                 self.__mainFields[1:] + self.__versionFields + ('Status', 'Message')))


class StatusSet(object):
  """
  This class is used to set component status as required and this method is used only by the
  ComponentMonitoringDB class.
  """
  def __init__(self, dbRecordsList=[]):
    self.__requiredSet = {}
    self.__requiredFields = ('Setup', 'Type', 'ComponentName')
    self.__maxSecsSinceHeartbeat = 600
    self.setDBRecords(dbRecordsList)

  def setDBRecords(self, recordsList):
    """
    This sets the DB records given a records list.

    :type recordsList: list
    :param recordsList: a set of records.
    :return: S_OK
    """
    self.__dbSet = {}
    for record in recordsList:
      cD = self.walkSet(self.__dbSet, record)
      cD.append(record)
    return S_OK()

  def addUniqueToSet(self, setDict, compDict):
    """
    Adds unique components to a separate set.

    :type setDict: dictionary
    :param setDict: The set dictionary.
    :type compDict: dictionary
    :param compDict: The dictionary containing the component information.
    """
    rC = self.walkSet(setDict, compDict)
    if compDict not in rC:
      rC.append(compDict)
      inactive = False
      for cD in rC:
        if 'Status' in cD and cD['Status'] == 'Inactive':
          inactive = True
          break
      if inactive:
        for cD in rC:
          cD['Status'] = 'Inactive'

  def walkSet(self, setDict, compDict, createMissing=True):
    """
    Updates the set dictionary.

    :type setDict: dictionary
    :param setDict: The set dictionary.
    :type compDict: dictionary
    :param compDict: The dictionary containing the component information.
    :type creatMissing: bool
    :param createMissing: A variable for adding missing values.
    :return: The set dictionary.
    """
    sD = setDict
    for field in self.__requiredFields:
      val = compDict[field]
      if val not in sD:
        if not createMissing:
          return None
        if field == self.__requiredFields[-1]:
          sD[val] = []
        else:
          sD[val] = {}
      sD = sD[val]
    return sD

  def __reduceComponentList(self, componentList):
    """
    Only keep the most restrictive components.

    :type componentList: list
    :param componentList: A list of components.
    :return: A list of reduced components.
    """
    for i in range(len(componentList)):
      component = componentList[i]
      for j in range(len(componentList)):
        if i == j or componentList[j] is False:
          continue
        potentiallyMoreRestrictiveComponent = componentList[j]
        match = True
        for key in component:
          if key not in potentiallyMoreRestrictiveComponent:
            match = False
            break
          if key == 'Host':
            result = Network.checkHostsMatch(component[key],
                                             potentiallyMoreRestrictiveComponent[key])
            if not result['OK'] or not result['Value']:
              match = False
              break
          else:
            if component[key] != potentiallyMoreRestrictiveComponent[key]:
              match = False
              break
        if match:
          componentList[i] = False
          break
    return [comp for comp in componentList if comp]

  def setComponentsAsRequired(self, requiredSet):
    """
    Sets component details according to the required set.

    :type requiredSet: dictionary
    :param requiredSet: The required set dictionary.
    """
    for setup in requiredSet:
      for cType in requiredSet[setup]:
        for name in requiredSet[setup][cType]:
          # Need to narrow down required
          cDL = requiredSet[setup][cType][name]
          cDL = self.__reduceComponentList(cDL)
          self.__setComponentListAsRequired(cDL)

  def __setComponentListAsRequired(self, compDictList):
    dbD = self.walkSet(self.__dbSet, compDictList[0], createMissing=False)
    if not dbD:
      self.__addMissingDefinedComponents(compDictList)
      return S_OK()
    self.__addFoundDefinedComponent(compDictList)
    return S_OK()

  def __addMissingDefinedComponents(self, compDictList):
    cD = self.walkSet(self.__requiredSet, compDictList[0])
    for compDict in compDictList:
      compDict = self.__setStatus(compDict, 'Error', "Component is not up or hasn't connected to register yet")
      cD.append(compDict)

  def __setStatus(self, compDict, status, message=False):
    """
    Sets status within the component dict.

    :type compDict: dictionary
    :param compDict: The component dictionary.
    :type status: string
    :param status: the status.
    :type message: bool
    :param message: the message.
    :return: A component dictionary.
    """
    if 'Status' in compDict:
      return compDict
    compDict['Status'] = status
    if message:
      compDict['Message'] = message
    return compDict

  def __addFoundDefinedComponent(self, compDictList):
    cD = self.walkSet(self.__requiredSet, compDictList[0])
    dbD = self.walkSet(self.__dbSet, compDictList[0])
    now = Time.dateTime()
    unmatched = compDictList
    for dbComp in dbD:
      if 'Status' not in dbComp:
        self.__setStatus(dbComp, 'OK')
        if dbComp['Type'] == "service":
          if 'Port' not in dbComp:
            self.__setStatus(dbComp, 'Error', "Port is not defined")
          elif dbComp['Port'] not in [compDict['Port'] for compDict in compDictList if 'Port' in compDict]:
            self.__setStatus(compDictList[-1], 'Error',
                             "Port (%s) is different that specified in the CS" % dbComp['Port'])
        elapsed = now - dbComp['LastHeartbeat']
        elapsed = elapsed.days * 86400 + elapsed.seconds
        if elapsed > self.__maxSecsSinceHeartbeat:
          self.__setStatus(dbComp, "Error",
                           "Last heartbeat was received at %s (%s secs ago)" % (dbComp['LastHeartbeat'],
                                                                                elapsed))
      cD.append(dbComp)
      # See if we have a perfect match
      newUnmatched = []
      for unmatchedComp in unmatched:
        perfectMatch = True
        for field in unmatchedComp:
          if field in ('Status', 'Message'):
            continue
          if field not in dbComp:
            perfectMatch = False
            continue
          if field == 'Host':
            result = Network.checkHostsMatch(unmatchedComp[field], dbComp[field])
            if not result['OK'] or not result['Value']:
              perfectMatch = False
          else:
            if unmatchedComp[field] != dbComp[field]:
              perfectMatch = False
        if not perfectMatch:
          newUnmatched.append(unmatchedComp)
      unmatched = newUnmatched
    for unmatchedComp in unmatched:
      self.__setStatus(unmatchedComp, "Error", "There is no component up with this properties")
      cD.append(unmatchedComp)

  def getRequiredComponents(self):
    return self.__requiredSet
