""" interacts with sqlite3 db
"""

import sqlite3
import os
import hashlib
import time

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.private.monitoring.Activity import Activity
from DIRAC.Core.Utilities import Time


class MonitoringCatalog(object):

  def __init__(self, dataPath):
    """
    Initialize monitoring catalog
    """
    self.dbConn = False
    self.dataPath = dataPath
    self.log = gLogger.getSubLogger("ActivityCatalog")
    self.createSchema()

  def __connect(self):
    """
    Connect to database
    """
    if not self.dbConn:
      dbPath = "%s/monitoring.db" % self.dataPath
      self.dbConn = sqlite3.connect(dbPath, isolation_level=None)

  def __dbExecute(self, query, values=False):
    """
    Execute a sql statement
    """
    cursor = self.dbConn.cursor()  # pylint: disable=no-member
    self.log.debug("Executing %s" % query)
    executed = False
    retry = 0
    while not executed and retry < 10:
      retry += 1
      try:
        if values:
          cursor.execute(query, values)
        else:
          cursor.execute(query)
        executed = True
      except Exception as e:
        self.log.exception("Exception executing statement", "query: %s, values: %s" % (query, values))
        time.sleep(0.01)
    self.log.error("Could not execute query, big mess ahead", "query: %s, values: %s" % (query, values))
    return cursor

  def __createTables(self):
    """
    Create tables if not already created
    """
    self.log.info("Creating tables in db")
    try:
      filePath = "%s/monitoringSchema.sql" % os.path.dirname(__file__)
      fd = open(filePath)
      buff = fd.read()
      fd.close()
    except IOError as e:
      DIRAC.abort(1, "Can't read monitoring schema", filePath)
    while buff.find(";") > -1:
      limit = buff.find(";") + 1
      sqlQuery = buff[: limit].replace("\n", "")
      buff = buff[limit:]
      try:
        self.__dbExecute(sqlQuery)
      except Exception as e:
        DIRAC.abort(1, "Can't create tables", str(e))

  def createSchema(self):
    """
    Create all the sql schema if it does not exist
    """
    self.__connect()
    try:
      sqlQuery = "SELECT name FROM sqlite_master WHERE type='table';"
      c = self.__dbExecute(sqlQuery)
      tablesList = c.fetchall()
      if len(tablesList) < 2:
        self.__createTables()
    except Exception as e:
      self.log.fatal("Failed to startup db engine", str(e))
      return False
    return True

  def __delete(self, table, dataDict):
    """
    Execute an sql delete
    """
    query = "DELETE FROM %s" % table
    valuesList = []
    keysList = []
    for key in dataDict:
      if isinstance(dataDict[key], list):
        orList = []
        for keyValue in dataDict[key]:
          valuesList.append(keyValue)
          orList.append("%s = ?" % key)
        keysList.append("( %s )" % " OR ".join(orList))
      else:
        valuesList.append(dataDict[key])
        keysList.append("%s = ?" % key)
    if keysList:
      query += " WHERE %s" % (" AND ".join(keysList))
    self.__dbExecute("%s;" % query, values=valuesList)

  def __select(self, fields, table, dataDict, extraCond="", queryEnd=""):
    """
    Execute a sql select
    """
    valuesList = []
    keysList = []
    for key in dataDict:
      if isinstance(dataDict[key], list):
        orList = []
        for keyValue in dataDict[key]:
          valuesList.append(keyValue)
          orList.append("%s = ?" % key)
        keysList.append("( %s )" % " OR ".join(orList))
      else:
        valuesList.append(dataDict[key])
        keysList.append("%s = ?" % key)
    if isinstance(fields, basestring):
      fields = [fields]
    if len(keysList) > 0:
      whereCond = "WHERE %s" % (" AND ".join(keysList))
    else:
      whereCond = ""
    if extraCond:
      if whereCond:
        whereCond += " AND %s" % extraCond
      else:
        whereCond = "WHERE %s" % extraCond
    query = "SELECT %s FROM %s %s %s;" % (",".join(fields),
                                          table,
                                          whereCond,
                                          queryEnd
                                          )
    c = self.__dbExecute(query, values=valuesList)
    return c.fetchall()

  def __insert(self, table, specialDict, dataDict):
    """
    Execute an sql insert
    """
    valuesList = []
    valuePoitersList = []
    namesList = []
    for key in specialDict:
      namesList.append(key)
      valuePoitersList.append(specialDict[key])
    for key in dataDict:
      namesList.append(key)
      valuePoitersList.append("?")
      valuesList.append(dataDict[key])
    query = "INSERT INTO %s (%s) VALUES (%s);" % (table,
                                                  ", ".join(namesList),
                                                  ",".join(valuePoitersList))
    c = self.__dbExecute(query, values=valuesList)
    return c.rowcount

  def __update(self, newValues, table, dataDict, extraCond=""):
    """
    Execute a sql update
    """
    valuesList = []
    keysList = []
    updateFields = []
    for key in newValues:
      updateFields.append("%s = ?" % key)
      valuesList.append(newValues[key])
    for key in dataDict:
      if isinstance(dataDict[key], list):
        orList = []
        for keyValue in dataDict[key]:
          valuesList.append(keyValue)
          orList.append("%s = ?" % key)
        keysList.append("( %s )" % " OR ".join(orList))
      else:
        valuesList.append(dataDict[key])
        keysList.append("%s = ?" % key)
    if len(keysList) > 0:
      whereCond = "WHERE %s" % (" AND ".join(keysList))
    else:
      whereCond = ""
    if extraCond:
      if whereCond:
        whereCond += " AND %s" % extraCond
      else:
        whereCond = "WHERE %s" % extraCond
    query = "UPDATE %s SET %s %s;" % (table,
                                      ",".join(updateFields),
                                      whereCond
                                      )
    c = self.__dbExecute(query, values=valuesList)
    return c.rowcount

  def registerSource(self, sourceDict):
    """
    Register an activity source
    """
    retList = self.__select("id", "sources", sourceDict)
    if len(retList) > 0:
      return retList[0][0]
    else:
      self.log.info("Registering source", str(sourceDict))
      if self.__insert("sources", {'id': 'NULL'}, sourceDict) == 0:
        return -1
      return self.__select("id", "sources", sourceDict)[0][0]

  def registerActivity(self, sourceId, acName, acDict):
    """
    Register an activity
    """
    m = hashlib.md5()
    acDict['name'] = acName
    acDict['sourceId'] = sourceId
    m.update(str(acDict))
    retList = self.__select("filename", "activities", acDict)
    if len(retList) > 0:
      return retList[0][0]
    else:
      acDict['lastUpdate'] = int(Time.toEpoch() - 86000)
      filePath = m.hexdigest()
      filePath = "%s/%s.rrd" % (filePath[:2], filePath)
      self.log.info("Registering activity", str(acDict))
      if self.__insert("activities", {'id': 'NULL',
                                      'filename': "'%s'" % filePath,
                                      },
                       acDict) == 0:
        return -1
      return self.__select("filename", "activities", acDict)[0][0]

  def getFilename(self, sourceId, acName):
    """
    Get rrd filename for an activity
    """
    queryDict = {'sourceId': sourceId, "name": acName}
    retList = self.__select("filename", "activities", queryDict)
    if len(retList) == 0:
      return ""
    else:
      return retList[0][0]

  def findActivity(self, sourceId, acName):
    """
    Find activity
    """
    queryDict = {'sourceId': sourceId, "name": acName}
    retList = self.__select(
        "id, name, category, unit, type, description, filename, bucketLength, lastUpdate",
        "activities",
        queryDict)
    if len(retList) == 0:
      return False
    else:
      return retList[0]

  def activitiesQuery(self, selDict, sortList, start, limit):
    fields = ['sources.id', 'sources.site', 'sources.componentType', 'sources.componentLocation',
              'sources.componentName', 'activities.id', 'activities.name', 'activities.category',
              'activities.unit', 'activities.type', 'activities.description',
              'activities.bucketLength', 'activities.filename', 'activities.lastUpdate']

    extraSQL = ""
    if sortList:
      for sorting in sortList:
        if sorting[0] not in fields:
          return S_ERROR("Sorting field %s is invalid" % sorting[0])
      extraSQL = "ORDER BY %s" % ",".join(["%s %s" % sorting for sorting in sortList])
    if limit:
      if start:
        extraSQL += " LIMIT %s OFFSET %s" % (limit, start)
      else:
        extraSQL += " LIMIT %s" % limit

    retList = self.__select(", ".join(fields), 'sources, activities', selDict, 'sources.id = activities.sourceId',
                            extraSQL)
    return S_OK((retList, fields))

  def setLastUpdate(self, sourceId, acName, lastUpdateTime):
    queryDict = {'sourceId': sourceId, "name": acName}
    return self.__update({'lastUpdate': lastUpdateTime}, "activities", queryDict)

  def getLastUpdate(self, sourceId, acName):
    queryDict = {'sourceId': sourceId, "name": acName}
    retList = self.__update('lastUpdate', "activities", queryDict)
    if len(retList) == 0:
      return False
    else:
      return retList[0]

  def queryField(self, field, definedFields):
    """
    Query the values of a field given a set of defined ones
    """
    retList = self.__select(field, "sources, activities", definedFields, "sources.id = activities.sourceId")
    return retList

  def getMatchingActivities(self, condDict):
    """
    Get all activities matching the defined conditions
    """
    retList = self.queryField(Activity.dbFields, condDict)
    acList = []
    for acData in retList:
      acList.append(Activity(acData))
    return acList

  def registerView(self, viewName, viewData, varFields):
    """
    Register a new view
    """
    retList = self.__select("id", "views", {'name': viewName})
    if len(retList) > 0:
      return S_ERROR("Name for view name already exists")
    retList = self.__select("name", "views", {'definition': viewData})
    if len(retList) > 0:
      return S_ERROR("View specification already defined with name '%s'" % retList[0][0])
    self.__insert("views", {'id': 'NULL'}, {'name': viewName,
                                            'definition': viewData,
                                            'variableFields': ", ".join(varFields)
                                            })
    return S_OK()

  def getViews(self, onlyStatic):
    """
    Get views
    """
    queryCond = {}
    if onlyStatic:
      queryCond['variableFields'] = ""
    return self.__select("id, name, variableFields", "views", queryCond)

  def getViewById(self, viewId):
    """
    Get a view for a given id
    """
    if isinstance(viewId, basestring):
      return self.__select("definition, variableFields", "views", {"name": viewId})
    else:
      return self.__select("definition, variableFields", "views", {"id": viewId})

  def deleteView(self, viewId):
    """
    Delete a view
    """
    self.__delete("views", {'id': viewId})

  def getSources(self, dbCond, fields=[]):
    if not fields:
      fields = "id, site, componentType, componentLocation, componentName"
    else:
      fields = ", ".join(fields)
    return self.__select(fields,
                         "sources",
                         dbCond)

  def getActivities(self, dbCond):
    return self.__select("id, name, category, unit, type, description, bucketLength",
                         "activities",
                         dbCond)

  def deleteActivity(self, sourceId, activityId):
    """
    Delete a view
    """
    acCond = {'sourceId': sourceId, 'id': activityId}
    acList = self.__select("filename", "activities", acCond)
    if len(acList) == 0:
      return S_ERROR("Activity does not exist")
    rrdFile = acList[0][0]
    self.__delete("activities", acCond)
    acList = self.__select("id", "activities", {'sourceId': sourceId})
    if len(acList) == 0:
      self.__delete("sources", {'id': sourceId})
    return S_OK(rrdFile)
