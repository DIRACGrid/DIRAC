""" Interacts with sqlite3 db
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import sqlite3
import os
import hashlib
import random
import time

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.private.monitoring.Activity import Activity
from DIRAC.Core.Utilities import Time


class MonitoringCatalog(object):
  """
  This class is used to perform all kinds queries to the sqlite3 database.
  """

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
    Connects to database
    """
    if not self.dbConn:
      dbPath = "%s/monitoring.db" % self.dataPath
      self.dbConn = sqlite3.connect(dbPath, timeout=20, isolation_level=None)
      # These two settings dramatically increase the performance
      # at the cost of a small corruption risk in case of OS crash
      # It is acceptable though, given the nature of the data
      # details here https://www.sqlite.org/pragma.html
      c = self.dbConn.cursor()
      c.execute("PRAGMA synchronous = OFF")
      c.execute("PRAGMA journal_mode = TRUNCATE")

  def __dbExecute(self, query, values=False):
    """
    Executes a sql statement.

    :type query: string
    :param query: The query to be executed.
    :type values: bool
    :param values: To execute query with values or not.
    :return: the cursor.
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
        time.sleep(random.random())
    if not executed:
      self.log.error("Could not execute query, big mess ahead", "query: %s, values: %s" % (query, values))
    return cursor

  def __createTables(self):
    """
    Creates tables if not already created
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
    Creates all the sql schema if it does not exist
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
    Executes an sql delete.

    :type table: string
    :param table: name of the table.
    :type dataDict: dictionary
    :param dataDict: the data dictionary.
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
    Executes a sql select.

    :type fields: string
    :param fields: The fields required in a string.
    :type table: string
    :param table: name of the table.
    :type dataDict: dictionary
    :param dataDict: the data dictionary.
    :return: a list of values.
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
    if isinstance(fields, six.string_types):
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
    Executes an sql insert.

    :type table: string
    :param table: name of the table.
    :type specialDict: dictionary
    :param specialDict: the special dictionary.
    :type dataDict: dictionary
    :param dataDict: the data dictionary.
    :return: the number of rows inserted.
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
    Executes a sql update.

    :type table: string
    :param table: name of the table.
    :type newValues: dictionary
    :param newValues: a dictionary with new values.
    :type dataDict: dictionary
    :param dataDict: the data dictionary.
    :return: the number of rows updated.
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
    Registers an activity source.

    :type sourceDict: dictionary
    :param sourceDict: the source dictionary.
    :return: a list of values.
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
    Register an activity.

    :type sourceId: string
    :param sourceId: The source id.
    :type acName: string
    :param acName: name of the activity.
    :type acDict: dictionary
    :param acDict: The activity dictionary containing information about 'category', 'description', 'bucketLength',
                                                                        'type', 'unit'.
    :return: a list of values.
    """
    m = hashlib.md5()
    acDict['name'] = acName
    acDict['sourceId'] = sourceId
    m.update(str(acDict).encode())
    retList = self.__select("filename", "activities", acDict)
    if len(retList) > 0:
      return retList[0][0]
    else:
      acDict['lastUpdate'] = int(Time.toEpoch() - 86000)
      filePath = m.hexdigest()
      filePath = "%s/%s.rrd" % (filePath[:2], filePath)
      self.log.info("Registering activity", str(acDict))
      # This is basically called by the ServiceInterface inside registerActivities method and then all the activity
      # information is stored in the sqlite3 db using the __insert method.

      if self.__insert("activities", {'id': 'NULL',
                                      'filename': "'%s'" % filePath,
                                      },
                       acDict) == 0:
        return -1
      return self.__select("filename", "activities", acDict)[0][0]

  def getFilename(self, sourceId, acName):
    """
    Gets rrd filename for an activity.

    :type sourceId: string
    :param sourceId: The source id.
    :type acName: string
    :param acName: name of the activity.
    :return: The filename in a string.
    """
    queryDict = {'sourceId': sourceId, "name": acName}
    retList = self.__select("filename", "activities", queryDict)
    if len(retList) == 0:
      return ""
    else:
      return retList[0][0]

  def findActivity(self, sourceId, acName):
    """
    Finds activity.

    :type sourceId: string
    :param sourceId: The source id.
    :type acName: string
    :param acName: name of the activity.
    :return: A list containing all the activity information.
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
    """
    Gets all the sources and activities details in a joined format.

    :type selDict: dictionary
    :param selDict: The fields inside the select query.
    :type sortList: list
    :param sortList: A list in sorted order of the data.
    :type start: int
    :param start: The point or tuple from where to start.
    :type limit: int
    :param limit: The number of tuples to select from the starting point.
    :return: S_OK with a tuple of the result list and fields list.
    """
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
    # This method basically takes in some condition and then based on those performs SQL Join on the
    # sources and activities table of the sqlite3 db and returns the corresponding result.
    retList = self.__select(", ".join(fields), 'sources, activities', selDict, 'sources.id = activities.sourceId',
                            extraSQL)
    return S_OK((retList, fields))

  def setLastUpdate(self, sourceId, acName, lastUpdateTime):
    """
    Updates the lastUpdate timestamp for a particular activity using the source id.

    :type sourceId: string
    :param sourceId: The source id.
    :type acName: string
    :param acName: name of the activity.
    :type lastUpdateTime: string
    :param lastUpdateTime: The last update time in the proper format.
    :return: the number of rows updated.
    """
    queryDict = {'sourceId': sourceId, "name": acName}
    return self.__update({'lastUpdate': lastUpdateTime}, "activities", queryDict)

  def getLastUpdate(self, sourceId, acName):
    """
    Gets the lastUpdate timestamp for a particular activity using the source id.

    :type sourceId: string
    :param sourceId: The source id.
    :type acName: string
    :param acName: name of the activity.
    :return: The last update time in string.
    """
    queryDict = {'sourceId': sourceId, "name": acName}
    retList = self.__update('lastUpdate', "activities", queryDict)
    if len(retList) == 0:
      return False
    else:
      return retList[0]

  def queryField(self, field, definedFields):
    """
    Query the values of a field given a set of defined ones.

    :type field: string
    :param field: The field required in a string.
    :type field: list
    :param definedFields: A set of defined fields.
    :return: A list of values.
    """
    retList = self.__select(field, "sources, activities", definedFields, "sources.id = activities.sourceId")
    return retList

  def getMatchingActivities(self, condDict):
    """
    Gets all activities matching the defined conditions.

    :type condDict: dictionary.
    :param condDict: A dictionary containing the conditions.
    :return: a list of matching activities.
    """
    retList = self.queryField(Activity.dbFields, condDict)
    acList = []
    for acData in retList:
      acList.append(Activity(acData))
    return acList

  def registerView(self, viewName, viewData, varFields):
    """
    Registers a new view.

    :type viewName: string
    :param viewName: Name of the view.
    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :type varFields: list
    :param varFields: A list of variable fields.
    :return: S_OK / S_ERROR with the corresponding error message.
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
    Gets views.

    :type onlyStatic: bool
    :param onlyStatic: Whether the views required are static or not.
    :return: A list of values.
    """
    queryCond = {}
    if onlyStatic:
      queryCond['variableFields'] = ""
    return self.__select("id, name, variableFields", "views", queryCond)

  def getViewById(self, viewId):
    """
    Gets a view for a given id.

    :type viewId: string
    :param viewId: The view id.
    :return: A list of values.
    """
    if isinstance(viewId, six.string_types):
      return self.__select("definition, variableFields", "views", {"name": viewId})
    else:
      return self.__select("definition, variableFields", "views", {"id": viewId})

  def deleteView(self, viewId):
    """
    Deletes a view for a given id.

    :type viewId: string
    :param viewId: The view id.
    """
    self.__delete("views", {'id': viewId})

  def getSources(self, dbCond, fields=[]):
    """
    Gets souces for a given db condition.

    :type dbCond: dictionary
    :param dbCond: The required database conditions.
    :type fields: list
    :param fields: A list of required fields.
    :return: The list of results after the query is performed.
    """
    if not fields:
      fields = "id, site, componentType, componentLocation, componentName"
    else:
      fields = ", ".join(fields)
    return self.__select(fields,
                         "sources",
                         dbCond)

  def getActivities(self, dbCond):
    """
    Gets activities given a db condition.

    :type dbCond: dictionary
    :param dbCond: The required database conditions.
    :return: a list of activities.
    """
    return self.__select("id, name, category, unit, type, description, bucketLength",
                         "activities",
                         dbCond)

  def deleteActivity(self, sourceId, activityId):
    """
    Deletes an activity.

    :type sourceId: string
    :param sourceId: The source id.
    :type activityId: string
    :param activityId: The activity id.
    :return: S_OK with rrd filename / S_ERROR with a message.
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
