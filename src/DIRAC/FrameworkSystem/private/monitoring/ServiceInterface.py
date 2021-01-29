""" This module exposes singleton gServiceInterface as istance of ServiceInterface (also in this module)

    Interacts with RRD (rrdtool), with ComponentMonitoringDB (mysql) and with MonitoringCatalog (sqlite3)

    Main clients are the monitoring handler (what's called by gMonitor object), and the web portal.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from past.builtins import long
from DIRAC import S_OK, S_ERROR

from DIRAC import gLogger, rootPath, gConfig
from DIRAC.Core.Utilities import DEncode, List

from DIRAC.FrameworkSystem.private.monitoring.RRDManager import RRDManager
from DIRAC.FrameworkSystem.private.monitoring.PlotCache import PlotCache
from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB import ComponentMonitoringDB
from DIRAC.FrameworkSystem.private.monitoring.MonitoringCatalog import MonitoringCatalog


class ServiceInterface(object):
  """
  The gServiceInterface singleton object which is used by the MonitoringHandle to register activities and
  commit the data is inherited from this class.
  """

  __sourceToComponentIdMapping = {}

  def __init__(self):
    self.dataPath = "%s/data/monitoring" % gConfig.getValue('/LocalSite/InstancePath', rootPath)
    self.plotsPath = "%s/plots" % self.dataPath
    self.rrdPath = "%s/rrd" % self.dataPath
    self.srvUp = False
    self.compmonDB = False

  def __createRRDManager(self):
    """
    Generates an RRDManager
    """
    return RRDManager(self.rrdPath, self.plotsPath)

  def __createCatalog(self):
    """
    Creates a Monitoring catalog connector
    """
    return MonitoringCatalog(self.dataPath)

  def initialize(self, dataPath):
    """
    Initializes monitoring server

    :type dataPath: string
    :param dataPath: The root path of the data directory.
    """

    self.dataPath = dataPath
    self.plotCache = PlotCache(RRDManager(self.rrdPath, self.plotsPath))
    self.srvUp = True
    try:
      self.compmonDB = ComponentMonitoringDB()
    except Exception as e:
      gLogger.exception("Cannot initialize component monitoring db: %s" % e)

  def initializeDB(self):
    """
    Initializes and creates monitoring catalog db if it doesn't exist
    """
    acCatalog = self.__createCatalog()
    if not acCatalog.createSchema():
      return False
    # Register default view if it's not there
    viewNames = [str(v[1]) for v in acCatalog.getViews(False)]
    if 'Dynamic component view' in viewNames:
      return True
    return acCatalog.registerView("Dynamic component view",
                                  DEncode.encode({'variable': ['sources.componentName'],
                                                  'definition': {},
                                                  'stacked': True,
                                                  'groupBy': ['activities.description'],
                                                  'label': '$SITE'}),
                                  ['sources.componentName'])

  def __checkSourceDict(self, sourceDict):
    """
    Checks that the dictionary is a valid source one

    :type sourceDict: dictionary
    :param sourceDict: The source dictionary containing information about "setup", "site", "componentType",
                                                                          "componentLocation", "componentName"
    :return: bool
    """
    validKeys = ("setup", "site", "componentType", "componentLocation", "componentName")
    for key in validKeys:
      if key not in sourceDict:
        return False
    return True

  def __checkActivityDict(self, acDict):
    """
    Checks that the dictionary is a valid activity one

    :type acDict: dictionary
    :param acDict: The activity dictionary containing information about 'category', 'description', 'bucketLength',
                                                                        'type', 'unit'.
    :return: bool
    """
    validKeys = ('category', 'description', 'bucketLength', 'type', 'unit')
    for key in validKeys:
      if key not in acDict:
        return False
    return True

  def registerActivities(self, sourceDict, activitiesDict, componentExtraInfo):
    """
    Registers new activities in the database.

    :type sourceDict: dictionary
    :param sourceDict: The source dictionary containing information about "setup", "site", "componentType",
                                                                          "componentLocation", "componentName"
    :type activitiesDict: dictionary
    :param activitiesDict: The activity dictionary containing information about 'category', 'description',
                                                                                'bucketLength', 'type', 'unit'
    :type componentExtraInfo: dictionary
    :param componentExtraInfo: This contains one or many of these  'version', 'DIRACVersion', 'description',
                                                                   'startTime', 'platform', 'cycles', 'queries'.
    :return: S_OK with sourceId / S_ERROR with the corresoponding message.
    """
    acCatalog = self.__createCatalog()
    rrdManager = self.__createRRDManager()
    # Register source
    if not self.__checkSourceDict(sourceDict):
      return S_ERROR("Source definition is not valid")
    sourceId = acCatalog.registerSource(sourceDict)
    # Register activities
    for name in activitiesDict:
      if not self.__checkActivityDict(activitiesDict[name]):
        return S_ERROR("Definition for activity %s is not valid" % name)
      activitiesDict[name]['name'] = name
      if 'bucketLength' not in activitiesDict[name]:
        activitiesDict[name]['bucketLength'] = 60
      if not self.__checkActivityDict(activitiesDict[name]):
        return S_ERROR("Activity %s definition is not valid" % name)
      gLogger.info("Received activity", "%s [%s]" % (name, str(activitiesDict[name])))
      rrdFile = acCatalog.registerActivity(sourceId, name, activitiesDict[name])
      if not rrdFile:
        return S_ERROR("Could not register activity %s" % name)
      retVal = rrdManager.create(activitiesDict[name]['type'], rrdFile, activitiesDict[name]['bucketLength'])
      if not retVal['OK']:
        return retVal
    self.__cmdb_registerComponent(sourceId, sourceDict, componentExtraInfo)
    return S_OK(sourceId)

  def commitMarks(self, sourceId, activitiesDict, componentExtraInfo):
    """
    Adds marks to activities.

    :type sourceId: int
    :param sourceId: This id is returned after the activity is registered inside the database and
                     is used for committing the marks.
    :type activitiesDict: dictionary
    :param activitiesDict: The activity dictionary containing information about 'category', 'description',
                                                                                'bucketLength', 'type', 'unit'
    :type componentExtraInfo: dictionary
    :param componentExtraInfo: This contains one or many of these  'version', 'DIRACVersion', 'description',
                                                                   'startTime', 'platform', 'cycles', 'queries'.
    :return: S_OK with a list of unregistered activities.
    """
    gLogger.info("Committing marks", "From %s for %s" % (sourceId, ", ".join(activitiesDict.keys())))
    acCatalog = self.__createCatalog()
    rrdManager = self.__createRRDManager()
    unregisteredActivities = []
    for acName in activitiesDict:
      acData = activitiesDict[acName]
      acInfo = acCatalog.findActivity(sourceId, acName)
      if not acInfo:
        unregisteredActivities.append(acName)
        gLogger.warn("Cant find rrd filename", "%s:%s activity" % (sourceId, acName))
        continue
      rrdFile = acInfo[6]
      if not rrdManager.existsRRDFile(rrdFile):
        gLogger.error("RRD file does not exist", "%s:%s activity (%s)" % (sourceId, acName, rrdFile))
        unregisteredActivities.append(acName)
        continue
      gLogger.info("Updating activity", "%s -> %s" % (acName, rrdFile))
      timeList = sorted(acData)
      entries = []
      for instant in timeList:
        entries.append((instant, acData[instant]))
      if len(entries) > 0:
        gLogger.verbose("There are %s entries for %s" % (len(entries), acName))
        retDict = rrdManager.update(acInfo[4], rrdFile, acInfo[7], entries, long(acInfo[8]))
        if not retDict['OK']:
          gLogger.error("There was an error updating", "%s:%s activity [%s]" % (sourceId, acName, rrdFile))
        else:
          acCatalog.setLastUpdate(sourceId, acName, retDict['Value'])
    if not self.__cmdb_heartbeatComponent(sourceId, componentExtraInfo):
      for acName in activitiesDict:
        if acName not in unregisteredActivities:
          unregisteredActivities.append(acName)
    return S_OK(unregisteredActivities)

  def fieldValue(self, field, definedFields):
    """
    Calculated values for a field given a set of defined values for other fields.

    :type field: list
    :param field: A set of fields.
    :type definedFields: list
    :param definedFields: A set of defined fields
    :return: S_OK with a list of the desired values.
    """
    retList = self.__createCatalog().queryField("DISTINCT %s" % field, definedFields)
    return S_OK(retList)

  def __getGroupedPlots(self, viewDescription):
    """
    Calculates grouped plots for a view.

    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :return: list of plots.
    """
    plotsList = []
    acCatalog = self.__createCatalog()
    groupList = acCatalog.queryField(
        "DISTINCT %s" %
        ", ".join(
            viewDescription['groupBy']),
        viewDescription['definition'])
    for grouping in groupList:
      gLogger.debug("Grouped plot for combination %s" % str(grouping))
      groupDefinitionDict = dict(viewDescription['definition'])
      for index in range(len(viewDescription['groupBy'])):
        groupDefinitionDict[viewDescription['groupBy'][index]] = grouping[index]
      activityList = acCatalog.getMatchingActivities(groupDefinitionDict)
      for activity in activityList:
        activity.setGroup(grouping)
        activity.setLabel(viewDescription['label'])
      plotsList.append(activityList)
    return plotsList

  def __generateGroupPlots(self, fromSecs, toSecs, viewDescription, size):
    """
    Generates grouped plots for a view.

    :type fromSecs: int
    :param fromSecs: A value in seconds from where to start.
    :type toSecs: int
    :param toSecs: A value in seconds for where to end.
    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :type size: int
    :param size: There is a matrix defined for size so here only one of these values go [0, 1, 2, 3].
    :return: S_OK with a list of files.
    """
    plotList = self.__getGroupedPlots(viewDescription)
    filesList = []
    for groupPlot in plotList:
      retVal = self.plotCache.groupPlot(fromSecs, toSecs, groupPlot, viewDescription['stacked'], size)
      if not retVal['OK']:
        gLogger.error("There was a problem ploting", retVal['Message'])
        return retVal
      graphFile = retVal['Value']
      gLogger.verbose("Generated graph", "file %s for group %s" % (graphFile, str(groupPlot[0])))
      filesList.append(graphFile)
    return S_OK(filesList)

  def __getPlots(self, viewDescription):
    """
    Calculates plots for a view.

    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :return: list of matching activities.
    """
    acCatalog = self.__createCatalog()
    return acCatalog.getMatchingActivities(viewDescription['definition'])

  def __generatePlots(self, fromSecs, toSecs, viewDescription, size):
    """
    Generates non grouped plots for a view.

    :type fromSecs: int
    :param fromSecs: A value in seconds from where to start.
    :type toSecs: int
    :param toSecs: A value in seconds for where to end.
    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :type size: int
    :param size: There is a matrix defined for size so here only one of these values go [0, 1, 2, 3].
    :return: S_OK with a list of files.
    """
    acList = self.__getPlots(viewDescription)
    filesList = []
    for activity in acList:
      activity.setLabel(viewDescription['label'])
      retVal = self.plotCache.plot(fromSecs, toSecs, activity, viewDescription['stacked'], size)
      if not retVal['OK']:
        gLogger.error("There was a problem ploting", retVal['Message'])
        return retVal
      graphFile = retVal['Value']
      gLogger.verbose("Generated graph", "file %s" % (graphFile))
      filesList.append(graphFile)
    return S_OK(filesList)

  def generatePlots(self, fromSecs, toSecs, viewDescription, size=1):
    """
    Generates plots for a view.

    :type fromSecs: int
    :param fromSecs: A value in seconds from where to start.
    :type toSecs: int
    :param toSecs: A value in seconds for where to end.
    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :type size: int
    :param size: There is a matrix defined for size so here only one of these values go [0, 1, 2, 3].
    :return: S_OK with a list of files.
    """
    gLogger.info("Generating plots", str(viewDescription))
    if 'stacked' not in viewDescription:
      viewDescription['stacked'] = False
    if 'label' not in viewDescription:
      viewDescription['label'] = ""
    if 'groupBy' in viewDescription and len(viewDescription['groupBy']):
      return self.__generateGroupPlots(fromSecs, toSecs, viewDescription, size)
    return self.__generatePlots(fromSecs, toSecs, viewDescription, size)

  def getGraphData(self, filename):
    """
    Reads the contents of a plot file.

    :type filename: string
    :param filname: The name of the file.
    :return: S_OK with all the data that is read.
    """
    try:
      with open("%s/%s" % (self.plotsPath, filename)) as fd:
        data = fd.read()
    except Exception as e:
      return S_ERROR(e)
    else:
      return S_OK(data)

  def saveView(self, viewName, viewDescription):
    """
    Saves a view in the catalog.

    :type viewName: string
    :param viewName: Name of the view.
    :type viewDescription: dictionary
    :param viewDescription: A dictionary containing the view description.
    :return: S_OK / S_ERROR with the corresponding error message.
    """
    if 'stacked' not in viewDescription:
      viewDescription['stacked'] = False
    if 'label' not in viewDescription:
      viewDescription['label'] = ""
    if 'variable' in viewDescription:
      for varField in viewDescription['variable']:
        if varField in viewDescription['definition']:
          del(viewDescription['definition'][varField])
    else:
      viewDescription['variable'] = []
    acCatalog = self.__createCatalog()
    return acCatalog.registerView(viewName, DEncode.encode(viewDescription), viewDescription['variable'])

  def getViews(self, onlyStatic=True):
    """
    Gets all stored views.

    :type onlyStatic: bool
    :param onlyStatic: Whether the views required are static or not.
    :return: S_OK with a list of views.
    """
    viewsList = self.__createCatalog().getViews(onlyStatic)
    return S_OK(viewsList)

  def plotView(self, viewRequest):
    """
    Generates all plots for a view.

    :type viewRequest: dictionary
    :param viewRequest: Containing information of the view like 'id', 'fromSecs', 'toSecs', etc.
    :return: S_OK with a list of files.
    """
    views = self.__createCatalog().getViewById(viewRequest['id'])
    if len(views) == 0:
      return S_ERROR("View does not exist")
    viewData = views[0]
    viewDefinition = DEncode.decode(str(viewData[0]))[0]
    neededVarFields = List.fromChar(viewData[1], ",")
    if len(neededVarFields) > 0:
      if 'varData' not in viewRequest:
        return S_ERROR("Missing variable fields %s!" % ", ".join(neededVarFields))
      missingVarFields = []
      for neededField in neededVarFields:
        if neededField in viewRequest['varData']:
          viewDefinition['definition'][neededField] = viewRequest['varData'][neededField]
        else:
          missingVarFields.append(neededField)
      if len(missingVarFields) > 0:
        return S_ERROR("Missing required fields %s!" % ", ".join(missingVarFields))
    return self.generatePlots(viewRequest['fromSecs'], viewRequest['toSecs'], viewDefinition, viewRequest['size'])

  def deleteView(self, viewId):
    """
    Deletes a view.

    :type viewId: string
    :param viewId: The view id.
    :return: S_OK
    """
    self.__createCatalog().deleteView(viewId)
    return S_OK()

  def getSources(self, dbCond={}, fields=[]):
    """
    Gets a list of activities.

    :type dbCond: dictionary
    :param dbCond: The required database conditions.
    :type fields: list
    :param fields: A list of required fields.
    :return: The list of results after the query is performed.
    """
    catalog = self.__createCatalog()
    return catalog.getSources(dbCond, fields)

  def getActivities(self, dbCond={}):
    """
    Gets a list of activities.

    :type dbCond: dictionary
    :param dbCond: The required database conditions.
    :return: an activity dictionary.
    """
    acDict = {}
    catalog = self.__createCatalog()
    for sourceTuple in catalog.getSources(dbCond):
      activityCond = {'sourceId': sourceTuple[0]}
      acDict[sourceTuple] = catalog.getActivities(activityCond)
    return acDict

  def getNumberOfActivities(self, dbCond={}):
    """
    Gets the number of activities.

    :type dbCond: dictionary
    :param dbCond: The required database conditions.
    :return: An integer indicating the total number of activities.
    """
    catalog = self.__createCatalog()
    total = 0
    for sourceTuple in catalog.getSources(dbCond):
      activityCond = {'sourceId': sourceTuple[0]}
      total += len(catalog.getActivities(activityCond))
    return total

  def getActivitiesContents(self, selDict, sortList, start, limit):
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
    return self.__createCatalog().activitiesQuery(selDict, sortList, start, limit)

  def deleteActivity(self, sourceId, activityId):
    """
    Deletes a view.

    :type sourceId: string
    :param sourceId: The source id.
    :type activityId: string
    :param activityId: The activity id.
    :return: S_OK / The corresponding error message.
    """
    retVal = self.__createCatalog().deleteActivity(sourceId, activityId)
    if not retVal['OK']:
      return retVal
    self.__createRRDManager().deleteRRD(retVal['Value'])
    return S_OK()

  # ComponentMonitoringDB functions

  def __cmdb__writeComponent(self, sourceId):
    """
    Used to write basic component details to the ComponentMonitoringDB(MySQLdb).

    :type sourceId: string
    :param sourceId: the source id.
    :return: True / False
    """
    if sourceId not in ServiceInterface.__sourceToComponentIdMapping:
      if not self.__cmdb__loadComponentFromActivityDB(sourceId):
        return False
    compDict = ServiceInterface.__sourceToComponentIdMapping[sourceId]
    result = self.compmonDB.registerComponent(compDict)
    if not result['OK']:
      gLogger.error("Cannot register component in ComponentMonitoringDB", result['Message'])
      return False
    compDict['compId'] = result['Value']
    self.__cmdb__writeHeartbeat(sourceId)
    gLogger.info("Registered component in component monitoring db")
    return True

  def __cmdb__merge(self, sourceId, extraDict):
    """
    Merges the cached dict.

    :type sourceId: string
    :param sourceId: the source id.
    :type extraDict: dictionary
    :param extraDict: The dictionary containing extra information.
    """
    compDict = ServiceInterface.__sourceToComponentIdMapping[sourceId]
    for field in self.compmonDB.getOptionalFields():
      if field in extraDict:
        compDict[field] = extraDict[field]
    ServiceInterface.__sourceToComponentIdMapping[sourceId] = compDict

  def __cmdb__loadComponentFromActivityDB(self, sourceId):
    """
    Loads the component dict from the activities it registered.

    :type sourceId: string
    :param sourceId: the source id.
    :return: True / False
    """
    sources = gServiceInterface.getSources({'id': sourceId},
                                           ['componentType', 'componentName', 'componentLocation', 'setup'])
    if len(sources) == 0:
      return False
    source = [ts for ts in sources if len(ts) > 0][0]
    compDict = {'type': source[0],
                'componentName': source[1],
                'host': source[2],
                'setup': source[3],
                }
    if compDict['type'] == 'service':
      loc = compDict['host']
      loc = loc[loc.find("://") + 3:]
      loc = loc[: loc.find("/")]
      compDict['host'] = loc[:loc.find(":")]
      compDict['port'] = loc[loc.find(":") + 1:]
    ServiceInterface.__sourceToComponentIdMapping[sourceId] = compDict
    return True

  def __cmdb__writeHeartbeat(self, sourceId):
    """
    Writes the heartbeat stamp to the ComponentMonitoringDB.

    :type sourceId: string
    :param sourceId: the source id.
    :return: True / False
    """
    compDict = ServiceInterface.__sourceToComponentIdMapping[sourceId]
    result = self.compmonDB.heartbeat(compDict)
    if not result['OK']:
      gLogger.error("Cannot heartbeat component in ComponentMonitoringDB", result['Message'])

  def __cmdb_registerComponent(self, sourceId, sourceDict, componentExtraInfo):
    """
    Writes all the basic component information to the ComponentMonitoringDB.

    :type sourceId: string
    :param sourceId: the source id.
    :type sourceDict: dictionary
    :param sourceDict: The dictionary containing source information.
    :type componentExtraInfo: dictionary
    :param componentExtraInfo: The dictionary containing extra information.
    """
    if sourceDict['componentType'] not in ('service', 'agent'):
      return
    compDict = {'componentName': sourceDict['componentName'],
                'setup': sourceDict['setup'],
                'type': sourceDict['componentType'],
                'host': sourceDict['componentLocation']
                }
    if compDict['type'] == 'service':
      loc = compDict['host']
      loc = loc[loc.find("://") + 3:]
      loc = loc[: loc.find("/")]
      compDict['host'] = loc[:loc.find(":")]
      compDict['port'] = loc[loc.find(":") + 1:]
    ServiceInterface.__sourceToComponentIdMapping[sourceId] = compDict
    self.__cmdb__merge(sourceId, componentExtraInfo)
    self.__cmdb__writeComponent(sourceId)

  def __cmdb_heartbeatComponent(self, sourceId, componentExtraInfo):
    """
    This method is used to write the Component heartbeat using the __cmdb__writeHeartbeat method.

    :type sourceId: string
    :param sourceId: the source id.
    :type componentExtraInfo: dictionary
    :param componentExtraInfo: The dictionary containing extra information.
    :return: True / False
    """
    # Component heartbeat
    if sourceId not in ServiceInterface.__sourceToComponentIdMapping:
      if not self.__cmdb__loadComponentFromActivityDB(sourceId):
        return False
    if ServiceInterface.__sourceToComponentIdMapping[sourceId]['type'] not in ('service', 'agent'):
      return True
    self.__cmdb__merge(sourceId, componentExtraInfo)
    self.__cmdb__writeHeartbeat(sourceId)
    return True

  def getComponentsStatus(self, condDict=False):
    """
    This method basically returns the component status by reading it from the ComponentMonitoringDB.

    :type condDict: dictionary
    :param condDict: The dictionary containing the conditions.
    :return: S_OK with the requires results.
    """
    if not condDict:
      condDict = {}
    return self.compmonDB.getComponentsStatus(condDict)


gServiceInterface = ServiceInterface()
