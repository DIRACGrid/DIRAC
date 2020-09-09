""" DISET request handler base class for the TransformationDB.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.TransformationSystem.DB.TransformationDB import TransformationDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

transTypes = [basestring, int, long]

__RCSID__ = "$Id$"

TASKS_STATE_NAMES = [
    'TotalCreated',
    'Created',
    'Running',
    'Submitted',
    'Failed',
    'Waiting',
    'Done',
    'Completed',
    'Stalled',
    'Killed',
    'Staging',
    'Checking',
    'Rescheduled',
    'Scheduled']
FILES_STATE_NAMES = ['PercentProcessed', 'Processed', 'Unused', 'Assigned', 'Total', 'Problematic',
                     'ApplicationCrash', 'MaxReset']

database = False


class TransformationManagerHandler(RequestHandler):

  def __init__(self, *args, **kargs):
    self.setDatabase(database)
    super(TransformationManagerHandler, self).__init__(*args, **kargs)

  def _parseRes(self, res):
    if not res['OK']:
      self.log.error('TransformationManager failure', res['Message'])
    return res

  def setDatabase(self, oDatabase):
    global database
    database = oDatabase

  types_getCounters = [basestring, list, dict]

  def export_getCounters(self, table, attrList, condDict, older=None, newer=None, timeStamp=None):
    res = database.getCounters(table, attrList, condDict, older=older, newer=newer, timeStamp=timeStamp)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the transformations table
  #

  types_addTransformation = [basestring, basestring, basestring, basestring, basestring, basestring, basestring]

  def export_addTransformation(self, transName, description, longDescription, transType, plugin, agentType, fileMask,
                               transformationGroup='General',
                               groupSize=1,
                               inheritedFrom=0,
                               body='',
                               maxTasks=0,
                               eventsPerTask=0,
                               addFiles=True,
                               inputMetaQuery=None,
                               outputMetaQuery=None):
    #    authorDN = self._clientTransport.peerCredentials['DN']
    #    authorGroup = self._clientTransport.peerCredentials['group']
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    authorGroup = credDict.get('group')
    res = database.addTransformation(transName, description, longDescription, authorDN, authorGroup, transType, plugin,
                                     agentType, fileMask,
                                     transformationGroup=transformationGroup,
                                     groupSize=groupSize,
                                     inheritedFrom=inheritedFrom,
                                     body=body,
                                     maxTasks=maxTasks,
                                     eventsPerTask=eventsPerTask,
                                     addFiles=addFiles,
                                     inputMetaQuery=inputMetaQuery,
                                     outputMetaQuery=outputMetaQuery)
    if res['OK']:
      self.log.info("Added transformation", res['Value'])
    return self._parseRes(res)

  types_deleteTransformation = [transTypes]

  def export_deleteTransformation(self, transName):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTransformation(transName, author=authorDN)
    return self._parseRes(res)

  types_cleanTransformation = [transTypes]

  def export_cleanTransformation(self, transName):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.cleanTransformation(transName, author=authorDN)
    return self._parseRes(res)

  types_setTransformationParameter = [transTypes, basestring]

  def export_setTransformationParameter(self, transName, paramName, paramValue):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.setTransformationParameter(transName, paramName, paramValue, author=authorDN)
    return self._parseRes(res)

  types_deleteTransformationParameter = [transTypes, basestring]

  def export_deleteTransformationParameter(self, transName, paramName):
    # credDict = self.getRemoteCredentials()
    # authorDN = credDict[ 'DN' ]
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTransformationParameter(transName, paramName)
    return self._parseRes(res)

  types_getTransformations = []

  def export_getTransformations(self, condDict=None, older=None, newer=None, timeStamp='CreationDate',
                                orderAttribute=None, limit=None, extraParams=False, offset=None):
    if not condDict:
      condDict = {}
    res = database.getTransformations(condDict=condDict,
                                      older=older,
                                      newer=newer,
                                      timeStamp=timeStamp,
                                      orderAttribute=orderAttribute,
                                      limit=limit,
                                      extraParams=extraParams,
                                      offset=offset)
    return self._parseRes(res)

  types_getTransformation = [transTypes]

  def export_getTransformation(self, transName, extraParams=False):
    res = database.getTransformation(transName, extraParams=extraParams)
    return self._parseRes(res)

  types_getTransformationParameters = [transTypes, [basestring, list, tuple]]

  def export_getTransformationParameters(self, transName, parameters):
    res = database.getTransformationParameters(transName, parameters)
    return self._parseRes(res)

  types_getTransformationWithStatus = [[basestring, list, tuple]]

  def export_getTransformationWithStatus(self, status):
    res = database.getTransformationWithStatus(status)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the TransformationFiles tables
  #

  types_addFilesToTransformation = [transTypes, [list, tuple]]

  def export_addFilesToTransformation(self, transName, lfns):
    res = database.addFilesToTransformation(transName, lfns)
    return self._parseRes(res)

  types_addTaskForTransformation = [transTypes]

  def export_addTaskForTransformation(self, transName, lfns=[], se='Unknown'):
    res = database.addTaskForTransformation(transName, lfns=lfns, se=se)
    return self._parseRes(res)

  def _wasFileInError(self, newStatus, currentStatus):
    """ Tells whether the file was Assigned and failed, i.e. was not Processed """
    return currentStatus.lower() == 'assigned' and newStatus.lower() != 'processed'

  types_setFileStatusForTransformation = [transTypes, dict]

  def export_setFileStatusForTransformation(self, transName, dictOfNewFilesStatus):
    """ Sets the file status for the transformation.

        The dictOfNewFilesStatus is a dictionary with the form:
        {12345: ('StatusA', errorA), 6789: ('StatusB',errorB),  ... } where the keys are fileIDs
        The tuple may be a string with only the status if the client was from an older version
    """

    if not dictOfNewFilesStatus:
      return S_OK({})

    statusSample = dictOfNewFilesStatus.values()[0]
    if isinstance(statusSample, (list, tuple)) and len(statusSample) == 2:
      newStatusForFileIDs = dictOfNewFilesStatus
    else:
      return S_ERROR("Status field should be two values")

    res = database._getConnectionTransID(False, transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']

    res = database.setFileStatusForTransformation(transID, newStatusForFileIDs, connection=connection)
    return self._parseRes(res)

  types_getTransformationStats = [transTypes]

  def export_getTransformationStats(self, transName):
    res = database.getTransformationStats(transName)
    return self._parseRes(res)

  types_getTransformationFilesCount = [transTypes, basestring]

  def export_getTransformationFilesCount(self, transName, field, selection={}):
    res = database.getTransformationFilesCount(transName, field, selection=selection)
    return self._parseRes(res)

  types_getTransformationFiles = []

  def export_getTransformationFiles(self, condDict=None, older=None, newer=None, timeStamp='LastUpdate',
                                    orderAttribute=None, limit=None, offset=None):
    if not condDict:
      condDict = {}
    res = database.getTransformationFiles(condDict=condDict, older=older, newer=newer, timeStamp=timeStamp,
                                          orderAttribute=orderAttribute, limit=limit, offset=offset,
                                          connection=False)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the TransformationTasks table
  #

  types_getTransformationTasks = []

  def export_getTransformationTasks(self, condDict=None, older=None, newer=None, timeStamp='CreationTime',
                                    orderAttribute=None, limit=None, inputVector=False, offset=None):
    if not condDict:
      condDict = {}
    res = database.getTransformationTasks(condDict=condDict, older=older, newer=newer, timeStamp=timeStamp,
                                          orderAttribute=orderAttribute, limit=limit, inputVector=inputVector,
                                          offset=offset)
    return self._parseRes(res)

  types_setTaskStatus = [transTypes, [list, int, long], basestring]

  def export_setTaskStatus(self, transName, taskID, status):
    res = database.setTaskStatus(transName, taskID, status)
    return self._parseRes(res)

  types_setTaskStatusAndWmsID = [transTypes, [long, int], basestring, basestring]

  def export_setTaskStatusAndWmsID(self, transName, taskID, status, taskWmsID):
    res = database.setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)
    return self._parseRes(res)

  types_getTransformationTaskStats = [transTypes]

  def export_getTransformationTaskStats(self, transName):
    res = database.getTransformationTaskStats(transName)
    return self._parseRes(res)

  types_deleteTasks = [transTypes, [long, int], [long, int]]

  def export_deleteTasks(self, transName, taskMin, taskMax):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.deleteTasks(transName, taskMin, taskMax, author=authorDN)
    return self._parseRes(res)

  types_extendTransformation = [transTypes, [long, int]]

  def export_extendTransformation(self, transName, nTasks):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    # authorDN = self._clientTransport.peerCredentials['DN']
    res = database.extendTransformation(transName, nTasks, author=authorDN)
    return self._parseRes(res)

  types_getTasksToSubmit = [transTypes, [long, int]]

  def export_getTasksToSubmit(self, transName, numTasks, site=''):
    """ Get information necessary for submission for a given number of tasks for a given transformation """
    res = database.getTransformation(transName)
    if not res['OK']:
      return self._parseRes(res)
    transDict = res['Value']
    submitDict = {}
    res = database.getTasksForSubmission(transName, numTasks=numTasks, site=site, statusList=['Created'])
    if not res['OK']:
      return self._parseRes(res)
    tasksDict = res['Value']
    for taskID, taskDict in tasksDict.items():
      res = database.reserveTask(transName, long(taskID))
      if not res['OK']:
        return self._parseRes(res)
      else:
        submitDict[taskID] = taskDict
    transDict['JobDictionary'] = submitDict
    return S_OK(transDict)

  ####################################################################
  #
  # These are the methods for TransformationMetaQueries table. It replaces methods
  # for the old TransformationInputDataQuery table
  #

  types_createTransformationMetaQuery = [transTypes, dict, basestring]

  def export_createTransformationMetaQuery(self, transName, queryDict, queryType):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    res = database.createTransformationMetaQuery(transName, queryDict, queryType, author=authorDN)
    return self._parseRes(res)

  types_deleteTransformationMetaQuery = [transTypes, basestring]

  def export_deleteTransformationMetaQuery(self, transName, queryType):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    res = database.deleteTransformationMetaQuery(transName, queryType, author=authorDN)
    return self._parseRes(res)

  types_getTransformationMetaQuery = [transTypes, basestring]

  def export_getTransformationMetaQuery(self, transName, queryType):
    # Here not calling _parseRes of the result because it should not log an error
    return database.getTransformationMetaQuery(transName, queryType)

  ####################################################################
  #
  # These are the methods for transformation logging manipulation
  #

  types_getTransformationLogging = [transTypes]

  def export_getTransformationLogging(self, transName):
    res = database.getTransformationLogging(transName)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods for transformation additional parameters
  #

  types_getAdditionalParameters = [transTypes]

  def export_getAdditionalParameters(self, transName):
    res = database.getAdditionalParameters(transName)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods for file manipulation
  #

  types_getFileSummary = [list]

  def export_getFileSummary(self, lfns):
    res = database.getFileSummary(lfns)
    return self._parseRes(res)

  types_addDirectory = [basestring]

  def export_addDirectory(self, path, force=False):
    res = database.addDirectory(path, force=force)
    return self._parseRes(res)

  types_exists = [list]

  def export_exists(self, lfns):
    res = database.exists(lfns)
    return self._parseRes(res)

  types_addFile = [[list, dict, basestring]]

  def export_addFile(self, fileDicts, force=False):
    """ Interface provides { LFN1 : { PFN1, SE1, ... }, LFN2 : { PFN2, SE2, ... } }
    """
    res = database.addFile(fileDicts, force=force)
    return self._parseRes(res)

  types_removeFile = [[list, dict]]

  def export_removeFile(self, lfns):
    """ Interface provides [ LFN1, LFN2, ... ]
    """
    if isinstance(lfns, dict):
      lfns = list(lfns)
    res = database.removeFile(lfns)
    return self._parseRes(res)

  types_setMetadata = [basestring, dict]

  def export_setMetadata(self, path, querydict):
    """ Set metadata to a file or to a directory (path)
    """
    res = database.setMetadata(path, querydict)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods used for web monitoring
  #

  # TODO Get rid of this (talk to Matvey)
  types_getDistinctAttributeValues = [basestring, dict]

  def export_getDistinctAttributeValues(self, attribute, selectDict):
    res = database.getTableDistinctAttributeValues('Transformations', [attribute], selectDict)
    if not res['OK']:
      return self._parseRes(res)
    return S_OK(res['Value'][attribute])

  types_getTableDistinctAttributeValues = [basestring, list, dict]

  def export_getTableDistinctAttributeValues(self, table, attributes, selectDict):
    res = database.getTableDistinctAttributeValues(table, attributes, selectDict)
    return self._parseRes(res)

  types_getTransformationStatusCounters = []

  def export_getTransformationStatusCounters(self):
    res = database.getCounters('Transformations', ['Status'], {})
    if not res['OK']:
      return self._parseRes(res)
    statDict = {}
    for attrDict, count in res['Value']:
      statDict[attrDict['Status']] = count
    return S_OK(statDict)

  types_getTransformationSummary = []

  def export_getTransformationSummary(self):
    """ Get the summary of the currently existing transformations """
    res = database.getTransformations()
    if not res['OK']:
      return self._parseRes(res)
    transList = res['Value']
    resultDict = {}
    for transDict in transList:
      transID = transDict['TransformationID']
      res = database.getTransformationTaskStats(transID)
      if not res['OK']:
        self.log.warn('Failed to get job statistics for transformation', transID)
        continue
      transDict['JobStats'] = res['Value']
      res = database.getTransformationStats(transID)
      if not res['OK']:
        transDict['NumberOfFiles'] = -1
      else:
        transDict['NumberOfFiles'] = res['Value']['Total']
      resultDict[transID] = transDict
    return S_OK(resultDict)

  types_getTabbedSummaryWeb = [basestring, dict, dict, list, int, int]

  def export_getTabbedSummaryWeb(self, table, requestedTables, selectDict, sortList, startItem, maxItems):
    tableDestinations = {'Transformations': {'TransformationFiles': ['TransformationID'],
                                             'TransformationTasks': ['TransformationID']},

                         'TransformationFiles': {'Transformations': ['TransformationID'],
                                                 'TransformationTasks': ['TransformationID', 'TaskID']},

                         'TransformationTasks': {'Transformations': ['TransformationID'],
                                                 'TransformationFiles': ['TransformationID', 'TaskID']}}

    tableSelections = {'Transformations': ['TransformationID', 'AgentType', 'Type', 'TransformationGroup',
                                           'Plugin'],
                       'TransformationFiles': ['TransformationID', 'TaskID', 'Status', 'UsedSE', 'TargetSE'],
                       'TransformationTasks': ['TransformationID', 'TaskID', 'ExternalStatus', 'TargetSE']}

    tableTimeStamps = {'Transformations': 'CreationDate',
                       'TransformationFiles': 'LastUpdate',
                       'TransformationTasks': 'CreationTime'}

    tableStatusColumn = {'Transformations': 'Status',
                         'TransformationFiles': 'Status',
                         'TransformationTasks': 'ExternalStatus'}

    resDict = {}
    res = self.__getTableSummaryWeb(table, selectDict, sortList, startItem, maxItems,
                                    selectColumns=tableSelections[table], timeStamp=tableTimeStamps[table],
                                    statusColumn=tableStatusColumn[table])
    if not res['OK']:
      self.log.error("Failed to get Summary for table", "%s %s" % (table, res['Message']))
      return self._parseRes(res)
    resDict[table] = res['Value']
    selections = res['Value']['Selections']
    tableSelection = {}
    for destination in tableDestinations[table].keys():
      tableSelection[destination] = {}
      for parameter in tableDestinations[table][destination]:
        tableSelection[destination][parameter] = selections.get(parameter, [])

    for table, paramDict in requestedTables.items():
      sortList = paramDict.get('SortList', [])
      startItem = paramDict.get('StartItem', 0)
      maxItems = paramDict.get('MaxItems', 50)
      res = self.__getTableSummaryWeb(table, tableSelection[table], sortList, startItem, maxItems,
                                      selectColumns=tableSelections[table], timeStamp=tableTimeStamps[table],
                                      statusColumn=tableStatusColumn[table])
      if not res['OK']:
        self.log.error("Failed to get Summary for table", "%s %s" % (table, res['Message']))
        return self._parseRes(res)
      resDict[table] = res['Value']
    return S_OK(resDict)

  types_getTransformationsSummaryWeb = [dict, list, int, int]

  def export_getTransformationsSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    return self.__getTableSummaryWeb('Transformations', selectDict, sortList, startItem, maxItems,
                                     selectColumns=['TransformationID', 'AgentType', 'Type', 'Group', 'Plugin'],
                                     timeStamp='CreationDate', statusColumn='Status')

  types_getTransformationTasksSummaryWeb = [dict, list, int, int]

  def export_getTransformationTasksSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    return self.__getTableSummaryWeb('TransformationTasks', selectDict, sortList, startItem, maxItems,
                                     selectColumns=['TransformationID', 'ExternalStatus', 'TargetSE'],
                                     timeStamp='CreationTime', statusColumn='ExternalStatus')

  types_getTransformationFilesSummaryWeb = [dict, list, int, int]

  def export_getTransformationFilesSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    return self.__getTableSummaryWeb('TransformationFiles', selectDict, sortList, startItem, maxItems,
                                     selectColumns=['TransformationID', 'Status', 'UsedSE', 'TargetSE'],
                                     timeStamp='LastUpdate', statusColumn='Status')

  def __getTableSummaryWeb(self, table, selectDict, sortList, startItem, maxItems, selectColumns=[],
                           timeStamp=None, statusColumn='Status'):
    fromDate = selectDict.get('FromDate', None)
    if fromDate:
      del selectDict['FromDate']
    # if not fromDate:
    #  fromDate = last_update
    toDate = selectDict.get('ToDate', None)
    if toDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None
    # Get the columns that match the selection
    fcn = None
    fcnName = "get%s" % table
    if hasattr(database, fcnName) and callable(getattr(database, fcnName)):
      fcn = getattr(database, fcnName)
    if not fcn:
      return S_ERROR("Unable to invoke database.%s, it isn't a member function of database" % fcnName)
    res = fcn(condDict=selectDict, older=toDate, newer=fromDate, timeStamp=timeStamp,
              orderAttribute=orderAttribute)
    if not res['OK']:
      return self._parseRes(res)

    # The full list of columns in contained here
    allRows = res['Records']
    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    # Create the total records entry
    resultDict['TotalRecords'] = len(allRows)
    # Create the ParameterNames entry
    resultDict['ParameterNames'] = res['ParameterNames']
    # Find which element in the tuple contains the requested status
    if statusColumn not in resultDict['ParameterNames']:
      return S_ERROR("Provided status column not present")
    statusColumnIndex = resultDict['ParameterNames'].index(statusColumn)

    # Get the rows which are within the selected window
    if resultDict['TotalRecords'] == 0:
      return S_OK(resultDict)
    ini = startItem
    last = ini + maxItems
    if ini >= resultDict['TotalRecords']:
      return S_ERROR('Item number out of range')
    if last > resultDict['TotalRecords']:
      last = resultDict['TotalRecords']
    selectedRows = allRows[ini:last]
    resultDict['Records'] = selectedRows

    # Generate the status dictionary
    statusDict = {}
    for row in selectedRows:
      status = row[statusColumnIndex]
      statusDict[status] = statusDict.setdefault(status, 0) + 1
    resultDict['Extras'] = statusDict

    # Obtain the distinct values of the selection parameters
    res = database.getTableDistinctAttributeValues(table, selectColumns, selectDict, older=toDate, newer=fromDate)
    distinctSelections = zip(selectColumns, [])
    if res['OK']:
      distinctSelections = res['Value']
    resultDict['Selections'] = distinctSelections

    return S_OK(resultDict)

  types_getTransformationSummaryWeb = [dict, list, int, int]

  def export_getTransformationSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the transformation information for a given page in the generic format """

    # Obtain the timing information from the selectDict
    last_update = selectDict.get('CreationDate', None)
    if last_update:
      del selectDict['CreationDate']
    fromDate = selectDict.get('FromDate', None)
    if fromDate:
      del selectDict['FromDate']
    if not fromDate:
      fromDate = last_update
    toDate = selectDict.get('ToDate', None)
    if toDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = []
      for i in sortList:
        orderAttribute += [i[0] + ":" + i[1]]
    else:
      orderAttribute = None

    # Get the transformations that match the selection
    res = database.getTransformations(condDict=selectDict, older=toDate, newer=fromDate,
                                      orderAttribute=orderAttribute)
    if not res['OK']:
      return self._parseRes(res)

    ops = Operations()
    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    trList = res['Records']
    # Create the total records entry
    nTrans = len(trList)
    resultDict['TotalRecords'] = nTrans
    # Create the ParameterNames entry
    # As this list is a reference to the list in the DB, we cannot extend it, therefore copy it
    resultDict['ParameterNames'] = list(res['ParameterNames'])
    # Add the job states to the ParameterNames entry
    taskStateNames = TASKS_STATE_NAMES + ops.getValue('Transformations/AdditionalTaskStates', [])
    resultDict['ParameterNames'] += ['Jobs_' + x for x in taskStateNames]
    # Add the file states to the ParameterNames entry
    fileStateNames = FILES_STATE_NAMES + ops.getValue('Transformations/AdditionalFileStates', [])
    resultDict['ParameterNames'] += ['Files_' + x for x in fileStateNames]

    # Get the transformations which are within the selected window
    if nTrans == 0:
      return S_OK(resultDict)
    ini = startItem
    last = ini + maxItems
    if ini >= nTrans:
      return S_ERROR('Item number out of range')
    if last > nTrans:
      last = nTrans
    transList = trList[ini:last]

    statusDict = {}
    extendableTranfs = ops.getValue('Transformations/ExtendableTransfTypes',
                                    ['Simulation', 'MCsimulation'])
    givenUpFileStatus = ops.getValue('Transformations/GivenUpFileStatus',
                                     ['MissingInFC'])
    problematicStatuses = ops.getValue('Transformations/ProblematicStatuses',
                                       ['Problematic'])
    # Add specific information for each selected transformation
    for trans in transList:
      transDict = dict(zip(resultDict['ParameterNames'], trans))

      # Update the status counters
      status = transDict['Status']
      statusDict[status] = statusDict.setdefault(status, 0) + 1

      # Get the statistics on the number of jobs for the transformation
      transID = transDict['TransformationID']
      res = database.getTransformationTaskStats(transID)
      taskDict = {}
      if res['OK'] and res['Value']:
        taskDict = res['Value']
      for state in taskStateNames:
        trans.append(taskDict.get(state, 0))

      # Get the statistics for the number of files for the transformation
      fileDict = {}
      transType = transDict['Type']
      if transType.lower() in extendableTranfs:
        fileDict['PercentProcessed'] = '-'
      else:
        res = database.getTransformationStats(transID)
        if res['OK']:
          fileDict = res['Value']
          total = fileDict['Total']
          for stat in givenUpFileStatus:
            total -= fileDict.get(stat, 0)
          processed = fileDict.get('Processed', 0)
          fileDict['PercentProcessed'] = "%.1f" % (int(processed * 1000. / total) / 10.) if total else 0.
      problematic = 0
      for stat in problematicStatuses:
        problematic += fileDict.get(stat, 0)
      fileDict['Problematic'] = problematic
      for state in fileStateNames:
        trans.append(fileDict.get(state, 0))

    resultDict['Records'] = transList
    resultDict['Extras'] = statusDict
    return S_OK(resultDict)

  ###########################################################################


def initializeTransformationManagerHandler(serviceInfo):
  global database
  database = TransformationDB('TransformationDB', 'Transformation/TransformationDB')
  return S_OK()
