""" DISET request handler base class for the TransformationDB."""
# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import *

transTypes = list(StringTypes)+[IntType,LongType]

class TransformationHandler(RequestHandler):

  def setDatabase(self,oDatabase):
    self.database = oDatabase

  types_getName = []
  def export_getName(self):
    res = self.database.getName()
    return self.__parseRes(res)

  types_getCounters = [StringType,ListType,DictType]
  def export_getCounters(self, table, attrList, condDict, older=None, newer=None, timeStamp=None):
    res = self.database.getCounters(table, attrList, condDict, older=older, newer=newer, timeStamp=timeStamp)
    return self.__parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the transformations table
  #
    
  types_addTransformation = [ StringType, StringType, StringType, StringType, StringType, StringType, StringType]
  def export_addTransformation(self,transName,description,longDescription,type,plugin,agentType,fileMask,
                                    transformationGroup = 'General',
                                    groupSize           = 1,
                                    inheritedFrom       = 0,
                                    body                = '', 
                                    maxTasks            = 0,
                                    eventsPerTask       = 0,
                                    addFiles            = True):    
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    res = self.database.addTransformation(transName,description,longDescription,authorDN,authorGroup,type,plugin,agentType,fileMask,
                                    transformationGroup = transformationGroup,
                                    groupSize           = groupSize,
                                    inheritedFrom       = inheritedFrom,
                                    body                = body, 
                                    maxTasks            = maxTasks,
                                    eventsPerTask       = eventsPerTask,
                                    addFiles            = addFiles)    
    if res['OK']:
      gLogger.info("Added transformation %d" % res['Value'])  
    return self.__parseRes(res)

  types_deleteTransformation = [transTypes]
  def export_deleteTransformation(self, transName):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.deleteTransformation(transName,author=authorDN)
    return self.__parseRes(res)
  
  types_cleanTransformation = [transTypes]
  def export_cleanTransformation(self, transName):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.cleanTransformation(transName,author=authorDN)
    return self.__parseRes(res)

  types_setTransformationParameter = [transTypes,StringTypes]
  def export_setTransformationParameter(self,transName,paramName,paramValue):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.setTransformationParameter(transName,paramName,paramValue,author=authorDN)
    return self.__parseRes(res)

  types_deleteTransformationParameter = [transTypes,StringTypes]
  def export_deleteTransformationParameter(self,transName,paramName):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.deleteTransformationParameter(transName,paramName)
    return self.__parseRes(res)  
  
  types_getTransformations = []
  def export_getTransformations(self,condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False):
    res = self.database.getTransformations(condDict=condDict,
                                  older=older,
                                  newer=newer,
                                  timeStamp=timeStamp,
                                  orderAttribute=orderAttribute,
                                  limit=limit, 
                                  extraParams=extraParams)
    return self.__parseRes(res)

  types_getTransformation = [transTypes]
  def export_getTransformation(self,transName,extraParams=False):
    res = self.database.getTransformation(transName,extraParams=extraParams)
    return self.__parseRes(res)

  types_getTransformationParameters = [transTypes,[ListType,TupleType]]
  def export_getTransformationParameters(self,transName,parameters):
    res = self.database.getTransformationParameters(transName,parameters)
    return self.__parseRes(res)

  types_getTransformationWithStatus = [list(StringTypes)+[ListType,TupleType]]
  def export_getTransformationWithStatus(self,status):
    res = self.database.getTransformationWithStatus(status)
    return self.__parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the T_* tables
  #

  types_addFilesToTransformation = [transTypes,[ListType,TupleType]]
  def export_addFilesToTransformation(self,transName,lfns):
    res = self.database.addFilesToTransformation(transName,lfns)
    return self.__parseRes(res)

  types_addTaskForTransformation = [transTypes]
  def export_addTaskForTransformation(self,transName,lfns=[],se='Unknown'):
    res = self.database.addTaskForTransformation(transName, lfns=lfns, se=se)
    return self.__parseRes(res)

  types_setFileStatusForTransformation = [transTypes,StringTypes,ListType]
  def export_setFileStatusForTransformation(self,transName,status,lfns):
    res = self.database.setFileStatusForTransformation(transName,status,lfns)
    return self.__parseRes(res)

  types_setFileUsedSEForTransformation = [transTypes,StringTypes,ListType]
  def export_setFileUsedSEForTransformation(self,transName,usedSE,lfns):
    res = self.database.setFileUsedSEForTransformation(transName,usedSE,lfns)
    return self.__parseRes(res)

  types_getTransformationStats = [transTypes]
  def export_getTransformationStats(self,transName):
    res = self.database.getTransformationStats(transName)
    return self.__parseRes(res)

  types_getTransformationFilesCount = [transTypes,StringTypes]
  def export_getTransformationFilesCount(self,transName,field):
    res = self.database.getTransformationFilesCount(transName,field)
    return self.__parseRes(res)

  types_getTransformationFiles = []
  def export_getTransformationFiles(self,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None):
    res = self.database.getTransformationFiles(condDict=condDict,older=older, newer=newer, timeStamp=timeStamp, orderAttribute=orderAttribute, limit=limit,connection=False)
    return self.__parseRes(res)
  
  ####################################################################
  #
  # These are the methods to manipulate the TransformationTasks table
  #
  
  types_getTransformationTasks = []
  def export_getTransformationTasks(self,condDict={},older=None, newer=None, timeStamp='CreationTime', orderAttribute=None, limit=None, inputVector=False):
    res = self.database.getTransformationTasks(condDict=condDict,older=older,newer=newer,timeStamp=timeStamp,orderAttribute=orderAttribute,limit=limit,inputVector=inputVector)
    return self.__parseRes(res)
  
  types_setTaskStatus = [transTypes, [ListType,IntType,LongType], StringTypes]
  def export_setTaskStatus(self, transName, taskID, status):
    res = self.database.setTaskStatus(transName, taskID, status)    
    return self.__parseRes(res)

  types_setTaskStatusAndWmsID = [ transTypes, [LongType,IntType], StringType, StringType]
  def export_setTaskStatusAndWmsID(self, transName, taskID, status, taskWmsID):
    res = self.database.setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)
    return self.__parseRes(res)
  
  types_getTransformationTaskStats = [transTypes]
  def export_getTransformationTaskStats(self, transName):
    res = self.database.getTransformationTaskStats(transName)
    return self.__parseRes(res)

  types_deleteTasks = [transTypes, [LongType,IntType], [LongType,IntType]]
  def export_deleteTasks(self, transName, taskMin, taskMax):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.deleteTasks(transName, taskMin, taskMax,author=authorDN)
    return self.__parseRes(res)

  types_extendTransformation = [transTypes, [LongType, IntType]]
  def export_extendTransformation( self, transName, nTasks):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.extendTransformation(transName, nTasks, author=authorDN)
    return self.__parseRes(res)

  types_getTasksToSubmit = [transTypes, [LongType, IntType]]
  def export_getTasksToSubmit(self,transName,numTasks,site=''):
    """ Get information necessary for submission for a given number of tasks for a given transformation """
    res = self.database.getTransformation(transName)
    if not res['OK']:
      return self.__parseRes(res)
    transDict = res['Value']
    status = transDict['Status']
    submitDict = {}
    if status in ['Active','Completing','Flush']:
      res = self.database.getTasksForSubmission(transName,numTasks=numTasks,site=site,statusList=['Created'])
      if not res['OK']:
        return self.__parseRes(res)
      tasksDict = res['Value']
      for taskID,taskDict in tasksDict.items():
        res = self.database.reserveTask(transName, long(taskID))
        if not res['OK']:
          return self.__parseRes(res)
        else:
          submitDict[taskID] = taskDict
    transDict['JobDictionary'] = submitDict
    return S_OK(transDict)

  ####################################################################
  #
  # These are the methods for transformation logging manipulation
  #

  types_getTransformationLogging = [transTypes]
  def export_getTransformationLogging(self,transName):
    res = self.database.getTransformationLogging(transName)
    return self.__parseRes(res)
  
  ####################################################################
  #
  # These are the methods for file manipulation
  #

  types_getFileSummary = [ListType,transTypes]
  def export_getFileSummary(self,lfns,transName):
    res = self.database.getFileSummary(lfns,transName)
    return self.__parseRes(res)

  types_addDirectory = [StringType]
  def export_addDirectory(self,path,force=False):
    res = self.database.addDirectory(path,force=force)
    return self.__parseRes(res)

  types_exists = [ListType]
  def export_exists(self,lfns):
    res = self.database.exists(lfns)
    return self.__parseRes(res)

  types_addFile = [ListType]
  def export_addFile(self,fileTuples,force=False):
    res = self.database.addFile(fileTuples,force=force)
    return self.__parseRes(res)

  types_removeFile = [ListType]
  def export_removeFile(self,lfns):
    res = self.database.removeFile(lfns)
    return self.__parseRes(res)

  ####################################################################
  #
  # These are the methods for replica manipulation
  #

  types_addReplica = [ListType]
  def export_addReplica(self,replicaTuples,force=False):
    res = self.database.addReplica(replicaTuples,force=force)
    return self.__parseRes(res)

  types_removeReplica = [ListType]
  def export_removeReplica(self,replicaTuples):
    res = self.database.removeReplica(replicaTuples)
    return self.__parseRes(res)

  types_getReplicas = [ListType]
  def export_getReplicas(self,lfns):
    res = self.database.getReplicas(lfns)
    return self.__parseRes(res)

  types_getReplicaStatus = [ListType]
  def export_getReplicaStatus(self,replicaTuples):
    res = self.database.getReplicaStatus(replicaTuples)
    return self.__parseRes(res)

  types_setReplicaStatus = [ListType]
  def export_setReplicaStatus(self,replicaTuples):
    res = self.database.setReplicaStatus(replicaTuples)
    return self.__parseRes(res)

  types_setReplicaHost = [ListType]
  def export_setReplicaHost(self,replicaTuples):
    res = self.database.setReplicaHost(replicaTuples)
    return self.__parseRes(res)

  ####################################################################
  #
  # These are the methods used for web monitoring
  #
    
  #TODO Get rid of this (talk to Matvey)
  types_getDistinctAttributeValues = [StringTypes, DictType]
  def export_getDistinctAttributeValues(self, attribute, selectDict):
    res = self.database.getTableDistinctAttributeValues('Transformations',[attribute],selectDict)
    if not res['OK']:
      return self.__parseRes(res)
    return S_OK(res['Value'][attribute])

  types_getTableDistinctAttributeValues = [StringTypes,ListType, DictType]
  def export_getTableDistinctAttributeValues(self, table, attributes, selectDict):   
    res = self.database.getTableDistinctAttributeValues(table,attributes,selectDict)
    return self.__parseRes(res)

  types_getTransformationStatusCounters = []
  def export_getTransformationStatusCounters( self ):
    res = self.database.getCounters('Transformations',['Status'],{})
    if not res['OK']:
      return self.__parseRes(res)
    statDict = {}
    for attrDict,count in res['Value']:
      statDict[attrDict['Status']] = count
    return S_OK(statDict)

  types_getTransformationSummary = []
  def export_getTransformationSummary(self):
    """ Get the summary of the currently existing transformations """
    res = self.database.getTransformations()
    if not res['OK']:
      return self.__parseRes(res)
    transList = res['Value']
    resultDict = {}
    for transDict in transList:
      transID = transDict['TransformationID']
      res = self.database.getTransformationTaskStats(transID)
      if not res['OK']:
        gLogger.warn('Failed to get job statistics for transformation %d' % transID)
        continue
      transDict['JobStats'] = res['Value']
      res = self.database.getTransformationStats(transID)
      if not res['OK']:
        transDict['NumberOfFiles'] = -1
      else:
        transDict['NumberOfFiles'] = res['Value']['Total']
      resultDict[transID] = transDict
    return S_OK(resultDict)

  types_getTabbedSummaryWeb = [StringTypes,DictType,DictType, ListType, IntType, IntType]
  def export_getTabbedSummaryWeb(self,table,requestedTables,selectDict,sortList,startItem,maxItems):
    tableDestinations = {  'Transformations'      : { 'TransformationFiles' : ['TransformationID'],
                                                      'TransformationTasks' : ['TransformationID']           },

                           'TransformationFiles'  : { 'Transformations'     : ['TransformationID'],
                                                      'TransformationTasks' : ['TransformationID','TaskID']  },

                           'TransformationTasks'  : { 'Transformations'     : ['TransformationID'],
                                                      'TransformationFiles' : ['TransformationID','TaskID']  } }

    tableSelections = {    'Transformations'      : ['TransformationID','AgentType','Type','TransformationGroup','Plugin'],
                           'TransformationFiles'  : ['TransformationID','TaskID','Status','UsedSE','TargetSE'], 
                           'TransformationTasks'  : ['TransformationID','TaskID','ExternalStatus','TargetSE'] } 

    tableTimeStamps = {    'Transformations'      : 'CreationDate',
                           'TransformationFiles'  : 'LastUpdate',
                           'TransformationTasks'  : 'CreationTime' }
   
    tableStatusColumn= {   'Transformations'      : 'Status',
                           'TransformationFiles'  : 'Status',
                           'TransformationTasks'  : 'ExternalStatus' }

    resDict = {}
    res = self.__getTableSummaryWeb(table,selectDict,sortList,startItem,maxItems,selectColumns=tableSelections[table],timeStamp=tableTimeStamps[table],statusColumn=tableStatusColumn[table])
    if not res['OK']:
      gLogger.error("Failed to get Summary for table","%s %s" % (table,res['Message']))
      return self.__parseRes(res)
    resDict[table] = res['Value']
    selections = res['Value']['Selections']
    tableSelection = {}
    for destination in tableDestinations[table].keys():
      tableSelection[destination] = {}
      for parameter in tableDestinations[table][destination]:
        tableSelection[destination][parameter] = selections.get(parameter,[])

    for table,paramDict in requestedTables.items():
      sortList = paramDict.get('SortList',[])
      startItem = paramDict.get('StartItem',0)
      maxItems = paramDict.get('MaxItems',50)
      res = self.__getTableSummaryWeb(table,tableSelection[table],sortList,startItem,maxItems,selectColumns=tableSelections[table],timeStamp=tableTimeStamps[table],statusColumn=tableStatusColumn[table])
      if not res['OK']:
        gLogger.error("Failed to get Summary for table","%s %s" % (table,res['Message']))
        return self.__parseRes(res)
      resDict[table] = res['Value']
    return S_OK(resDict)
             
  types_getTransformationsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationsSummaryWeb(self,selectDict,sortList,startItem,maxItems):
    return self.__getTableSummaryWeb('Transformations',selectDict,sortList,startItem,maxItems,selectColumns=['TransformationID','AgentType','Type','Group','Plugin'],timeStamp='CreationDate',statusColumn='Status')

  types_getTransformationTasksSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationTasksSummaryWeb(self,selectDict,sortList,startItem,maxItems):
    return self.__getTableSummaryWeb('TransformationTasks',selectDict,sortList,startItem,maxItems,selectColumns=['TransformationID','ExternalStatus','TargetSE'],timeStamp='CreationTime',statusColumn='ExternalStatus')
 
  types_getTransformationFilesSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationFilesSummaryWeb(self,selectDict,sortList,startItem,maxItems):
    return self.__getTableSummaryWeb('TransformationFiles',selectDict,sortList,startItem,maxItems,selectColumns=['TransformationID','Status','UsedSE','TargetSE'],timeStamp='LastUpdate',statusColumn='Status')  

  def __getTableSummaryWeb(self,table,selectDict,sortList,startItem,maxItems,selectColumns=[],timeStamp=None,statusColumn='Status'):
    fromDate = selectDict.get('FromDate',None)
    if fromDate:
      del selectDict['FromDate']
    #if not fromDate:
    #  fromDate = last_update  
    toDate = selectDict.get('ToDate',None)    
    if toDate:
      del selectDict['ToDate']  
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None
    # Get the columns that match the selection
    execString = "res = self.database.get%s(condDict=selectDict,older=toDate, newer=fromDate, timeStamp=timeStamp, orderAttribute=orderAttribute)" % table
    exec(execString)
    if not res['OK']:
      return self.__parseRes(res)

    # The full list of columns in contained here
    allRows = res['Records']
    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    # Create the total records entry
    resultDict['TotalRecords'] = len(allRows)
    # Create the ParameterNames entry
    resultDict['ParameterNames'] = res['ParameterNames']
    # Find which element in the tuple contains the requested status
    if not statusColumn in resultDict['ParameterNames']:
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
      if not statusDict.has_key(status):
        statusDict[status]= 0
      statusDict[status] += 1 
    resultDict['Extras'] = statusDict

    # Obtain the distinct values of the selection parameters
    res = self.database.getTableDistinctAttributeValues(table,selectColumns,selectDict,older=toDate,newer=fromDate)
    distinctSelections = zip(selectColumns,[])
    if res['OK']:
      distinctSelections = res['Value']
    resultDict['Selections'] = distinctSelections

    return S_OK(resultDict)

  types_getTransformationSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the transformation information for a given page in the generic format """

    # Obtain the timing information from the selectDict
    last_update = selectDict.get('CreationDate',None)    
    if last_update:
      del selectDict['CreationDate']
    fromDate = selectDict.get('FromDate',None)    
    if fromDate:
      del selectDict['FromDate']
    if not fromDate:
      fromDate = last_update  
    toDate = selectDict.get('ToDate',None)    
    if toDate:
      del selectDict['ToDate']  
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None

    # Get the transformations that match the selection
    res = self.database.getTransformations(condDict=selectDict,older=toDate, newer=fromDate, orderAttribute=orderAttribute)
    if not res['OK']:
      return self.__parseRes(res)

    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    trList = res['Records']
    # Create the total records entry
    nTrans = len(trList)
    resultDict['TotalRecords'] = nTrans
    # Create the ParameterNames entry
    paramNames = res['ParameterNames']
    resultDict['ParameterNames'] = paramNames
    # Add the job states to the ParameterNames entry
    taskStateNames   = ['Created','Running','Submitted','Failed','Waiting','Done','Stalled']
    resultDict['ParameterNames'] += ['Jobs_'+x for x in taskStateNames]
    # Add the file states to the ParameterNames entry
    fileStateNames  = ['PercentProcessed','Processed','Unused','Assigned','Total','Problematic']
    resultDict['ParameterNames'] += ['Files_'+x for x in fileStateNames]

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
    # Add specific information for each selected transformation
    for trans in transList:
      transDict = dict(zip(paramNames,trans))
      
      # Update the status counters
      status = transDict['Status']
      if not statusDict.has_key(status):
        statusDict[status] = 0
      statusDict[status] += 1
      
      # Get the statistics on the number of jobs for the transformation
      transID = transDict['TransformationID']
      res = self.database.getTransformationTaskStats(transID)
      taskDict = {}
      if res['OK'] and res['Value']:
        taskDict = res['Value']
      for state in taskStateNames:
        if taskDict and taskDict.has_key(state):
          trans.append(taskDict[state])
        else:
          trans.append(0)

      # Get the statistics for the number of files for the transformation
      fileDict = {}
      transType = transDict['Type']
      if transType.lower().find('simulation') != -1:
        fileDict['PercentProcessed']  = '-'
      else:
        res = self.database.getTransformationStats(transID)
        if res['OK']:
          fileDict = res['Value']
          if fileDict['Total'] == 0:
            fileDict['PercentProcessed']  = 0
          else:
            processed = fileDict.get('Processed')
            if not processed:
              processed = 0
            fileDict['PercentProcessed'] = "%.1f" % ((processed*100.0)/fileDict['Total'])
      for state in fileStateNames:
        if fileDict and fileDict.has_key(state):
          trans.append(fileDict[state])
        else:
          trans.append(0)

    resultDict['Records'] = transList
    resultDict['Extras'] = statusDict
    return S_OK(resultDict)

  ###########################################################################

  def __parseRes(self,res):
    if not res['OK']:
      gLogger.error(res['Message'])
    return res
