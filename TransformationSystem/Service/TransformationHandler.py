""" DISET request handler base class for the TransformationDB."""
# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import *

class TransformationHandler(RequestHandler):

  transTypes = list(StringTypes)+[IntType,LongType]

  def setDatabase(self,oDatabase):
    self.database = oDatabase

  types_getName = []
  def export_getName(self):
    res = self.database.getName()
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
                                    maxJobs             = 0,
                                    eventsPerJob        = 0,
                                    addFiles            = True):    
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    res = self.database.addTransformation(transName,description,longDescription,authorDN,authorGroup,type,plugin,agentType,fileMask,
                                    transformationGroup = transformationGroup,
                                    groupSize           = groupSize,
                                    inheritedFrom       = inheritedFrom,
                                    body                = body, 
                                    maxJobs             = maxJobs,
                                    eventsPerJob        = eventsPerJob,
                                    addFiles            = addFiles)    
    if res['OK']:
      gLogger.info("Added transformation %d" % res['Value'])  
    return self.__parseRes(res)

  types_deleteTransformation = [transTypes]
  def export_deleteTransformation(self, transName):
    res = self.database.deleteTransformation(transName)
    return self.__parseRes(res)
  
  types_cleanTransformation = [transTypes]
  def export_cleanTransformation(self, transName):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.cleanTransformation(transName,author=authorDN)
    return self.__parseRes(res)

  types_addTransformationParameter = [transTypes,StringType]
  def export_addTransformationParameter(self,transName,paramName,paramValue):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.setTransformationParameter(transName,paramName,paramValue,author=authorDN)
    return self.__parseRes(res)
  
  types_setTransformationStatus = [transTypes,StringTypes]
  def export_setTransformationStatus(self,transName,status):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.setTransformationStatus(transName,status,author=authorDN)
    return self.__parseRes(res)

  types_setTransformationAgentType = [transTypes,StringTypes]
  def export_setTransformationAgentType( self, transName, status ):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.setTransformationAgentType(transName, status, author=authorDN)
    return self.__parseRes(res)

  types_getTransformations = []
  def export_getTransformations(self,condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False):
    res = self.getTransformations(condDict=condDict,
                                  older=older,
                                  newer=newer,
                                  timeStamp=timeStamp,
                                  orderAttribute=orderAttribute,
                                  limit=limit, 
                                  extraParams=extraParams)
    return self.__parseRes(res)

  types_getTransformation = [transTypes]
  def export_getTransformation(self,transName):
    res = self.database.getTransformation(transName)
    return self.__parseRes(res)

  types_getTransformationParameters = [transTypes,[ListType,TupleType]]
  def export_getTransformationParameters(self,transName,parameters):
    res = self.database.getTransformationParameters(transName,parameters)
    return self.__parseRes(res)

  types_getTransformationLastUpdate = [transTypes]
  def export_getTransformationLastUpdate(self,transName):
    res = self.database.getTransformationLastUpdate(transName)
    return self.__parseRes(res)

  types_getTransformationWithStatus = [[StringTypes,ListType,TupleType]]
  def export_getTransformationWithStatus(self,status):
    res = self.database.getTransformationWithStatus(status)
    return self.__parseRes(res)


  ####################################################################
  #
  # These are the methods to manipulate the T_* tables
  #

  types_addTaskForTransformation = [transTypes]
  def export_addTaskForTransformation(self,transName,lfns=[],se='Unknown'):
    res = self.database.addTaskForTransformation(transName, lfns=lfns, se=se)
    return self.__parseRes(res)

  types_setFileStatusForTransformation = [transTypes,StringTypes,ListType]
  def export_setFileStatusForTransformation(self,transName,status,lfns):
    res = self.database.setFileStatusForTransformation(transName,status,lfns)
    return self.__parseRes(res)

  types_getTransformationLFNs = [transTypes]
  def export_getTransformationLFNs(self,transName,status='Unused'):
    res = self.database.getTransformationLFNs(transName,status)
    return self.__parseRes(res)
  
  types_getTransformationStats = [transTypes]
  def export_getTransformationStats(self,transName):
    res = self.database.getTransformationStats(transName)
    return self.__parseRes(res)
  
  ####################################################################
  #
  # These are the methods to manipulate the Tasks (Jobs) table
  #

  types_setTaskStatus = [transTypes, [IntType,LongType], StringTypes]
  def export_setTaskStatus(self, transName, taskID, status):
    res = self.database.setTaskStatus(transName, taskID, status)    
    return self.__parseRes(res)

  types_setTaskStatusAndWmsID = [ transTypes, [LongType,IntType], StringType, StringType]
  def export_setTaskStatusAndWmsID(self, transName, taskID, status, taskWmsID):
    res = self.database.setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)
    return self.__parseRes(res)

  types_selectWMSTasks = [transTypes]
  def export_selectWMSTasks(self,transName,statusList=[],newer=0):
    res = self.database.selectWMSTasks(transName,statusList=statusList,newer=newer)  
    return self.__parseRes(res)
  
  types_getTransformationTaskStats = [transTypes]
  def export_getTransformationTaskStats(self, transName):
    res = self.database.getTransformationTaskStats(transName)
    return self.__parseRes(res)

  types_getTaskInfo = [transTypes, [LongType,IntType]]
  def export_getTaskInfo(self, transName, taskID):
    res = self.database.getTaskInfo(transName,taskID)
    return self.__parseRes(res)

  types_getTaskStats = [transTypes]
  def export_getTaskStats(self,transName):
    res = self.database.getTaskStats(transName)
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
    if status in ['Active','Flush']:
      res = self.database.getTasksForSubmission(transName,['Created'],numTasks=numTasks,site=site)
      if not res['OK']:
        return self.__parseRes(res)
      tasksDict = result['Value']
      for taskID,taskDict in tasksDict.items():
        res = self.database.reserveJob(transName, long(taskID))
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
    
  types_getDistinctAttributeValues = [StringTypes, DictType]
  def export_getDistinctAttributeValues(self, attribute, selectDict):
    res = self.database.getDistinctAttributeValues(attribute,selectDict)
    return self.__parseRes(res)

  types_getTransformationStatusCounters = []
  def export_getTransformationStatusCounters( self ):
    res = self.database.getCounters('Transformations',['Status'],{})
    if not res['OK']:
      return self.__parseRes(res)
    statDict = {}
    for attrDict,count in result['Value']:
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
      res = self.database.getTaskStats(transID)
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
    # Add the bk fields to the ParameterNames entry
    #bkParameters    = ['ConfigName','ConfigVersion','EventType','Jobs','Files','Events','Steps']  
    #resultDict['ParameterNames'] += ['Bk_'+x for x in bkParameters]
    #fileOrder       = ['SETC','DST','DIGI','SIM']

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
      prodID = transDict['TransformationID']
      res = self.database.getTaskStats(prodID)
      taskDict = {}
      if res['OK']:
        taskDict = result['Value']
      for state in taskStateNames:
        if taskDict and taskDict.has_key(state):
          trans.append(taskDict[state])
        else:
          trans.append(0)

      # Get the statistics for the number of files for the transformation
      fileDict = {}
      transType = transDict['Type']
      if transType.lower().find('simulation') == -1:      
        res = self.database.getTransformationStats(prodID)
        if res['OK']:
          fileDict = result['Value']
          processed = fileDict.get['Processed']
          if not processed:
            processed = 0
          percentProcessed = "%.1f" % ((processed*100.0)/fileDict['Total'])
          fileDict['PercentProcessed'] = "%.1f" % ((processed*100.0)/fileDict['Total'])
      for state in fileStateNames:
        if fileDict and fileDict.has_key(state):
          trans.append(fileDict[state])
        else:
          trans.append(0)
          
      # Get Bookkeeping information
#      result = bkClient.getProductionInformations_new(long(prodID))
#      if not result['OK']:
#        for p in bkParameters:
#          trans.append('-')
#          
#      if result['Value']['Production informations']:
#        for row in result['Value']['Production informations']:
#          if row[2]:
#            trans += list(row)
#            break
#      else:
#        trans += ['-','-','-']
#        
#      if result['Value']['Number of jobs']:  
#        trans.append(result['Value']['Number of jobs'][0][0])    
#      else:
#        trans.append(0)
#        
#      # Number of files  
#      files_done = False  
#      if result['Value']['Number of files']:  
#        for dfile in fileOrder:
#          for row in result['Value']['Number of files']:
#            if dfile == row[1]:
#              trans.append(row[0])
#              files_done = True
#              break
#          if files_done:
#            break    
#      if not files_done:
#        trans.append(0)            
#    
#      # Number of events
#      events_done = False
#      if result['Value']['Number of events']:  
#        for dfile in fileOrder:  
#          for row in result['Value']['Number of events']:
#            if dfile == row[0]:
#              trans.append(row[1])
#              events_done = True
#              break
#          if events_done:
#            break 
#      if not events_done:
#        trans.append(0)      
#        
#      # Number of steps
#      if result['Value']['Steps']:      
#        trans.append(len(result['Value']['Steps'])) 
#      else:
#        trans.append(0)          

    resultDict['Records'] = transList
    resultDict['Extras'] = statusDict
    return S_OK(resultDict)

  ###########################################################################

  def __parseRes(self,res):
    if not res['OK']:
      gLogger.error(res['Message'])
    return res