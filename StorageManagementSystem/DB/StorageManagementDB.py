########################################################################
# $Header: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/DB/StagerDB.py,v 1.3 2009/11/03 16:06:29 acsmith Exp $
########################################################################

""" StorageManagementDB is a front end to the Stager Database.

    There are five tables in the StorageManagementDB: Tasks, CacheReplicas, TaskReplicas, StageRequests.

    The Tasks table is the place holder for the tasks that have requested files to be staged. These can be from different systems and have different associated call back methods.
    The CacheReplicas table keeps the information on all the CacheReplicas in the system. It maps all the file information LFN, PFN, SE to an assigned ReplicaID.
    The TaskReplicas table maps the TaskIDs from the Tasks table to the ReplicaID from the CacheReplicas table.
    The StageRequests table contains each of the prestage request IDs for each of the replicas.
"""

__RCSID__ = "$Id: StagerDB.py,v 1.3 2009/11/03 16:06:29 acsmith Exp $"

from DIRAC                                        import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB                           import DB
from DIRAC.Core.Utilities.List                    import intListToString,stringListToString
from DIRAC.Core.Utilities.Time                    import toString
import string,threading,types

class StorageManagementDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'StorageManagementDB','StorageManagement/StorageManagementDB',maxQueueSize)
    self.lock = threading.Lock()
    self.TASKPARAMS = ['TaskID','Status','Source','SubmitTime','LastUpdate','CompleteTime','CallBackMethod','SourceTaskID']
    self.REPLICAPARAMS = ['ReplicaID','Type','Status','SE','LFN','PFN','Size','FileChecksum','GUID','SubmitTime','LastUpdate','Reason','Links']
    self.STAGEPARAMS = ['ReplicaID','StageStatus','RequestID','StageRequestSubmitTime','StageRequestCompletedTime','PinLength','PinExpiryTime']

  def __getConnection(self,connection):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection",res['Message'])
    return connection

  ################################################################
  #
  # State machine management
  # 

  def updateTaskStatus(self,taskIDs,newTaskStatus,connection=False):
    return self.__updateTaskStatus(taskIDs,newTaskStatus,connection=connection)

  def __updateTaskStatus(self,taskIDs,newTaskStatus,force=False,connection=False):
    connection = self.__getConnection(connection)
    if not taskIDs:
      return S_OK(taskIDs)
    if force:
      toUpdate = taskIDs
    else:
      res = self._checkTaskUpdate(taskIDs,newTaskStatus,connection=connection)
      if not res['OK']:
        return res
      toUpdate = res['Value']
    if not toUpdate:
      return S_OK(toUpdate)
    req = "UPDATE Tasks SET Status='%s',LastUpdate=UTC_TIMESTAMP() WHERE TaskID IN (%s) AND Status != '%s';" % (newTaskStatus,intListToString(toUpdate),newTaskStatus)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    return S_OK(toUpdate)

  def _checkTaskUpdate(self,taskIDs,newTaskState,connection=False):
    connection = self.__getConnection(connection)
    if not taskIDs:
      return S_OK(taskIDs)
    # * -> Failed
    if newTaskState == 'Failed':
      oldTaskState = []
    # StageCompleting -> Done
    elif newTaskState == 'Done':
      oldTaskState = ['StageCompleting']
    # StageSubmitted -> StageCompleting
    elif newTaskState == 'StageCompleting':
      oldTaskState = ['StageSubmitted']
    # Waiting -> StageSubmitted
    elif newTaskState == 'StageSubmitted':
      oldTaskState = ['Waiting']
    # New -> Waiting
    elif newTaskState == 'Waiting':
      oldTaskState = ['New']
    else:
      return S_ERROR("Task status not recognized")
    if not oldTaskState:
      toUpdate = taskIDs
    else:
      req = "SELECT TaskID FROM Tasks WHERE Status in (%s) AND TaskID IN (%s)" % (stringListToString(oldTaskState),intListToString(taskIDs))
      res = self._query(req,connection)
      if not res['OK']:
        return res
      toUpdate = [row[0] for row in res['Value']]
    return S_OK(toUpdate)

  def updateReplicaStatus(self,replicaIDs,newReplicaStatus,connection=False):
    connection = self.__getConnection(connection)
    if not replicaIDs:
      return S_OK(replicaIDs)
    res = self._checkReplicaUpdate(replicaIDs,newReplicaStatus)
    if not res['OK']:
      return res
    toUpdate = res['Value']
    if not toUpdate:
      return S_OK(toUpdate)
    req = "UPDATE CacheReplicas SET Status='%s',LastUpdate=UTC_TIMESTAMP() WHERE ReplicaID IN (%s) AND Status != '%s';" % (newReplicaStatus,intListToString(toUpdate),newReplicaStatus)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    # Now update the tasks associated to the replicaIDs
    newTaskStatus = self.__getTaskStateFromReplicaState(newReplicaStatus)
    res = self._getReplicaTasks(toUpdate,connection=connection)
    if not res['OK']:
      return res
    taskIDs = res['Value']
    if taskIDs:
      res = self.__updateTaskStatus(taskIDs,newTaskStatus,True,connection=connection)
      if not res['OK']:
        gLogger.warn("Failed to update tasks associated to replicas",res['Message'])
    return S_OK(toUpdate)

  def _getReplicaTasks(self,replicaIDs,connection=False):
    connection = self.__getConnection(connection)
    req = "SELECT DISTINCT(TaskID) FROM TaskReplicas WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    taskIDs = [row[0] for row in res['Value']]
    return S_OK(taskIDs)

  def _checkReplicaUpdate(self,replicaIDs,newReplicaState,connection=False):
    connection = self.__getConnection(connection)
    if not replicaIDs:
      return S_OK(replicaIDs)
    # * -> Failed
    if newReplicaState == 'Failed':
      oldReplicaState = []
    # New -> Waiting 
    elif newReplicaState == 'Waiting':
      oldReplicaState = ['New']
    # Waiting -> StageSubmitted
    elif newReplicaState == 'StageSubmitted':
      oldReplicaState = ['Waiting']
    # StageSubmitted -> Staged
    elif newReplicaState == 'Staged':
      oldReplicaState = ['StageSubmitted']
    else:
      return S_ERROR("Replica status not recognized")
    if not oldReplicaState:
      toUpdate = replicaIDs
    else:
      req = "SELECT ReplicaID FROM CacheReplicas WHERE Status = '%s' AND ReplicaID IN (%s)" % (oldReplicaState,intListToString(replicaIDs))
      res = self._query(req,connection)
      if not res['OK']:
        return res
      toUpdate = [row[0] for row in res['Value']]
    return S_OK(toUpdate)

  def __getTaskStateFromReplicaState(self,replicaState):
    # For the moment the task state just references to the replicaState
    return replicaState

  def updateStageRequestStatus(self,replicaIDs,newStageStatus,connection=False):
    connection = self.__getConnection(connection)
    if not replicaIDs:
      return S_OK(replicaIDs)
    res = self._checkStageUpdate(replicaIDs,newStageStatus,connection=connection)
    if not res['OK']:
      return res
    toUpdate = res['Value']
    if not toUpdate:
      return S_OK(toUpdate)
    req = "UPDATE CacheReplicas SET Status='%s',LastUpdate=UTC_TIMESTAMP() WHERE ReplicaID IN (%s) AND Status != '%s';" % (newStageStatus,intListToString(toUpdate),newStageStatus)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    # Now update the replicas associated to the replicaIDs
    newReplicaStatus = self.__getReplicaStateFromStageState(newStageStatus)
    res = self.updateReplicaStatus(toUpdate,newReplicaStatus,connection=connection)
    if not res['OK']:
      gLogger.warn("Failed to update cache replicas associated to stage requests",res['Message'])
    return S_OK(toUpdate)

  def _checkStageUpdate(self,replicaIDs,newStageState,connection=False):
    connection = self.__getConnection(connection)
    if not replicaIDs:
      return S_OK(replicaIDs)
    # * -> Failed
    if newStageState == 'Failed':
      oldStageState = []
    elif newStageState == 'Staged':
      oldStageState = ['StageSubmitted']
    else:
      return S_ERROR("StageRequest status not recognized")
    if not oldStageState:
      toUpdate = replicaIDs
    else:
      req = "SELECT ReplicaID FROM StageRequests WHERE StageStatus = '%s' AND ReplicaID IN (%s)" % (oldStageState,intListToString(replicaIDs))
      res = self._query(req,connection)
      if not res['OK']:
        return res
      toUpdate = [row[0] for row in res['Value']]
    return S_OK(toUpdate)

  def __getReplicaStateFromStageState(self,stageState):
    # For the moment the replica state just references to the stage state
    return stageState

  #
  #                               End of state machine management
  #
  ################################################################

  ################################################################
  #
  # Monitoring of stage tasks
  #
  def getTaskStatus(self,taskID,connection=False):
    """ Obtain the task status from the Tasks table. """
    connection = self.__getConnection(connection)
    res = self.getTaskInfo(taskID,connection=connection)
    if not res['OK']:
      return res
    taskInfo = res['Value'][taskID]
    return S_OK(taskInfo['Status'])

  def getTaskInfo(self,taskID,connection=False):
    """ Obtain all the information from the Tasks table for a supplied task. """
    connection = self.__getConnection(connection)
    req = "SELECT TaskID,Status,Source,SubmitTime,CompleteTime,CallBackMethod,SourceTaskID from Tasks WHERE TaskID = %s;" % taskID
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error('StagerDB.getTaskInfo: Failed to get task information.', res['Message'])
      return res
    resDict = {}
    for taskID,status,source,submitTime,completeTime,callBackMethod,sourceTaskID in res['Value']:
      resDict[taskID] = {'Status':status,'Source':source,'SubmitTime':submitTime,'CompleteTime':completeTime,'CallBackMethod':callBackMethod,'SourceTaskID':sourceTaskID}
    if not resDict:
      gLogger.error('StagerDB.getTaskInfo: The supplied task did not exist')
      return S_ERROR('The supplied task did not exist')
    return S_OK(resDict)

  def getTaskSummary(self,taskID,connection=False):
    """ Obtain the task summary from the database. """
    connection = self.__getConnection(connection)
    res = self.getTaskInfo(taskID,connection=connection)
    if not res['OK']:
      return res
    taskInfo = res['Value']
    req = "SELECT R.LFN,R.SE,R.PFN,R.Size,R.Status,R.Reason FROM CacheReplicas AS R, TaskReplicas AS TR WHERE TR.TaskID = %s AND TR.ReplicaID=R.ReplicaID;" % taskID
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error('StagerDB.getTaskSummary: Failed to get Replica summary for task.',res['Message'])
      return res
    replicaInfo = {}
    for lfn,storageElement,pfn,fileSize,status,reason in res['Value']:
      replicaInfo[lfn] = {'StorageElement':storageElement,'PFN':pfn,'FileSize':fileSize,'Status':status,'Reason':reason}
    resDict = {'TaskInfo':taskInfo,'ReplicaInfo':replicaInfo}
    return S_OK(resDict)

  def getTasks(self,condDict={},older=None,newer=None,timeStamp='SubmitTime',orderAttribute=None, limit=None,connection=False):
    """ Get stage requests for the supplied selection with support for web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM Tasks" % (intListToString(self.TASKPARAMS))
    if condDict or older or newer:
      if condDict.has_key('ReplicaID'):
        replicaIDs = condDict.pop('ReplicaID')
        if type(replicaIDs) not in (types.ListType,types.TupleType):
          replicaIDs = [replicaIDs]
        res = self._getReplicaIDTasks(replicaIDs,connection=connection)
        if not res['OK']:
          return res
        condDict['TaskID'] = res['Value']
      req = "%s %s" % (req,self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    tasks = res['Value']
    resultDict = {}
    for row in tasks:
      resultDict[row[0]] = dict(zip(self.TASKPARAMS[1:],row[1:]))
    result = S_OK(resultDict)
    result['Records'] = tasks
    result['ParameterNames'] = self.TASKPARAMS
    return result

  def getCacheReplicas(self,condDict={}, older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None,connection=False):
    """ Get cache replicas for the supplied selection with support for the web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM CacheReplicas" % (intListToString(self.REPLICAPARAMS))
    originalFileIDs = {}
    if condDict or older or newer:
      if condDict.has_key('TaskID'):
        taskIDs = condDict.pop('TaskID')
        if type(taskIDs) not in (types.ListType,types.TupleType):
          taskIDs = [taskIDs]
        res = self._getTaskReplicaIDs(taskIDs,connection=connection)
        if not res['OK']:
          return res
        condDict['ReplicaID'] = res['Value']
      req = "%s %s" % (req,self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    cacheReplicas = res['Value']
    resultDict = {}
    for row in cacheReplicas:
      resultDict[row[0]] = dict(zip(self.REPLICAPARAMS[1:],row[1:]))
    result = S_OK(resultDict)
    result['Records'] = cacheReplicas
    result['ParameterNames'] = self.REPLICAPARAMS
    return result

  def getStageRequests(self,condDict={},older=None,newer=None,timeStamp='StageRequestSubmitTime',orderAttribute=None, limit=None,connection=False):
    """ Get stage requests for the supplied selection with support for web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM StageRequests" % (intListToString(self.STAGEPARAMS))
    if condDict or older or newer:
      if condDict.has_key('TaskID'):
        taskIDs = condDict.pop('TaskID')
        if type(taskIDs) not in (types.ListType,types.TupleType):
          taskIDs = [taskIDs]
        res = self._getTaskReplicaIDs(taskIDs,connection=connection)
        if not res['OK']:
          return res
        condDict['ReplicaID'] = res['Value']
      req = "%s %s" % (req,self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    stageRequests = res['Value']
    resultDict = {}
    for row in stageRequests:
      resultDict[row[0]] = dict(zip(self.STAGEPARAMS[1:],row[1:]))
    result = S_OK(resultDict)
    result['Records'] = stageRequests
    result['ParameterNames'] = self.STAGEPARAMS
    return result

  def _getTaskReplicaIDs(self,taskIDs,connection=False):
    req = "SELECT ReplicaID FROM TaskReplicas WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    replicaIDs = []
    for tuple in res['Value']:
      replicaID = tuple[0]
      if not replicaID in replicaIDs:
        replicaIDs.append(replicaID)
    return S_OK(replicaIDs)

  def _getReplicaIDTasks(self,replicaIDs,connection=False):
    req = "SELECT TaskID FROM TaskReplicas WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    taskIDs = []
    for tuple in res['Value']:
      taskID = tuple[0]
      if not taskID in taskIDs:
        taskIDs.append(taskID)
    return S_OK(taskIDs)

  #
  #                               End of monitoring of stage tasks
  #
  ################################################################

  ####################################################################
  #
  # Submission of stage requests
  #

  def setRequest(self,lfnDict,source,callbackMethod,sourceTaskID,connection=False):
    """ This method populates the StagerDB Files and Tasks tables with the requested files. """
    connection = self.__getConnection(connection)
    if not lfnDict:
      return S_ERROR("No files supplied in request")
    # The first step is to create the task in the Tasks table
    res = self._createTask(source,callbackMethod,sourceTaskID,connection=connection)
    if not res['OK']:
      return res
    taskID = res['Value']
    # Get the Replicas which already exist in the CacheReplicas table
    allReplicaIDs = []
    taskStates = []
    for se,lfns in lfnDict.items():
      if type(lfns) in types.StringTypes:
        lfns = [lfns]
      res = self._getExistingReplicas(se,lfns,connection=connection)
      if not res['OK']:
        return res
      existingReplicas = res['Value']
      # Insert the CacheReplicas that do not already exist
      for lfn in lfns:
        if lfn in existingReplicas.keys():
          gLogger.verbose('StagerDB.setRequest: Replica already exists in CacheReplicas table %s @ %s' % (lfn,se))
          existingFileState = existingReplicas[lfn][1]
          taskState = self.__getTaskStateFromReplicaState(existingFileState)
          if not taskState in taskStates:
            taskStates.append(taskState)
        else:
          res = self._insertReplicaInformation(lfn,se,'Stage',connection=connection)
          if not res['OK']:
            self._cleanTask(taskID,connection=connection)
            return res
          else:
            existingReplicas[lfn] = (res['Value'],'New')
      allReplicaIDs.extend(existingReplicas.values())
    # Insert all the replicas into the TaskReplicas table
    res = self._insertTaskReplicaInformation(taskID,allReplicaIDs,connection=connection)
    if not res['OK']:
      self._cleanTask(taskID,connection=connection)
      return res
    # Check whether the the task status is Done based on the existing file states
    if taskStates == ['Staged']:
      self.__updateTaskStatus([taskID],'Done',True,connection=connection)
    if 'Failed' in taskStates:
      self.__updateTaskStatus([taskID],'Failed',True,connection=connection) 
    return S_OK(taskID)

  def _cleanTask(self,taskID,connection=False):
    """ Remove a task and any related information """
    connection = self.__getConnection(connection)
    self.removeTasks([taskID],connection=connection)
    self.removeUnlinkedReplicas(connection=connection)

  def _createTask(self,source,callbackMethod,sourceTaskID,connection=False):
    """ Enter the task details into the Tasks table """
    connection = self.__getConnection(connection)
    req = "INSERT INTO Tasks (Source,SubmitTime,CallBackMethod,SourceTaskID) VALUES ('%s',UTC_TIMESTAMP(),'%s','%s');" % (source,callbackMethod,sourceTaskID)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("StagerDB._createTask: Failed to create task.", res['Message'])
      return res
    taskID = res['lastRowId']
    gLogger.info("StagerDB._createTask: Created task with ('%s','%s','%s') and obtained TaskID %s" % (source,callbackMethod,sourceTaskID,taskID))
    return S_OK(taskID)

  def _getExistingReplicas(self,storageElement,lfns,connection=False):
    """ Obtains the ReplicasIDs for the replicas already entered in the CacheReplicas table """
    connection = self.__getConnection(connection)
    req = "SELECT ReplicaID,LFN,Status FROM CacheReplicas WHERE SE = '%s' AND LFN IN (%s);" % (storageElement,stringListToString(lfns))
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error('StagerDB._getExistingReplicas: Failed to get existing replicas.', res['Message'])
      return res
    existingReplicas = {}
    for replicaID,lfn,status in res['Value']:
      existingReplicas[lfn] = (replicaID,status)
    return S_OK(existingReplicas)

  def _insertReplicaInformation(self,lfn,storageElement,type,connection=False):
    """ Enter the replica into the CacheReplicas table """
    connection = self.__getConnection(connection)
    req = "INSERT INTO CacheReplicas (Type,SE,LFN,PFN,Size,FileChecksum,GUID,SubmitTime,LastUpdate) VALUES ('%s','%s','%s','',0,'','',UTC_TIMESTAMP(),UTC_TIMESTAMP());" % (type,storageElement,lfn)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("_insertReplicaInformation: Failed to insert to CacheReplicas table.",res['Message'])
      return res
    replicaID = res['lastRowId']
    gLogger.verbose("_insertReplicaInformation: Inserted Replica ('%s','%s') and obtained ReplicaID %s" % (lfn,storageElement,replicaID))
    return S_OK(replicaID)

  def _insertTaskReplicaInformation(self,taskID,replicaIDs,connection=False):
    """ Enter the replicas into TaskReplicas table """
    connection = self.__getConnection(connection)
    req = "INSERT INTO TaskReplicas (TaskID,ReplicaID) VALUES "
    for replicaID,status in replicaIDs:
      replicaString = "(%s,%s)," % (taskID,replicaID)
      req = "%s %s" % (req,replicaString)
    req = req.rstrip(',')
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error('StagerDB._insertTaskReplicaInformation: Failed to insert to TaskReplicas table.',res['Message'])
      return res
    gLogger.info("StagerDB._insertTaskReplicaInformation: Successfully added %s CacheReplicas to Task %s." % (res['Value'],taskID))
    return S_OK()

  #
  #                               End of insertion methods
  #
  ################################################################

  ####################################################################

  def getWaitingReplicas(self,connection=False):
    connection = self.__getConnection(connection)
    req = "SELECT TR.TaskID, R.Status, COUNT(*) from TaskReplicas as TR, CacheReplicas as R where TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID,R.Status;"
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error('StagerDB.getWaitingReplicas: Failed to get eligible TaskReplicas',res['Message'])
      return res
    badTasks = []
    goodTasks = []
    for taskID,status,count in res['Value']:
      if taskID in badTasks:
        continue
      elif status in ('New','Failed'):
        badTasks.append(taskID)
      elif status == 'Waiting':
        goodTasks.append(taskID)
    replicas = {}
    if not goodTasks:
      return S_OK(replicas)
    return self.getCacheReplicas({'Status':'Waiting','TaskID':goodTasks},connection=connection)

  ####################################################################

  def getTasksWithStatus(self,status):
    """ This method retrieves the TaskID from the Tasks table with the supplied Status. """
    req = "SELECT TaskID,Source,CallBackMethod,SourceTaskID from Tasks WHERE Status = '%s';" % status
    res = self._query(req)
    if not res['OK']:
      return res
    taskIDs = {}
    for taskID,source,callback,sourceTask in res['Value']:
      taskIDs[taskID] = (source,callback,sourceTask)
    return S_OK(taskIDs)

  ####################################################################
  #
  # The state transition of the CacheReplicas from *->Failed
  #

  def updateReplicaFailure(self,terminalReplicaIDs):
    """ This method sets the status to Failure with the failure reason for the supplied Replicas. """
    res = self.updateReplicaStatus(terminalReplicaIDs.keys(),'Failed')
    if not res['OK']:
      return res
    updated = res['Value']
    if not updated:
      return S_OK(updated)
    for replicaID in updated:
      req = "UPDATE CacheReplicas SET Reason = '%s' WHERE ReplicaID = %d" % (terminalReplicaIDs[replicaID],replicaID)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.updateReplicaFailure: Failed to update replica fail reason.',res['Message'])
    return S_OK(updated)

  ####################################################################
  #
  # The state transition of the CacheReplicas from New->Waiting
  #

  def updateReplicaInformation(self,replicaTuples):
    """ This method set the replica size information and pfn for the requested storage element.  """
    for replicaID,pfn,size in replicaTuples:
      req = "UPDATE CacheReplicas SET PFN = '%s', Size = %s, Status = 'Waiting' WHERE ReplicaID = %s and Status != 'Cancelled';" % (pfn,size,replicaID)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.updateReplicaInformation: Failed to insert replica information.', res['Message'])
    return S_OK()

  ####################################################################
  #
  # The state transition of the CacheReplicas from Waiting->StageSubmitted
  #

  def getSubmittedStagePins(self):
    req = "SELECT SE,COUNT(*),SUM(Size) from CacheReplicas WHERE Status NOT IN ('New','Waiting','Failed') GROUP BY SE;"
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getSubmittedStagePins: Failed to obtain submitted requests.',res['Message'])
      return res
    storageRequests = {}
    for storageElement,replicas,totalSize in res['Value']:
      storageRequests[storageElement] = {'Replicas':int(replicas),'TotalSize':int(totalSize)}
    return S_OK(storageRequests)

  def insertStageRequest(self,requestDict,pinLifeTime):
    req = "INSERT INTO StageRequests (ReplicaID,RequestID,StageRequestSubmitTime,PinLength) VALUES "
    for requestID,replicaIDs in requestDict.items():
      for replicaID in replicaIDs:
        replicaString = "(%s,'%s',UTC_TIMESTAMP(),%d)," % (replicaID,requestID,pinLifeTime)
        req = "%s %s" % (req,replicaString)
    req = req.rstrip(',')
    res = self._update(req)
    if not res['OK']:
      gLogger.error('StagerDB.insertStageRequest: Failed to insert to StageRequests table.',res['Message'])
      return res
    gLogger.info("StagerDB.insertStageRequest: Successfully added %s StageRequests with RequestID %s." % (res['Value'],requestID))
    return S_OK()

  ####################################################################
  #
  # The state transition of the CacheReplicas from StageSubmitted->Staged
  #

  def setStageComplete(self,replicaIDs):
    req = "UPDATE StageRequests SET StageStatus='Staged',StageRequestCompletedTime = UTC_TIMESTAMP(),PinExpiryTime = DATE_ADD(UTC_TIMESTAMP(),INTERVAL 84000 SECOND) WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req)
    if not res['OK']:
      gLogger.error("StagerDB.setStageComplete: Failed to set StageRequest completed.", res['Message'])
      return res
    return res

  ####################################################################
  #
  # This code handles the finalization of stage tasks
  #

  def updateStageCompletingTasks(self):
    """ This will select all the Tasks in StageCompleting status and check whether all the associated files are Staged. """
    req = "SELECT TR.TaskID,COUNT(if(R.Status NOT IN ('Staged'),1,NULL)) FROM Tasks AS T, TaskReplicas AS TR, CacheReplicas AS R WHERE T.Status='StageCompleting' AND T.TaskID=TR.TaskID AND TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID;"
    res = self._query(req)
    if not res['OK']:
      return res
    taskIDs = []
    for taskID,count in res['Value']:
      if int(count) == 0:
        taskIDs.append(taskID)
    if not taskIDs:
      return S_OK(taskIDs)
    req = "UPDATE Tasks SET Status = 'Staged' WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._update(req)
    if not res['OK']:
      return res
    return S_OK(taskIDs)

  def setTasksDone(self,taskIDs):
    """ This will update the status for a list of taskIDs to Done. """
    req = "UPDATE Tasks SET Status = 'Done', CompleteTime = UTC_TIMESTAMP() WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._update(req)
    return res

  def removeTasks(self,taskIDs,connection=False):
    """ This will delete the entries from the TaskReplicas for the provided taskIDs. """
    connection = self.__getConnection(connection)
    req = "DELETE FROM TaskReplicas WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    req = "DELETE FROM Tasks WHERE TaskID in (%s);" % intListToString(taskIDs)
    res = self._update(req,connection)
    return res

  def removeUnlinkedReplicas(self,connection=False):
    """ This will remove from the CacheReplicas tables where there are no associated links. """
    connection = self.__getConnection(connection)
    req = "SELECT ReplicaID from CacheReplicas WHERE Links = 0;"
    res = self._query(req,connection)
    if not res['OK']:
      return res
    replicaIDs = []
    for tuple in res['Value']:
      replicaIDs.append(tuple[0])
    if not replicaIDs:
      return S_OK()
    req = "DELETE FROM StageRequests WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    req = "DELETE FROM CacheReplicas WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req,connection)
    return res
