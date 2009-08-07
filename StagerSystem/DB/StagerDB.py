########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.py,v 1.25 2009/08/07 13:19:33 acsmith Exp $
########################################################################

""" StagerDB is a front end to the Stager Database.

    There are five tables in the StagerDB: Tasks, Replicas, TaskReplicas, StageRequests and Pins.

    The Tasks table is the place holder for the tasks that have requested files to be staged. These can be from different systems and have different associated call back methods.
    The Replicas table keeps the information on all the Replicas in the system. It maps all the file information LFN, PFN, SE to an assigned ReplicaID.
    The TaskReplicas table maps the TaskIDs from the Tasks table to the ReplicaID from the Replicas table.
    The StageRequests table contains each of the prestage request IDs for each of the replicas.
    The Pins table keeps the pinning request ID along with when it was issued and for how long for each of the replicas.
"""

__RCSID__ = "$Id: StagerDB.py,v 1.25 2009/08/07 13:19:33 acsmith Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString,stringListToString
from DIRAC.Core.Utilities.Time import toString
from DIRAC.Core.Base.DB import DB

import string,threading

class StagerDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'StagerDB','Stager/StagerDB',maxQueueSize)
    self.lock = threading.Lock()

  ####################################################################
  #
  # The setRequest method is used to initially insert tasks and their associated files.
  # TODO: Implement a rollback in case of a failure at any step

  def setRequest(self,lfns,storageElement,source,callbackMethod,sourceTaskID):
    """ This method populates the StagerDB Files and Tasks tables with the requested files.
    """
    # The first step is to create the task in the Tasks table
    res = self._createTask(source,callbackMethod,sourceTaskID)
    if not res['OK']:
      return res
    taskID = res['Value']
    # Get the Replicas which already exist in the Replicas table
    res = self._getExistingReplicas(storageElement,lfns)
    if not res['OK']:
      return res
    existingReplicas = res['Value']
    # Insert the Replicas that do not already exist
    for lfn in lfns:
      if lfn in existingReplicas.keys():
        gLogger.verbose('StagerDB.setRequest: Replica already exists in Replicas table %s @ %s' % (lfn,storageElement))
      else:
        res = self._insertReplicaInformation(lfn,storageElement)
        if not res['OK']:
          gLogger.warn("Perform roll back")
        else:
          existingReplicas[lfn] = (res['Value'],'New')
    # Insert all the replicas into the TaskReplicas table
    res = self._insertTaskReplicaInformation(taskID,existingReplicas.values())
    if not res['OK']:
      gLogger.error("Perform roll back")
      return res
    return S_OK(taskID)

  def _createTask(self,source,callbackMethod,sourceTaskID):
    """ Enter the task details into the Tasks table """
    self.lock.acquire()
    req = "INSERT INTO Tasks (Source,SubmitTime,CallBackMethod,SourceTaskID) VALUES ('%s',UTC_TIMESTAMP(),'%s','%s');" % (source,callbackMethod,sourceTaskID)
    res = self._update(req)
    self.lock.release() 
    if not res['OK']:
      gLogger.error("StagerDB._createTask: Failed to create task.", res['Message'])
      return res
    taskID = res['lastRowId']
    gLogger.info("StagerDB._createTask: Created task with ('%s','%s','%s') and obtained TaskID %s" % (source,callbackMethod,sourceTaskID,taskID))
    return S_OK(taskID)

  def _getExistingReplicas(self,storageElement,lfns):
    """ Obtains the ReplicasIDs for the replicas already entered in the Replicas table """
    req = "SELECT ReplicaID,LFN,Status FROM Replicas WHERE StorageElement = '%s' AND LFN IN (%s);" % (storageElement,stringListToString(lfns))
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB._getExistingReplicas: Failed to get existing replicas.', res['Message'])
      return res
    existingReplicas = {}
    for replicaID,lfn,status in res['Value']:
      existingReplicas[lfn] = (replicaID,status)
    return S_OK(existingReplicas)

  def _insertReplicaInformation(self,lfn,storageElement):
    """ Enter the replica into the Replicas table """
    res = self._insert('Replicas',['LFN','StorageElement'],[lfn,storageElement])
    if not res['OK']:
      gLogger.error('StagerDB._insertReplicaInformation: Failed to insert to Replicas table.',res['Message'])
      return res
    replicaID = res['lastRowId']
    gLogger.verbose("StagerDB._insertReplicaInformation: Inserted Replica ('%s','%s') and obtained ReplicaID %s" % (lfn,storageElement,replicaID))
    return S_OK(replicaID)

  def _insertTaskReplicaInformation(self,taskID,replicaIDs):
    """ Enter the replicas into TaskReplicas table """
    req = "INSERT INTO TaskReplicas (TaskID,ReplicaID) VALUES "
    for replicaID,status in replicaIDs:
      replicaString = "(%s,%s)," % (taskID,replicaID)
      req = "%s %s" % (req,replicaString)
    req = req.rstrip(',')
    res = self._update(req)
    if not res['OK']:
      gLogger.error('StagerDB._insertTaskReplicaInformation: Failed to insert to TaskReplicas table.',res['Message'])
      return res
    gLogger.info("StagerDB._insertTaskReplicaInformation: Successfully added %s Replicas to Task %s." % (res['Value'],taskID))
    return S_OK()

  ####################################################################

  def getReplicasWithStatus(self,status):
    """ This method retrieves the ReplicaID and LFN from the Replicas table with the supplied Status. """
    req = "SELECT ReplicaID,LFN,StorageElement,FileSize,PFN from Replicas WHERE Status = '%s';" % status
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getReplicasWithStatus: Failed to get replicas for %s status' % status,res['Message'])
      return res
    replicas = {}
    for replicaID,lfn,storageElement,fileSize,pfn in res['Value']:
      replicas[replicaID] = (lfn,storageElement,fileSize,pfn)
    return S_OK(replicas)
 
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
  # The state transition of the Replicas from *->Failed
  #   

  def updateReplicaFailure(self,terminalReplicaIDs):
    """ This method sets the status to Failure with the failure reason for the supplied Replicas. """
    for replicaID,reason in terminalReplicaIDs.items():
      req = "UPDATE Replicas SET Status = 'Failed',Reason = '%s' WHERE ReplicaID = %s;" % (reason,replicaID)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.updateReplicaFailure: Failed to update replica to failed.',res['Message'])
    return S_OK()

  ####################################################################
  #
  # The state transition of the Replicas from New->Waiting
  #

  def updateReplicaInformation(self,replicaTuples):
    """ This method set the replica size information and pfn for the requested storage element.  """
    for replicaID,pfn,size in replicaTuples:
      req = "UPDATE Replicas SET PFN = '%s', FileSize = %s, Status = 'Waiting' WHERE ReplicaID = %s and Status != 'Cancelled';" % (pfn,size,replicaID)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.updateReplicaInformation: Failed to insert replica information.', res['Message'])
    return S_OK()

  ####################################################################
  #
  # The state transition of the Replicas from Waiting->StageSubmitted
  #

  def getSubmittedStagePins(self):
    req = "SELECT StorageElement,COUNT(*),SUM(FileSize) from Replicas WHERE Status NOT IN ('New','Waiting','Failed') GROUP BY StorageElement;"
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getSubmittedStagePins: Failed to obtain submitted requests.',res['Message'])
      return res
    storageRequests = {}
    for storageElement,replicas,totalSize in res['Value']:
      storageRequests[storageElement] = {'Replicas':int(replicas),'TotalSize':int(totalSize)}
    return S_OK(storageRequests)

  def getWaitingReplicas(self):
    req = "SELECT TR.TaskID, R.Status, COUNT(*) from TaskReplicas as TR, Replicas as R where TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID,R.Status;"
    res = self._query(req)
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
    req = "SELECT R.ReplicaID,R.LFN,R.StorageElement,R.FileSize,R.PFN from Replicas as R, TaskReplicas as TR WHERE R.Status = 'Waiting' AND TR.TaskID in (%s) AND TR.ReplicaID=R.ReplicaID;" % intListToString(goodTasks)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getWaitingReplicas: Failed to get Waiting replicas',res['Message'])
      return res
    for replicaID,lfn,storageElement,fileSize,pfn in res['Value']:
      replicas[replicaID] = (lfn,storageElement,fileSize,pfn)
    return S_OK(replicas)

  def insertStageRequest(self,requestDict):
    req = "INSERT INTO StageRequests (ReplicaID,RequestID,StageRequestSubmitTime) VALUES "
    for requestID,replicaIDs in requestDict.items():
      for replicaID in replicaIDs:
        replicaString = "(%s,%s,UTC_TIMESTAMP())," % (replicaID,requestID)
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
  # The state transition of the Replicas from StageSubmitted->Staged
  #

  def getStageSubmittedReplicas(self):
    req = "SELECT R.ReplicaID,R.StorageElement,R.LFN,R.PFN,R.FileSize,SR.RequestID from Replicas as R, StageRequests as SR WHERE R.Status = 'StageSubmitted' and R.ReplicaID=SR.ReplicaID;"
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getStageSubmittedReplicas: Failed to obtain submitted requests.',res['Message'])
      return res  
    replicas = {}
    for replicaID,storageElement,lfn,pfn,fileSize,requestID in res['Value']:
      replicas[replicaID] = {'LFN':lfn,'StorageElement':storageElement,'PFN':pfn,'Size':fileSize,'RequestID':requestID}
    return S_OK(replicas)

  def setStageComplete(self,replicaIDs):
    req = "UPDATE StageRequests SET StageStatus='Staged',StageRequestCompletedTime = UTC_TIMESTAMP() WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req)
    if not res['OK']:
      gLogger.error("StagerDB.setStageComplete: Failed to set StageRequest completed.", res['Message'])
      return res
    return res

  ####################################################################
  #
  # The state transition of the Replicas from Staged->Pinned
  #

  def getStagedReplicas(self):
    req = "SELECT R.ReplicaID, R.LFN, R.StorageElement, R.FileSize, R.PFN, SR.RequestID FROM Replicas AS R, StageRequests AS SR WHERE R.Status = 'Staged' AND R.ReplicaID=SR.ReplicaID;"
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getStagedReplicas: Failed to get replicas for Staged status',res['Message'])
      return res
    replicas = {}
    for replicaID,lfn,storageElement,fileSize,pfn,requestID in res['Value']:
      replicas[replicaID] = (lfn,storageElement,fileSize,pfn,requestID)  
    return S_OK(replicas)

  def insertPinRequest(self,requestDict,pinLifeTime):
    req = "INSERT INTO Pins (ReplicaID,RequestID,PinCreationTime,PinExpiryTime) VALUES "
    for requestID,replicaIDs in requestDict.items():
      for replicaID in replicaIDs:
        replicaString = "(%s,%s,UTC_TIMESTAMP(),DATE_ADD(UTC_TIMESTAMP(),INTERVAL %s SECOND))," % (replicaID,requestID,pinLifeTime)
        req = "%s %s" % (req,replicaString)
    req = req.rstrip(',')
    res = self._update(req)
    if not res['OK']:
      gLogger.error('StagerDB.insertPinRequest: Failed to insert to Pins table.',res['Message'])
      return res
    gLogger.info("StagerDB.insertPinRequest: Successfully added %s Pins with RequestID %s." % (res['Value'],requestID))
    return S_OK()

  ####################################################################
  #
  # This code handles the finalization of stage tasks
  #

  def updateStageCompletingTasks(self):
    """ This will select all the Tasks in StageCompleting status and check whether all the associated files are Staged. """
    req = "SELECT TR.TaskID,COUNT(if(R.Status NOT IN ('Staged'),1,NULL)) FROM Tasks AS T, TaskReplicas AS TR, Replicas AS R WHERE T.Status='StageCompleting' AND T.TaskID=TR.TaskID AND TR.ReplicaID=R.ReplicaID GROUP BY TR.TaskID;"
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

  def removeTasks(self,taskIDs):
    """ This will delete the entries from the TaskReplicas for the provided taskIDs. """
    req = "DELETE FROM TaskReplicas WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._update(req)
    if not res['OK']:
      return res
    req = "DELETE FROM Tasks WHERE TaskID in (%s);" % intListToString(taskIDs)
    res = self._update(req)
    return res

  def removeUnlinkedReplicas(self):
    """ This will remove from the Replicas tables where there are no associated links. """
    #TODO: If there are entries in the Pins tables these need to be released
    req = "SELECT ReplicaID from Replicas WHERE Links = 0;"
    res = self._query(req)
    if not res['OK']:
      return res
    replicaIDs = []
    for tuple in res['Value']:
      replicaIDs.append(tuple[0])
    if not replicaIDs:
      return S_OK()
    req = "DELETE FROM StageRequests WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req)
    if not res['OK']:
      return res
    req = "DELETE FROM Replicas WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
    res = self._update(req)
    return res

  ####################################################################
  #
  # This code allows the monitoring of the stage tasks
  #
  
  def getTaskStatus(self,taskID):
    """ Obtain the task status from the Tasks table. """
    res = self.getTaskInfo(taskID)
    if not res['OK']:
      return res
    taskInfo = res['Value'][taskID]
    return S_OK(taskInfo['Status'])

  def getTaskInfo(self,taskID):
    """ Obtain all the information from the Tasks table for a supplied task. """
    req = "SELECT TaskID,Status,Source,SubmitTime,CompleteTime,CallBackMethod,SourceTaskID from Tasks WHERE TaskID = %s;" % taskID
    res = self._query(req)
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

  def getTaskSummary(self,taskID):
    """ Obtain the task summary from the database. """
    res = self.getTaskInfo(taskID)
    if not res['OK']:
      return res
    taskInfo = res['Value']
    req = "SELECT R.LFN,R.StorageElement,R.PFN,R.FileSize,R.Status,R.Reason FROM Replicas AS R, TaskReplicas AS TR WHERE TR.TaskID = %s AND TR.ReplicaID=R.ReplicaID;" % taskID
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getTaskSummary: Failed to get Replica summary for task.',res['Message'])
      return res
    replicaInfo = {} 
    for lfn,storageElement,pfn,fileSize,status,reason in res['Value']:
      replicaInfo[lfn] = {'StorageElement':storageElement,'PFN':pfn,'FileSize':fileSize,'Status':status,'Reason':reason}
    resDict = {'TaskInfo':taskInfo,'ReplicaInfo':replicaInfo}
    return S_OK(resDict)
