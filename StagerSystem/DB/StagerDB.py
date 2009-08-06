########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.py,v 1.22 2009/08/06 15:26:25 acsmith Exp $
########################################################################

""" StagerDB is a front end to the Stager Database.

    There are five tables in the StagerDB: Tasks, Replicas, TaskReplicas, StageRequests and Pins.

    The Tasks table is the place holder for the tasks that have requested files to be staged. These can be from different systems and have different associated call back methods.
    The Replicas table keeps the information on all the Replicas in the system. It maps all the file information LFN, PFN, SE to an assigned ReplicaID.
    The TaskReplicas table maps the TaskIDs from the Tasks table to the ReplicaID from the Replicas table.
    The StageRequests table contains each of the prestage request IDs for each of the replicas.
    The Pins table keeps the pinning request ID along with when it was issued and for how long for each of the replicas.
"""

__RCSID__ = "$Id: StagerDB.py,v 1.22 2009/08/06 15:26:25 acsmith Exp $"

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
        replicaString = "(%s,%s,NOW())," % (replicaID,requestID)
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
    req = "UPDATE StageRequests SET StageStatus='Staged',StageRequestCompletedTime = NOW() WHERE ReplicaID IN (%s);" % intListToString(replicaIDs)
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















  ###########################################################
  #
  # These are not used
  #

  def getFileSRMReqInfo(self,status):
    """ This methods retrieves the FileID, StorageElement, PFN, SRMRequestID from the Files and StageRequests table for the supplied status
    """
    req = "SELECT f.FileID,f.StorageElement, f.PFN, sr.SRMRequestID FROM Files AS f, StageRequests AS sr WHERE f.Status='%s' AND f.FileID=sr.FileID;" % status
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getFileSRMReqInfo: Failed to get files information for %s status files.' % status ,res['Message'])
      return res
    files = {}
    for fileID,storageElement,pfn,srmReqID in res['Value']:
      files[fileID] = (storageElement,pfn,srmReqID)
    return S_OK(files)

  def insertPins(self,fileIDs,requestID,pinLifeTime):
    """ This method inserts the FileIDs, SRMRequestID, PinCreationTime and PinExpiryTime into the Pins table
    """
    successful = []
    for fileID in fileIDs:
      req = "INSERT INTO Pins (FileID,SRMRequestID,PinCreationTime,PinExpiryTime) VALUES (%s,%s,NOW(),DATE_ADD(NOW(),INTERVAL %s SECOND));" % (fileID,requestID,pinLifeTime)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.insertPins: Failed to insert to Pins table',res['Message'])
      else:
        successful.append(fileID)
    return S_OK(successful)

  ###########################################################################
  #
  # Manipulate the tasks table to get FileID<->TaskID mappings
  #

  def getFileIDsForTasks(self,taskIDs):
    """ This obtains the files assocaited to a list of tasks."""
    req = "SELECT TaskID,FileID FROM Tasks WHERE TaskID IN (%s);" % intListToString(taskIDs)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getFileIDsForTasks: Failed to get FileIDs associated to tasks.', res['Message'])
      return res
    taskFiles = {}
    for taskID,fileID in res['Value']:
      if not taskFiles.has_key(taskID):
        taskFiles[taskID] = []
      taskFiles[taskID].append(fileID)
    return S_OK(taskFiles)

  def getTasksForFileIDs(self,fileIDs):
    """ This obtains the tasks associated to a list of fileIDs."""
    req = "SELECT TaskID,FileID FROM Tasks WHERE FileID IN (%s);" % intListToString(fileIDs)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getTasksForFileIDs: Failed to get TaskIDs associated to files.', res['Message'])
      return res
    taskIDs = {}
    for taskID,fileID in res['Value']:
      if not taskIDs.has_key(fileID):
        taskIDs[fileID] = []
      taskIDs[fileID].append(taskID)
    return S_OK(taskIDs)

  ###########################################################################
  #
  # Update the status field in any of the four tables: Tasks, Files, StageRequests, Pins
  #
  
  def updateTasksStatus(self,fileIDs,newStatus,oldStatus=False):
   """ Update the supplied FileIDs in the Tasks table to the supplied status
   """
   return self.__updateStatus(fileIDs,newStatus,'Tasks',oldStatus)

  def updateFilesStatus(self,fileIDs,newStatus,oldStatus=False):
   """ Update the supplied FileIDs in the Files table to the supplied status
   """
   return self.__updateStatus(fileIDs,newStatus,'Files',oldStatus)

  def updateStageRequestsStatus(self,fileIDs,newStatus,oldStatus=False):
   """ Update the supplied FileIDs in the StageRequests table to the supplied status
   """
   return self.__updateStatus(fileIDs,newStatus,'StageRequests',oldStatus)

  def updatePinsStatus(self,fileIDs,newStatus,oldStatus=False):
    """ Update the supplied FileIDs in the Pins table to the supplied status
    """ 
    return self.__updateStatus(fileIDs,newStatus,'Pins',oldStatus)

  def updateStatus(self,fileIDs,newStatus,table,oldStatus=False):
    """ A simple wrapper for __updateStatus()
    """
    return self.__updateStatus(fileIDs,newStatus,table,oldStatus)

  def __updateStatus(self,fileIDs,newStatus,table,oldStatus=False):
    """ This method updates the Status field in the supplied table for the supplied fileIDs
    """
    if oldStatus:
      req = "UPDATE %s SET Status = '%s' WHERE Status = '%s' AND FileID IN (%s);" % (table,newStatus,oldStatus,intListToString(fileIDs))
    else:
      req = "UPDATE %s SET Status = '%s' WHERE FileID IN (%s);" % (table,newStatus,intListToString(fileIDs))
    res = self._update(req)
    if not res['OK']:
      gLogger.error('StagerDB.__updateFilesStatus: Failed to update files status from %s to %s' % (oldStatus,newStatus),res['Message'])
    return res

  ########################################################################################
  #
  # 
  #
  
  def getStageRequestsFilesForState(self,requestIDs,status):
    return self.__getFilesForRequestID(requestIDs,status,'StageRequests')
  
  def getPinRequestsFilesForState(self,requestIDs,status):
    return self.__getFilesForRequestID(requestIDs,status,'Pins')

  def __getFilesForRequestID(self,requestIDs,status,table):
    """ This allows the retrieval of the FileIDs associated to the supplied requestIDs from the supplied table
    """
    req = "SELECT FileID,SRMRequestID from %s WHERE SRMRequestID IN (%s) AND Status = '%s';" % (table,intListToString(requestIDs),status)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getFilesForRequestID: Failed to get files for %s status from %s.' % (status,table),res['Message'])
      return res
    files = {}
    for fileID,srmReqID in res['Value']:
      files[fileID] = srmReqID
    return S_OK(files)
