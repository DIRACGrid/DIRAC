########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.py,v 1.18 2009/06/17 22:40:31 acsmith Exp $
########################################################################

""" StagerDB is a front end to the Stager Database.

    There are five tables in the StagerDB: Tasks, Replicas, TaskReplicas, StageRequests and Pins.

    The Tasks table is the place holder for the tasks that have requested files to be staged. These can be from different systems and have different associated call back methods.
    The Replicas table keeps the information on all the Replicas in the system. It maps all the file information LFN, PFN, SE to an assigned ReplicaID.
    The TaskReplicas table maps the TaskIDs from the Tasks table to the ReplicaID from the Replicas table.
    The StageRequests table contains each of the prestage request IDs for each of the replicas.
    The Pins table keeps the pinning request ID along with when it was issued and for how long for each of the replicas.
"""

__RCSID__ = "$Id: StagerDB.py,v 1.18 2009/06/17 22:40:31 acsmith Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString,stringListToString
from DIRAC.Core.Utilities.Time import toString
from DIRAC.Core.Base.DB import DB

import string,threading

class StagerDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'StagerDB','Stager/StagerDB',maxQueueSize)
    self.getIdLock = threading.Lock()

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
          existingReplicas[lfn] = res['Value']
    # Insert all the replicas into the TaskReplicas table
    res = self._insertTaskReplicaInformation(taskID,existingReplicas.values())
    if not res['OK']:
      gLogger.warn("Perform roll back")
    return S_OK(taskID)

  def _createTask(self,source,callbackMethod,sourceTaskID):
    """ Enter the task details into the Tasks table """
    self.getIdLock.acquire()
    req = "INSERT INTO Tasks (Source,SubmitTime,CallBackMethod,SourceTaskID) VALUES ('%s',UTC_TIMESTAMP(),'%s','%s');" % (source,callbackMethod,sourceTaskID)
    res = self._update(req)
    self.getIdLock.release() 
    if not res['OK']:
      gLogger.error("StagerDB._createTask: Failed to create task.", res['Message'])
      return res
    taskID = res['lastRowId']
    gLogger.info("StagerDB._createTask: Created task with ('%s','%s','%s') and obtained TaskID %s" % (source,callbackMethod,sourceTaskID,taskID))
    return S_OK(taskID)

  def _getExistingReplicas(self,storageElement,lfns):
    """ Obtains the ReplicasIDs for the replicas already entered in the Replicas table """
    req = "SELECT ReplicaID,LFN FROM Replicas WHERE StorageElement = '%s' AND LFN IN (%s);" % (storageElement,stringListToString(lfns))
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB._getExistingReplicas: Failed to get existing replicas.', res['Message'])
      return res
    existingReplicas = {}
    for replicaID,lfn in res['Value']:
      existingReplicas[lfn] = replicaID
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
    for replicaID in replicaIDs:
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
  #
  # This code manages the state transition of the Replicas from New to Waiting.
  #

  def getFilesWithStatus(self,status):
    """ This method retrieves the FileID and LFN from the Files table with the supplied Status.
    """
    req = "SELECT t.TaskID,f.FileID,f.LFN,f.StorageElement,f.FileSize,f.PFN FROM Files as f, Tasks as t WHERE f.Status = '%s' and f.FileID=t.FileID;" % (status)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getFilesWithStatus: Failed to get files for %s status' % status,res['Message'])
      return res
    files = {}
    for taskID,fileID,lfn,storageElement,fileSize,pfn in res['Value']:
      files[fileID] = (lfn,storageElement,fileSize,pfn)
    return S_OK(files)

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

  def updateFileInformation(self,fileTuples):
    """ This method set the file size information and pfn for the requested storage element
    """
    for lfn,storageElement,pfn,size in fileTuples:
      req = "UPDATE Files SET PFN = '%s', FileSize = %s, Status = 'Waiting' WHERE StorageElement = '%s' AND LFN = '%s';" % (pfn,size,storageElement,lfn)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.insertFileInformation: Failed to insert file information.', res['Message'])
        return res
    return S_OK()

  def insertStageRequests(self,fileIDs,requestID):
    """ This method inserts the FileIDs and SRMRequestID provided into the StageRequests table
    """
    successful = []
    for fileID in fileIDs:
      req = "INSERT INTO StageRequests (FileID,SRMRequestID,StageRequestSubmitTime) VALUES (%s,%s,NOW());" % (fileID,requestID)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.insertStageRequest: Failed to insert to StageRequests table',res['Message'])
      else:
        successful.append(fileID)
    res = self.updateFilesStatus(successful,'Staged')
    print res
    return S_OK(successful)

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
