########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.py,v 1.16 2009/02/02 17:31:15 acsmith Exp $
########################################################################

""" StagerDB is a front end to the Stager Database.

    There are four tables in the StagerDB: Files, Tasks, StageRequests and Pins.

    The Files table keeps the information on all the files to be staged. It maps all the file information LFN, PFN, SE to an assigned FileID.
    The Tasks table is a mapping of each FileID to the different tasks that requested the file to be staged. These can be from different systems.
    The StageRequests table contains each of the SRM bring-online request IDs for each of the files.
    The Pins table keeps the SRM Request ID for the requests that issued the pin request along with when it was issued and for how long.
"""

__RCSID__ = "$Id: StagerDB.py,v 1.16 2009/02/02 17:31:15 acsmith Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString,stringListToString
from DIRAC.Core.Utilities.Time import toString
from DIRAC.Core.Base.DB import DB

import string,threading

class StagerDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'StagerDB','Stager/StagerDB',maxQueueSize)
    self.getIdLock = threading.Lock()

  def _getExistingFiles(self,storageElement,lfns):
    """ internal method for retrieving the fileIDs for existing files
    """
    req = "SELECT FileID,LFN FROM Files WHERE StorageElement = '%s' AND LFN IN (%s);" % (storageElement,stringListToString(lfns))
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB._getExistingFiles: Failed to get existing files',res['Message'])
      self.getIdLock.release()
      return res
    existingFiles = {}
    for fileID,lfn in res['Value']:
      existingFiles[lfn] = fileID
    return S_OK(existingFiles)

  def _getExistingTasks(self,fileIDs):
    """ internal method for retrieving the taskIDs for a list of file IDs
    """
    req = "SELECT FileID,TaskID FROM Tasks WHERE FileID IN (%s);" % (intListToString(fileIDs))
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB._getExistingTasks: Failed to get existing file tasks',res['Message'])
      self.getIdLock.release()
      return res
    existingTasks = {}
    for fileID,taskID in res['Value']:
      if not existingTasks.has_key(fileID):
        existingTasks[fileID] = []
      existingTasks[fileID].append(taskID)
    return S_OK(existingTasks)

  def setRequest(self,lfns,storageElement,source,callbackMethod,taskID):
    """ This method populates the StagerDB Files and Tasks tables with the requested files.
    """
    self.getIdLock.acquire()
    # The first step is to obtain the FileIDs for the files already requested for the storage element
    res = self._getExistingFiles(storageElement,lfns)
    if not res['OK']:
      return res
    existingFiles = res['Value']

    # Create the entry in the Files table for files that do not already exist
    for lfn in lfns:
      if lfn in existingFiles.keys():
        gLogger.debug('StagerDB.setRequest: File already exists in Files table',lfn)
      else:
        res = self._insert('Files',['LFN','StorageElement'],[lfn,storageElement])
        if not res['OK']:
          gLogger.error('StagerDB.setRequest: Failed to insert to Files table',res['Message'])
          self.getIdLock.release()
          return res
        else:
          fileID = res['lastRowId']
          gLogger.debug('StagerDB.setRequest: File inserted with FileID %s' % fileID)
          existingFiles[lfn] = fileID

    # Need to obtain the files for which we do not already have an identical request
    res = self._getExistingTasks(existingFiles.values())
    if not res['OK']:
      return res
    existingTasks = res['Value']

    filesToAddToTasks = []
    for lfn in lfns:
      fileID = existingFiles[lfn]
      if not fileID in existingTasks.keys():
        filesToAddToTasks.append(fileID)
      elif not taskID in existingTasks[fileID]:
        filesToAddToTasks.append(fileID)

    for fileID in filesToAddToTasks:
      req = "INSERT INTO Tasks (FileID,Source,TaskID,SubmitTime,CallBackMethod) VALUES (%s,'%s',%s,NOW(),'%s');" % (fileID,source,taskID,callbackMethod)
      res = self._update(req)
      if not res['OK']:
        gLogger.error('StagerDB.setRequest: Failed to insert to Tasks table',res['Message'])
        self.getIdLock.release()
        return res
      else:
        gLogger.debug('StagerDB.setRequest: Task inserted for file %s and task %s' % (fileID,taskID))

    # We successfully inserted all of the elements into the database
    self.getIdLock.release()
    return S_OK()

  def getFilesWithStatus(self,status):
    """ This method retrieves the FileID and LFN from the Files table with the supplied Status.
    """
    req = "SELECT t.TaskID,f.FileID,f.LFN,f.StorageElement,f.FileSize,f.PFN FROM Files as f, Tasks as t WHERE f.Status = '%s' and f.FileID=t.FileID;" % (status)
    res = self._query(req)
    if not res['OK']:
      gLogger.error('StagerDB.getFilesWithStatus: Failed to get files for %s status' % status,res['Message'])
      return res
    files = {}
    for fileID,lfn,storageElement,fileSize,pfn in res['Value']:
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
