""" RAWIntegrityDB is the front end for the database containing the files which are awating migration.
    It offers a simple interface to add files, get files and modify their status.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

gLogger.initialize('DMS','/Databases/RAWIntegrityDB/Test')

class RAWIntegrityDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'RAWIntegrityDB','DataManagement/RAWIntegrityDB',maxQueueSize)

  def getActiveFiles(self):
    """ Obtain all the active files in the database along with all their associated metadata
    """
    try:
      gLogger.info("RAWIntegrityDB.getActiveFiles: Obtaining files awaiting migration from database.")
      req = "SELECT LFN,PFN,Size,StorageElement,GUID,FileChecksum from Files WHERE Status = 'Active';"
      res = self._query(req)
      if not res['OK']:
        gLogger.error("RAWIntegrityDB.getActiveFiles: Failed to get files from database.",res['Message'])
        return res
      else:
        fileDict = {}
        for lfn,pfn,size,se,guid,checksum in res['Value']:
          fileDict[lfn] = {'PFN':pfn,'Size':size,'SE':se,'GUID':guid,'Checksum':checksum}
        gLogger.info("RAWIntegrityDB.getActiveFiles: Obtained %s files awaiting migration from database." % len(fileDict.keys()))
        return S_OK(fileDict)
    except Exception,x:
      errStr = "RAWIntegrityDB.getActiveFiles: Exception while getting files from database."
      gLogger.exception(errStr, str(x))
      return S_ERROR(errStr)

  def setFileStatus(self,lfn,status):
    """ Update the status of the file in the database
    """
    try:
      gLogger.info("RAWIntegrityDB.setFileStatus: Attempting to update status of %s to '%s'." % (lfn,status))
      req = "UPDATE Files SET Status='%s',CompleteTime=NOW() WHERE LFN = '%s' AND Status = 'Active';" % (status,lfn)
      res = self._update(req)
      if not res['OK']:
        gLogger.error("RAWIntegrityDB.setFileStatus: Failed update file status.",res['Message'])
      else:
        gLogger.info("RAWIntegrityDB.setFileStatus: Successfully updated file status.")
      return res
    except Exception,x:
      errStr = "RAWIntegrityDB.setFileStatus: Exception while updating file status."
      gLogger.exception(errStr, str(x))
      return S_ERROR(errStr)

  def addFile(self,lfn,pfn,size,se,guid,checksum):
    """ Insert file into the database
    """
    try:
      gLogger.info("RAWIntegrityDB.addFile: Attempting to add %s to database." % lfn)
      req = "INSERT INTO Files (LFN,PFN,Size,StorageElement,GUID,FileChecksum,SubmitTime) VALUES\
            ('%s','%s',%s,'%s','%s','%s',NOW());" % (lfn,pfn,size,se,guid,checksum)
      res = self._update(req)
      if not res['OK']:
        gLogger.error("RAWIntegrityDB.addFile: Failed update add file to database.",res['Message'])
      else:
        gLogger.info("RAWIntegrityDB.addFile: Successfully added file.")
      return res
    except Exception,x:
      errStr = "RAWIntegrityDB.addFile: Exception while updating file status."
      gLogger.exception(errStr, str(x))
      return S_ERROR(errStr)
