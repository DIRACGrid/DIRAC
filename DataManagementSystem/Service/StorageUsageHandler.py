""" StorageUsageHandler is the implementation of the Storage Usage service in the DISET framework
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
import time,os
# This is a global instance of the DataIntegrityDB class
storageUsageDB = False

def initializeStorageUsageHandler(serviceInfo):

  global storageUsageDB
  storageUsageDB = StorageUsageDB()
  return S_OK()

class StorageUsageHandler(RequestHandler):

  types_insertDirectory = [StringType,IntType,IntType]
  def export_insertDirectory(self,directory,directoryFiles,directorySize):
    """ Insert the directory and parameters in the database
    """
    try:
      gLogger.info("StorageUsageHandler.insertDirectory: Attempting to insert %s into database." % directory)
      res = storageUsageDB.insertDirectory(directory,directoryFiles,directorySize)
      if res['OK']:
        gLogger.info("StorageUsageHandler.insertDirectory: Successfully added directory.")
      else:
        gLogger.error("StorageUsageHandler.insertDirectory: Failed add directory.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.insertDirectory: Exception while inserting directory."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errorStr)

  types_publishDirectoryUsage = [StringType,StringType,IntType,IntType]
  def export_publishDirectoryUsage(self,directory,storageElement,storageElementSize,storageElementFiles):
    """ Publish the storage usage for for a particular directory and storage element
    """
    try:
      gLogger.info("StorageUsageHandler.publishDirectoryUsage: Attempting to insert usage at %." % storageElement)
      res = storageUsageDB.storageUsageDB.publishDirectoryUsage(directory,storageElement,storageElementSize,storageElementFiles)
      if res['OK']:
        gLogger.info("StorageUsageHandler.publishDirectoryUsage: Successfully added usage.")
      else:
        gLogger.error("StorageUsageHandler.publishDirectoryUsage: Failed add usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.publishDirectoryUsage: Exception while inserting usage."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errorStr)
