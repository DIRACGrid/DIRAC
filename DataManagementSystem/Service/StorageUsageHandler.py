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

  types_insertDirectory = [StringType,IntType,LongType]
  def export_insertDirectory(self,directory,directoryFiles,directorySize):
    """ Insert the directory and parameters in the database
    """
    try:
      res = storageUsageDB.removeDirectory(directory)
      if res['OK']:
        gLogger.info("StorageUsageHandler.insertDirectory: Successfully removed existing directory.")
      else:
        gLogger.error("StorageUsageHandler.insertDirectory: Failed to remove existing directory.")

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
      return S_ERROR(errStr)

  types_publishDirectoryUsage = [StringType,StringType,LongType,IntType]
  def export_publishDirectoryUsage(self,directory,storageElement,storageElementSize,storageElementFiles):
    """ Publish the storage usage for for a particular directory and storage element
    """
    try:
      gLogger.info("StorageUsageHandler.publishDirectoryUsage: Attempting to insert usage at %s." % storageElement)
      res = storageUsageDB.publishDirectoryUsage(directory,storageElement,storageElementSize,storageElementFiles)
      if res['OK']:
        gLogger.info("StorageUsageHandler.publishDirectoryUsage: Successfully added usage.")
      else:
        gLogger.error("StorageUsageHandler.publishDirectoryUsage: Failed add usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.publishDirectoryUsage: Exception while inserting usage."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

  types_publishEmptyDirectory = [StringType]
  def export_publishEmptyDirectory(self,directory):
    """ Publish that the supplied directory is empty
    """
    try:
      gLogger.info("StorageUsageHandler.publishEmptyDirectory: Attempting to remove usage for %s." % directory)
      res = storageUsageDB.recursiveRemoveDirectory(directory)
      if res['OK']:
        gLogger.info("StorageUsageHandler.publishEmptyDirectory: Successfully removed directory.")
      else:
        gLogger.error("StorageUsageHandler.publishDirectoryUsage: Failed to remove directory.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.publishEmptyDirectory: Exception removing directory."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

  types_getStorageSummary = []
  def export_getStorageSummary(self):
    """ Retieve a summary for the storage usage
    """
    try:
      gLogger.info("StorageUsageHandler.getStorageSummary: Attempting to get usage summary.")
      res = storageUsageDB.getStorageSummary()
      if res['OK']:
        gLogger.info("StorageUsageHandler.getStorageSummary: Successfully obtained usage.")
      else:
        gLogger.error("StorageUsageHandler.getStorageSummary: Failed obtain usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.getStorageSummary: Exception while obtaining usage."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

