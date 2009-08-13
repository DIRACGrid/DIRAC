""" StorageUsageHandler is the implementation of the Storage Usage service in the DISET framework
"""
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

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
      gLogger.exception(errStr,lException=x)
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
      gLogger.exception(errStr,lException=x)
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
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  ##################################################################
  #
  # These are the methods for monitoring the usage
  #

  types_getStorageSummary = []
  def export_getStorageSummary(self,directory='',filetype='',production='',sites=[]):
    """ Retieve a summary for the storage usage
    """
    try:
      gLogger.info("StorageUsageHandler.getStorageSummary: Attempting to get usage summary.")
      res = storageUsageDB.getStorageSummary(directory,filetype,production,sites)
      if res['OK']:
        gLogger.info("StorageUsageHandler.getStorageSummary: Successfully obtained usage.")
      else:
        gLogger.error("StorageUsageHandler.getStorageSummary: Failed obtain usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.getStorageSummary: Exception while obtaining usage."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getStorageDirectorySummary = []
  def export_getStorageDirectorySummary(self,directory='',filetype='',production='',sites=[]):
    """ Retieve a directory summary for the storage usage
    """
    try:
      gLogger.info("StorageUsageHandler.getStorageDirectorySummary: Attempting to get usage summary.")
      res = storageUsageDB.getStorageDirectorySummary(directory,filetype,production,sites)
      if res['OK']:
        gLogger.info("StorageUsageHandler.getStorageDirectorySummary: Successfully obtained usage.")
      else:
        gLogger.error("StorageUsageHandler.getStorageDirectorySummary: Failed obtain usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.getStorageDirectorySummary: Exception while obtaining usage."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getStorageDirectorySummaryWeb = []
  def export_getStorageDirectorySummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the directory storage summary
    """
    resultDict = {}

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None

    directory = ''
    if selectDict.has_key('Directory'):
      directory = selectDict['Directory']
    filetype = ''
    if selectDict.has_key('FileType'):
      filetype = selectDict['FileType']
    production = ''
    if selectDict.has_key('Production'):
      production = selectDict['Production']   
    ses = []
    if selectDict.has_key('SEs'):
      ses = selectDict['SEs']

    res = storageUsageDB.getStorageDirectorySummary(directory,filetype,production,ses)
    if not res['OK']:
      gLogger.error("StorageUsageHandler.getStorageDirectorySummaryWeb: Failed to obtain directory summary.",res['Message'])
      return res
    dirList = res['Value']

    nDirs = len(dirList)
    resultDict['TotalRecords'] = nDirs
    if nDirs == 0:
      return S_OK(resultDict)
    iniDir = startItem
    lastDir = iniDir + maxItems
    if iniDir >= nDirs:
      return S_ERROR('Item number out of range')
    if lastDir > nDirs:
      lastDir = nDirs

    # prepare the standard structure now
    resultDict['ParameterNames'] = ['Directory Path','Size','Files']
    resultDict['Records'] = dirList[iniDir:lastDir]
    resultDict['Extras'] = {}
    return S_OK(resultDict)

  types_getStorageElementSelection = []
  def export_getStorageElementSelection(self):
    """ Retrieve the possible selections 
    """
    try:
      gLogger.info("StorageUsageHandler.getStorageElementSelection: Attempting to get the SE selections.")
      res = storageUsageDB.getStorageElementSelection()
      if res['OK']:
        gLogger.info("StorageUsageHandler.getStorageElementSelection: Successfully obtained SE selections.")
      else:
        gLogger.error("StorageUsageHandler.getStorageElementSelection: Failed obtain selections.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.getStorageElementSelection: Exception while obtaining usage."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)        

  types_getUserStorageUsage = []
  def export_getUserStorageUsage(self,userName=''):
    """ Retieve a summary of the user usage
    """
    try:
      gLogger.info("StorageUsageHandler.getUserStorageUsage: Attempting to get user usage summary.")
      res = storageUsageDB.getUserStorageUsage(userName)
      if res['OK']:
        gLogger.info("StorageUsageHandler.getUserStorageUsage: Successfully obtained usage.")
      else:
        gLogger.error("StorageUsageHandler.getUserStorageUsage: Failed obtain usage.")
      return res
    except Exception, x:
      errStr = "StorageUsageHandler.getUserStorageUsage: Exception while obtaining usage."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)
