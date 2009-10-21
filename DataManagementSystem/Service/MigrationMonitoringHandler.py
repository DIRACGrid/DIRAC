""" Simple DISET service interface to the Migration Monitoring DB
"""
__RCSID__ = "$Id: MigrationMonitoringHandler.py,v 1.2 2009/10/21 14:18:13 acsmith Exp $"

from DIRAC                                               import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                     import RequestHandler
from DIRAC.DataManagementSystem.DB.MigrationMonitoringDB import MigrationMonitoringDB
from types                                               import *

database = False

def initializeMigrationMonitoringHandler(serviceInfo):
  global database
  database = MigrationMonitoringDB()
  return S_OK()

class MigrationMonitoringHandler(RequestHandler):

  #################################################################
  #
  # The methods for adding data files from the database
  #

  types_addFiles = [[ListType,TupleType]]
  def export_addFiles(self,fileTuples):
    """ Add a list of files to the Migration Monitoring DB
    """
    try:
      gLogger.info("addFiles: Attempting to add %d files to the database." % len(fileTuples))
      res = database.addFiles(fileTuples)
      return res
    except Exception,x:
      errStr = "addFiles: Exception while adding files to database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_removeFiles = [[ListType,TupleType]]
  def export_removeFiles(self,lfns):
    """ Remove a list of LFNs from the Migration Monitoring DB
    """
    try:
      gLogger.info("removeFiles: Attempting to remove %d files from the database." % len(lfns))
      res = database.removeFiles(lfns)
      return res
    except Exception,x:
      errStr = "removeFiles: Exception while removing files from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_removeReplicas = [[ListType,TupleType]]
  def export_removeReplicas(self,replicaList):
    """ Remove a list of replicas (lfn,pfn,se) from the Migration Monitoring DB
    """
    try:
      gLogger.info("removeReplicas: Attempting to remove %d replicas from the database." % len(replicaList))
      res = database.removeReplicas(replicaList)
      return res
    except Exception,x:
      errStr = "removeReplicas: Exception while removing replicas from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  #################################################################
  #
  # The methods for retrieving data files from the database
  #

  types_getFiles = [StringType,StringType]
  def export_getFiles(self,se,status):
    """ Get a list of files in the Migration Monitoring DB
    """
    try:
      gLogger.info("getFiles: Attempting to get '%s' files at %s." % (status,se))
      res = database.getFiles(se,status)
      return res
    except Exception,x:
      errStr = "getFiles: Exception while getting files from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_setFilesStatus = [ListType,StringType]
  def export_setFilesStatus(self,fileIDs,status):
    """ Update the file statuses in the migration monitoring DB
    """
    try:
      gLogger.info("setFilesStatus: Attempting to update status of %d files to '%s'." % (len(fileIDs),status))
      res = database.setFilesStatus(fileIDs,status)
      return res
    except Exception,x:
      errStr = "setFilesStatus: Exception while updating file statuses."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  #################################################################
  #
  # The methods for monitoring the contents of the database
  #

  types_getGlobalStatistics = []
  def export_getGlobalStatistics(self):
    """ Get global replica statistics
    """
    try:
      gLogger.info("Attempting to get global statistics.")
      res = database.getGlobalStatistics()
      if not res['OK']:
        gLogger.error("getGlobalStatistics: Failed to get global statistics",res['Message'])
      else:
        gLogger.info("getGlobalStatistics: Obtained global statistics")
      return res
    except Exception,x:
      errStr = "getGlobalStatistics: Exception while getting global statistics."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getDistinctSelections = []
  def export_getDistinctSelections(self):
    """ Get the possible selections available
    """
    try:
      gLogger.info("Attempting to get distinct selections.")
      res = database.getDistinctSelections()
      if not res['OK']:
        gLogger.error("getDistinctSelections: Failed to get distinct selections",res['Message'])
      else:
        gLogger.info("getDistinctSelections: Obtained distinct selections")
      return res
    except Exception,x:
      errStr = "getDistinctSelections: Exception while getting distinct selections."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getSummaryWeb = [DictType,ListType,IntType,IntType]
  def export_getSummaryWeb(self,selectDict, sortList, startItem, maxItems):
    """ Get the replica information according to conventions
    """
    resultDict = {}
    startDate = selectDict.get('FromDate',None)
    if startDate:
      del selectDict['FromDate']
    endDate = selectDict.get('ToDate',None)
    if endDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None
    res = database.selectRows(selectDict, orderAttribute=orderAttribute,newer=startDate, older=endDate )
    if not res['OK']:
      return S_ERROR('Failed to select rows: '+res['Message'])
    # Get the files and the counters correctly
    fileList = res['Value']
    nRows = len(fileList)
    resultDict['TotalRecords'] = nRows
    if nRows == 0:
      return S_OK(resultDict)
    iniRow = startItem
    lastRow = iniRow + maxItems
    if iniRow >= nRows:
      return S_ERROR('Item number out of range')
    if lastRow > nRows:
      lastRow = nRows
    summaryRowList = fileList[iniRow:lastRow]
    # Prepare the standard format
    resultDict['ParameterNames'] = ['LFN','PFN','Size','SE','GUID','Checksum','StartTime','EndTime','Status']
    records = []
    statusCountDict = {}
    for lfn,pfn,size,se,guid,checksum,submit,complete,status in fileList:
      if not statusCountDict.has_key(status):
        statusCountDict[status] = {}
        if not statusCountDict[status].has_key(se):
          statusCountDict[status][se] = 0
        statusCountDict[status][se] += 1
    for tuple in summaryRowList:
      lfn,pfn,size,se,guid,checksum,submit,complete,status = tuple
      startTime = str(submit)
      endTime = str(complete)
      records.append((lfn,pfn,size,se,guid,checksum,startTime,endTime,status))
    resultDict['Records'] = records
    resultDict['Extras'] = statusCountDict
    return S_OK(resultDict)
