""" Simple DISET service interface to the Migration Monitoring DB
"""
__RCSID__ = "$Id$"

from DIRAC                                                    import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                          import RequestHandler
from DIRAC.StorageManagementSystem.DB.MigrationMonitoringDB   import MigrationMonitoringDB
from types                                                    import *

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

  types_addMigratingReplicas = [[ListType,TupleType]]
  def export_addMigratingReplicas(self,fileTuples):
    """ Add a list of files to the Migration Monitoring DB
    """
    try:
      gLogger.info("addMigratingReplicas: Attempting to add %d files to the database." % len(fileTuples))
      res = database.addMigratingReplicas(fileTuples)
      return res
    except Exception,x:
      errStr = "addMigratingReplicas: Exception while adding files to database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_removeMigratingFiles = [[ListType,TupleType]]
  def export_removeMigratingFiles(self,lfns):
    """ Remove a list of LFNs from the Migration Monitoring DB
    """
    try:
      gLogger.info("removeMigratingFiles: Attempting to remove %d files from the database." % len(lfns))
      res = database.removeMigratingFiles(lfns)
      return res
    except Exception,x:
      errStr = "removeMigratingFiles: Exception while removing files from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_removeMigratingReplicas = [[ListType,TupleType]]
  def export_removeMigratingReplicas(self,replicaList):
    """ Remove a list of replicas (lfn,pfn,se) from the Migration Monitoring DB
    """
    try:
      gLogger.info("removeMigratingReplicas: Attempting to remove %d replicas from the database." % len(replicaList))
      res = database.removeMigratingReplicas(replicaList)
      return res
    except Exception,x:
      errStr = "removeMigratingReplicas: Exception while removing replicas from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  #################################################################
  #
  # The methods for retrieving data files from the database
  #

  types_getMigratingReplicas = [StringType,StringType]
  def export_getMigratingReplicas(self,se,status):
    """ Get a list of replicas in the Migration Monitoring DB
    """
    try:
      gLogger.info("getMigratingReplicas: Attempting to get '%s' replicas at %s." % (status,se))
      res = database.getMigratingReplicas(se,status)
      return res
    except Exception,x:
      errStr = "getMigratingReplicas: Exception while getting replicas from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_setMigratingReplicaStatus = [ListType,StringType]
  def export_setMigratingReplicaStatus(self,fileIDs,status):
    """ Update the replica statuses in the migration monitoring DB
    """
    try:
      gLogger.info("setMigratingReplicaStatus: Attempting to update status of %d replicas to '%s'." % (len(fileIDs),status))
      res = database.setMigratingReplicaStatus(fileIDs,status)
      return res
    except Exception,x:
      errStr = "setMigratingReplicaStatus: Exception while updating replica statuses."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  #################################################################
  #
  # The methods for monitoring the contents of the database
  #

  types_getMigratingReplicasStatistics = []
  def export_getMigratingReplicasStatistics(self):
    """ Get global replica statistics
    """
    try:
      gLogger.info("Attempting to get global statistics.")
      res = database.getMigratingReplicasStatistics()
      if not res['OK']:
        gLogger.error("getMigratingReplicasStatistics: Failed to get global statistics",res['Message'])
      else:
        gLogger.info("getMigratingReplicasStatistics: Obtained global statistics")
      return res
    except Exception,x:
      errStr = "getMigratingReplicasStatistics: Exception while getting global statistics."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getDistinctMigratingReplicasSelections = []
  def export_getDistinctMigratingReplicasSelections(self):
    """ Get the possible selections available
    """
    try:
      gLogger.info("getDistinctMigratingReplicasSelections: Attempting to get distinct selections.")
      res = database.getDistinctMigratingReplicasSelections()
      if not res['OK']:
        gLogger.error("getDistinctMigratingReplicasSelections: Failed to get distinct selections",res['Message'])
      else:
        gLogger.info("getDistinctMigratingReplicasSelections: Obtained distinct selections")
      return res
    except Exception,x:
      errStr = "getDistinctMigratingReplicasSelections: Exception while getting distinct selections."
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
    res = database.selectMigratingReplicasRows(selectDict, orderAttribute=orderAttribute,newer=startDate, older=endDate )
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
