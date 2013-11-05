""" MigrationMonitoringDB is the front end for the database containing the files which are awating migration.
    It offers a simple interface to add files, get files and modify their status.
"""

__RCSID__ = "$Id$"

from DIRAC                        import gLogger, S_OK
from DIRAC.Core.Base.DB           import DB
from DIRAC.Core.Utilities.List    import stringListToString, intListToString

class MigrationMonitoringDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self, 'StorageManagementDB', 'StorageManagement/StorageManagementDB', maxQueueSize)

  #################################################################
  #
  # The methods for adding/removing files to/from the database
  #

  def addMigratingReplicas(self, fileList):
    """ Insert replicas into the database
    """
    gLogger.info("addMigratingReplicas: Attempting to add %d replicas to database." % len(fileList))
    req = "INSERT INTO MigratingReplicas (Status,LFN,PFN,Size,SE,GUID,FileChecksum,SubmitTime) VALUES"
    for lfn, pfn, size, se, guid, checksum in fileList:
      status = 'New'
      if (size and checksum):
        status = 'Migrating'
      req = "%s ('%s','%s','%s',%s,'%s','%s','%s',UTC_TIMESTAMP())," % (req, status, lfn, pfn, size, se, guid, checksum)
    req = req.rstrip(',')
    res = self._update(req)
    if not res['OK']:
      gLogger.error("addMigratingReplicas: Failed update add replicas to database.", res['Message'])
    else:
      gLogger.info("addMigratingReplicas: Successfully added replicas.")
    return res

  def removeMigratingFiles(self, lfns):
    """ Remove all replicas with associated LFNs from the database
    """
    gLogger.info("removeMigratingFiles: Attempting to remove %d files from the database." % len(lfns))
    req = "DELETE FROM MigratingReplicas WHERE LFN IN (%s);" % stringListToString(lfns)
    res = self._update(req)
    if not res['OK']:
      gLogger.error("removeMigratingFiles: Failed removing files from database.", res['Message'])
    else:
      gLogger.info("removeMigratingFiles: Successfully removed files.")
    return res

  def removeMigratingReplicas(self, replicaList):
    """ Remove replicas from the database
    """
    gLogger.info("removeMigratingReplicas: Attempting to remove %d replicas from the database." % len(replicaList))
    successful = {}
    failed = {}
    for lfn, pfn, se in replicaList:
      req = "DELETE FROM MigratingReplicas WHERE SE='%s' AND LFN = '%s';" % (se, lfn)
      res = self._update(req)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK(resDict)

  #################################################################
  #
  # The methods for retrieving data files from the database
  #

  def getMigratingReplicas(self, se, status):
    """ Obtain all the replicas of a given status in the database along with all their associated metadata
    """
    if se:
      req = "SELECT ReplicaID,LFN,PFN,SE,Size,FileChecksum,SubmitTime FROM MigratingReplicas WHERE Status = '%s' AND SE = '%s';" % (status, se)
    else:
      req = "SELECT ReplicaID,LFN,PFN,SE,Size,FileChecksum,SubmitTime FROM MigratingReplicas WHERE Status = '%s';" % (status)
    res = self._query(req)
    if not res['OK']:
      gLogger.error("getMigratingReplicas: Failed to get replicas from database.", res['Message'])
      return res
    fileDict = {}
    for fid, lfn, pfn, se, size, checksum, submitTime in res['Value']:
      fileDict[fid] = {'LFN' : lfn, 'PFN' : pfn, 'SE' : se, 'Size' : size, 'Checksum' : checksum, 'SubmitTime' : submitTime}
    gLogger.info("getMigratingReplicas: Obtained %d replicas with status %s." % (len(fileDict.keys()), status))
    return S_OK(fileDict)

  def setMigratingReplicaStatus(self, fileIDs, status):
    """ Update the status of the replica in the database
    """
    gLogger.info("setMigratingReplicaStatus: Attempting to update status of %d replicas to '%s'." % (len(fileIDs), status))
    req = "UPDATE MigratingReplicas SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE ReplicaID IN (%s);" % (status, intListToString(fileIDs))
    res = self._update(req)
    if not res['OK']:
      gLogger.error("setMigratingReplicaStatus: Failed update replica statuses.", res['Message'])
    else:
      gLogger.info("setMigratingReplicaStatus: Successfully updated replica statuses.")
    return res

  #################################################################
  #
  # The methods for monitoring the contents of the database
  #

  def getMigratingReplicasStatistics(self):
    """ Get the count of the file statutes and storage elements in the DB
    """
    return self.getCounters('MigratingReplicas', ['Status', 'SE'], {})

  def getDistinctMigratingReplicasSelections(self):
    """ Get the unique values of the selection fields
    """
    selDict = {}
    res = self.getDistinctAttributeValues('MigratingReplicas', 'SE')
    if not res['OK']:
      return res
    selDict['SE'] = res['Value']
    res = self.getDistinctAttributeValues('MigratingReplicas', 'Status')
    if not res['OK']:
      return res
    selDict['Status'] = res['Value']
    return S_OK(selDict)

  def selectMigratingReplicasRows(self, selectDict, orderAttribute='LFN', newer=None, older=None, limit=None):
    """ Select the rows which match the selection criteria.
    """
    condition = self.buildCondition(selectDict, older=older, newer=newer)
    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderType = orderAttribute.split(':')[1].upper()
        orderField = orderAttribute.split(':')[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType
    if limit:
      condition = condition + ' LIMIT ' + str(limit)
    cmd = 'SELECT LFN,PFN,Size,StorageElement,GUID,FileChecksum,SubmitTime,CompleteTime,Status from MigratingReplicas %s' % condition
    res = self._query(cmd)
    if not res['OK']:
      return res
    if not len(res['Value']):
      return S_OK([])
    return S_OK(res['Value'])
