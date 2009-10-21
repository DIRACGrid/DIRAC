""" MigrationMonitoringDB is the front end for the database containing the files which are awating migration.
    It offers a simple interface to add files, get files and modify their status.
"""

__RCSID__ = "$Id: MigrationMonitoringDB.py,v 1.2 2009/10/21 14:16:54 acsmith Exp $"

from DIRAC                        import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB           import DB
from DIRAC.Core.Utilities.List    import stringListToString
from types                        import ListType

class MigrationMonitoringDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'MigrationMonitoringDB','DataManagement/MigrationMonitoringDB',maxQueueSize)

  #################################################################
  #
  # The methods for adding/removing files to/from the database
  #

  def addFiles(self,fileList):
    """ Insert files into the database
    """
    gLogger.info("addFiles: Attempting to add %d files to database." % len(fileList))
    req = "INSERT INTO Files (Status,LFN,PFN,Size,SE,GUID,FileChecksum,SubmitTime) VALUES"
    for lfn,pfn,size,se,guid,checksum in fileList:
      req = "%s ('Migrating','%s','%s',%s,'%s','%s','%s',UTC_TIMESTAMP())," % (req,lfn,pfn,size,se,guid,checksum)
    req = req.rstrip(',')
    res = self._update(req)
    if not res['OK']:
      gLogger.error("addFiles: Failed update add files to database.",res['Message'])
    else:
      gLogger.info("addFile: Successfully added files.")
    return res

  def removeFiles(self,lfns):
    """ Remove files from the database
    """
    gLogger.info("removeFiles: Attempting to remove %d files from the database." % len(lfns))
    req = "DELETE FROM Files WHERE LFN IN (%s);" % stringListToString(lfns)
    res = self._update(req)
    if not res['OK']:
      gLogger.error("removeFiles: Failed remove files from database.",res['Message'])
    else:
      gLogger.info("removeFiles: Successfully removed files.")
    return res

  def removeReplicas(self,replicaList):
    """ Remove replicas from the database
    """
    gLogger.info("removeReplicas: Attempting to remove %d replicas from the database." % len(replicaList))
    successful = {}
    failed = {}
    for lfn,pfn,se in replicaList:
      req = "DELETE FROM Files WHERE SE='%s' AND LFN = '%s';" % (se,lfn)
      res = self._update(req)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #################################################################
  #
  # The methods for retrieving data files from the database
  #

  def getFiles(self,se,status):
    """ Obtain all the files of a given status in the database along with all their associated metadata
    """
    req = "SELECT FileID,LFN,PFN,SE,Size,FileChecksum,SubmitTime FROM Files WHERE Status = '%s' AND SE = '%s';" % (status,se)
    res = self._query(req)
    if not res['OK']:
      gLogger.error("getFiles: Failed to get files from database.",res['Message'])
      return res
    for id,lfn,pfn,se,size,checksum,submitTime in res['Value']:
      fileDict[id] = {'LFN':lfn,'PFN':pfn,'SE':se,'Size':size,'Checksum':checksum,'SubmitTime':submitTime}
    gLogger.info("getActiveFiles: Obtained %d files awaiting migration from database." % len(fileDict.keys()))
    return S_OK(fileDict)

  def setFilesStatus(self,fileIDs,status):
    """ Update the status of the file in the database
    """
    gLogger.info("setFileStatus: Attempting to update status of %d files to '%s'." % (len(fileIDs),status))
    req = "UPDATE Files SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE FileID IN (%s);" % (status,intListToString(fileIDs))
    res = self._update(req)
    if not res['OK']:
      gLogger.error("setFileStatus: Failed update file statuses.",res['Message'])
    else:
      gLogger.info("setFileStatus: Successfully updated file statuses.")
    return res

  #################################################################
  #
  # The methods for monitoring the contents of the database
  #

  def getGlobalStatistics(self):
    """ Get the count of the file statutes and storage elements in the DB
    """
    req = "SELECT Status,SE,COUNT(*) FROM Files GROUP BY Status,SE;"
    res = self._query(req)
    if not res['OK']:
      return res
    statusDict = {}
    for tuple in res['Value']:
      status,storageElement,count = tuple
      if not statusDict.has_key(status):
        statusDict[status] ={}
      statusDict[status][storageElement] = count
    return S_OK(statusDict)

  def getDistinctSelections(self):
    """ Get the unique values of the selection fields
    """
    selDict = {}
    selDict['SE'] = []
    req = "SELECT DISTINCT(SE) FROM Files;"
    res = self._query(req)
    if not res['OK']:
      return res
    for tuple in res['Value']:
      selDict['SE'].append(tuple[0])
    selDict['Status'] = []
    req = "SELECT DISTINCT(Status) FROM Files;"
    res = self._query(req)
    if not res['OK']:
      return res
    for tuple in res['Value']:
      selDict['Status'].append(tuple[0])
    return S_OK(selDict)

  def selectRows(self,selectDict, orderAttribute='LFN',newer=None, older=None, limit=None):
    """ Select the rows which match the selection criteria.
    """
    condition = self.__buildCondition(selectDict, older, newer)
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
    cmd = 'SELECT LFN,PFN,Size,StorageElement,GUID,FileChecksum,SubmitTime,CompleteTime,Status from Files %s' % condition
    print cmd
    res = self._query(cmd)
    if not res['OK']:
      return res
    if not len(res['Value']):
      return S_OK([])
    return S_OK(res['Value'])

  def __buildCondition(self, condDict, older=None, newer=None, timeStamp='SubmitTime'):
    """ build SQL condition statement from provided condDict and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"
    if condDict != None:
      for attrName, attrValue in condDict.items():
        ret = self._escapeString(attrName)
        if not ret['OK']:
          return ret
        attrName = "`"+ ret['Value'][1:-1]+"`"
        if type(attrValue) == ListType:
          multiValueList = []
          for x in attrValue:
            ret = self._escapeString(x)
            if not ret['OK']:
              return ret
            x = ret['Value']
            multiValueList.append(x)
          multiValue = ','.join(multiValueList)
          condition = ' %s %s %s in (%s)' % (condition, conjunction, attrName, multiValue)
        else:
          ret = self._escapeString(attrValue)
          if not ret['OK']:
            return ret
          attrValue = ret['Value']
          condition = ' %s %s %s=%s' % (condition, conjunction, attrName, attrValue)
        conjunction = "AND"
    if older:
      ret = self._escapeString(older)
      if not ret['OK']:
        return ret
      older = ret['Value']
      condition = ' %s %s %s <= %s' % (condition, conjunction, timeStamp, older)
      conjunction = "AND"
    if newer:
      ret = self._escapeString(newer)
      if not ret['OK']:
        return ret
      newer = ret['Value']
      condition = ' %s %s %s >= %s' % (condition, conjunction, timeStamp, newer)
    return condition
