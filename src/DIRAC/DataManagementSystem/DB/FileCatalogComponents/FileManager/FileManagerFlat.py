from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import six
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import stringListToString, intListToString
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager.FileManagerBase import FileManagerBase


class FileManagerFlat(FileManagerBase):

  ######################################################
  #
  # The all important _findFiles and _getDirectoryFiles methods
  #

  def _findFiles(self, lfns, metadata=['FileID'], connection=False):
    connection = self._getConnection(connection)
    """ Find file ID if it exists for the given list of LFNs """
    dirDict = self._getFileDirectories(lfns)
    failed = {}
    directoryIDs = {}
    for dirPath in dirDict.keys():
      res = self.db.dtree.findDir(dirPath)
      if not res['OK'] or not res['Value']:
        error = res.get('Message', 'No such file or directory')
        for fileName in dirDict[dirPath]:
          failed['%s/%s' % (dirPath, fileName)] = error
      else:
        directoryIDs[dirPath] = res['Value']
    successful = {}
    for dirPath in directoryIDs.keys():
      fileNames = dirDict[dirPath]
      res = self._getDirectoryFiles(directoryIDs[dirPath], fileNames, metadata, connection=connection)
      if not res['OK'] or not res['Value']:
        error = res.get('Message', 'No such file or directory')
        for fileName in fileNames:
          failed['%s/%s' % (dirPath, fileName)] = error
      else:
        for fileName, fileDict in res['Value'].items():
          successful["%s/%s" % (dirPath, fileName)] = fileDict
    return S_OK({"Successful": successful, "Failed": failed})

  def _getDirectoryFiles(self, dirID, fileNames, metadata, allStatus=False, connection=False):
    connection = self._getConnection(connection)
    # metadata can be any of
    # ['FileID','Size','UID','GID','Checksum','ChecksumType','Type','CreationDate','ModificationDate','Mode','Status']
    req = "SELECT FileName,%s FROM FC_Files WHERE DirID=%d" % (intListToString(metadata), dirID)
    if not allStatus:
      statusIDs = []
      res = self._getStatusInt('AprioriGood', connection=connection)
      if res['OK']:
        statusIDs.append(res['Value'])
      if statusIDs:
        req = "%s AND Status IN (%s)" % (req, intListToString(statusIDs))
    if fileNames:
      req = "%s AND FileName IN (%s)" % (req, stringListToString(fileNames))
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    files = {}
    for fTuple in res['Value']:
      fileName = fTuple[0]
      files[fileName] = dict(zip(metadata, fTuple[1:]))
    return S_OK(files)

  ######################################################
  #
  # _addFiles related methods
  #

  def _insertFiles(self, lfns, uid, gid, connection=False):
    connection = self._getConnection(connection)
    # Add the files
    failed = {}
    directoryFiles = {}
    insertTuples = []
    res = self._getStatusInt('AprioriGood', connection=connection)
    statusID = 0
    if res['OK']:
      statusID = res['Value']
    for lfn in sorted(lfns.keys()):
      fileInfo = lfns[lfn]
      size = fileInfo['Size']
      guid = fileInfo.get('GUID', '')
      checksum = fileInfo['Checksum']
      checksumtype = fileInfo.get('ChecksumType', 'Adler32')
      dirName = os.path.dirname(lfn)
      dirID = fileInfo['DirID']
      fileName = os.path.basename(lfn)
      if dirName not in directoryFiles:
        directoryFiles[dirName] = []
      directoryFiles[dirName].append(fileName)
      insertTuples.append("(%d,%d,%d,%d,%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d)" %
                          (dirID, size, uid, gid, statusID, fileName, guid, checksum, checksumtype, self.db.umask))
    fields = "DirID,Size,UID,GID,Status,FileName,GUID,Checksum,ChecksumType,CreationDate,ModificationDate,Mode"
    req = "INSERT INTO FC_Files (%s) VALUES %s" % (fields, ','.join(insertTuples))
    res = self.db._update(req, connection)
    if not res['OK']:
      return res
    # Get the fileIDs for the inserted files
    res = self._findFiles(list(lfns), ['FileID'], connection=connection)
    if not res['OK']:
      for lfn in list(lfns):
        failed[lfn] = 'Failed post insert check'
        lfns.pop(lfn)
    else:
      failed.update(res['Value']['Failed'])
      for lfn, fileDict in res['Value']['Successful'].items():
        lfns[lfn]['FileID'] = fileDict['FileID']
    return S_OK({'Successful': lfns, 'Failed': failed})

  def _getFileIDFromGUID(self, guid, connection=False):
    connection = self._getConnection(connection)
    if not guid:
      return S_OK({})
    if not isinstance(guid, (list, tuple)):
      guid = [guid]
    req = "SELECT FileID,GUID FROM FC_Files WHERE GUID IN (%s)" % stringListToString(guid)
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    guidDict = {}
    for fileID, guid in res['Value']:
      guidDict[guid] = fileID
    return S_OK(guidDict)

  ######################################################
  #
  # _deleteFiles related methods
  #

  def _deleteFiles(self, fileIDs, connection=False):
    connection = self._getConnection(connection)
    replicaPurge = self.__deleteFileReplicas(fileIDs)
    filePurge = self.__deleteFiles(fileIDs, connection=connection)
    if not replicaPurge['OK']:
      return replicaPurge
    if not filePurge['OK']:
      return filePurge
    return S_OK()

  def __deleteFileReplicas(self, fileIDs, connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM FC_Replicas WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req, connection)

  def __deleteFiles(self, fileIDs, connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM FC_Files WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req, connection)

  ######################################################
  #
  # _addReplicas related methods
  #

  def _insertReplicas(self, lfns, master=False, connection=False):
    connection = self._getConnection(connection)
    res = self._getStatusInt('AprioriGood', connection=connection)
    statusID = 0
    if res['OK']:
      statusID = res['Value']
    replicaType = 'Replica'
    if master:
      replicaType = 'Master'
    insertTuples = {}
    deleteTuples = []
    successful = {}
    failed = {}
    directorySESizeDict = {}
    for lfn in sorted(lfns.keys()):
      fileID = lfns[lfn]['FileID']
      pfn = lfns[lfn]['PFN']
      seName = lfns[lfn]['SE']
      res = self.db.seManager.findSE(seName)
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      seID = res['Value']
      if not master:
        res = self.__existsReplica(fileID, seID, connection=connection)
        if not res['OK']:
          failed[lfn] = res['Message']
          continue
        elif res['Value']:
          successful[lfn] = True
          continue
      dirID = lfns[lfn]['DirID']
      if dirID not in directorySESizeDict:
        directorySESizeDict[dirID] = {}
      if seID not in directorySESizeDict[dirID]:
        directorySESizeDict[dirID][seID] = {'Files': 0, 'Size': 0}
      directorySESizeDict[dirID][seID]['Size'] += lfns[lfn]['Size']
      directorySESizeDict[dirID][seID]['Files'] += 1
      insertTuples[lfn] = (
          "(%d,%d,%d,'%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" %
          (fileID, seID, statusID, replicaType, pfn))
      deleteTuples.append((fileID, seID))
    if insertTuples:
      fields = "FileID,SEID,Status,RepType,CreationDate,ModificationDate,PFN"
      req = "INSERT INTO FC_Replicas (%s) VALUES %s" % (fields, ','.join(insertTuples.values()))
      res = self.db._update(req, connection)
      if not res['OK']:
        self.__deleteReplicas(deleteTuples, connection=connection)
        for lfn in insertTuples.keys():
          failed[lfn] = res['Message']
      else:
        # Update the directory usage
        self._updateDirectoryUsage(directorySESizeDict, '+', connection=connection)
        for lfn in insertTuples.keys():
          successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def __existsReplica(self, fileID, seID, connection=False):
    # TODO: This is in efficient. Should perform bulk operation
    connection = self._getConnection(connection)
    """ Check if a replica already exists """
    if isinstance(seID, six.string_types):
      res = self.db.seManager.findSE(seID)
      if not res['OK']:
        return res
      seID = res['Value']
    req = "SELECT FileID FROM FC_Replicas WHERE FileID=%d AND SEID=%d" % (fileID, seID)
    result = self.db._query(req, connection)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK(False)
    return S_OK(True)

  ######################################################
  #
  # _deleteReplicas related methods
  #

  def _deleteReplicas(self, lfns, connection=False):
    connection = self._getConnection(connection)
    successful = {}
    res = self._findFiles(list(lfns), ['DirID', 'FileID', 'Size'], connection=connection)
    failed = res['Value']['Failed']
    lfnFileIDDict = res['Value']['Successful']
    toRemove = []
    directorySESizeDict = {}
    for lfn, fileDict in lfnFileIDDict.items():
      fileID = fileDict['FileID']
      se = lfns[lfn]['SE']
      toRemove.append((fileID, se))
      # Now prepare the storage usage dict
      res = self.db.seManager.findSE(se)
      if not res['OK']:
        return res
      seID = res['Value']
      dirID = fileDict['DirID']
      if dirID not in directorySESizeDict:
        directorySESizeDict[dirID] = {}
      if seID not in directorySESizeDict[dirID]:
        directorySESizeDict[dirID][seID] = {'Files': 0, 'Size': 0}
      directorySESizeDict[dirID][seID]['Size'] += fileDict['Size']
      directorySESizeDict[dirID][seID]['Files'] += 1
    res = self.__deleteReplicas(toRemove)
    if not res['OK']:
      for lfn in lfnFileIDDict.keys():
        failed[lfn] = res['Message']
    else:
      # Update the directory usage
      self._updateDirectoryUsage(directorySESizeDict, '-', connection=connection)
      for lfn in lfnFileIDDict.keys():
        successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def __deleteReplicas(self, replicaTuples, connection=False):
    connection = self._getConnection(connection)
    deleteTuples = []
    for fileID, seID in replicaTuples:
      if isinstance(seID, six.string_types):
        res = self.db.seManager.findSE(seID)
        if not res['OK']:
          return res
        seID = res['Value']
      deleteTuples.append("(%d,%d)" % (fileID, seID))
    req = "DELETE FROM FC_Replicas WHERE (FileID,SEID) IN (%s)" % intListToString(deleteTuples)
    return self.db._update(req, connection)

  ######################################################
  #
  # _setReplicaStatus _setReplicaHost _setReplicaParameter methods
  # _setFileParameter method
  #

  def _setReplicaStatus(self, fileID, se, status, connection=False):
    connection = self._getConnection(connection)
    res = self._getStatusInt(status, connection=connection)
    if not res['OK']:
      return res
    statusID = res['Value']
    return self._setReplicaParameter(fileID, se, 'Status', statusID, connection=connection)

  def _setReplicaHost(self, fileID, se, newSE, connection=False):
    connection = self._getConnection(connection)
    res = self.db.seManager.findSE(newSE)
    if not res['OK']:
      return res
    newSE = res['Value']
    return self._setReplicaParameter(fileID, se, 'SEID', newSE, connection=connection)

  def _setReplicaParameter(self, fileID, seID, paramName, paramValue, connection=False):
    connection = self._getConnection(connection)
    if isinstance(seID, six.string_types):
      res = self.db.seManager.findSE(seID)
      if not res['OK']:
        return res
      seID = res['Value']
    req = "UPDATE FC_Replicas SET %s='%s', ModificationDate=UTC_TIMESTAMP() WHERE FileID=%d AND SEID=%d;" % (
        paramName,
        paramValue,
        fileID,
        seID)
    return self.db._update(req, connection)

  def _setFileParameter(self, fileID, paramName, paramValue, connection=False):
    connection = self._getConnection(connection)
    if not isinstance(fileID, (list, tuple)):
      fileID = [fileID]
    req = "UPDATE FC_Files SET %s='%s', ModificationDate=UTC_TIMESTAMP() WHERE FileID IN (%s)" % (
        paramName,
        paramValue,
        intListToString(fileID))
    return self.db._update(req, connection)

  ######################################################
  #
  # _getFileReplicas related methods
  #

  def _getFileReplicas(self, fileIDs, fields=['PFN'], connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_ERROR("No such file or directory")
    req = "SELECT FileID,SEID,Status,%s FROM FC_Replicas WHERE FileID IN (%s);" % (
        intListToString(fields), intListToString(fileIDs))
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    replicas = {}
    for fTuple in res['Value']:
      fileID = fTuple[0]
      if fileID not in replicas:
        replicas[fileID] = {}
      seID = fTuple[1]
      res = self.db.seManager.getSEName(seID)
      if not res['OK']:
        continue
      seName = res['Value']
      statusID = fTuple[2]
      res = self._getIntStatus(statusID, connection=connection)
      if not res['OK']:
        continue
      status = res['Value']
      replicas[fileID][seName] = {'Status': status}
      replicas[fileID][seName].update(dict(zip(fields, fTuple[3:])))
    for fileID in fileIDs:
      if fileID not in replicas:
        replicas[fileID] = {}
    return S_OK(replicas)
