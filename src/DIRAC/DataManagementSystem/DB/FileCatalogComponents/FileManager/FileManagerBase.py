""" FileManagerBase is a base class for all the specific File Managers
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# pylint: disable=protected-access

import six
import os
import stat

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.Core.Utilities.Pfn import pfnunparse


class FileManagerBase(object):
  """ Base class for all the specific File Managers
  """

  def __init__(self, database=None):
    self.db = database
    self.statusDict = {}

  def _getConnection(self, connection):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection", res['Message'])
    return connection

  def setDatabase(self, database):
    self.db = database

  def getFileCounters(self, connection=False):
    """ Get a number of counters to verify the sanity of the Files in the catalog
    """
    connection = self._getConnection(connection)

    resultDict = {}
    req = "SELECT COUNT(*) FROM FC_Files;"
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    resultDict['Files'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_Files WHERE FileID NOT IN ( SELECT FileID FROM FC_Replicas )"
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    resultDict['Files w/o Replicas'] = res['Value'][0][0]

    req = "SELECT COUNT(RepID) FROM FC_Replicas WHERE FileID NOT IN ( SELECT FileID FROM FC_Files )"
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    resultDict['Replicas w/o Files'] = res['Value'][0][0]

    treeTable = self.db.dtree.getTreeTable()
    req = "SELECT COUNT(FileID) FROM FC_Files WHERE DirID NOT IN ( SELECT DirID FROM %s)" % treeTable
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    resultDict['Orphan Files'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_Files WHERE FileID NOT IN ( SELECT FileID FROM FC_FileInfo)"
    res = self.db._query(req, connection)
    if not res['OK']:
      resultDict['Files w/o FileInfo'] = 0
    else:
      resultDict['Files w/o FileInfo'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_FileInfo WHERE FileID NOT IN ( SELECT FileID FROM FC_Files)"
    res = self.db._query(req, connection)
    if not res['OK']:
      resultDict['FileInfo w/o Files'] = 0
    else:
      resultDict['FileInfo w/o Files'] = res['Value'][0][0]

    return S_OK(resultDict)

  def getReplicaCounters(self, connection=False):
    """ Get a number of counters to verify the sanity of the Replicas in the catalog
    """

    connection = self._getConnection(connection)
    req = "SELECT COUNT(*) FROM FC_Replicas;"
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    return S_OK({'Replicas': res['Value'][0][0]})

  ######################################################
  #
  # File write methods
  #

  def _insertFiles(self, lfns, uid, gid, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _deleteFiles(self, toPurge, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _insertReplicas(self, lfns, master=False, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _findFiles(self, lfns, metadata=["FileID"], allStatus=False, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _getFileReplicas(self, fileIDs, fields_input=['PFN'], allStatus=False, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _getFileIDFromGUID(self, guid, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def getLFNForGUID(self, guids, connection=False):
    """Returns the LFN matching a given GUID
    """
    return S_ERROR("To be implemented on derived class")

  def _setFileParameter(self, fileID, paramName, paramValue, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _deleteReplicas(self, lfns, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _setReplicaStatus(self, fileID, se, status, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _setReplicaHost(self, fileID, se, newSE, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _getDirectoryFiles(self, dirID, fileNames, metadata, allStatus=False, connection=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _getDirectoryFileIDs(self, dirID, requestString=False):
    """To be implemented on derived class
    """
    return S_ERROR("To be implemented on derived class")

  def _findFileIDs(self, lfns, connection=False):
    """ To be implemented on derived class
    Should return following the successful/failed convention
    Successful is a dictionary with keys the lfn, and values the FileID"""

    return S_ERROR("To be implemented on derived class")

  def _getDirectoryReplicas(self, dirID, allStatus=False, connection=False):
    """ To be implemented on derived class

    Should return with only one value, being a list of all the replicas (FileName,FileID,SEID,PFN)
    """

    return S_ERROR("To be implemented on derived class")

  def countFilesInDir(self, dirId):
    """ Count how many files there is in a given Directory

        :param int dirID: directory id

        :returns: S_OK(value) or S_ERROR
    """
    return S_ERROR("To be implemented on derived class")

  def _getFileLFNs(self, fileIDs):
    """ Get the file LFNs for a given list of file IDs
    """
    stringIDs = intListToString(fileIDs)
    treeTable = self.db.dtree.getTreeTable()

    req = "SELECT F.FileID, CONCAT(D.DirName,'/',F.FileName) from FC_Files as F,\
        %s as D WHERE F.FileID IN ( %s ) AND F.DirID=D.DirID" % (
        treeTable, stringIDs)
    result = self.db._query(req)
    if not result['OK']:
      return result

    fileNameDict = {}
    for row in result['Value']:
      fileNameDict[row[0]] = row[1]

    failed = {}
    successful = fileNameDict
    if len(fileNameDict) != len(fileIDs):
      for id_ in fileIDs:
        if id_ not in fileNameDict:
          failed[id_] = "File ID not found"

    return S_OK({'Successful': successful, 'Failed': failed})

  def addFile(self, lfns, credDict, connection=False):
    """ Add files to the catalog

        :param dict lfns: dict{ lfn : info}. 'info' is a dict containing PFN, SE, Size and Checksum
                      the SE parameter can be a list if we have several replicas to register


     """
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, info in list(lfns.items()):
      res = self._checkInfo(info, ['PFN', 'SE', 'Size', 'Checksum'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._addFiles(lfns, credDict, connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful': successful, 'Failed': failed})

  def _addFiles(self, lfns, credDict, connection=False):
    """ Main file adding method
    """
    connection = self._getConnection(connection)
    successful = {}
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid, gid = result['Value']

    # prepare lfns with master replicas - the first in the list or a unique replica
    masterLfns = {}
    extraLfns = {}
    for lfn in lfns:
      masterLfns[lfn] = dict(lfns[lfn])
      if isinstance(lfns[lfn].get('SE'), list):
        masterLfns[lfn]['SE'] = lfns[lfn]['SE'][0]
        if len(lfns[lfn]['SE']) > 1:
          extraLfns[lfn] = dict(lfns[lfn])
          extraLfns[lfn]['SE'] = lfns[lfn]['SE'][1:]

    # Check whether the supplied files have been registered already
    res = self._getExistingMetadata(list(masterLfns), connection=connection)
    if not res['OK']:
      return res
    existingMetadata, failed = res['Value']
    if existingMetadata:
      success, fail = self._checkExistingMetadata(existingMetadata, masterLfns)
      successful.update(success)
      failed.update(fail)
      for lfn in list(success) + list(fail):
        masterLfns.pop(lfn)

    # If GUIDs are supposed to be unique check their pre-existance
    if self.db.uniqueGUID:
      fail = self._checkUniqueGUID(masterLfns, connection=connection)
      failed.update(fail)
      for lfn in fail:
        masterLfns.pop(lfn)

    # If we have files left to register
    if masterLfns:
      # Create the directories for the supplied files and store their IDs
      directories = self._getFileDirectories(list(masterLfns))
      for directory, fileNames in directories.items():
        res = self.db.dtree.makeDirectories(directory, credDict)
        if not res['OK']:
          for fileName in fileNames:
            lfn = os.path.join(directory, fileName)
            failed[lfn] = res['Message']
            masterLfns.pop(lfn)
          continue
        for fileName in fileNames:
          if not fileName:
            failed[directory] = "Is no a valid file"
            masterLfns.pop(directory)
            continue

          lfn = "%s/%s" % (directory, fileName)
          lfn = lfn.replace('//', '/')

          # This condition should never be true, we would not be here otherwise...
          if not res['OK']:
            failed[lfn] = "Failed to create directory for file"
            masterLfns.pop(lfn)
          else:
            masterLfns[lfn]['DirID'] = res['Value']

    # If we still have files left to register
    if masterLfns:
      res = self._insertFiles(masterLfns, uid, gid, connection=connection)
      if not res['OK']:
        for lfn in list(masterLfns):  # pylint: disable=consider-iterating-dictionary
          failed[lfn] = res['Message']
          masterLfns.pop(lfn)
      else:
        for lfn, error in res['Value']['Failed'].items():
          failed[lfn] = error
          masterLfns.pop(lfn)
        masterLfns = res['Value']['Successful']

    # Add the ancestors
    if masterLfns:
      res = self._populateFileAncestors(masterLfns, connection=connection)
      toPurge = []
      if not res['OK']:
        for lfn in masterLfns.keys():
          failed[lfn] = "Failed while registering ancestors"
          toPurge.append(masterLfns[lfn]['FileID'])
      else:
        failed.update(res['Value']['Failed'])
        for lfn, error in res['Value']['Failed'].items():
          toPurge.append(masterLfns[lfn]['FileID'])
      if toPurge:
        self._deleteFiles(toPurge, connection=connection)

    # Register the replicas
    newlyRegistered = {}
    if masterLfns:
      res = self._insertReplicas(masterLfns, master=True, connection=connection)
      toPurge = []
      if not res['OK']:
        for lfn in masterLfns.keys():
          failed[lfn] = "Failed while registering replica"
          toPurge.append(masterLfns[lfn]['FileID'])
      else:
        newlyRegistered = res['Value']['Successful']
        successful.update(newlyRegistered)
        failed.update(res['Value']['Failed'])
        for lfn, error in res['Value']['Failed'].items():
          toPurge.append(masterLfns[lfn]['FileID'])
      if toPurge:
        self._deleteFiles(toPurge, connection=connection)

    # Add extra replicas for successfully registered LFNs
    for lfn in list(extraLfns):
      if lfn not in successful:
        extraLfns.pop(lfn)

    if extraLfns:
      res = self._findFiles(list(extraLfns), ['FileID', 'DirID'], connection=connection)
      if not res['OK']:
        for lfn in list(lfns):
          failed[lfn] = 'Failed while registering extra replicas'
          successful.pop(lfn)
          extraLfns.pop(lfn)
      else:
        failed.update(res['Value']['Failed'])
        for lfn in res['Value']['Failed']:
          successful.pop(lfn)
          extraLfns.pop(lfn)
        for lfn, fileDict in res['Value']['Successful'].items():
          extraLfns[lfn]['FileID'] = fileDict['FileID']
          extraLfns[lfn]['DirID'] = fileDict['DirID']

      if extraLfns:
        res = self._insertReplicas(extraLfns, master=False, connection=connection)
        if not res['OK']:
          for lfn in extraLfns:  # pylint: disable=consider-iterating-dictionary
            failed[lfn] = "Failed while registering extra replicas"
            successful.pop(lfn)
        else:
          newlyRegistered = res['Value']['Successful']
          successful.update(newlyRegistered)
          failed.update(res['Value']['Failed'])

    return S_OK({'Successful': successful, 'Failed': failed})

  def _updateDirectoryUsage(self, directorySEDict, change, connection=False):
    connection = self._getConnection(connection)
    for directoryID in directorySEDict.keys():
      result = self.db.dtree.getPathIDsByID(directoryID)
      if not result['OK']:
        return result
      parentIDs = result['Value']
      dirDict = directorySEDict[directoryID]
      for seID in dirDict.keys():
        seDict = dirDict[seID]
        files = seDict['Files']
        size = seDict['Size']
        insertTuples = []
        for dirID in parentIDs:
          insertTuples.append('(%d,%d,%d,%d,UTC_TIMESTAMP())' % (dirID, seID, size, files))

        req = "INSERT INTO FC_DirectoryUsage (DirID,SEID,SESize,SEFiles,LastUpdate) "
        req += "VALUES %s" % ','.join(insertTuples)
        req += " ON DUPLICATE KEY UPDATE SESize=SESize%s%d, SEFiles=SEFiles%s%d, LastUpdate=UTC_TIMESTAMP() " \
            % (change, size, change, files)
        res = self.db._update(req)
        if not res['OK']:
          gLogger.warn("Failed to update FC_DirectoryUsage", res['Message'])
    return S_OK()

  def _populateFileAncestors(self, lfns, connection=False):
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, lfnDict in lfns.items():
      originalFileID = lfnDict['FileID']
      originalDepth = lfnDict.get('AncestorDepth', 1)
      ancestors = lfnDict.get('Ancestors', [])
      if isinstance(ancestors, six.string_types):
        ancestors = [ancestors]
      if lfn in ancestors:
        ancestors.remove(lfn)
      if not ancestors:
        successful[lfn] = True
        continue
      res = self._findFiles(ancestors, connection=connection)
      if res['Value']['Failed']:
        failed[lfn] = "Failed to resolve ancestor files"
        continue
      ancestorIDs = res['Value']['Successful']
      fileIDLFNs = {}
      toInsert = {}
      for ancestor in ancestorIDs.keys():
        fileIDLFNs[ancestorIDs[ancestor]['FileID']] = ancestor
        toInsert[ancestorIDs[ancestor]['FileID']] = originalDepth
      res = self._getFileAncestors(list(fileIDLFNs))
      if not res['OK']:
        failed[lfn] = "Failed to obtain all ancestors"
        continue
      fileIDAncestorDict = res['Value']
      for fileIDDict in fileIDAncestorDict.values():
        for ancestorID, relativeDepth in fileIDDict.items():
          toInsert[ancestorID] = relativeDepth + originalDepth
      res = self._insertFileAncestors(originalFileID, toInsert, connection=connection)
      if not res['OK']:
        if "Duplicate" in res['Message']:
          failed[lfn] = "Failed to insert ancestor files: duplicate entry"
        else:
          failed[lfn] = "Failed to insert ancestor files"
      else:
        successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def _insertFileAncestors(self, fileID, ancestorDict, connection=False):
    connection = self._getConnection(connection)
    ancestorTuples = []
    for ancestorID, depth in ancestorDict.items():
      ancestorTuples.append("(%d,%d,%d)" % (fileID, ancestorID, depth))
    if not ancestorTuples:
      return S_OK()
    req = "INSERT INTO FC_FileAncestors (FileID, AncestorID, AncestorDepth) VALUES %s" \
        % intListToString(ancestorTuples)
    return self.db._update(req, connection)

  def _getFileAncestors(self, fileIDs, depths=[], connection=False):
    connection = self._getConnection(connection)
    req = "SELECT FileID, AncestorID, AncestorDepth FROM FC_FileAncestors WHERE FileID IN (%s)" \
        % intListToString(fileIDs)
    if depths:
      req = "%s AND AncestorDepth IN (%s);" % (req, intListToString(depths))
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    fileIDAncestors = {}
    for fileID, ancestorID, depth in res['Value']:
      if fileID not in fileIDAncestors:
        fileIDAncestors[fileID] = {}
      fileIDAncestors[fileID][ancestorID] = depth
    return S_OK(fileIDAncestors)

  def _getFileDescendents(self, fileIDs, depths, connection=False):
    connection = self._getConnection(connection)
    req = "SELECT AncestorID, FileID, AncestorDepth FROM FC_FileAncestors WHERE AncestorID IN (%s)" \
        % intListToString(fileIDs)
    if depths:
      req = "%s AND AncestorDepth IN (%s);" % (req, intListToString(depths))
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    fileIDAncestors = {}
    for ancestorID, fileID, depth in res['Value']:
      if ancestorID not in fileIDAncestors:
        fileIDAncestors[ancestorID] = {}
      fileIDAncestors[ancestorID][fileID] = depth
    return S_OK(fileIDAncestors)

  def addFileAncestors(self, lfns, connection=False):
    """ Add file ancestors to the catalog """
    connection = self._getConnection(connection)
    failed = {}
    successful = {}
    result = self._findFiles(list(lfns), connection=connection)
    if not result['OK']:
      return result
    if result['Value']['Failed']:
      failed.update(result['Value']['Failed'])
      for lfn in result['Value']['Failed']:
        lfns.pop(lfn)
    if not lfns:
      return S_OK({'Successful': successful, 'Failed': failed})

    for lfn in result['Value']['Successful']:
      lfns[lfn]['FileID'] = result['Value']['Successful'][lfn]['FileID']

    result = self._populateFileAncestors(lfns, connection)
    if not result['OK']:
      return result
    failed.update(result['Value']['Failed'])
    successful = result['Value']['Successful']
    return S_OK({'Successful': successful, 'Failed': failed})

  def _getFileRelatives(self, lfns, depths, relation, connection=False):
    connection = self._getConnection(connection)
    failed = {}
    successful = {}
    result = self._findFiles(list(lfns), connection=connection)
    if not result['OK']:
      return result
    if result['Value']['Failed']:
      failed.update(result['Value']['Failed'])
      for lfn in result['Value']['Failed']:
        lfns.pop(lfn)
    if not lfns:
      return S_OK({'Successful': successful, 'Failed': failed})

    inputIDDict = {}
    for lfn in result['Value']['Successful']:
      inputIDDict[result['Value']['Successful'][lfn]['FileID']] = lfn

    inputIDs = list(inputIDDict)
    if relation == 'ancestor':
      result = self._getFileAncestors(inputIDs, depths, connection)
    else:
      result = self._getFileDescendents(inputIDs, depths, connection)

    if not result['OK']:
      return result

    failed = {}
    successful = {}
    relDict = result['Value']
    for id_ in inputIDs:
      if id_ in relDict:
        result = self._getFileLFNs(list(relDict[id_]))
        if not result['OK']:
          failed[inputIDDict[id]] = "Failed to find %s" % relation
        else:
          if result['Value']['Successful']:
            resDict = {}
            for aID in result['Value']['Successful']:
              resDict[result['Value']['Successful'][aID]] = relDict[id_][aID]
            successful[inputIDDict[id_]] = resDict
          for aID in result['Value']['Failed']:
            failed[inputIDDict[id_]] = "Failed to get the ancestor LFN"
      else:
        successful[inputIDDict[id_]] = {}

    return S_OK({'Successful': successful, 'Failed': failed})

  def getFileAncestors(self, lfns, depths, connection=False):
    return self._getFileRelatives(lfns, depths, 'ancestor', connection)

  def getFileDescendents(self, lfns, depths, connection=False):
    return self._getFileRelatives(lfns, depths, 'descendent', connection)

  def _getExistingMetadata(self, lfns, connection=False):
    connection = self._getConnection(connection)
    # Check whether the files already exist before adding
    res = self._findFiles(lfns, ['FileID', 'Size', 'Checksum', 'GUID'], connection=connection)
    if not res['OK']:
      return res
    successful = res['Value']['Successful']
    failed = res['Value']['Failed']
    for lfn, error in list(failed.items()):
      if error == 'No such file or directory':
        failed.pop(lfn)
    return S_OK((successful, failed))

  def _checkExistingMetadata(self, existingLfns, lfns):
    failed = {}
    successful = {}
    fileIDLFNs = {}
    for lfn, fileDict in existingLfns.items():
      fileIDLFNs[fileDict['FileID']] = lfn
    # For those that exist get the replicas to determine whether they are already registered
    res = self._getFileReplicas(list(fileIDLFNs))
    if not res['OK']:
      for lfn in fileIDLFNs.values():
        failed[lfn] = 'Failed checking pre-existing replicas'
    else:
      replicaDict = res['Value']
      for fileID, lfn in fileIDLFNs.items():
        fileMetadata = existingLfns[lfn]
        existingGuid = fileMetadata['GUID']
        existingSize = fileMetadata['Size']
        existingChecksum = fileMetadata['Checksum']
        newGuid = lfns[lfn]['GUID']
        newSize = lfns[lfn]['Size']
        newChecksum = lfns[lfn]['Checksum']
        # Ensure that the key file metadata is the same
        if (existingGuid != newGuid) or \
           (existingSize != newSize) or \
           (existingChecksum != newChecksum):
          failed[lfn] = "File already registered with alternative metadata"
        # If the DB does not have replicas for this file return an error
        elif fileID not in replicaDict or not replicaDict[fileID]:
          failed[lfn] = "File already registered with no replicas"
        # If the supplied SE is not in the existing replicas return an error
        elif not lfns[lfn]['SE'] in replicaDict[fileID].keys():
          failed[lfn] = "File already registered with alternative replicas"
        # If we get here the file being registered already exists exactly in the DB
        else:
          successful[lfn] = True
    return successful, failed

  def _checkUniqueGUID(self, lfns, connection=False):
    connection = self._getConnection(connection)
    guidLFNs = {}
    failed = {}
    for lfn, fileDict in lfns.items():
      guidLFNs[fileDict['GUID']] = lfn
    res = self._getFileIDFromGUID(list(guidLFNs), connection=connection)
    if not res['OK']:
      return dict.fromkeys(lfns, res['Message'])
    for guid, fileID in res['Value'].items():
      # resolve this to LFN
      failed[guidLFNs[guid]] = "GUID already registered for another file %s" % fileID
    return failed

  def removeFile(self, lfns, connection=False):
    connection = self._getConnection(connection)
    """ Remove file from the catalog """
    successful = {}
    failed = {}
    res = self._findFiles(lfns, ['DirID', 'FileID', 'Size'], connection=connection)
    if not res['OK']:
      return res
    for lfn, error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = True
      else:
        failed[lfn] = error
    fileIDLfns = {}
    lfns = res['Value']['Successful']
    for lfn, lfnDict in lfns.items():
      fileIDLfns[lfnDict['FileID']] = lfn

    res = self._computeStorageUsageOnRemoveFile(lfns, connection=connection)
    if not res['OK']:
      return res
    directorySESizeDict = res['Value']

    # Now do removal
    res = self._deleteFiles(list(fileIDLfns), connection=connection)
    if not res['OK']:
      for lfn in fileIDLfns.values():
        failed[lfn] = res['Message']
    else:
      # Update the directory usage
      self._updateDirectoryUsage(directorySESizeDict, '-', connection=connection)
      for lfn in fileIDLfns.values():
        successful[lfn] = True
    return S_OK({"Successful": successful, "Failed": failed})

  def _computeStorageUsageOnRemoveFile(self, lfns, connection=False):
    # Resolve the replicas to calculate reduction in storage usage
    fileIDLfns = {}
    for lfn, lfnDict in lfns.items():
      fileIDLfns[lfnDict['FileID']] = lfn
    res = self._getFileReplicas(list(fileIDLfns), connection=connection)
    if not res['OK']:
      return res
    directorySESizeDict = {}
    for fileID, seDict in res['Value'].items():
      dirID = lfns[fileIDLfns[fileID]]['DirID']
      size = lfns[fileIDLfns[fileID]]['Size']
      directorySESizeDict.setdefault(dirID, {})
      directorySESizeDict[dirID].setdefault(0, {'Files': 0, 'Size': 0})
      directorySESizeDict[dirID][0]['Size'] += size
      directorySESizeDict[dirID][0]['Files'] += 1
      for seName in seDict.keys():
        res = self.db.seManager.findSE(seName)
        if not res['OK']:
          return res
        seID = res['Value']
        size = lfns[fileIDLfns[fileID]]['Size']
        directorySESizeDict[dirID].setdefault(seID, {'Files': 0, 'Size': 0})
        directorySESizeDict[dirID][seID]['Size'] += size
        directorySESizeDict[dirID][seID]['Files'] += 1

    return S_OK(directorySESizeDict)

  def setFileStatus(self, lfns, connection=False):
    """ Get set the group for the supplied files """
    connection = self._getConnection(connection)
    res = self._findFiles(lfns, ['FileID', 'UID'], connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful']:
      status = lfns[lfn]
      if isinstance(status, six.string_types):
        if status not in self.db.validFileStatus:
          failed[lfn] = 'Invalid file status %s' % status
          continue
        result = self._getStatusInt(status, connection=connection)
        if not result['OK']:
          failed[lfn] = res['Message']
          continue
        status = result['Value']
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setFileParameter(fileID, "Status", status, connection=connection)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  ######################################################
  #
  # Replica write methods
  #

  def addReplica(self, lfns, connection=False):
    """ Add replica to the catalog """
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, info in list(lfns.items()):
      res = self._checkInfo(info, ['PFN', 'SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._addReplicas(lfns, connection=connection)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
    else:
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful': successful, 'Failed': failed})

  def _addReplicas(self, lfns, connection=False):

    connection = self._getConnection(connection)
    successful = {}
    res = self._findFiles(list(lfns), ['DirID', 'FileID', 'Size'], connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    for lfn in failed:
      lfns.pop(lfn)
    lfnFileIDDict = res['Value']['Successful']
    for lfn, fileDict in lfnFileIDDict.items():
      lfns[lfn].update(fileDict)
    res = self._insertReplicas(lfns, connection=connection)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
    else:
      successful = res['Value']['Successful']
      failed.update(res['Value']['Failed'])
    return S_OK({'Successful': successful, 'Failed': failed})

  def removeReplica(self, lfns, connection=False):
    """ Remove replica from catalog """
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, info in list(lfns.items()):
      res = self._checkInfo(info, ['SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._deleteReplicas(lfns, connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful': successful, 'Failed': failed})

  def setReplicaStatus(self, lfns, connection=False):
    """ Set replica status in the catalog """
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo(info, ['SE', 'Status'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      status = info['Status']
      se = info['SE']
      res = self._findFiles([lfn], ['FileID'], connection=connection)
      if lfn not in res['Value']['Successful']:
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaStatus(fileID, se, status, connection=connection)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful': successful, 'Failed': failed})

  def setReplicaHost(self, lfns, connection=False):
    """ Set replica host in the catalog """
    connection = self._getConnection(connection)
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo(info, ['SE', 'NewSE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      newSE = info['NewSE']
      se = info['SE']
      res = self._findFiles([lfn], ['FileID'], connection=connection)
      if lfn not in res['Value']['Successful']:
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaHost(fileID, se, newSE, connection=connection)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful': successful, 'Failed': failed})

  ######################################################
  #
  # File read methods
  #

  def exists(self, lfns, connection=False):
    """ Determine whether a file exists in the catalog """
    connection = self._getConnection(connection)
    res = self._findFiles(lfns, allStatus=True, connection=connection)
    if not res['OK']:
      return res
    successful = res['Value']['Successful']
    origFailed = res['Value']['Failed']
    for lfn in successful:
      successful[lfn] = lfn
    failed = {}

    if self.db.uniqueGUID:
      guidList = []
      val = None
      # Try to identify if the GUID is given
      # We consider only 2 options :
      # either {lfn : guid}
      # or P lfn : {PFN : .., GUID : ..} }
      if isinstance(lfns, dict):
        val = list(lfns.values())

      # We have values, take the first to identify the type
      if val:
        val = val[0]

      if isinstance(val, dict) and 'GUID' in val:
        # We are in the case {lfn : {PFN:.., GUID:..}}
        guidList = [lfns[lfn]['GUID'] for lfn in lfns]
      elif isinstance(val, six.string_types):
        # We hope that it is the GUID which is given
        guidList = list(lfns.values())

      if guidList:
        # A dict { guid: lfn to which it is supposed to be associated }
        guidToGivenLfn = dict(zip(guidList, lfns))
        res = self.getLFNForGUID(guidList, connection)
        if not res['OK']:
          return res
        guidLfns = res['Value']['Successful']
        for guid, realLfn in guidLfns.items():
          successful[guidToGivenLfn[guid]] = realLfn

    for lfn, error in origFailed.items():
      # It could be in successful because the guid exists with another lfn
      if lfn in successful:
        continue
      if error == 'No such file or directory':
        successful[lfn] = False
      else:
        failed[lfn] = error
    return S_OK({"Successful": successful, "Failed": failed})

  def isFile(self, lfns, connection=False):
    """ Determine whether a path is a file in the catalog """
    connection = self._getConnection(connection)
    # TO DO, should check whether it is a directory if it fails
    return self.exists(lfns, connection=connection)

  def getFileSize(self, lfns, connection=False):
    """ Get file size from the catalog """
    connection = self._getConnection(connection)
    # TO DO, should check whether it is a directory if it fails
    res = self._findFiles(lfns, ['Size'], connection=connection)
    if not res['OK']:
      return res

    totalSize = 0
    for lfn in res['Value']['Successful']:
      size = res['Value']['Successful'][lfn]['Size']
      res['Value']['Successful'][lfn] = size
      totalSize += size

    res['TotalSize'] = totalSize
    return res

  def getFileMetadata(self, lfns, connection=False):
    """ Get file metadata from the catalog """
    connection = self._getConnection(connection)
    # TO DO, should check whether it is a directory if it fails
    return self._findFiles(lfns, ['Size', 'Checksum',
                                  'ChecksumType', 'UID',
                                  'GID', 'GUID',
                                  'CreationDate', 'ModificationDate',
                                  'Mode', 'Status'], connection=connection)

  def getPathPermissions(self, paths, credDict, connection=False):
    """ Get the permissions for the supplied paths """
    connection = self._getConnection(connection)
    res = self.db.ugManager.getUserAndGroupID(credDict)
    if not res['OK']:
      return res
    uid, gid = res['Value']
    res = self._findFiles(paths, metadata=['Mode', 'UID', 'GID'], connection=connection)
    if not res['OK']:
      return res
    successful = {}
    for dirName, dirDict in res['Value']['Successful'].items():
      mode = dirDict['Mode']
      p_uid = dirDict['UID']
      p_gid = dirDict['GID']
      successful[dirName] = {}
      if p_uid == uid:
        successful[dirName]['Read'] = mode & stat.S_IRUSR
        successful[dirName]['Write'] = mode & stat.S_IWUSR
        successful[dirName]['Execute'] = mode & stat.S_IXUSR
      elif p_gid == gid:
        successful[dirName]['Read'] = mode & stat.S_IRGRP
        successful[dirName]['Write'] = mode & stat.S_IWGRP
        successful[dirName]['Execute'] = mode & stat.S_IXGRP
      else:
        successful[dirName]['Read'] = mode & stat.S_IROTH
        successful[dirName]['Write'] = mode & stat.S_IWOTH
        successful[dirName]['Execute'] = mode & stat.S_IXOTH
    return S_OK({'Successful': successful, 'Failed': res['Value']['Failed']})

  ######################################################
  #
  # Replica read methods
  #

  def __getReplicasForIDs(self, fileIDLfnDict, allStatus, connection=False):
    """ Get replicas for files with already resolved IDs
    """
    replicas = {}
    if fileIDLfnDict:
      fields = []
      if not self.db.lfnPfnConvention or self.db.lfnPfnConvention == "Weak":
        fields = ['PFN']
      res = self._getFileReplicas(list(fileIDLfnDict), fields_input=fields,
                                  allStatus=allStatus, connection=connection)
      if not res['OK']:
        return res
      for fileID, seDict in res['Value'].items():
        lfn = fileIDLfnDict[fileID]
        replicas[lfn] = {}
        for se, repDict in seDict.items():
          pfn = repDict.get('PFN', '')
          replicas[lfn][se] = pfn

    result = S_OK(replicas)
    return result

  def getReplicas(self, lfns, allStatus, connection=False):
    """ Get file replicas from the catalog """
    connection = self._getConnection(connection)

    # Get FileID <-> LFN correspondence first
    res = self._findFileIDs(lfns, connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn, fileID in res['Value']['Successful'].items():
      fileIDLFNs[fileID] = lfn

    result = self.__getReplicasForIDs(fileIDLFNs, allStatus, connection)
    if not result['OK']:
      return result
    replicas = result['Value']

    return S_OK({"Successful": replicas, 'Failed': failed})

  def getReplicasByMetadata(self, metaDict, path, allStatus, credDict, connection=False):
    """ Get file replicas for files corresponding to the given metadata """
    connection = self._getConnection(connection)

    # Get FileID <-> LFN correspondence first
    failed = {}
    result = self.db.fmeta.findFilesByMetadata(metaDict, path, credDict)
    if not result['OK']:
      return result
    idLfnDict = result['Value']

    result = self.__getReplicasForIDs(idLfnDict, allStatus, connection)
    if not result['OK']:
      return result
    replicas = result['Value']

    return S_OK({"Successful": replicas, 'Failed': failed})

  def getReplicaStatus(self, lfns, connection=False):
    """ Get replica status from the catalog """
    connection = self._getConnection(connection)
    res = self._findFiles(lfns, connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn, fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    successful = {}
    if fileIDLFNs:
      res = self._getFileReplicas(list(fileIDLFNs), allStatus=True, connection=connection)
      if not res['OK']:
        return res
      for fileID, seDict in res['Value'].items():
        lfn = fileIDLFNs[fileID]
        requestedSE = lfns[lfn]
        if not requestedSE:
          failed[lfn] = "Replica info not supplied"
        elif requestedSE not in seDict:
          failed[lfn] = "No replica at supplied site"
        else:
          successful[lfn] = seDict[requestedSE]['Status']
    return S_OK({'Successful': successful, 'Failed': failed})

  ######################################################
  #
  # General usage methods
  #

  def _getStatusInt(self, status, connection=False):
    connection = self._getConnection(connection)
    req = "SELECT StatusID FROM FC_Statuses WHERE Status = '%s';" % status
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK(res['Value'][0][0])
    req = "INSERT INTO FC_Statuses (Status) VALUES ('%s');" % status
    res = self.db._update(req, connection)
    if not res['OK']:
      return res
    return S_OK(res['lastRowId'])

  def _getIntStatus(self, statusID, connection=False):
    if statusID in self.statusDict:
      return S_OK(self.statusDict[statusID])
    connection = self._getConnection(connection)
    req = "SELECT StatusID,Status FROM FC_Statuses"
    res = self.db._query(req, connection)
    if not res['OK']:
      return res
    if res['Value']:
      for row in res['Value']:
        self.statusDict[int(row[0])] = row[1]
    if statusID in self.statusDict:
      return S_OK(self.statusDict[statusID])
    return S_OK('Unknown')

  def getFileIDsInDirectory(self, dirID, requestString=False):
    """ Get a list of IDs for all the files stored in given directories or their
        subdirectories

    :param dirID: single directory ID or a list of directory IDs
    :type dirID: int or python:list[int]
    :param bool requestString: if True return result as a SQL SELECT string
    :return: list of file IDs or SELECT string
    """
    return self._getDirectoryFileIDs(dirID, requestString=requestString)

  def getFilesInDirectory(self, dirID, verbose=False, connection=False):
    connection = self._getConnection(connection)
    files = {}
    res = self._getDirectoryFiles(dirID, [], ['FileID', 'Size', 'GUID',
                                              'Checksum', 'ChecksumType',
                                              'Type', 'UID',
                                              'GID', 'CreationDate',
                                              'ModificationDate', 'Mode',
                                              'Status'], connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(files)
    fileIDNames = {}
    for fileName, fileDict in res['Value'].items():
      try:
        files[fileName] = {}
        files[fileName]['MetaData'] = fileDict
        fileIDNames[fileDict['FileID']] = fileName
      except KeyError:
        # If we return S_ERROR here, it gets treated as an empty directory in most cases
        # and the user isn't actually warned
        raise Exception("File entry for '%s' is corrupt (DirID %s), please contact the catalog administrator"
                        % (fileName, dirID))

    if verbose:
      result = self._getFileReplicas(list(fileIDNames), connection=connection)
      if not result['OK']:
        return result
      for fileID, seDict in result['Value'].items():
        fileName = fileIDNames[fileID]
        files[fileName]['Replicas'] = seDict

    return S_OK(files)

  def getDirectoryReplicas(self, dirID, path, allStatus=False, connection=False):
    """ Get the replicas for all the Files in the given Directory

        :param int dirID: ID of the directory
        :param unused path: useless
        :param bool allStatus: whether all replicas and file status are considered
                            If False, take the visibleFileStatus and visibleReplicaStatus values from the configuration
    """
    connection = self._getConnection(connection)
    result = self._getDirectoryReplicas(dirID, allStatus, connection)
    if not result['OK']:
      return result

    resultDict = {}
    seDict = {}
    for fileName, fileID, seID, pfn in result['Value']:
      resultDict.setdefault(fileName, {})
      if seID not in seDict:
        res = self.db.seManager.getSEName(seID)
        if not res['OK']:
          seDict[seID] = 'Unknown'
        else:
          seDict[seID] = res['Value']
      se = seDict[seID]
      resultDict[fileName][se] = pfn

    return S_OK(resultDict)

  def _getFileDirectories(self, lfns):
    """ For a list of lfn, returns a dictionary with key the directory, and value
        the files in that directory. It does not make any query, just splits the names

        :param lfns: list of lfns
        :type lfns: python:list
    """
    dirDict = {}
    for lfn in lfns:
      lfnDir = os.path.dirname(lfn)
      lfnFile = os.path.basename(lfn)
      dirDict.setdefault(lfnDir, [])
      dirDict[lfnDir].append(lfnFile)
    return dirDict

  def _checkInfo(self, info, requiredKeys):
    if not info:
      return S_ERROR("Missing parameters")
    for key in requiredKeys:
      if key not in info:
        return S_ERROR("Missing '%s' parameter" % key)
    return S_OK()

  # def _checkLFNPFNConvention( self, lfn, pfn, se ):
  #   """ Check that the PFN corresponds to the LFN-PFN convention """
  #   if pfn == lfn:
  #     return S_OK()
  #   if ( len( pfn ) < len( lfn ) ) or ( pfn[-len( lfn ):] != lfn ) :
  #     return S_ERROR( 'PFN does not correspond to the LFN convention' )
  #  return S_OK()

  def changeFileGroup(self, lfns):
    """ Get set the group for the supplied files

        :param lfns: dictionary < lfn : group >
        :param int/str newGroup: optional new group/groupID the same for all the supplied lfns
     """
    res = self._findFiles(lfns, ['FileID', 'GID'])
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful']:
      group = lfns[lfn]
      if isinstance(group, six.string_types):
        groupRes = self.db.ugManager.findGroup(group)
        if not groupRes['OK']:
          return groupRes
        group = groupRes['Value']
      currentGroup = res['Value']['Successful'][lfn]['GID']
      if int(group) == int(currentGroup):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileParameter(fileID, "GID", group)
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def changeFileOwner(self, lfns):
    """ Set the owner for the supplied files

        :param lfns: dictionary < lfn : owner >
        :param int/str newOwner: optional new user/userID the same for all the supplied lfns
    """
    res = self._findFiles(lfns, ['FileID', 'UID'])
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful']:
      owner = lfns[lfn]
      if isinstance(owner, six.string_types):
        userRes = self.db.ugManager.findUser(owner)
        if not userRes['OK']:
          return userRes
        owner = userRes['Value']
      currentOwner = res['Value']['Successful'][lfn]['UID']
      if int(owner) == int(currentOwner):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileParameter(fileID, "UID", owner)
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def changeFileMode(self, lfns):
    """" Set the mode for the supplied files

        :param lfns: dictionary < lfn : mode >
        :param int newMode: optional new mode the same for all the supplied lfns
    """
    res = self._findFiles(lfns, ['FileID', 'Mode'])
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful']:
      mode = lfns[lfn]
      currentMode = res['Value']['Successful'][lfn]['Mode']
      if int(currentMode) == int(mode):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileParameter(fileID, "Mode", mode)
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful': successful, 'Failed': failed})

  def setFileOwner(self, path, owner):
    """ Set the file owner

        :param path: file path as a string or int or list of ints or select statement
        :type path: str, int or python:list[int]
        :param owner: new user as a string or int uid
        :type owner: str or int
    """

    result = self.db.ugManager.findUser(owner)
    if not result['OK']:
      return result

    uid = result['Value']

    return self._setFileParameter(path, 'UID', uid)

  def setFileGroup(self, path, gname):
    """ Set the file group

        :param path: file path as a string or int or list of ints or select statement
        :type path: str, int or python:list[int]
        :param gname: new group as a string or int gid
        :type gname: str or int
    """

    result = self.db.ugManager.findGroup(gname)
    if not result['OK']:
      return result

    gid = result['Value']

    return self._setFileParameter(path, 'GID', gid)

  def setFileMode(self, path, mode):
    """ Set the file mode

        :param path: file path as a string or int or list of ints or select statement
        :type path: str, int or python:list[int]
        :param int mode: new mode
    """
    return self._setFileParameter(path, 'Mode', mode)

  def getSEDump(self, seName):
    """
         Return all the files at a given SE, together with checksum and size

        :param seName: name of the StorageElement

        :returns: S_OK with list of tuples (lfn, checksum, size)
    """
    return S_ERROR("To be implemented on derived class")
