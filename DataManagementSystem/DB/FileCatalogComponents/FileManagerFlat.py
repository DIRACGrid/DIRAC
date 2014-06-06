########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                  import S_OK, S_ERROR
from DIRAC.Core.Utilities.List              import stringListToString, intListToString, sortList
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManagerBase import FileManagerBase

import os
# import time
from types import TupleType, ListType, StringTypes

class FileManagerFlat(FileManagerBase):
  
  _tables = {}
  _tables['FC_Files'] = { "Fields": { 
                                     "FileID": "INT AUTO_INCREMENT",
                                     "DirID": "INT NOT NULL",
                                     "Size": "BIGINT UNSIGNED NOT NULL",
                                     "UID": "SMALLINT UNSIGNED NOT NULL",
                                     "GID": "TINYINT UNSIGNED NOT NULL",
                                     "Status": "SMALLINT UNSIGNED NOT NULL",
                                     "FileName": "VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                     "GUID": "char(36) NOT NULL",
                                     "Checksum": "VARCHAR(32)",
                                     "CheckSumType": "ENUM('Adler32','MD5')",
                                     "Type": "ENUM('File','Link') NOT NULL DEFAULT 'File'",
                                     "CreationDate": "DATETIME",
                                     "ModificationDate": "DATETIME",
                                     "LastAccessDate": "DateTime",
                                     "Mode": "SMALLINT UNSIGNED NOT NULL DEFAULT 559"
                                    }, 
                          "PrimaryKey": "FileID",
                          "Indexes": {
                                       "DirID": ["DirID"],
                                       "UID_GID": ["UID","GID"],
                                       "Status": ["Status"],
                                       "FileName": ["FileName"],
                                       "Dir_File": ["DirID","FileName"],
                                       "GUID": ["GUID"]
                                     }  
                                   }
  _tables['FC_Replicas'] = { "Fields": { 
                                         "RepID": "INT AUTO_INCREMENT",
                                         "FileID": "INT NOT NULL",
                                         "SEID": "INTEGER NOT NULL",
                                         "Status": "SMALLINT UNSIGNED NOT NULL",
                                         "RepType": "ENUM ('Master','Replica') NOT NULL DEFAULT 'Master'",
                                         "CreationDate": "DATETIME",
                                         "ModificationDate": "DATETIME",
                                         "PFN": "VARCHAR(1024)"
                                        },
                             "PrimaryKey": "RepID",
                             "Indexes": {
                                          "FileID": ["FileID"],
                                          "SEID": ["SEID"],
                                          "Status": ["Status"] 
                                        },
                             "UniqueIndexes": { "File_SE": ["FileID","SEID"] }
                            } 
  
  ######################################################
  #
  # The all important _findFiles and _getDirectoryFiles methods
  #

  def _findFiles(self,lfns,metadata=['FileID'],connection=False):
    connection = self._getConnection(connection)
    """ Find file ID if it exists for the given list of LFNs """
    #startTime = time.time()
    dirDict = self._getFileDirectories(lfns)
    failed = {}
    directoryIDs = {}
    for dirPath in dirDict.keys():
      #startTime = time.time()
      res = self.db.dtree.findDir(dirPath)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in dirDict[dirPath]:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:
        directoryIDs[dirPath] = res['Value']
    successful = {}
    for dirPath in directoryIDs.keys():
      fileNames = dirDict[dirPath]
      #startTime = time.time()
      res = self._getDirectoryFiles(directoryIDs[dirPath],fileNames,metadata,connection=connection)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in fileNames:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:    
        for fileName,fileDict in res['Value'].items():
          successful["%s/%s" % (dirPath,fileName)] = fileDict
    return S_OK({"Successful":successful,"Failed":failed})

  def _getDirectoryFiles(self,dirID,fileNames,metadata,allStatus=False,connection=False):
    connection = self._getConnection(connection)
    # metadata can be any of ['FileID','Size','UID','GID','Checksum','ChecksumType','Type','CreationDate','ModificationDate','Mode','Status']
    req = "SELECT FileName,%s FROM FC_Files WHERE DirID=%d" % (intListToString(metadata),dirID)
    if not allStatus:
      statusIDs = []
      res = self.db.getStatusInt('AprioriGood',connection=connection)
      if res['OK']:
        statusIDs.append(res['Value'])
      if statusIDs:
        req = "%s AND Status IN (%s)" % (req,intListToString(statusIDs))
    if fileNames:
      req = "%s AND FileName IN (%s)" % (req,stringListToString(fileNames))
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    files = {}
    for tuple_ in res['Value']:
      fileName = tuple_[0]
      files[fileName] = dict(zip(metadata,tuple_[1:]))
    return S_OK(files)

  ######################################################
  #
  # _addFiles related methods
  #

  def _insertFiles(self,lfns,uid,gid,connection=False):
    connection = self._getConnection(connection)
    # Add the files
    failed = {}
    directoryFiles = {}
    insertTuples = []
    res = self.db.getStatusInt('AprioriGood',connection=connection)
    statusID = 0
    if res['OK']:
      statusID = res['Value']
    for lfn in sortList(lfns.keys()):
      fileInfo = lfns[lfn]
      size = fileInfo['Size']
      guid = fileInfo.get('GUID','')
      checksum = fileInfo['Checksum']
      checksumtype = fileInfo.get('ChecksumType','Adler32')
      dirName = os.path.dirname(lfn)
      dirID = fileInfo['DirID']
      fileName = os.path.basename(lfn)
      if not directoryFiles.has_key(dirName):
        directoryFiles[dirName] = []
      directoryFiles[dirName].append(fileName)  
      insertTuples.append("(%d,%d,%d,%d,%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d)" % (dirID,size,uid,gid,statusID,fileName,guid,checksum,checksumtype,self.db.umask))
    req = "INSERT INTO FC_Files (DirID,Size,UID,GID,Status,FileName,GUID,Checksum,ChecksumType,CreationDate,ModificationDate,Mode) VALUES %s" % (','.join(insertTuples))
    res = self.db._update(req,connection)
    if not res['OK']:
      return res
    # Get the fileIDs for the inserted files
    res = self._findFiles(lfns.keys(),['FileID'],connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = 'Failed post insert check'
        lfns.pop(lfn)
    else:
      failed.update(res['Value']['Failed'])
      for lfn,fileDict in res['Value']['Successful'].items():
        lfns[lfn]['FileID'] = fileDict['FileID']
    return S_OK({'Successful':lfns,'Failed':failed})

  def _getFileIDFromGUID(self,guid,connection=False):
    connection = self._getConnection(connection)
    if not guid:
      return S_OK({})
    if type(guid) not in [ListType,TupleType]:
      guid = [guid] 
    req = "SELECT FileID,GUID FROM FC_Files WHERE GUID IN (%s)" % stringListToString(guid)
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    guidDict = {}
    for fileID,guid in res['Value']:
      guidDict[guid] = fileID
    return S_OK(guidDict)

  ######################################################
  #
  # _deleteFiles related methods
  #
  
  def _deleteFiles(self,fileIDs,connection=False):
    connection = self._getConnection(connection)
    replicaPurge = self.__deleteFileReplicas(fileIDs)
    filePurge = self.__deleteFiles(fileIDs,connection=connection)
    if not replicaPurge['OK']:
      return replicaPurge
    if not filePurge['OK']:
      return filePurge
    return S_OK()

  def __deleteFileReplicas(self,fileIDs,connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM FC_Replicas WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req,connection)

  def __deleteFiles(self,fileIDs,connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM FC_Files WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req,connection)

  ######################################################
  #
  # _addReplicas related methods
  #
  
  def _insertReplicas(self,lfns,master=False,connection=False): 
    connection = self._getConnection(connection)
    res = self.db.getStatusInt('AprioriGood',connection=connection)
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
    for lfn in sortList(lfns.keys()):
      fileID = lfns[lfn]['FileID']
      pfn = lfns[lfn]['PFN']
      seName = lfns[lfn]['SE']
      res = self.db.seManager.findSE(seName)
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      seID = res['Value']
      if not master:
        res = self.__existsReplica(fileID,seID,connection=connection)
        if not res['OK']:
          failed[lfn] = res['Message']
          continue
        elif res['Value']:
          successful[lfn] = True
          continue
      dirID = lfns[lfn]['DirID']
      if not directorySESizeDict.has_key(dirID):
        directorySESizeDict[dirID] = {}
      if not directorySESizeDict[dirID].has_key(seID):
        directorySESizeDict[dirID][seID] = {'Files':0,'Size':0}
      directorySESizeDict[dirID][seID]['Size'] += lfns[lfn]['Size']
      directorySESizeDict[dirID][seID]['Files'] += 1
      insertTuples[lfn] = ("(%d,%d,%d,'%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (fileID,seID,statusID,replicaType,pfn))
      deleteTuples.append((fileID,seID))
    if insertTuples:
      req = "INSERT INTO FC_Replicas (FileID,SEID,Status,RepType,CreationDate,ModificationDate,PFN) VALUES %s" % ','.join(insertTuples.values())
      res = self.db._update(req,connection)
      if not res['OK']:
        self.__deleteReplicas(deleteTuples,connection=connection)
        for lfn in insertTuples.keys():
          failed[lfn] = res['Message']
      else:
        # Update the directory usage
        self._updateDirectoryUsage(directorySESizeDict,'+',connection=connection)
        for lfn in insertTuples.keys():
          successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})
  
  def __existsReplica(self,fileID,seID,connection=False):
    # TODO: This is in efficient. Should perform bulk operation
    connection = self._getConnection(connection)
    """ Check if a replica already exists """
    if type(seID) in StringTypes:
      res = self.db.seManager.findSE(seID)
      if not res['OK']:
        return res
      seID = res['Value']
    req = "SELECT FileID FROM FC_Replicas WHERE FileID=%d AND SEID=%d" % (fileID,seID)
    result = self.db._query(req,connection)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK(False)
    return S_OK(True)

  ######################################################
  #
  # _deleteReplicas related methods
  #
  
  def _deleteReplicas(self,lfns,connection=False):
    connection = self._getConnection(connection)
    successful = {}
    res = self._findFiles(lfns.keys(),['DirID','FileID','Size'],connection=connection)
    failed = res['Value']['Failed']
    lfnFileIDDict = res['Value']['Successful']
    toRemove = []
    directorySESizeDict = {}
    for lfn,fileDict in lfnFileIDDict.items():
      fileID = fileDict['FileID']
      se = lfns[lfn]['SE']
      toRemove.append((fileID,se))
      #Now prepare the storage usage dict
      res = self.db.seManager.findSE(se)
      if not res['OK']:
        return res
      seID = res['Value']
      dirID = fileDict['DirID']
      if not directorySESizeDict.has_key(dirID):
        directorySESizeDict[dirID] = {}
      if not directorySESizeDict[dirID].has_key(seID):
        directorySESizeDict[dirID][seID] = {'Files':0,'Size':0}
      directorySESizeDict[dirID][seID]['Size'] += fileDict['Size']
      directorySESizeDict[dirID][seID]['Files'] += 1
    res = self.__deleteReplicas(toRemove)
    if not res['OK']:
      for lfn in lfnFileIDDict.keys():
        failed[lfn] = res['Message']
    else:
      # Update the directory usage
      self._updateDirectoryUsage(directorySESizeDict,'-',connection=connection)
      for lfn in lfnFileIDDict.keys():
        successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})
      
  def __deleteReplicas(self,replicaTuples,connection=False):    
    connection = self._getConnection(connection)
    deleteTuples = []
    for fileID,seID in replicaTuples:
      if type(seID) in StringTypes:
        res = self.db.seManager.findSE(seID)
        if not res['OK']:
          return res
        seID = res['Value']
      deleteTuples.append("(%d,%d)" % (fileID,seID))
    req = "DELETE FROM FC_Replicas WHERE (FileID,SEID) IN (%s)" % intListToString(deleteTuples)
    return self.db._update(req,connection)

  ######################################################
  #
  # _setReplicaStatus _setReplicaHost _setReplicaParameter methods
  # _setFileParameter method
  #
  
  def _setReplicaStatus(self,fileID,se,status,connection=False):
    connection = self._getConnection(connection)
    res = self.db.getStatusInt(status,connection=connection)
    if not res['OK']:
      return res
    statusID = res['Value']
    return self._setReplicaParameter(fileID,se,'Status',statusID,connection=connection)
  
  def _setReplicaHost(self,fileID,se,newSE,connection=False):
    connection = self._getConnection(connection)
    res = self.db.seManager.findSE(newSE)
    if not res['OK']:
      return res
    newSE = res['Value']
    return self._setReplicaParameter(fileID,se,'SEID',newSE,connection=connection)
    
  def _setReplicaParameter(self,fileID,seID,paramName,paramValue,connection=False):
    connection = self._getConnection(connection)
    if type(seID) in StringTypes:
      res = self.db.seManager.findSE(seID)
      if not res['OK']:
        return res
      seID = res['Value']
    req = "UPDATE FC_Replicas SET %s='%s', ModificationDate=UTC_TIMESTAMP() WHERE FileID=%d AND SEID=%d;" % (paramName,paramValue,fileID,seID)
    return self.db._update(req,connection)

  def _setFileParameter(self,fileID,paramName,paramValue,connection=False):
    connection = self._getConnection(connection)
    if type(fileID) not in [TupleType,ListType]:
      fileID = [fileID]
    req = "UPDATE FC_Files SET %s='%s', ModificationDate=UTC_TIMESTAMP() WHERE FileID IN (%s)" % (paramName,paramValue,intListToString(fileID))
    return self.db._update(req,connection)

  ######################################################
  #
  # _getFileReplicas related methods
  #

  def _getFileReplicas(self,fileIDs,fields=['PFN'],connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_ERROR("No such file or directory")
    req = "SELECT FileID,SEID,Status,%s FROM FC_Replicas WHERE FileID IN (%s);" % (intListToString(fields),intListToString(fileIDs))
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    replicas = {}
    for tuple_ in res['Value']:
      fileID = tuple_[0]
      if not replicas.has_key(fileID):
        replicas[fileID] = {}
      seID = tuple_[1]
      res = self.db.seManager.getSEName(seID)
      if not res['OK']:
        continue
      seName = res['Value']
      statusID = tuple_[2]
      res = self.db.getIntStatus(statusID,connection=connection)
      if not res['OK']:
        continue
      status = res['Value']
      replicas[fileID][seName] = {'Status':status}
      replicas[fileID][seName].update(dict(zip(fields,tuple_[3:])))
    for fileID in fileIDs:
      if not replicas.has_key(fileID):
        replicas[fileID] = {}
    return S_OK(replicas)
