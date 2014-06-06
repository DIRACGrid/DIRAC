########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManagerBase  import FileManagerBase
from DIRAC.Core.Utilities.List                                            import stringListToString, \
                                                                                 intListToString, \
                                                                                 breakListIntoChunks

DEBUG = 0

import os
from types import StringTypes, ListType, TupleType

class FileManager(FileManagerBase):

  _tables = {}
  _tables['FC_Files'] = { "Fields": { 
                                     "FileID": "INT AUTO_INCREMENT",
                                     "DirID": "INT NOT NULL",
                                     "Size": "BIGINT UNSIGNED NOT NULL",
                                     "UID": "SMALLINT UNSIGNED NOT NULL",
                                     "GID": "TINYINT UNSIGNED NOT NULL",
                                     "Status": "SMALLINT UNSIGNED NOT NULL",
                                     "FileName": "VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                    }, 
                          "PrimaryKey": "FileID",
                          "Indexes": {
                                       "DirID": ["DirID"],
                                       "UID_GID": ["UID","GID"],
                                       "Status": ["Status"],
                                       "FileName": ["FileName"],
                                       "Dir_File": ["DirID","FileName"]
                                     }  
                                   }
  _tables['FC_FileInfo'] = { "Fields": { 
                                         "FileID": "INTEGER NOT NULL",
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
                                          "GUID": ["GUID"]  
                                        }  
                            }
  _tables['FC_Replicas'] = { "Fields": { 
                                         "RepID": "INT AUTO_INCREMENT",
                                         "FileID": "INT NOT NULL",
                                         "SEID": "INTEGER NOT NULL",
                                         "Status": "SMALLINT UNSIGNED NOT NULL"
                                       },
                             "PrimaryKey": "RepID",
                             "Indexes": {
                                          "FileID": ["FileID"],
                                          "SEID": ["SEID"],
                                          "Status": ["Status"] 
                                        },
                             "UniqueIndexes": { "File_SE": ["FileID","SEID"] }
                            } 
  _tables['FC_ReplicaInfo'] = { "Fields": { 
                                            "RepID": "INTEGER NOT NULL AUTO_INCREMENT",
                                            "RepType": "ENUM ('Master','Replica') NOT NULL DEFAULT 'Master'",
                                            "CreationDate": "DATETIME",
                                            "ModificationDate": "DATETIME",
                                            "PFN": "VARCHAR(1024)"
                                          },
                                "PrimaryKey": "RepID"
                              } 

  ######################################################
  #
  # The all important _findFiles and _getDirectoryFiles methods
  #

  def _findFiles(self,lfns,metadata=['FileID'],connection=False):
    """ Find file ID if it exists for the given list of LFNs """
    
    connection = self._getConnection(connection)
    dirDict = self._getFileDirectories(lfns)
    failed = {}
    result = self.db.dtree.findDirs( dirDict.keys() )
    if not result['OK']:
      return result
    directoryIDs = result['Value']

    for dirPath in dirDict:
      if not dirPath in directoryIDs:
        for fileName in dirDict[dirPath]:
          fname = '%s/%s' % (dirPath,fileName)
          fname = fname.replace('//','/')
          failed[fname] = 'No such file or directory'

    successful = {}
    for dirPath in directoryIDs:
      fileNames = dirDict[dirPath]
      res = self._getDirectoryFiles(directoryIDs[dirPath],fileNames,metadata,connection=connection)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in fileNames:
          fname = '%s/%s' % (dirPath,fileName)
          fname = fname.replace('//','/')
          failed[fname] = error
      else:
        for fileName,fileDict in res['Value'].items():
          fname = '%s/%s' % (dirPath,fileName)
          fname = fname.replace('//','/')
          successful[fname] = fileDict
      for fileName in fileNames:
        if not fileName in res['Value']:
          fname = '%s/%s' % (dirPath,fileName)
          fname = fname.replace('//','/')
          failed[fname] = 'No such file or directory'    
    return S_OK({"Successful":successful,"Failed":failed})

  def _findFileIDs( self, lfns, connection=False ):
    """ Find lfn <-> FileID correspondence
    """
    connection = self._getConnection(connection)
    dirDict = self._getFileDirectories(lfns)
    failed = {}
    successful = {}
    result = self.db.dtree.findDirs( dirDict.keys() )
    if not result['OK']:
      return result
    directoryIDs = result['Value']
    directoryPaths = {}

    for dirPath in dirDict:
      if not dirPath in directoryIDs:
        for fileName in dirDict[dirPath]:
          fname = '%s/%s' % (dirPath,fileName)
          fname = fname.replace('//','/')
          failed[fname] = 'No such directory'
      else:
        directoryPaths[directoryIDs[dirPath]] = dirPath
    directoryIDList = directoryIDs.keys()
    for dirIDs in breakListIntoChunks( directoryIDList, 1000 ):

      wheres = []
      for dirPath in dirIDs:
        fileNames = dirDict[dirPath]
        dirID = directoryIDs[dirPath]
        wheres.append( "( DirID=%d AND FileName IN (%s) )" % (dirID, stringListToString(fileNames) ) )

      req = "SELECT FileName,DirID,FileID FROM FC_Files WHERE %s" % " OR ".join( wheres )
      result = self.db._query(req,connection)
      if not result['OK']:
        return result
      for fileName, dirID, fileID in result['Value']:
        fname = '%s/%s' % (directoryPaths[dirID],fileName)
        fname = fname.replace('//','/')
        successful[fname] = fileID

    for lfn in lfns:
      if not lfn in successful:
        failed[lfn] = "No such file"

    return S_OK({"Successful":successful,"Failed":failed})

  def _getDirectoryFiles(self,dirID,fileNames,metadata_input,allStatus=False,connection=False):
    """ Get the metadata for files in the same directory
    """
    metadata = list(metadata_input)
    
    connection = self._getConnection(connection)
    # metadata can be any of ['FileID','Size','UID','GID','Status','Checksum','CheckSumType',
    # 'Type','CreationDate','ModificationDate','Mode']
    req = "SELECT FileName,DirID,FileID,Size,UID,GID,Status FROM FC_Files WHERE DirID=%d" % (dirID)
    if not allStatus:
      statusIDs = []
      for status in self.db.visibleFileStatus:
        res = self._getStatusInt( status, connection=connection )
        if res['OK']:
          statusIDs.append( res['Value'] )
      if statusIDs:
        req = "%s AND Status IN (%s)" % (req,intListToString(statusIDs))
    if fileNames:
      req = "%s AND FileName IN (%s)" % (req,stringListToString(fileNames))
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    fileNameIDs = res['Value']
    if not fileNameIDs:
      return S_OK({})
    filesDict = {}
    # If we only requested the FileIDs then there is no need to do anything else
    if metadata == ['FileID']:
      for fileName,dirID,fileID,size,uid,gid,status in fileNameIDs:
        filesDict[fileName] = {'FileID':fileID}
      return S_OK(filesDict)
    # Otherwise get the additionally requested metadata from the FC_FileInfo table
    files = {}
    userDict = {}
    groupDict = {}
    for fileName,dirID,fileID,size,uid,gid,status in fileNameIDs:
      filesDict[fileID] = fileName
      files[fileName] = {}
      if 'Size' in metadata:
        files[fileName]['Size'] = size
      if 'DirID' in metadata:
        files[fileName]['DirID'] = dirID
      if 'UID' in metadata:
        files[fileName]['UID'] = uid
        if uid in userDict:
          owner = userDict[uid]
        else:  
          owner = 'unknown'
          result = self.db.ugManager.getUserName(uid)
          if result['OK']:
            owner = result['Value']
          userDict[uid] = owner  
        files[fileName]['Owner'] = owner   
      if 'GID' in metadata:
        files[fileName]['GID'] = gid
        if gid in groupDict:
          group = groupDict[gid]
        else:    
          group = 'unknown'
          result = self.db.ugManager.getGroupName(gid)
          if result['OK']:
            group = result['Value']
          groupDict[gid] = group  
        files[fileName]['OwnerGroup'] = group    
      if 'Status' in metadata:
        files[fileName]['Status'] = status      
    for element in ['FileID','Size','DirID','UID','GID','Status']:
      if element in metadata:
        metadata.remove(element)    
    metadata.append('FileID')
    metadata.reverse()
    req = "SELECT %s FROM FC_FileInfo WHERE FileID IN (%s)" % (intListToString(metadata),intListToString(filesDict.keys()))  
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    for tuple_ in res['Value']:
      fileID = tuple_[0]
      rowDict = dict(zip(metadata,tuple_))
      files[filesDict[fileID]].update(rowDict)
    return S_OK(files)

  def _getFileMetadataByID( self, fileIDs, connection=False ):
    """ Get standard file metadata for a list of files specified by FileID
    """
    stringIDs = ','.join( [ '%s' % id_ for id_ in fileIDs ] )
    req = "SELECT FileID,Size,UID,GID,Status FROM FC_Files WHERE FileID in ( %s )" % stringIDs
    result = self.db._query(req,connection)
    if not result['OK']:
      return result
    resultDict = {}
    for fileID, size, uid, gid, status in result['Value']:
      resultDict[fileID] = { "Size": int(size), "UID": int(uid), "GID": int(gid), "Status": status }
      
    req = "SELECT FileID,GUID,CreationDate from FC_FileInfo WHERE FileID in ( %s )" % stringIDs  
    result = self.db._query(req,connection)
    if not result['OK']:
      return result
    for fileID, guid, date in result['Value']:
      resultDict.setdefault( fileID, {} )
      resultDict[fileID].update( { "GUID": guid, "CreationDate": date } )
      
    return S_OK( resultDict )  

  ######################################################
  #
  # _addFiles related methods
  #

  def _insertFiles(self,lfns,uid,gid,connection=False):

    connection = self._getConnection(connection)
    # Add the files
    failed = {}
    insertTuples = []
    res = self.db.getStatusInt('AprioriGood',connection=connection)
    statusID = 0
    if res['OK']:
      statusID = res['Value']
      
    directorySESizeDict = {}  
    for lfn in lfns.keys():
      dirID = lfns[lfn]['DirID']
      fileName = os.path.basename(lfn)
      size = lfns[lfn]['Size']
      ownerDict = lfns[lfn].get('Owner',None)
      s_uid = uid
      s_gid = gid
      if ownerDict:
        result = self.db.ugManager.getUserAndGroupID( ownerDict )
        if result['OK']:
          s_uid, s_gid = result['Value']
      insertTuples.append("(%d,%d,%d,%d,%d,'%s')" % (dirID,size,s_uid,s_gid,statusID,fileName))
      directorySESizeDict.setdefault( dirID, {} )
      directorySESizeDict[dirID].setdefault( 0, {'Files':0,'Size':0} )
      directorySESizeDict[dirID][0]['Size'] += lfns[lfn]['Size']
      directorySESizeDict[dirID][0]['Files'] += 1

    req = "INSERT INTO FC_Files (DirID,Size,UID,GID,Status,FileName) VALUES %s" % (','.join(insertTuples))
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
      for lfn in res['Value']['Failed'].keys():
        lfns.pop(lfn)
      for lfn,fileDict in res['Value']['Successful'].items():
        lfns[lfn]['FileID'] = fileDict['FileID']
    insertTuples = []
    toDelete = []
    for lfn in lfns.keys():
      fileInfo = lfns[lfn]     
      fileID = fileInfo['FileID']
      dirID = fileInfo['DirID']
      checksum = fileInfo['Checksum']
      checksumtype = fileInfo.get('ChecksumType','Adler32')
      guid = fileInfo.get('GUID','')
      mode = fileInfo.get('Mode',self.db.umask)
      toDelete.append(fileID)
      insertTuples.append("(%d,'%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d)" % (fileID,guid,checksum,checksumtype,mode))
    if insertTuples:
      req = "INSERT INTO FC_FileInfo (FileID,GUID,Checksum,CheckSumType,CreationDate,ModificationDate,Mode) VALUES %s" % ','.join(insertTuples)
      res = self.db._update(req)
      if not res['OK']:
        self._deleteFiles(toDelete,connection=connection)
        for lfn in lfns.keys():
          failed[lfn] = res['Message']
          lfns.pop(lfn)
      else:
        # Update the directory usage
        result = self._updateDirectoryUsage(directorySESizeDict,'+',connection=connection)
        if not result['OK']:
          gLogger.warn( "Failed to insert FC_DirectoryUsage", result['Message'] )
          
    return S_OK({'Successful':lfns,'Failed':failed})

  def _getFileIDFromGUID(self,guid,connection=False):
    connection = self._getConnection(connection)
    if not guid:
      return S_OK({})
    if type(guid) not in [ListType,TupleType]:
      guid = [guid] 
    req = "SELECT FileID,GUID FROM FC_FileInfo WHERE GUID IN (%s)" % stringListToString(guid)
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
    res = self.__getFileIDReplicas(fileIDs,connection=connection)
    if not res['OK']:
      return res
    repIDs = res['Value'].keys()
    return self.__deleteReplicas(repIDs, connection=connection)

  def __deleteFiles(self,fileIDs,connection=False):
    connection = self._getConnection(connection)
    if type(fileIDs) not in [ ListType, TupleType]:
      fileIDs = [fileIDs]
    if not fileIDs:
      return S_OK()
    fileIDString = intListToString(fileIDs)
    failed = []
    for table in ['FC_Files','FC_FileInfo']:
      req = "DELETE FROM %s WHERE FileID in (%s)" % (table,fileIDString)
      res = self.db._update(req,connection)
      if not res['OK']:
        gLogger.error("Failed to remove files from %s" % table,res['Message'])
        failed.append(table)
    if failed:
      return S_ERROR("Failed to remove files from %s" % stringListToString(failed))
    return S_OK()

  ######################################################
  #
  # _addReplicas related methods
  #
  
  def _insertReplicas(self,lfns,master=False,connection=False): 
    connection = self._getConnection(connection)
    # Add the files
    failed = {}
    successful = {}
    insertTuples = []
    fileIDLFNs = {}
    res = self.db.getStatusInt('AprioriGood')
    statusID = 0
    if res['OK']:
      statusID = res['Value']
    for lfn in lfns.keys():
      fileID = lfns[lfn]['FileID']
      fileIDLFNs[fileID] = lfn
      seName = lfns[lfn]['SE']
      if type(seName) in StringTypes:
        seList = [seName]
      elif type(seName) == ListType:
        seList = seName
      else:
        return S_ERROR('Illegal type of SE list: %s' % str( type( seName ) ) )
      for seName in seList:    
        res = self.db.seManager.findSE(seName)
        if not res['OK']:
          failed[lfn] = res['Message']
          lfns.pop( lfn )
          continue
        seID = res['Value']
        insertTuples.append((fileID,seID))
    if not master:
      res = self._getRepIDsForReplica(insertTuples, connection=connection)
      if not res['OK']:
        return res
      for fileID,repDict in res['Value'].items():
        for seID,repID in repDict.items():
          successful[fileIDLFNs[fileID]] = True
          insertTuples.remove((fileID,seID))

    if not insertTuples:
      return S_OK({'Successful':successful,'Failed':failed})

    req = "INSERT INTO FC_Replicas (FileID,SEID,Status) VALUES %s" % \
          (','.join(["(%d,%d,%d)" % (tuple_[0],tuple_[1],statusID) for tuple_ in insertTuples]))
    res = self.db._update(req,connection)
    if not res['OK']:
      return res
    res = self._getRepIDsForReplica(insertTuples, connection=connection)
    if not res['OK']:
      return res
    replicaDict = res['Value']
    directorySESizeDict = {}
    for fileID,repDict in replicaDict.items():
      lfn = fileIDLFNs[fileID]
      dirID = lfns[lfn]['DirID']
      directorySESizeDict.setdefault( dirID, {} )
      for seID,repID in repDict.items():
        lfns[lfn]['RepID'] = repID
        directorySESizeDict[dirID].setdefault( seID, {'Files':0,'Size':0} )
        directorySESizeDict[dirID][seID]['Size'] += lfns[lfn]['Size']
        directorySESizeDict[dirID][seID]['Files'] += 1

    replicaType = 'Replica'
    if master:
      replicaType = 'Master'
    insertReplicas = []
    toDelete = []
    for lfn in lfns.keys():
      fileDict = lfns[lfn]
      repID = fileDict.get( 'RepID', 0 )
      if repID:
        pfn = fileDict['PFN']
        toDelete.append(repID)
        insertReplicas.append("(%d,'%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (repID,replicaType,pfn))    
    if insertReplicas:
      req = "INSERT INTO FC_ReplicaInfo (RepID,RepType,CreationDate,ModificationDate,PFN) VALUES %s" % (','.join(insertReplicas))
      res = self.db._update(req,connection)
      if not res['OK']:
        for lfn in lfns.keys():
          failed[lfn] = res['Message']
        self.__deleteReplicas(toDelete,connection=connection)
      else:
        # Update the directory usage
        self._updateDirectoryUsage(directorySESizeDict,'+',connection=connection)
        for lfn in lfns.keys():
          successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})

  def _getRepIDsForReplica(self,replicaTuples,connection=False):
    connection = self._getConnection(connection)
    queryTuples = []
    for fileID,seID in replicaTuples:
      queryTuples.append("(%d,%d)" % (fileID,seID))
    req = "SELECT RepID,FileID,SEID FROM FC_Replicas WHERE (FileID,SEID) IN (%s)" % intListToString(queryTuples) 
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    replicaDict = {}
    for repID,fileID,seID in res['Value']:
      replicaDict.setdefault( fileID, {} )
      replicaDict[fileID][seID] = repID
    return S_OK(replicaDict)  

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
      if type(se) in StringTypes:
        res = self.db.seManager.findSE(se)
        if not res['OK']:
          return res
      seID = res['Value']
      toRemove.append((fileID,seID))
      # Now prepare the storage usage update
      dirID = fileDict['DirID']
      directorySESizeDict.setdefault( dirID, {} )
      directorySESizeDict[dirID].setdefault( seID, {'Files':0,'Size':0} )
      directorySESizeDict[dirID][seID]['Size'] += fileDict['Size']
      directorySESizeDict[dirID][seID]['Files'] += 1
    res = self._getRepIDsForReplica(toRemove, connection)
    if not res['OK']:
      for lfn in lfnFileIDDict.keys():
        failed[lfn] = res['Message']
    else:
      repIDs = []
      for fileID,seDict in res['Value'].items():
        for seID,repID in seDict.items():
          repIDs.append(repID)    
      res = self.__deleteReplicas(repIDs,connection=connection)
      if not res['OK']:
        for lfn in lfnFileIDDict.keys():
          failed[lfn] = res['Message']
      else:
        # Update the directory usage
        self._updateDirectoryUsage(directorySESizeDict,'-',connection=connection)
        for lfn in lfnFileIDDict.keys():
          successful[lfn] = True
    return S_OK({"Successful":successful,"Failed":failed})

  def __deleteReplicas(self,repIDs,connection=False):
    connection = self._getConnection(connection)
    if type(repIDs) not in [ ListType, TupleType]:
      repIDs = [repIDs]
    if not repIDs:
      return S_OK()
    repIDString = intListToString(repIDs)
    failed = []
    for table in ['FC_Replicas','FC_ReplicaInfo']:
      req = "DELETE FROM %s WHERE RepID in (%s)" % (table,repIDString)
      res = self.db._update(req,connection)
      if not res['OK']:
        gLogger.error("Failed to remove replicas from %s" % table,res['Message'])
        failed.append(table)
    if failed:
      return S_ERROR("Failed to remove replicas from %s" % stringListToString(failed))
    return S_OK()
  
  ######################################################
  #
  # _setReplicaStatus _setReplicaHost _setReplicaParameter methods
  # _setFileParameter method
  #
  
  def _setReplicaStatus(self,fileID,se,status,connection=False):
    if not status in self.db.validReplicaStatus:
      return S_ERROR( 'Invalid replica status %s' % status )
    connection = self._getConnection(connection)
    res = self.db.getStatusInt(status,connection=connection)
    if not res['OK']:
      return res
    statusID = res['Value']
    res = self.__getRepIDForReplica(fileID,se,connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    repID = res['Value']
    req = "UPDATE FC_Replicas SET Status=%d WHERE RepID=%d" % (statusID,repID)
    return self.db._update(req,connection)

  def _setReplicaHost(self,fileID,se,newSE,connection=False):
    connection = self._getConnection(connection)
    res = self.db.seManager.findSE(newSE)
    if not res['OK']:
      return res
    newSE = res['Value']
    res = self.__getRepIDForReplica(fileID,se,connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    repID = res['Value']
    req = "UPDATE FC_Replicas SET SEID=%d WHERE RepID = %d;" % (newSE,repID)
    return self.db._update(req,connection)
    
  def _setReplicaParameter(self,fileID,se,paramName,paramValue,connection=False):
    connection = self._getConnection(connection)
    res = self.__getRepIDForReplica(fileID,se,connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    repID = res['Value']
    req = "UPDATE FC_ReplicaInfo SET %s='%s', ModificationDate = UTC_TIMESTAMP() WHERE RepID IN (%d)" % (paramName,paramValue,repID)
    return self.db._update(req,connection)

  def _setFileParameter(self,fileID,paramName,paramValue,connection=False):
    connection = self._getConnection(connection)
    if type(fileID) not in [TupleType,ListType]:
      fileID = [fileID]
      
    if paramName == 'Status' and not paramValue in self.db.validFileStatus:
      return S_ERROR( 'Invalid file status %s' % paramValue )   
      
    if paramName in ['UID','GID','Status','Size']:
      # Treat primary file attributes specially
      req = "UPDATE FC_Files SET %s='%s' WHERE FileID IN (%s)" % (paramName,paramValue,intListToString(fileID))
      result = self.db._update(req,connection)
      if not result['OK']:
        return result
      req = "UPDATE FC_FileInfo SET ModificationDate=UTC_TIMESTAMP() WHERE FileID IN (%s)" % intListToString(fileID)
    else:  
      req = "UPDATE FC_FileInfo SET %s='%s', ModificationDate=UTC_TIMESTAMP() WHERE FileID IN (%s)" % (paramName,paramValue,intListToString(fileID))
    return self.db._update(req,connection)
    
  def __getRepIDForReplica(self,fileID,seID,connection=False):
    connection = self._getConnection(connection)
    if type(seID) in StringTypes:
      res = self.db.seManager.findSE(seID)
      if not res['OK']:
        return res
      seID = res['Value']
    res = self._getRepIDsForReplica([(fileID,seID)],connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      result = S_OK()
      result['Exists'] = False
    else:
      result = S_OK(res['Value'][fileID][seID])
      result['Exists'] = True
    return result

  ######################################################
  #
  # _getFileReplicas related methods
  #

  def _getFileReplicas(self,fileIDs,fields_input=['PFN'],allStatus=False,connection=False):
    """ Get replicas for the given list of files specified by their fileIDs
    """
    fields = list(fields_input)
    connection = self._getConnection(connection)
    res = self.__getFileIDReplicas(fileIDs,allStatus=allStatus,connection=connection)
    if not res['OK']:
      return res
    fileIDDict = res['Value']
    if fileIDDict:
      if 'Status' in fields:
        fields.remove('Status')
      repIDDict = {}  
      if fields:  
        req = "SELECT RepID,%s FROM FC_ReplicaInfo WHERE RepID IN (%s);" % \
              (intListToString(fields),intListToString(fileIDDict.keys()))
        res = self.db._query(req,connection)
        if not res['OK']:
          return res
        for tuple_ in res['Value']:
          repID = tuple_[0]
          repIDDict[repID] = dict(zip(fields,tuple_[1:])) 
          statusID = fileIDDict[repID][2]
          res = self.db.getIntStatus(statusID,connection=connection)
          if not res['OK']:
            continue
          repIDDict[repID]['Status'] = res['Value']
      else:
        for repID in fileIDDict:
          statusID = fileIDDict[repID][2]
          res = self.db.getIntStatus(statusID,connection=connection)
          if not res['OK']:
            continue
          repIDDict[repID] = {'Status' : res['Value'] }  
    seDict = {}
    replicas = {}
    for repID in fileIDDict.keys():
      fileID, seID, statusID = fileIDDict[repID]
      replicas.setdefault(fileID,{})
      if not seID in seDict:
        res = self.db.seManager.getSEName(seID)
        if not res['OK']:
          continue
        seDict[seID] = res['Value']
      seName = seDict[seID]
      replicas[fileID][seName] = repIDDict.get(repID,{})
      
    if len( replicas ) != len( fileIDs ):  
      for fileID in fileIDs:
        if not replicas.has_key(fileID):
          replicas[fileID] = {}
          
    return S_OK(replicas)

  def __getFileIDReplicas(self,fileIDs,allStatus=False,connection=False):
    connection = self._getConnection(connection)
    if not fileIDs:
      return S_ERROR("No such file or directory")
    req = "SELECT FileID,SEID,RepID,Status FROM FC_Replicas WHERE FileID IN (%s)" % (intListToString(fileIDs))
    if not allStatus:
      statusIDs = []
      for status in self.db.visibleReplicaStatus:
        result = self._getStatusInt( status, connection=connection )
        if result['OK']:
          statusIDs.append( result['Value'] )
      req += " AND Status in (%s)" % (intListToString(statusIDs))
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    fileIDDict = {}
    for fileID,seID,repID,statusID in res['Value']:
      fileIDDict[repID] = ( fileID, seID, statusID )
    return S_OK(fileIDDict)

  def _getDirectoryReplicas( self, dirID, allStatus=False, connection=False ):
    """ Get replicas for files in a given directory
    """
    replicaStatusIDs = []
    if not allStatus:
      for status in self.db.visibleReplicaStatus:
        result = self._getStatusInt( status, connection=connection )
        if result['OK']:
          replicaStatusIDs.append( result['Value'] )
    fileStatusIDs = []
    if not allStatus:
      for status in self.db.visibleFileStatus:
        result = self._getStatusInt( status, connection=connection )
        if result['OK']:
          fileStatusIDs.append( result['Value'] )
    
    if not self.db.lfnPfnConvention or self.db.lfnPfnConvention == "Weak":
      req = 'SELECT FF.FileName,FR.FileID,FR.SEID,FI.PFN FROM FC_Files as FF,'
      req += ' FC_Replicas as FR, FC_ReplicaInfo as FI'
      req += ' WHERE FF.FileID=FR.FileID AND FR.RepID=FI.RepID AND FF.DirID=%d ' % dirID
      if replicaStatusIDs:
        req += ' AND FR.Status in (%s)' % intListToString( replicaStatusIDs )
      if fileStatusIDs:
        req += ' AND FF.Status in (%s)' % intListToString( fileStatusIDs )  
    else:
      req = "SELECT FF.FileName,FR.FileID,FR.SEID,'' FROM FC_Files as FF,"
      req += ' FC_Replicas as FR'
      req += ' WHERE FF.FileID=FR.FileID AND FF.DirID=%d ' % dirID
      if replicaStatusIDs:
        req += ' AND FR.Status in (%s)' % intListToString( replicaStatusIDs )
      if fileStatusIDs:
        req += ' AND FF.Status in (%s)' % intListToString( fileStatusIDs )                                                                             
    
    result = self.db._query( req, connection )
    return result
