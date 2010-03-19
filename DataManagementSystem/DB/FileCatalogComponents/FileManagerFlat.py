########################################################################
# $Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################

__RCSID__ = "$Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $"

from DIRAC                                  import S_OK,S_ERROR,gLogger
from DIRAC.Core.Utilities.List              import stringListToString,intListToString,sortList
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManagerBase import FileManagerBase

import time,os
from types import *

class FileManagerFlat(FileManagerBase):

  def getFileCounters(self):
    req = "SELECT COUNT(*) FROM FileInfo;"
    res = self.db._query(req)
    if not res['OK']:
      return res
    return S_OK({'FileInfo':res['Value'][0][0]})
  
  def getReplicaCounters(self):
    req = "SELECT COUNT(*) FROM ReplicaInfo;"
    res = self.db._query(req)
    if not res['OK']:
      return res
    return S_OK({'ReplicaInfo':res['Value'][0][0]})

  def exists(self,lfns):
    res = self._findFiles(lfns)
    successful = dict.fromkeys(res['Value']['Successful'],True)
    failed = {}
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = False
      else:
        failed[lfn] = error
    return S_OK({"Successful":successful,"Failed":failed})

  def isFile(self,lfns):
    return self.exists(lfns)

  def getFileSize(self, lfns):
    res = self._findFiles(lfns,['Size'])
    if not res['OK']:
      return res
    for lfn in res['Value']['Successful'].keys():
      size = res['Value']['Successful'][lfn]['Size']
      res['Value']['Successful'][lfn] = size
    return res
    
  def getFileMetadata(self, lfns):
    return self._findFiles(lfns,['Size','Checksum','ChecksumType','UID','GID','CreationDate','ModificationDate','Mode','Status'])

  def getReplicas(self,lfns,allStatus):
    startTime = time.time()
    res = self._findFiles(lfns)
    #print 'findFiles',time.time()-startTime
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn,fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    replicas = {}
    if fileIDLFNs:
      startTime = time.time() 
      res = self.__getFileReplicas(fileIDLFNs.keys())
      #print '__getFileReplicas',time.time()-startTime
      if not res['OK']:
        return res
      for fileID,seDict in res['Value'].items():
        lfn = fileIDLFNs[fileID]
        replicas[lfn] = {}
        for se,repDict in seDict.items():
          repStatus = repDict['Status']
          if (repStatus.lower() != 'p') or (allStatus):
            # TODO Must consider LFN->PFN resolutions
            replicas[lfn][se] = repDict['PFN']
    return S_OK({'Successful':replicas,'Failed':failed})

  def getReplicaStatus(self,lfns):
    res = self._findFiles(lfns)
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn,fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    successful = {}
    if fileIDLFNs:
      res = self.__getFileReplicas(fileIDLFNs.keys())
      if not res['OK']:
        return res
      for fileID,seDict in res['Value'].items():
        lfn = fileIDLFNs[fileID]
        requestedSE = lfns[lfn]
        if not requestedSE:
          failed[lfn] = "Replica info not supplied"
        elif requestedSE not in seDict.keys():
          failed[lfn] = "No replica at supplied site"
        else:
          successful[lfn] = seDict[requestedSE]['Status']
    return S_OK({'Successful':successful,'Failed':failed})

  def removeFile(self,lfns):
    """ Bulk file removal method """
    successful = {}
    failed = {}
    res = self._findFiles(lfns)     
    successful = {} 
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = True
      else:
        failed[lfn] = error
    fileIDLfns = {}
    for lfn,lfnDict in res['Value']['Successful'].items():
      fileIDLfns[lfnDict['FileID']] = lfn
    res = self.__deleteReplicas(fileIDLfns.keys())  
    if not res['OK']:
      return res
    res = self.__deleteFiles(fileIDLfns.keys())
    if not res['OK']:
      return res
    for lfn in fileIDLfns.values():
      successful[lfn] = True
    return S_OK({"Successful":successful,"Failed":failed})

  def getFilesInDirectory(self,dirID,path,verbose=False):
    files = {}
    res = self.__getDirectoryFiles(dirID, [], ['FileID','Size','GUID','Checksum','ChecksumType','Type','UID','GID','CreationDate','ModificationDate','Mode','Status'])
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(files)
    for fileName,fileDict in res['Value'].items():
      lfn = "%s/%s" % (path,fileName)
      files[lfn] = fileDict
    return S_OK(files)
  
  def addFile(self,lfns,credDict):
    """ Add files to the catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self.__checkInfo(info,['PFN','SE','Size', 'GUID', 'Checksum'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self.__addFiles(lfns,credDict)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update(res['Value']['Failed'])    
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})   
  
  def addReplica(self,lfns):
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self.__checkInfo(info,['PFN','SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      pfn = info['PFN']
      se = info['SE']
      res = self._findFiles([lfn],['FileID'])
      if not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self.__addReplica(fileID,se,pfn=pfn,rtype='Replica')
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful':successful,'Failed':failed})

  def removeReplica(self,lfns):
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self.__checkInfo(info,['PFN','SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      pfn = info['PFN']
      se = info['SE']
      res = self._findFiles([lfn],['FileID'])
      if not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self.__deleteReplica(fileID,se)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful':successful,'Failed':failed})

  def setReplicaStatus(self,lfns):
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self.__checkInfo(info,['SE','Status'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      status = info['PFN']
      se = info['SE']
      res = self._findFiles([lfn],['FileID'])
      if not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self.__setReplicaParameter(fileID,se,'Status',status)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful':successful,'Failed':failed})

  def _findFiles(self,lfns,metadata=['FileID']):
    """ Find file ID if it exists for the given list of LFNs """
    startTime = time.time()
    dirDict = self._getFileDirectories(lfns)
    print 'files',len(lfns),'dirs',len(dirDict)
    #print '_getFileDirectories',time.time()-startTime

    failed = {}
    directoryIDs = {}
    for dirPath in dirDict.keys():
      startTime = time.time()
      res = self.db.dtree.findDir(dirPath)
      #print 'findDir',time.time()-startTime
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in dirDict[dirPath]:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:
        directoryIDs[dirPath] = res['Value']

    successful = {}
    for dirPath in directoryIDs.keys():
      fileNames = dirDict[dirPath]
      startTime = time.time()
      res = self.__getDirectoryFiles(directoryIDs[dirPath],fileNames,metadata)
      #print '__getDirectoryFiles',time.time()-startTime,dirPath,directoryIDs[dirPath],len(lfns),len(fileNames)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in fileNames:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:    
        for fileName,fileDict in res['Value'].items():
          successful["%s/%s" % (dirPath,fileName)] = fileDict
    return S_OK({"Successful":successful,"Failed":failed})

  def __getDirectoryFiles(self,dirID,fileNames,metadata):
    # metadata can be any of ['FileID','Size','GUID','Checksum','ChecksumType','Type','UID','GID','CreationDate','ModificationDate','Mode','Status']
    req = "SELECT FileName,%s FROM FileInfo WHERE DirID=%d" % (intListToString(metadata),dirID)
    if fileNames:
      req = "%s AND FileName IN (%s)" % (req,stringListToString(fileNames))
    res = self.db._query(req)
    if not res['OK']:
      return res
    files = {}
    for tuple in res['Value']:
      fileName = tuple[0]
      files[fileName] = dict(zip(metadata,tuple[1:]))
    return S_OK(files)

  def __getFileReplicas(self,fileIDs,fields=['Status','PFN']):
    if not fileIDs:
      return S_ERROR("No such file or directory")
    req = "SELECT FileID,SEName,%s FROM ReplicaInfo WHERE FileID IN (%s);" % (intListToString(fields),intListToString(fileIDs))
    res = self.db._query(req)
    if not res['OK']:
      return res
    replicas = {}
    for tuple in res['Value']:
      fileID = tuple[0]
      if not replicas.has_key(fileID):
        replicas[fileID] = {}
      seName = tuple[1]
      replicas[fileID][seName] = dict(zip(fields,tuple[2:]))
    return S_OK(replicas)

  def __getFileFromGUID(self,guid):
    req = "SELECT FileName,DirID FROM FileInfo WHERE GUID='%s'" % guid
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('GUID not found')
    return S_OK(result['Value'][0])
    
  def __getGUIDFiles(self,guids):
    req = "SELECT FileID,GUID FROM FileInfo WHERE GUID IN (%s)" % stringListToString(guids)
    res = self.db._query(req)
    if not res['OK']:
      return res
    guidFiles = {}
    for fileID,guid in res['Value']:
      guidFiles[guid]=fileID
    return S_OK(guidFiles)
    
  def __addFiles(self,lfns,credDict):
    successful = {}
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid, gid = result['Value']
    # Check whether the files already exist
    res = self._findFiles(lfns.keys(),['FileID','Size','Checksum','GUID'])
    failed = res['Value']['Failed']
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        failed.pop(lfn)
      else:
        lfns.pop(lfn)
    # For those that exist get the replicas to determine whether they are already registered
    existingMetadata = res['Value']['Successful']
    if existingMetadata:
      fileIDLFNs = {}
      for lfn,fileDict in existingMetadata.items():
        fileIDLFNs[fileDict['FileID']] = lfn
      res = self.__getFileReplicas(fileIDLFNs.keys())  
      if not res['OK']:
        for lfn in fileIDLFNs.values():
          failed[lfn] = 'Failed checking pre-existing replicas'
          lfns.pop(lfn)
      else:
        for fileID,lfn in fileIDLFNs.items():
          fileMetadata = existingMetadata[lfn]
          existingGuid = fileMetadata['GUID']
          existingSize = fileMetadata['Size']
          existingChecksum = fileMetadata['Checksum']
          newGuid = lfns[lfn]['GUID']
          newSize = lfns[lfn]['Size']
          newChecksum = lfns[lfn]['Checksum']
          # If the DB does not have replicas for this file return an error
          if not res['Value'].has_key(fileID):
            failed[lfn] = "File already registered with alternative replicas"
            lfns.pop(lfn)
          # If the supplied SE is not in the existing replicas return an error
          elif not lfns[lfn]['SE'] in res['Value'][fileID].keys():
            failed[lfn] = "File already registered with alternative replicas"
            lfns.pop(lfn)
          # Ensure that the key file metadata is the same
          elif (existingGuid != newGuid) or (existingSize != newSize) or (existingChecksum != newChecksum):
            failed[lfn] = "File already registered with alternative metadata"
            lfns.pop(lfn)
          # If we get here the file being registered already exists exactly in the DB
          else:
            successful[lfn] = True  
            lfns.pop(lfn)
    # If GUIDs are supposed to be unique check their pre-existance 
    if self.db.uniqueGUID:
      guidLFNs = {}
      for lfn,fileDict in lfns.keys():
        guidLFNs[fileDict['GUID']] = lfn
      res = self.__getGUIDFiles(guidLFNs.keys())
      if not res['OK']:
        return res
      for guid,fileID in res['Value']:
        failed[guidLFNs[guid]] = "GUID already registered for another file %s" % fileID # resolve this to LFN
        lfns.pop(lfn)
    # If we have files left to register
    if lfns:
      directoryIDs = {}
      # Create the directories for the supplied files and store their IDs
      directories = self._getFileDirectories(lfns.keys())
      for directory,fileNames in directories.items():
        res = self.db.dtree.makeDirectories(directory,credDict)
        if not res['OK']:
          for fileName in filesNames:
            lfn = "%s/%s" % (directory,fileName)
            failed[lfn] = "Failed to create directory for file"
            lfns.pop(lfn)
        else:
          directoryIDs[directory] = res['Value']
    # If we still have files left to register
    if lfns:
      # Add the files
      directoryFiles = {}
      insertTuples = []
      for lfn in sortList(lfns.keys()):
        fileInfo = lfns[lfn]
        size = fileInfo['Size']
        guid = fileInfo['GUID']
        checksum = fileInfo['Checksum']
        checksumtype = fileInfo.get('ChecksumType','Adler')
        dirName = os.path.dirname(lfn)
        dirID = directoryIDs[dirName]
        fileName = os.path.basename(lfn)
        if not directoryFiles.has_key(dirName):
          directoryFiles[dirName] = []
        directoryFiles[dirName].append(fileName)  
        insertTuples.append("(%d,'%s',%d,'%s','%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,0)" % (dirID,fileName,size,guid,checksum,checksumtype,uid,gid,self.db.umask))
      req = "INSERT INTO FileInfo (DirID,FileName,Size,GUID,Checksum,ChecksumType,UID,GID,CreationDate,ModificationDate,Mode,Status) VALUES %s" % ','.join(insertTuples)
      res = self.db._update(req)
      if not res['OK']:
        print res['Message']
        return res # Perhaps could gracefully fail these files.
      # Get the fileIDs for the inserted files
      print res['Value']
      lfnFileIDs = {}
      for dirName,fileNames in directoryFiles.items():
        dirID =  directoryIDs[dirName]
        res = self.__getDirectoryFiles(dirID,fileNames,['FileID'])
        if (not res['OK']) or (not res['Value']):
          error = res.get('Message','Failed post insert check')
          for fileName in fileNames:
            lfn = '%s/%s' % (dirName,fileName)
            failed[lfn] = error
            lfns.pop(lfn)
        else:    
          for fileName,fileDict in res['Value'].items():
            lfnFileIDs["%s/%s" % (dirName,fileName)] = fileDict['FileID']
      # Register the replicas for the inserted files
      if lfnFileIDs:
        insertTuples = []
        for lfn in sortList(lfnFileIDs.keys()):
          fileID = lfnFileIDs[lfn]
          se = lfns[lfn]['SE']
          pfn = lfns[lfn]['PFN']
          rtype = lfns[lfn].get('Type','Master')
          insertTuples.append("(%d,'%s','%s','U',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (fileID,se,rtype,pfn))
        req = "INSERT INTO ReplicaInfo (FileID,SEName,RepType,Status,CreationDate,ModificationDate,PFN) VALUES %s" % ','.join(insertTuples)
        res = self.db._update(req)
        if not res['OK']:
          print res['Message']
          for lfn in lfnFileIDs.keys():
            failed[lfn] = "Failed while registering replica"
          self.__purgeFiles(lfnFileIDs.values())
        else:
          for lfn in lfnFileIDs.keys():
            successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})
    
  def __addFile(self,lfn,credDict,size,se,guid='',pfn='',checksum='',checksumtype=''):
    """ Add (register) a file to the catalog."""
    start = time.time()
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid, gid = result['Value']
    # Check if the lfn already exists
    res = self._findFiles([lfn],['FileID','Size','Checksum','GUID'])
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']
    if successful and (successful.has_key(lfn)):
      allOK = True
      fileDict = successful[lfn]
      if size != fileDict['Size']:
        allOK = False
      if guid != fileDict['GUID']:
        allOK = False
      if checksum != fileDict['Checksum'] :
        allOK = False
      fileID = fileDict['FileID']
      if allOK:
        res = self.__getFileReplicas([fileID])
        if not res['OK']:
          allOK = False
        elif not res['Value'].has_key(fileID):
          allOK = False
        else:
          replicas = res['Value'][fileID]
          if not se in replicas.keys():
            allOK = False
      if allOK:
        # the file is already registered exactly the same so return a success
        return S_OK()
      else:
        return S_ERROR("File already registered with alternative metadata")

    if not guid:
      guid = generateGuid(checksum,checksumtype)
    elif self.db.UNIQUE_GUID:
      res = self.__getFileFromGUID(guid)
      if res['OK']:
        return S_ERROR("GUID already registered")

    # Create the file directory if necessary
    directory = os.path.dirname(lfn)
    result = self.db.dtree.makeDirectories(directory,credDict)
    if not result['OK']:
      return result
    dirID = result['Value']
    if not dirID:
      return S_ERROR('Failed to create (or find) the file directory')
    
    req = "INSERT INTO FileInfo (DirID,FileName,Size,GUID,Checksum,ChecksumType,UID,GID,CreationDate,ModificationDate,Mode,Status) VALUES\
          (%d,'%s',%d,'%s','%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,0)" % (dirID,os.path.basename(lfn),size,guid,checksum,checksumtype,uid,gid,self.db.umask)                        
    res = self.db._update(req)            
    if not res['OK']:
      return res
    fileID = res['lastRowId']
    res = self.__addReplica(fileID,se,pfn)
    if not res['OK']:
      self.__purgeFiles([fileID])
      return res
    return S_OK(fileID)

  def __addReplica(self,fileID,se,pfn='',rtype='Master'):
    """ Add a replica to the file catalog
    """
    res = self.__existsReplica(fileID,se)
    if (not res['OK']) or (res['Value']):
      return res
    req = "INSERT INTO ReplicaInfo (FileID,SEName,RepType,Status,CreationDate,ModificationDate,PFN) VALUES\
          (%d,'%s','%s','U',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (fileID,se,rtype,pfn)    
    res = self.db._update(req)    
    if not res['OK']:
      self.__deleteReplica(fileID,se)
      return S_ERROR('Failed to add replica info')
    return S_OK(True)

  def __existsReplica(self,fileID,se):
    """ Check if a replica already exists """    
    req = "SELECT * FROM ReplicaInfo WHERE FileID=%d AND SEName='%s'" % (fileID,se)
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK(False)
    return S_OK(True)

  def __purgeFiles(self,fileIDs):
    replicaPurge = self.__deleteReplicas(fileIDs)
    filePurge = self.__deleteFiles(fileIDs)
    if not replicaPurge['OK']:
      return replicaPurge
    if not filePurge['OK']:
      return filePurge
    return S_OK()

  def __deleteReplicas(self,fileIDs):
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM ReplicaInfo WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req)

  def __deleteReplica(self,fileID,se):
    req = "DELETE FROM ReplicaInfo WHERE FileID=%d AND SEName='%s'" % (fileID,se)
    return self.db._update(req)

  def __deleteFiles(self,fileIDs):
    if not fileIDs:
      return S_OK()
    req = "DELETE FROM FileInfo WHERE FileID in (%s)" % (intListToString(fileIDs))
    return self.db._update(req)

  def __setReplicaParameter(self,fileID,se,paramName,paramValue):
    req = "UPDATE ReplicaInfo SET %s='%s' WHERE FileID=%d AND SEName='%s';" % (paramName,paramValue,fileID,se)
    return self.db._update(req)

  def __checkInfo(self,info,requiredKeys):
    if not info:
      return S_ERROR("Missing parameters")
    for key in requiredKeys:
      if not key in info:
        return S_ERROR("Missing '%s' parameter" % key)
    return S_OK()

class ShiteIgnore:

  def __setFileOwner(self,fileID,owner):
    """ Set the file owner """
    result = self.findUser(owner)
    if not result['OK']:
      return result
    userID = result['Value']
    req = 'UPDATE FC_FileInfo SET UID=%d WHERE FileID=%d' % (int(userID),int(fileID))
    return self.db._update(req) 

  def __setFileGroup(self,fileID,group):
    """ Set the file group """
    result = self.findGroup(group)
    if not result['OK']:
      return result
    groupID = result['Value']
    req = 'UPDATE FC_FileInfo SET GID=%d WHERE FileID=%d' % (int(groupID),int(fileID))    
    return self.db._update(req) 

  def __setFileMode(self,fileID,mode):
    """ Set the file mode """
    req = 'UPDATE FC_FileInfo SET Mode=%d WHERE FileID=%d' % (int(mode),int(fileID))
    return self.db._update(req) 


#####################################################################
#
#  End of FileManager code
#
#####################################################################
  
  def changeFileOwner(self,lfns,s_uid=0,s_gid=0):
    """ Bulk method to set the file owner
    """
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    lfnList = arguments.keys()
    result = self.findFile(lfnList)
    if not result['OK']:
      return result
    lfnDict = result['Value']['Successful']
    
    successful = {}
    failed = {}
    for lfn,owner in arguments.items():
      if lfn in lfnDict:
        result = self.__setFileOwner(lfnDict[lfn],owner)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})
    
  def changeFileGroup(self,lfns,s_uid=0,s_gid=0):
    """ Bulk method to set the file owner
    """
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    lfnList = arguments.keys()
    result = self.findFile(lfnList)
    if not result['OK']:
      return result
    lfnDict = result['Value']['Successful']
    
    successful = {}
    failed = {}
    for lfn,group in arguments.items():
      if lfn in lfnDict:
        result = self.__setFileGroup(lfnDict[lfn],group)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})
  
  def changeFileMode(self,lfns,s_uid=0,s_gid=0):
    """ Bulk method to set the file owner
    """
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    lfnList = arguments.keys()
    result = self.findFile(lfnList)
    if not result['OK']:
      return result
    lfnDict = result['Value']['Successful']
    
    successful = {}
    failed = {}
    for lfn,mode in arguments.items():
      if lfn in lfnDict:
        result = self.__setFileMode(lfnDict[lfn],mode)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})    
  
#####################################################################

  def changePathOwner(self,paths,credDict):
    """ Change the owner for the given paths
    """
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    ( uid, gid ) = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    result = self.isDirectory(paths,credDict)
    if not result['OK']:
      return result
    dirList = result['Value']['Successful'].keys()
    fileList = []
    if len(dirList) < len(paths):
      result = self.isFile(paths,uid,gid)
      if not result['OK']:
        return result
      fileList = result['Value']['Successful'].keys()
    
    successful = {}
    failed = {}
    
    dirArgs = {}
    fileArgs = {}
    
    for path in arguments:
      if not path in dirList and not path in fileList:
        failed[path] = 'Path not found'
      if path in dirList:
        dirArgs[path] = arguments[path]
      elif path in fileList:
        fileArgs[path] = arguments[path]
        
    result = self.changeDirectoryOwner(dirArgs,uid,gid)
    if not result['OK']:
      return result
    successful.update(result['Value']['Successful'])
    failed.update(result['Value']['Successful'])
    
    result = self.changeFileOwner(fileArgs,uid,gid)
    if not result['OK']:
      return result
    successful.update(result['Value']['Successful'])
    failed.update(result['Value']['Successful'])    
    
    return S_OK({'Successful':successful,'Failed':failed})    
  
  def __changePathFunction(self,paths,credDict,change_function_directory,change_function_file):
    """ A generic function to change Owner, Group or Mode for the given paths
    """
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    ( uid, gid ) = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    dirList = []
    result = self.isDirectory(paths,credDict)    
    if not result['OK']:
      return result
    for p in result['Value']['Successful']:
      if result['Value']['Successful'][p]:
        dirList.append(p)
    fileList = []
    if len(dirList) < len(paths):
      result = self.isFile(paths,uid,gid)      
      if not result['OK']:
        return result
      fileList = result['Value']['Successful'].keys()
    
    successful = {}
    failed = {}
    
    dirArgs = {}
    fileArgs = {}
    
    for path in arguments:
      if (not path in dirList) and (not path in fileList):
        failed[path] = 'Path not found'
      if path in dirList:
        dirArgs[path] = arguments[path]
      elif path in fileList:
        fileArgs[path] = arguments[path]        
    if dirArgs:        
      result = change_function_directory(dirArgs,uid,gid)
      if not result['OK']:
        return result
      successful.update(result['Value']['Successful'])
      failed.update(result['Value']['Successful'])    
    if fileArgs:
      result = change_function_file(fileArgs,uid,gid)
      if not result['OK']:
        return result
      successful.update(result['Value']['Successful'])
      failed.update(result['Value']['Successful'])    
    
    return S_OK({'Successful':successful,'Failed':failed})
  
  def changePathOwner(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths
    """
    return self.__changePathFunction(paths,credDict,self.changeDirectoryOwner,self.changeFileOwner)
  
  def changePathGroup(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths
    """
    return self.__changePathFunction(paths,credDict,self.changeDirectoryGroup,self.changeFileGroup)
  
  def changePathMode(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths
    """
    return self.__changePathFunction(paths,credDict,self.changeDirectoryMode,self.changeFileMode)
