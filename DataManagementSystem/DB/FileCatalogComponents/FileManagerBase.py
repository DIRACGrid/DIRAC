########################################################################
# $Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################

__RCSID__ = "$Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $"

from DIRAC                                  import S_OK,S_ERROR,gLogger
from DIRAC.Core.Utilities.List              import stringListToString, intListToString, sortList
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse

import time,os
from types import *


class FileManagerBase:

  def __init__(self,database=None):
    self.db = database

  def _getConnection(self,connection):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection",res['Message'])
    return connection

  def setDatabase(self,database):
    self.db = database  
    
  def getFileCounters(self,connection=False):
    connection = self._getConnection(connection)
    req = "SELECT COUNT(*) FROM FC_Files;"
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    return S_OK({'FC_Files':res['Value'][0][0]})
  
  def getReplicaCounters(self,connection=False):
    connection = self._getConnection(connection)
    req = "SELECT COUNT(*) FROM FC_Replicas;"
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    return S_OK({'FC_Replicas':res['Value'][0][0]})
  
  ######################################################
  #
  # File write methods
  #

  def addFile(self,lfns,credDict,connection=False):
    connection = self._getConnection(connection)
    """ Add files to the catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self._checkInfo(info,['PFN','SE','Size','Checksum'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._addFiles(lfns,credDict,connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update(res['Value']['Failed'])    
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})
  
  def _addFiles(self,lfns,credDict,connection=False):
    connection = self._getConnection(connection)
    successful = {}
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid, gid = result['Value']
    # Check whether the supplied files have been registered already
    existingMetadata,failed = self._getExistingMetadata(lfns.keys(),connection=connection)
    if existingMetadata:
      success,fail = self._checkExistingMetadata(existingMetadata)
      successful.update(success)
      failed.update(fail)
      for lfn in (success.keys()+fail.keys()):
        lfns.pop(lfn)
    # If GUIDs are supposed to be unique check their pre-existance 
    if self.db.uniqueGUID:
      fail = self._checkUniqueGUID(lfns,connection=connection)
      failed.update(fail)
      for lfn in fail:
        lfns.pop(lfn)
    # If we have files left to register
    if lfns:
      # Create the directories for the supplied files and store their IDs
      directories = self._getFileDirectories(lfns.keys())
      for directory,fileNames in directories.items():
        res = self.db.dtree.makeDirectories(directory,credDict)
        for fileName in fileNames:
          lfn = "%s/%s" % (directory,fileName)
          if not res['OK']:
            failed[lfn] = "Failed to create directory for file"
            lfns.pop(lfn)
          else:
            lfns[lfn]['DirectoryID'] = res['Value']
    # If we still have files left to register
    if lfns:
      res = self._insertFiles(lfns,uid,gid,connection=connection)
      if not res['OK']:
        for lfn in lfns.keys():
          failed[lfn] = res['Message']
          lfns.pop(lfn)
      else:
        for lfn,error in res['Value']['Failed'].items():
          failed[lfn] = error
          lfns.pop(lfn)
        lfns = res['Value']['Successful']
    if lfns:
      res = self._insertReplicas(lfns,master=True,connection=connection)
      toPurge =[]
      if not res['OK']:
        for lfn in lfns.keys():
          failed[lfn] = "Failed while registering replica"
          toPurge.append(lfns[lfn]['FileID'])
      else:
        successful.update(res['Value']['Successful'])
        failed.update(res['Value']['Failed'])
        for lfn in res['Value']['Failed'].items():
          toPurge.append(lfns[lfn]['FileID'])
      if toPurge:
        self._deleteFiles(toPurge,connection=connection)
    return S_OK({'Successful':successful,'Failed':failed})

  def _getExistingMetadata(self,lfns,connection=False):
    connection = self._getConnection(connection)
    # Check whether the files already exist before adding
    res = self._findFiles(lfns,['FileID','Size','Checksum'],connection=connection)
    successful = res['Value']['Successful']
    failed = res['Value']['Failed']
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        failed.pop(lfn)
    return successful,failed

  def _checkExistingMetadata(self,existingMetadata):
    failed = {}
    successful = {}
    fileIDLFNs = {}
    for lfn,fileDict in existingMetadata.items():
      fileIDLFNs[fileDict['FileID']] = lfn
    # For those that exist get the replicas to determine whether they are already registered
    res = self._getFileReplicas(fileIDLFNs.keys())  
    if not res['OK']:
      for lfn in fileIDLFNs.values():
        failed[lfn] = 'Failed checking pre-existing replicas'
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
        # If the supplied SE is not in the existing replicas return an error
        elif not lfns[lfn]['SE'] in res['Value'][fileID].keys():
          failed[lfn] = "File already registered with alternative replicas"
        # Ensure that the key file metadata is the same
        elif (existingGuid != newGuid) or (existingSize != newSize) or (existingChecksum != newChecksum):
          failed[lfn] = "File already registered with alternative metadata"
        # If we get here the file being registered already exists exactly in the DB
        else:
          successful[lfn] = True  
    return successful,failed

  def _checkUniqueGUID(self,lfns,connection=False):
    connection = self._getConnection(connection)
    guidLFNs = {}
    failed = {}
    for lfn,fileDict in lfns.items():
      guidLFNs[fileDict['GUID']] = lfn
    res = self._getFileIDFromGUID(guidLFNs.keys(),connection=connection)
    if not res['OK']:
      failed = dict.fromkeys(lfns,res['Message'])
    for guid,fileID in res['Value'].items():
      failed[guidLFNs[guid]] = "GUID already registered for another file %s" % fileID # resolve this to LFN
    return failed

  def removeFile(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Remove file from the catalog """
    successful = {}
    failed = {}
    res = self._findFiles(lfns,connection=connection)     
    successful = {} 
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = True
      else:
        failed[lfn] = error
    fileIDLfns = {}
    for lfn,lfnDict in res['Value']['Successful'].items():
      fileIDLfns[lfnDict['FileID']] = lfn
    res = self._deleteFiles(fileIDLfns.keys(),connection=connection)
    if not res['OK']:
      for lfn in fileIDLfns.values():
        failed[lfn] = res['Message']
    else:
      for lfn in fileIDLfns.values():
        successful[lfn] = True
    return S_OK({"Successful":successful,"Failed":failed})

  def _setFileOwner(self,fileID,owner,connection=False):
    connection = self._getConnection(connection)
    """ Set the file owner """
    if type(owner) in StringTypes:
      result = self.db.ugManager.findUser(owner)
      if not result['OK']:
        return result
      owner = result['Value']
    return self._setFileParameter(fileID,'UID',owner,connection=connection)

  def _setFileGroup(self,fileID,group,connection=False):
    connection = self._getConnection(connection)
    """ Set the file group """
    if type(group) in StringTypes:
      result = self.db.ugManager.findGroup(group)
      if not result['OK']:
        return result
      group = result['Value']
    return self._setFileParameter(fileID,'GID',group,connection=connection)

  def _setFileMode(self,fileID,mode,connection=False):
    connection = self._getConnection(connection)
    """ Set the file mode """
    return self._setFileParameter(fileID,'Mode',mode,connection=connection)
  
  ######################################################
  #
  # Replica write methods
  #
  
  def addReplica(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Add replica to the catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self._checkInfo(info,['PFN','SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._addReplicas(lfns,connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']               
    else:
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})
  
  def _addReplicas(self,lfns,connection=False):
    connection = self._getConnection(connection)
    successful = {}
    res = self._findFiles(lfns.keys(),['FileID'],connection=connection)
    failed = res['Value']['Failed']
    for lfn in failed.keys():
      lfns.pop(lfn)
    lfnFileIDDict = res['Value']['Successful']
    for lfn,fileDict in lfnFileIDDict.items():
      fileID = fileDict['FileID']
      lfns[lfn]['FileID'] = fileID
    res = self._insertReplicas(lfns,connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      successful = res['Value']['Successful']
      failed.update(res['Value']['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})

  def removeReplica(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Remove replica from catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self._checkInfo(info,['PFN','SE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop(lfn)
    res = self._deleteReplicas(lfns,connection=connection)
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']  
    else:
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})

  def setReplicaStatus(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Set replica status in the catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self._checkInfo(info,['SE','Status'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      status = info['Status']
      se = info['SE']
      res = self._findFiles([lfn],['FileID'],connection=connection)
      if not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaStatus(fileID,se,status,connection=connection)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful':successful,'Failed':failed})

  def setReplicaHost(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Set replica host in the catalog """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      res = self._checkInfo(info,['SE','NewSE'])
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      newSE = info['NewSE']
      se = info['SE']
      res = self._findFiles([lfn],['FileID'],connection=connection)
      if not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaHost(fileID,se,newSE,connection=connection)
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK({'Successful':successful,'Failed':failed})

  ######################################################
  #
  # File read methods
  #

  def exists(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Determine whether a file exists in the catalog """
    res = self._findFiles(lfns,connection=connection)
    successful = dict.fromkeys(res['Value']['Successful'],True)
    failed = {}
    for lfn,error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = False
      else:
        failed[lfn] = error
    return S_OK({"Successful":successful,"Failed":failed})

  def isFile(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Determine whether a path is a file in the catalog """
    #TO DO, should check whether it is a directory if it fails
    return self.exists(lfns,connection=connection)
  
  def getFileSize(self, lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get file size from the catalog """
    #TO DO, should check whether it is a directory if it fails
    res = self._findFiles(lfns,['Size'],connection=connection)
    if not res['OK']:
      return res
    for lfn in res['Value']['Successful'].keys():
      size = res['Value']['Successful'][lfn]['Size']
      res['Value']['Successful'][lfn] = size
    return res

  def getFileMetadata(self, lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get file metadata from the catalog """
    #TO DO, should check whether it is a directory if it fails
    return self._findFiles(lfns,['Size','Checksum','ChecksumType','UID','GID','GUID','CreationDate','ModificationDate','Mode','Status'],connection=connection)

  def getPathPermissions(self,paths,credDict,connection=False):
    connection = self._getConnection(connection)
    """ Get the permissions for the supplied paths """
    res = self.db.ugManager.getUserAndGroupID(credDict)
    if not res['OK']:
      return res
    uid,gid = res['Value']
    res = self._findFiles(paths,metadata=['Mode','UID','GID'],connection=connection)
    if not res['OK']:
      return res
    successful = {}
    for dirName,dirDict in res['Value']['Successful'].items():
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
    return S_OK({'Successful':successful,'Failed':res['Value']['Failed']})

  ######################################################
  #
  # Replica read methods
  #
  
  def getReplicas(self,lfns,allStatus,connection=False):
    connection = self._getConnection(connection)
    """ Get file replicas from the catalog """
    startTime = time.time()
    res = self._findFiles(lfns,connection=connection)
    #print 'findFiles',time.time()-startTime
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn,fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    replicas = {}
    if fileIDLFNs:
      startTime = time.time() 
      res = self._getFileReplicas(fileIDLFNs.keys(),connection=connection)
      #print '_getFileReplicas',time.time()-startTime
      if not res['OK']:
        return res
      for fileID,seDict in res['Value'].items():
        lfn = fileIDLFNs[fileID]
        replicas[lfn] = {}
        for se,repDict in seDict.items():
          repStatus = repDict['Status']
          if (repStatus.lower() != 'p') or (allStatus):
            pfn = repDict['PFN']
            if not pfn:
              res = self._resolvePFN(lfn,se)
              if res['OK']:
                pfn = res['Value']
            replicas[lfn][se] = pfn
    return S_OK({'Successful':replicas,'Failed':failed})

  def _resolvePFN(self,lfn,se):
    resSE = self.db.seManager.getSEDefinition(se)
    if not resSE['OK']:
      return res
    pfnDict = dict(resSE['Value']['SEDict'])
    pfnDict['FileName'] = lfn
    return pfnunparse(pfnDict)

  def getReplicaStatus(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get replica status from the catalog """
    res = self._findFiles(lfns,connection=connection)
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn,fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    successful = {}
    if fileIDLFNs:
      res = self._getFileReplicas(fileIDLFNs.keys(),connection=connection)
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


  ######################################################
  #
  # Methods for interacting with the FC_GUID_to_File table
  #

  def _getFileIDFromGUID(self,guid,connection=False):
    connection = self._getConnection(connection)
    """ Get the FileID for a given GUID """
    if type(guid) not in [ListType,TupleType]:
      guid = [guid] 
    req = "SELECT FileID,GUID FROM FC_GUID_to_File WHERE GUID IN (%s)" % stringListToString(guid)
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    guidDict = {}
    for fileID,guid in res['Value']:
      guidDict[guid] = fileID
    return S_OK(guidDict)

  def _insertFileGUIDs(self,fileGuids,connection=False):
    connection = self._getConnection(connection)
    stringTuples = []
    for fileID,guid in fileGuids.items():
      stringTuples.append("(%d,'%s')" % (fileID,guid))
    req = "INSERT INTO FC_GUID_to_File (FileID,GUID) VALUES %s" % intListToString(stringTuples)
    return self.db._update(req,connection)

  def _getGuidFromFileID(self,fileID,connection=False):
    connection = self._getConnection(connection)
    """ Get GUID of the given file """
    if type(fileID) not in [ListType,TupleType]:
      fileID = [fileID] 
    req = "SELECT FileID,GUID FROM FC_GUID_to_File WHERE FileID IN (%s)" % stringListToString(fileID)
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    guidDict = {}
    for fileID,guid in res['Value']:
      guidDict[fileID] = guid
    return S_OK(guidDict)

  ######################################################
  #
  # General usage methods
  #

  def _getStatusInt(self,status,connection=False):
    connection = self._getConnection(connection)
    req = "SELECT StatusID FROM FC_Statuses WHERE Status = '%s';" % status
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK(res['Value'][0][0])
    req = "INSERT INTO FC_Statuses (Status) VALUES ('%s');" % status
    res = self.db._update(req,connection)
    if not res['OK']:
      return res
    return S_OK(res['lastRowId'])
    
  def _getIntStatus(self,statusID,connection=False):
    connection = self._getConnection(connection)
    req = "SELECT Status FROM FC_Statuses WHERE StatusID = %d" % statusID
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK(res['Value'][0][0])
    return S_OK('Unknown')

  def getFilesInDirectory(self,dirID,path,verbose=False,connection=False):
    connection = self._getConnection(connection)
    files = {}
    res = self._getDirectoryFiles(dirID, [], ['FileID','Size','Checksum','ChecksumType','Type','UID','GID','CreationDate','ModificationDate','Mode','Status'],connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(files)
    for fileName,fileDict in res['Value'].items():
      lfn = "%s/%s" % (path,fileName)
      files[lfn] = fileDict
    return S_OK(files)

  def _getFileDirectories(self,lfns):
    dirDict = {}
    for lfn in lfns:
      lfnDir = os.path.dirname(lfn)
      lfnFile = os.path.basename(lfn)
      if not lfnDir in dirDict:
        dirDict[lfnDir] = []
      dirDict[lfnDir].append(lfnFile)
    return dirDict
  
  def _checkInfo(self,info,requiredKeys):
    if not info:
      return S_ERROR("Missing parameters")
    for key in requiredKeys:
      if not key in info:
        return S_ERROR("Missing '%s' parameter" % key)
    return S_OK()
  
  def _checkLFNPFNConvention(self,lfn,pfn,se):
    """ Check that the PFN corresponds to the LFN-PFN convention """
    if pfn == lfn:
      return S_OK()
    if (len(pfn)<len(lfn)) or (pfn[-len(lfn):] != lfn) :
      return S_ERROR('PFN does not correspond to the LFN convention')
    return S_OK()

  def _checkLFNPFNConvention(self,lfn,pfn,se):
    """ Check that the PFN corresponds to the LFN-PFN convention
    """
    # Check if the PFN corresponds to the LFN convention
    if pfn == lfn:
      return S_OK()
    lfn_pfn = True   # flag that the lfn is contained in the pfn
    if (len(pfn)<len(lfn)) or (pfn[-len(lfn):] != lfn) :
      return S_ERROR('PFN does not correspond to the LFN convention')
    if not pfn.endswith(lfn):
      return S_ERROR()
    # Check if the pfn corresponds to the SE definition
    result = self._getStorageElement(se)
    if not result['OK']:
      return result
    selement = result['Value']
    res = pfnparse(pfn)
    if not res['OK']:
      return res
    pfnDict = res['Value']
    protocol = pfnDict['Protocol']
    pfnpath = pfnDict['Path']
    result = selement.getStorageParameters(protocol)
    if not result['OK']:
      return result
    seDict = result['Value']
    sePath = seDict['Path']
    ind = pfnpath.find(sePath)
    if ind == -1:
      return S_ERROR('The given PFN %s does not correspond to the %s SE definition' % (pfn,se))
    # Check the full LFN-PFN-SE convention
    lfn_pfn_se = True
    if lfn_pfn:
      seAccessDict = dict(seDict)
      seAccessDict['Path'] = sePath + '/' + lfn
      check_pfn = pfnunparse(seAccessDict)
      if check_pfn != pfn:
        return S_ERROR('PFN does not correspond to the LFN convention')
    return S_OK()

  def _getStorageElement(self, seName):
    from DIRAC.Resources.Storage.StorageElement              import StorageElement
    storageElement = StorageElement(seName)
    if not storageElement.valid:
      return S_ERROR(storageElement.errorReason)
    return S_OK(storageElement)

  def setFileGroup(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get set the group for the supplied files """
    res = self._findFiles(lfns,['FileID','GID'],connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      group = lfns[lfn]['Group']
      if type(group) in StringTypes:
        groupRes = self.db.ugManager.findGroup(group)
        if not groupRes['OK']:
          return groupRes
        group = groupRes['Value']
      currentGroup = res['Value']['Successful'][lfn]['GID']
      if int(group) == int(currentGroup):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileGroup(fileID,group,connection=connection) 
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})

  def setFileOwner(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get set the group for the supplied files """
    res = self._findFiles(lfns,['FileID','UID'],connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      owner = lfns[lfn]['Owner']
      if type(owner) in StringTypes:
        userRes = self.db.ugManager.findUser(owner)
        if not userRes['OK']:
          return userRes
        owner = userRes['Value']
      currentOwner = res['Value']['Successful'][lfn]['UID']
      if int(owner) == int(currentOwner):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileOwner(fileID,owner,connection=connection) 
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})

  def setFileMode(self,lfns,connection=False):
    connection = self._getConnection(connection)
    """ Get set the mode for the supplied files """
    res = self._findFiles(lfns,['FileID','Mode'],connection=connection)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      mode = lfns[lfn]['Mode']
      currentMode = res['Value']['Successful'][lfn]['Mode']
      if int(currentMode) == int(mode):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileMode(fileID,mode,connection=connection) 
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})

class ToFinalize:

  def changePathOwner(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction(paths,credDict,self.db.dtree.changeDirectoryOwner,self.setFileOwner)
  
  def changePathGroup(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction(paths,credDict,self.db.dtree.changeDirectoryGroup,self.setFileGroup)
  
  def changePathMode(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction(paths,credDict,self.db.dtree.changeDirectoryMode,self.setFileMode)

  def _changePathFunction(self,paths,credDict,change_function_directory,change_function_file):
    """ A generic function to change Owner, Group or Mode for the given paths """
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid,gid = result['Value']
    
    dirList = []
    result = self.db.isDirectory(paths,credDict)    
    if not result['OK']:
      return result
    for p in result['Value']['Successful']:
      if result['Value']['Successful'][p]:
        dirList.append(p)
    fileList = []
    if len(dirList) < len(paths):
      result = self.isFile(paths)      
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
