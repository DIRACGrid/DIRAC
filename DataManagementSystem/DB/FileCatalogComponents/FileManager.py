########################################################################
# $Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################

__RCSID__ = "$Id: FileManager.py 22623 2010-03-09 19:54:25Z acsmith $"

from DIRAC import S_OK,S_ERROR,gLogger
from DIRAC.Core.Utilities.List import stringListToString,intListToString
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities             import * 
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse


DEBUG = 0

import time,os
from types import *

class FileManager:

  def __init__(self,database=None):
    self.db = database
    
  def setDatabase(self,database):
    self.db = database  
    self.resolvePFN = True
    self.umask = 0775

    self.lfnConvention = convention
    self.resolvePFN = resolve
    
    
#####################################################################
#
#  File and replica related methods
#
#####################################################################

  def findFile(self,lfns):
    """ Find file ID if it exists for the given list of LFNs """
    dirDict = self.__getFileDirectories(lfns)

    failed = {}
    directoryIDs = {}
    for dirPath in dirDict.keys():
      res = self.db.dtree.findDir(dirPath)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in dirDict[dirPath]:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:
        directoryIDs[dirPath] = res['Value']

    successful = {}
    #Need to consider permissions here
    for dirPath in directoryIDs.keys():
      fileNames = dirDict[dirPath]
      res = self.__getFileIDForDirectoryFiles(directoryIDs[dirPath],fileNames)
      if (not res['OK']) or (not res['Value']):
        error = res.get('Message','No such file or directory')
        for fileName in fileNames:
          failed['%s/%s' % (dirPath,fileName)] = error
      else:
        for fileID,fileName in res['Value']:
          successful["%s/%s" % (dirPath,fileName)] = fileID
    return S_OK({"Successful":successful,"Failed":failed})

  def getFileSize(self, lfns):
    res = self._getMetadataForLFNs(lfns, ['Size'])
    if not res['OK']:
      return res
    for lfn in res['Value']['Successful'].keys():
      size = res['Value']['Successful'][lfn]['Size']
      res['Value']['Successful'][lfn] = size
    return res
    
  def getFileMetadata(self, lfns):
    """ Get metadata for the list of LFNS
    """
    result = self._getMetadataForLFNs(lfns,['Size','CheckSum','CheckSumType','UID','GID','CreationDate','ModificationDate','Mode','Status'])
    if not result['OK']:
      return result
    
    if not result['Value']['Successful']:
      return result
    
    successful = result['Value']['Successful']
    failed = result['Value']['Failed']
    fileIDDict = result['Value']['FileIDDict']
    
    fileIDList = []
    for id,lfn in fileIDDict.items():
      if lfn in successful:
        fileIDList.append(id)
        
    result = self.__getGuidFromFileIDs(fileIDList)
    if not result['OK']:
      return result
    
    guidDict = result['Value']
    for id,guid in guidDict.items():
      successful[fileIDDict[id]]['GUID'] = guid    
        
    return S_OK({'Successful':successful,'Failed':failed})    

  def __getFileIDForDirectoryFiles(self,dirID,fileNames):
    req = "SELECT FileID,FileName FROM FC_Files WHERE DirID=%d AND FileName IN (%s)" % (dirID,stringListToString(fileNames))            
    return self.db._query(req)

  def __getFileIDFromGUID(self,guid):
    """ Get the FileID for a given GUID """
    req = "SELECT FileID FROM FC_GUID_to_File WHERE GUID='%s'" % guid
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('GUID not found')
    return S_OK(result['Value'][0][0])
    
  def __getGuidFromFileID(self,fileID):
    """ Get GUID of the given file """
    req = "SELECT GUID FROM FC_GUID_to_File WHERE FileID=%d" % fileID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File not found')
    return S_OK(result['Value'][0][0])
  
  def __getGuidFromFileIDs(self,fileID):
    """ Get GUID of the given list of files """
    
    if type(fileID) != ListType:
      fileList = [fileID]
    else:
      fileList = fileID  
    
    fileString = ','.join([ str(x) for x in fileList])
    
    req = "SELECT FileID,GUID FROM FC_GUID_to_File WHERE FileID in (%s)" % fileString
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File not found')
    fileDict = {}
    for row in result['Value']:
      fileDict[row[0]] = row[1]
      
    return S_OK(fileDict)

  def __setFileOwner(self,fileID,owner):
    """ Set the file owner """
    result = self.db.ugManager.findUser(owner)
    if not result['OK']:
      return result
    userID = result['Value']
    req = 'UPDATE FC_FileInfo SET UID=%d WHERE FileID=%d' % (int(userID),int(fileID))
    return self.db._update(req) 

  def __setFileGroup(self,fileID,group):
    """ Set the file group """
    result = self.db.ugManager.findGroup(group)
    if not result['OK']:
      return result
    groupID = result['Value']
    req = 'UPDATE FC_FileInfo SET GID=%d WHERE FileID=%d' % (int(groupID),int(fileID))    
    return self.db._update(req) 

  def __setFileMode(self,fileID,mode):
    """ Set the file mode """
    req = 'UPDATE FC_FileInfo SET Mode=%d WHERE FileID=%d' % (int(mode),int(fileID))
    return self.db._update(req) 

  def __getMetadataForFileIDs(self,fileIDs,fields):
    req = "SELECT FileID,%s FROM FC_FileInfo WHERE FileID IN (%s)" % (intListToString(fields),intListToString(fileIDs))
    return self.db._query(req)

  def _getMetadataForLFNs(self,lfns,requestedFields):
    # Get the fileIDs for the supplied files
    result = self.findFile(lfns.keys())
    if not result['OK']:
      return result
    failed = result['Value']['Failed']
    fileIDLFNs = {}
    for lfn,fileID in result['Value']['Successful'].items():
      fileIDLFNs[fileID] = lfn
    
    successful = {}  
    # Get the mapping of fileID to size
    res = self.__getMetadataForFileIDs(fileIDLFNs.keys(),requestedFields)  
    if not res['OK']:
      for lfn in fileIDLFNs.values():
        failed[lfn] = res['Message']
    else:
      for tuple in res['Value']:
        fileID = tuple[0]
        metadata = dict(zip(requestedFields,tuple[1:]))
        # A.T. Hack to be consistent with the LcgFileCatalogClient
        if metadata.has_key('CheckSum'):
          metadata['CheckSumValue'] = metadata['CheckSum']
        if metadata.has_key('Mode'): 
          metadata['Permissions'] = metadata['Mode'] 
        if metadata.has_key('ModificationDate'):
          metadata['ModificationTime'] = metadata['ModificationDate']
        if metadata.has_key('UID'):
          owner = 'unknown'
          uid = metadata['UID']
          if uid == 0:
            owner = 'root'
          else:  
            resGet = self.db.ugManager.getUserName(uid)
            if resGet['OK']:
              owner = resGet['Value'] 
        if metadata.has_key('GID'):
          metadata['Owner'] = owner
          gid = metadata['GID']
          group = 'unknown'
          if gid == 0:
            group = 'root'
          else:  
            resGet = self.db.ugManager.getGroupName(gid)      
            if resGet['OK']:
              group = resGet['Value']  
          metadata['OwnerGroup'] = group  

        successful[fileIDLFNs[fileID]] = metadata  
    
    # Ensure all the files are in the result
    for lfn in lfns:
      if (not lfn in failed.keys()) and (not lfn in successful.keys()):
        # must have been removed between findFile and __getSizeForFileIDs (pretty damn unlikely)
        failed[lfn] = 'File not found' 
    return S_OK({'Successful':successful,'Failed':failed,'FileIDDict':fileIDLFNs}) 

  def __getFileDirectories(self,lfns):
    dirDict = {}
    for lfn in lfns:
      lfnDir = os.path.dirname(lfn)
      lfnFile = os.path.basename(lfn)
      if not lfnDir in dirDict:
        dirDict[lfnDir] = []
      dirDict[lfnDir].append(lfnFile)
    return dirDict

  def __purgeFiles(self,fileIDs):
    fileIDString = intListToString(fileIDs)
    failed = []
    for table in ['FC_Files','FC_FileInfo','FC_GUID_to_File']:
      req = "DELETE FROM %s WHERE FileID in (%s)" % (table,fileIDString)
      res = self.db._update(req)
      if not res['OK']:
        gLogger.error("Failed to remove files from %s" % table,res['Message'])
        failed.append(table)
    if failed:
      return S_ERROR("Failed to remove files from %s" % stringListToString(failed))
    return S_OK()

  def __deleteReplicaForFileID(self,fileID,seID):
    req = "DELETE FROM FC_Replicas WHERE FileID=%d and SEID=%d" % (fileID,seID)    
    return self.db._update(req)

  def __deleteReplica(self,repID):
    """ Delete replica specified by repID """
    return self.__deleteReplicas([repID])
    
  def __deleteReplicas(self,repIDs):
    repIDString = intListToString(repIDs)
    failed = []
    for table in ['FC_Replicas','FC_ReplicaInfo']:
      req = "DELETE FROM %s WHERE RepID in (%s)" % (table,repIDString)
      res = self.db._update(req)
      if not res['OK']:
        gLogger.error("Failed to remove replicas from %s" % table,res['Message'])
        failed.append(table)
    if failed:
      return S_ERROR("Failed to remove replicas from %s" % stringListToString(failed))
    return S_OK()

  def __existsReplica(self,fileID,se):
    """ Check if a replica already exists """    
    seID = se
    if type(se) in StringTypes:
      result = self.db.seManager.findSE(se)
      if not result['OK']:
        return result
      seID = result['Value']      
    req = "SELECT RepID FROM FC_Replicas WHERE FileID=%d AND SEID=%d" % (fileID,seID)
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      result = S_OK(0)
      result['Exists'] = False
    else:
      repID = result['Value'][0][0]
      result = S_OK(repID)
      result['Exists'] = True
      result['SEID'] = seID
    return result    

  def __addReplica(self,fileID,se,pfn='',rtype='Master'):
    """ Add a replica to the file catalog
    """
    result = self.__existsReplica(fileID,se)
    if not result['OK']:
      return result
    if result['Exists']:
      repID = result['Value']
      return S_OK(repID)

    seID = se
    if type(se) in StringTypes:
      result = self.db.seManager.findSE(se)
      if not result['OK']:
        return result
      seID = result['Value']
      
    result = self.db._insert('FC_Replicas',['FileID','SEID'],[fileID,seID])
    if not result['OK']:
      return result
    repID = result['lastRowId']
    req = "INSERT INTO FC_ReplicaInfo (RepID,RepType,Status,CreationDate,ModificationDate,PFN) VALUES "
    req += "(%d,'%s',%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (repID,rtype,0,pfn)    
    result = self.db._update(req)    
    if not result['OK']:
      result = self.__deleteReplica(repID)
      return S_ERROR('Failed to add replica info')
    
    return S_OK(repID)
  
  def getFilesInDirectory(self,dirID,path,verbose=False):
    req = "SELECT FileID,FileName FROM FC_Files WHERE DirID=%d" % dirID
    result = self.db._query(req)
    if not result['OK']:
      return result
    files = {}
    for fileID,fName in result['Value']:
      lfn = "%s/%s" % (path,fName)
      if verbose:
        result = self.getFileInfo(fileID)
        if not result['OK']:
          files[lfn] = False
        else:
          files[lfn] = result['Value']  
      else:  
        files[lfn] = True
    return S_OK(files)
  
  def getReplicas(self,lfns,allStatus=False):
    """ Get Replicas for the given LFNs """
    files = lfns.keys()    
    start = time.time()
    result = self.findFile(files) 
    gLogger.debug("findFiles %s" % (time.time()-start))

    start = time.time()
    fileDict = result['Value']
    if not fileDict["Successful"]:
      return S_OK(fileDict)
    
    failed = fileDict['Failed']
    successful = {}
    
    lfnDict = {}
    for lfn,id in fileDict['Successful'].items():
      if id:
        lfnDict[id] = lfn
    
    fileIDString = ','.join([str(id) for id in lfnDict.keys()])
    
    start = time.time()
    req = "SELECT RepID,FileID,SEID FROM FC_Replicas WHERE FileID in (%s)" % fileIDString    
    result = self.db._query(req)
    if not result['OK']:
      for id,lfn in lfnDict.items():
        failed[lfn] = result["Message"]
    
    if not result['Value']:
      for id,lfn in lfnDict.items():
        failed[lfn] = 'No replicas found'
    
    for row in result['Value']:
      repID,fileID,seID = row
      lfn = lfnDict[fileID]
      if not self.db.resolvePfn:
        res = self.__getReplicaPFN(repID)
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          pfn = res['Value']
          res = self.db.seManager.getSEName(seID)
          if not res['OK']:
            failed[lfn] = res['Message']
          else:  
            if not successful.has_key(lfn):
              successful[lfn] = {}  
            successful[lfn][res['Value']] = pfn
      else:
        resSE = self.db.seManager.getSEDefinition(seID)     
        if resSE['OK']:
          seDict = resSE['Value']['SEDict']
          se = resSE['Value']['SEName']
          # Construct PFN
          pfnDict = dict(seDict)
          pfnDict['FileName'] = lfn
          result = pfnunparse(pfnDict)
          if not result['OK']:
            failed[lfn] = result['Message']
            continue
          if not successful.has_key(lfn):
            successful[lfn] = {}  
          successful[lfn][se] = result['Value']
        else:
          failed[lfn] = resSE['Message']
    gLogger.debug("findReps %s" % (time.time()-start))
    return S_OK({'Successful':successful,'Failed':failed})  

  def __getReplicaPFN(self,repID):
    req = "SELECT PFN FROM FC_ReplicaInfo WHERE RepID = %d" % repID
    result = self.db._query(req)
    if not result['OK']:
      return S_ERROR()
    return S_OK(result['Value'][0][0])

  def __addLFN(self,lfn,dirID=0):
    """ Create new LFN entry
    """
    
    gLogger.debug("addLFN %s %s" % (lfn,dirID))

    if not dirID:
      dirPath = os.path.dirname(lfn)
      result = self.db.dtree.findDir(dirPath)      
      if not result['OK']:
        return result
      dID = result['Value']
      if not dID:
        return S_ERROR('Directory %s: not found' % dirPath)
    else:
      dID = dirID  
    
    fileName = os.path.basename(lfn)    
    result = self.db._insert('FC_Files',['DirID','FileName'],[dID,fileName])    
    if not result['OK']:
      return result
      
    return S_OK(result['lastRowId'])

  def __addFile(self,lfn,credDict,pfn='',size=0,se='',guid='',checksum='',checksumtype=''):
    """Add (register) a file to the catalog. The file is specified by its
       logical file name lfn, physical replica pfn, size, storage element se
       and global unique identifier guid
    """

    start = time.time()
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    ( uid, gid ) = result['Value']

    # Check if the lfn already exists
    fileID = 0
    resExists = self.__exists(lfn)
    if not resExists['OK']:
      return resExists
    if resExists['Value']:
      fileID = resExists['Value']
      # Check file GUID
      result = self.__getGuidFromFileID(fileID)      
      if not result['OK']:
        return result
      eguid = result['Value']
      if guid and (eguid != guid):
        return S_ERROR('GUID mismatch')  
      
      gLogger.debug("addFile existence checks %.4f" % (time.time()-start))
      start = time.time()  
      return S_OK(eguid)
      
    # Check if the GUID already exists   
    if guid:
      resGuid = self.__getFileIDFromGUID(guid)
      if resGuid['OK']:
        gfileID = resGuid['Value']
        if fileID and fileID != gfileID:
          return S_ERROR('GUID already exists for another file, consider making a link')
      elif resGuid['Message'] != 'GUID not found':
        return S_ERROR('Failed to check the GUID existence')
      
    gLogger.debug("addFile initial checks %.4f" % (time.time()-start))
    start = time.time()  
    
    # Add file if not yet there
    if not fileID:
      # Evaluate the file GUID
      fileGUID = guid
      if not guid:
        fileGUID = generateGuid(checksum,checksumtype)
  
      # Create the file directory if necessary
      dirID = 0
      directory = os.path.dirname(lfn)
      result = self.db.dtree.makeDirectories(directory,credDict)
      if not result['OK']:
        return result
      dirID = result['Value']
      if not dirID:
        return S_ERROR('Failed to create (or find) the file directory')
      
      gLogger.debug("addFile made directories %.4f" % (time.time()-start))
      start = time.time()
  
      # Create the file record
      result = self.__addLFN(lfn,dirID)      
      if not result['OK']:
        return result
      fileID = result['Value']
      
      gLogger.debug("addFile added LFN %.4f" % (time.time()-start))
      start = time.time()      
      req = "INSERT INTO FC_FileInfo (FileID,Size,CheckSum,CheckSumType,UID,GID,CreationDate," 
      req = req + "ModificationDate,Mode,Status) VALUES "
      req = req + "(%d,%d,'%s','%s',%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,0)" % (fileID,size,checksum,checksumtype,uid,gid,self.db.umask)                        
      resAdd = self.db._update(req)            
      if resAdd['OK']:
        req = "INSERT INTO FC_GUID_to_File (GUID,FileID) VALUES ('%s','%s')" % (fileGUID,fileID)        
        resGuid = self.db._update(req)
        if resGuid['OK']:
          result = S_OK()
          result['GUID'] = fileGUID
        else:
          self.__purgeFiles([fileID])
          result = S_ERROR('Failed to register the file guid')
      else:
        self.__purgeFiles([fileID])
        result = S_ERROR('Failed to add file info')
      if not result['OK']:
        return result  
      
    gLogger.debug("addFile added File Info %.4f" % (time.time()-start))
    start = time.time()  

    if se:
      result = self.__addReplica(fileID,se,pfn)
      if not result['OK']:
        return result

    return result
  
  def getFileInfo(self,fileID):
    """ Get file information for the given file ID
    """ 
    
    req = "SELECT FileID,Size,CheckSum,CheckSumType,UID,GID,CreationDate,ModificationDate,Mode,Status"
    req += " FROM FC_FileInfo WHERE FileID=%d" % fileID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    resultDict = {}
    resultDict['FileID'] = result['Value'][0][0]
    resultDict['Size'] = result['Value'][0][1]
    resultDict['CheckSum'] = result['Value'][0][2]
    resultDict['CheckSumType'] = result['Value'][0][3]
    
    uid = int(result['Value'][0][4])
    resultDict['UID'] = uid
    owner = 'unknown'
    if uid == 0:
      owner = 'root'
    else:  
      resGet = self.db.ugManager.getUserName(uid)
      if resGet['OK']:
        owner = resGet['Value'] 
    resultDict['Owner'] = owner
    gid = int(result['Value'][0][5])
    resultDict['GID'] = gid
    group = 'unknown'
    if gid == 0:
      group = 'root'
    else:  
      resGet = self.db.ugManager.getGroupName(gid)      
      if resGet['OK']:
        group = resGet['Value']  
    resultDict['OwnerGroup'] = group
    
    resultDict['CreationTime'] = result['Value'][0][6]
    resultDict['ModificationTime'] = result['Value'][0][7]
    resultDict['Permissions'] = result['Value'][0][8]
    resultDict['Status'] = result['Value'][0][9]
    
    req = "SELECT DirID,FileName from FC_Files WHERE FileID=%d" % fileID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    resultDict['DirID'] = result['Value'][0][0]
    resultDict['FileName'] = result['Value'][0][1]
    
    # ToDo: Number of links to be evaluated
    resultDict['NumberOfLinks'] = 0
    
    dirID = resultDict['DirID']
    result = self.db.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    resultDict['LFN'] = dirPath+'/'+resultDict['FileName']      
    return S_OK(resultDict)    
#####################################################################
#
#  End of FileManager code
#
#####################################################################

  def setReplicaStatus(self,lfns):
    return S_ERROR()

  def getReplicaStatus(self,lfns):
    return S_ERROR()
  
  def setReplicaHost(self,lfns):
    return S_ERROR()

  
  def exists(self,lfns):
    successful = {}
    failed = {}
    for lfn in lfns:
      res = self.__exists(lfn)  
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    return S_OK({'Successful':successful,'Failed':failed})

  def __exists(self,lfn):
    res = self.findFile([lfn])
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Failed to find file")
    if res['Value']['Successful'].has_key(lfn):
      return S_OK(res['Value']['Successful'][lfn])
    if res['Value']['Failed'].has_key(lfn):
      error = res['Value']['Failed'][lfn]
      if error == 'No such file or directory':
        return S_OK(False)
      else:
        return S_ERROR(error)
    return S_ERROR("Completely failed to find file")  

  def existsLFNs(self, lfns):
    return self.findFile(lfns)

  def __getFileLFN(self,fileID):
    """ Get LFN of the given file
    """
    result = self.getFileInfo(fileID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    dirID = result['Value']['DirID']
    fname = result['Value']['FileName']
    result = self.db.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    return S_OK(dirPath+'/'+fname)
  
  def addFile(self,lfns,credDict):
    """ Add files to the catalog
    """  
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      if not info:
        return S_ERROR( 'Missing Replica information for LFN: %s' % lfn )
      for key in [ 'PFN', 'SE', 'Size', 'GUID', 'Checksum']:
        if not key in info:
          return S_ERROR( 'Missing "%s" for LFN: %s' % ( key, lfn ) )      
      pfn = info['PFN']
      se = info['SE']
      size = int(info['Size'])
      guid = info['GUID']
      checksum = info['Checksum']
      result = self.__addFile(lfn,credDict,pfn,size,se,guid,checksum)
      if not result['OK']:
        failed[lfn] = result['Message']
      else:
        successful[lfn] = True
        
    return S_OK({'Successful':successful,'Failed':failed})      
    
  def getFileLFN(self,fileID):
    """ Get file LFN for the given fileID
    """
    req = "SELECT DirID,FileName from FC_Files WHERE FileID=%d" % fileID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    dirID = result['Value'][0][0]
    fname = result['Value'][0][1]
    
    result = self.db.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    lfn = dirPath+'/'+fname
    
    return S_OK(lfn)
  
  def removeFile(self,lfns):
    """ Bulk file removal method """
    successful = {}
    failed = {}
    files = lfns.keys()
    result = self.findFile(files)     
    fileDict = result['Value']
    
    failed = fileDict['Failed']
    for lfn,error in failed.items():
      if error == 'No such file or directory':
        fileDict['Successful'][lfn] = True
        fileDict['Failed'].pop(lfn)

    if not fileDict["Successful"]:
      return S_OK(fileDict)
    
    lfnDict = {}
    for lfn,id in fileDict['Successful'].items():
      if id:
        lfnDict[id] = lfn
    res = self.__purgeFiles(lfnDict.keys())
    if res['OK']:
      successful = fileDict['Successful']
    else:
      failed.update(fileDict['Successful'])
    return S_OK({'Successful':successful,'Failed':failed})    
  
  def changeFileOwner(self,lfns,s_uid=0,s_gid=0):
    """ Bulk method to set the file owner
    """
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
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
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
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
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
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

  
##################################################################### 
  def isFile(self,lfns,s_uid=0,s_gid=0):
    """ Check for the existence of files """
    result = self.findFile(lfns)
    if not result['OK']:
      return result
    
    successful = {}
    failed = {}
    for lfn in lfns:
      if lfn in result['Value']['Successful']:
        successful[lfn] = True 
      else:
        successful[lfn] = False   
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
    result = self.db.isDirectory(paths,credDict)    
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
    return self.__changePathFunction(paths,credDict,self.db.dtree.changeDirectoryOwner,self.changeFileOwner)
  
  def changePathGroup(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths
    """
    return self.__changePathFunction(paths,credDict,self.db.dtree.changeDirectoryGroup,self.changeFileGroup)
  
  def changePathMode(self,paths,credDict):  
    """ Bulk method to change Owner for the given paths
    """
    return self.__changePathFunction(paths,credDict,self.db.dtree.changeDirectoryMode,self.changeFileMode)

#########################################################################
#
#  Replica related methods
#    
  
  

  def __checkLFNPFNConvention(self,lfn,pfn,se):
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
    return S_OK()
  
    # Check if the pfn corresponds to the SE definition
    result = self.__getStorageElement(se)
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
      return S_ERROR('The given PFN %s does not correspond to the %s SE definition' % \
                     (pfn,se))

    # Check the full LFN-PFN-SE convention
    lfn_pfn_se = True
    if lfn_pfn:
      seAccessDict = dict(seDict)
      seAccessDict['Path'] = sePath + '/' + lfn
      check_pfn = pfnunparse(seAccessDict)
      if check_pfn != pfn:
        return S_ERROR('PFN does not correspond to the LFN convention')

    return S_OK()

  def __getStorageElement(self, seName):
    """
    """
    from DIRAC.Resources.Storage.StorageElement              import StorageElement
    storageElement = StorageElement(seName)
    if not storageElement.valid:
      return S_ERROR(storageElement.errorReason)
    return S_OK(storageElement)
    
#####################################################################
  def addReplica(self,lfns):
    """ Add replica pfn in storage element se for the file specified by its lfn
        to the catalog. Pass optionally guid for extra verification
    """
    successful = {}
    failed = {}
    for lfn,info in lfns.items():
      if not info:
        failed[lfn] = 'SE and PFN dict not supplied'
        continue
      for key in [ 'PFN', 'SE' ]:
        if not key in info.keys():
          failed[lfn] = 'Missing %s' % key
          continue
      pfn = info['PFN']
      se = info['SE']

      # Check if the lfn already exists
      fileID = 0
      resExists = self.__exists(lfn)
      if not resExists['OK']:
        failed[lfn] = resExists['Message']
        continue
      if resExists['Value']:
        fileID = resExists['Value']
      else:
        failed[lfn] = "LFN %s: does not exist" % lfn  
        continue
  
      # Check that the replica does not yet exist
      result = self.__existsReplica(fileID,se)      
      if not result['OK']:
        failed[lfn] = result['Message']
        continue
      repID = result['Value']
      if repID:
        # Replica already exists. 
        successful[lfn] = 'Replica already exists %d' % repID
        continue  
      if pfn:
        if self.db.lfnPfnConvention:
          result = self.__checkLFNPFNConvention(lfn,pfn,se)          
          if not result['OK']:
            failed[lfn] = result['Message']
            continue
          result = self.__addReplica(fileID,se,'')
        else:
          result = self.__addReplica(fileID,se,pfn)  
      else:
        result = self.__addReplica(fileID,se,'')        
      if not result['OK']:
        failed[lfn] = result['Message']
        continue
      else:
        successful[lfn] = repID
      
    return S_OK({'Successful':successful,'Failed':failed})
  
#####################################################################  

  def removeReplica(self,lfns):
    """ Bulk replica removal method """
    files = lfns.keys()
    result = self.findFile(files)     
    fileDict = result['Value']
    if not fileDict["Successful"]:
      return S_OK(fileDict)
    
    failed = fileDict['Failed']
    successful = {}
    
    lfnDict = {}
    for lfn,id in fileDict['Successful'].items():
      if id:
        lfnDict[lfn] = id
    
    for lfn,info in lfns.items():
      se = info['SE']
      result = self.db.seManager.findSE(se)
      if not result['OK']:
        failed[lfn] = result['Message']
        continue
      seID = result['Value']
      fileID = lfnDict[lfn]
      result = self.__deleteReplicaForFileID(fileID, seID)
      if not result['OK']:
        failed[lfn] = result['Message']
      else:
        successful[lfn]=True  
      
    return S_OK({'Successful':successful,'Failed':failed})
