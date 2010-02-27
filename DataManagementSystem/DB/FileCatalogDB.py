########################################################################
# $Id$
########################################################################
""" DIRAC FileCatalog Database

    This is the database backend of the DIRAC File Catalog

    The interface supports the following methods:

    addUser
    addGroup
    makeDir
    makeDirs
    exists
    existsDir
    addFile
    addPfn
"""

__RCSID__ = "$Id$"

import re, os, sys, md5, random
import string, time, datetime
import threading
from types import *

from DIRAC                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                     import DB
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SEManager             import SEManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.UserAndGroupManager   import UserAndGroupManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryMetadata     import DirectoryMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectorySimpleTree   import DirectorySimpleTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryNodeTree     import DirectoryNodeTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryLevelTree    import DirectoryLevelTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities             import * 

DEBUG = 0
     
#############################################################################
class FileCatalogDB(DB,
                    SEManager,
                    UserAndGroupManager,
                    DirectoryMetadata):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'FileCatalogDB','DataManagement/FileCatalogDB',maxQueueSize)
    # In memory storage of the directory parameters
    self.directories = {}
    # In memory storage of the SE definitions
    self.seDefinitions = {}
    self.seNames = {}
    self.SEUpdatePeriod = 600
    # Operational flags
    self.LFN_PFN_convention = True
    self.globalReadAccess = True
    # Directory Tree instance
    self.dtree = DirectoryLevelTree()
    self.dtree.setDatabase(self)
    # umask default setting
    self.umask = 0775
    self.users = {}
    self.groups = {}
    
  def setUmask(self,umask):
    
    self.umask = umask

#####################################################################
#
#  Directories related methods
#
#####################################################################
  def makeDirectory(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary
       containing all the parameters of the newly created directory
    """

    if path[0] != '/':
      return S_ERROR('Not an absolute path')

    result = self.dtree.findDir(path)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'])

    if path == '/':
      # Create the root directory
      l_uid = 0
      l_gid = 0
    else:
      result = self.getUserAndGroupID(credDict)
      if not result['OK']:
        return result
      ( l_uid, l_gid ) = result['Value']

    dirDict = {}
    result = self.dtree.makeDir(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
    req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % (dirID,l_uid,l_gid,self.umask,status)            
    result = self._update(req)            
    if result['OK']:
      resGet = self.getDirectoryParameters(dirID)            
      if resGet['OK']:
        dirDict = resGet['Value']

    if not dirDict:
      result = self.dtree.removeDir(path)
      return S_ERROR('Failed to create directory %s' % path)
    return S_OK(dirID)

#####################################################################
  def makeDirectories(self,path,credDict):
    """Make all the directories recursively in the path. The return value
       is the dictionary containing all the parameters of the newly created
       directory
    """
    result = self.existsDir(path)
    if not result['OK']:
      return result
    if result['Exists']:
      return S_OK(result['Value']) 

    if path == '/':
      result = self.makeDirectory(path,credDict)
      return result

    parentDir = os.path.dirname(path)
    result = self.existsDir(parentDir)
    if not result['OK']:
      return result
    if result['Exists']:
      result = self.makeDirectory(path,credDict)
    else:
      result = self.makeDirectories(parentDir,credDict)
      if not result['OK']:
        return result
      result = self.makeDirectory(path,credDict)

    return result

#####################################################################
  def existsDir(self,path):
    """ Check the existence of the directory path
    """
    result = self.dtree.findDir(path)
    if not result['OK']:
      return result
    if result['Value']:
      result = S_OK(int(result['Value']))
      result['Exists'] = True
    else:
      result = S_OK(0)
      result['Exists'] = False 

    return result
  
  #####################################################################
  def isDirectory(self,paths,credDict):
    """ Checking for existence of directories
    """
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    dirs = arguments.keys()
    successful = {}
    failed = {}
    for dir in dirs:
      result = self.existsDir(dir)
      if not result['OK']:
        failed[dir] = result['Message']
      elif result['Exists']:
        successful[dir] = True
      else: 
        successful[dir] = False  
          
    return S_OK({'Successful':successful,'Failed':failed})
  
  #####################################################################
  def createDirectory(self,arguments,credDict):
    """ Checking for existence of directories
    """
    result = checkArgumentFormat(arguments)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    dirs = arguments.keys()
    successful = {}
    failed = {}
    for dir in dirs:
      result = self.makeDirectories(dir,credDict)
      if not result['OK']:
        failed[dir] = result['Message']
      else: 
        successful[dir] = True  
          
    return S_OK({'Successful':successful,'Failed':failed}) 

#####################################################################
  def removeDirectory(self,dirname,force=False):
    """Remove an empty directory from the catalog
    """

    pass

#####################################################################
  def __getDirID(self,path):
    """ Get directory ID from the given path or already evaluated ID
    """

    if type(path) in StringTypes:
      result = self.dtree.findDir(path)
      if not result['OK']:
        return result
      dirID = result['Value']
      if not dirID:
        return S_ERROR('%s: not found' % str(path) )
      return S_OK(dirID)
    else:
      return S_OK(path)

#####################################################################
  def getDirectoryParameters(self,path):
    """ Get the given directory parameters
    """

    result = self.__getDirID(path)
    if not result['OK']:
      return result
    dirID = result['Value']

    query = "SELECT DirID,UID,GID,Status,Mode,CreationDate,ModificationDate from FC_DirectoryInfo"
    query = query + " WHERE DirID=%d" % dirID
    resQuery = self._query(query)
    if not resQuery['OK']:
      return resQuery

    if not resQuery['Value']:
      return S_ERROR('Directory not found')

    dirDict = {}
    dirDict['DirID'] = int(resQuery['Value'][0][0])
    uid = int(resQuery['Value'][0][1])
    dirDict['UID'] = uid
    owner = 'unknown'
    result = self.getUserName(uid)
    if result['OK']:
      owner = result['Value'] 
    dirDict['Owner'] = owner
    gid = int(resQuery['Value'][0][2])
    dirDict['GID'] = int(resQuery['Value'][0][2])
    group = 'unknown'
    result = self.getGroupName(gid)
    if result['OK']:
      group = result['Value']  
    dirDict['OwnerGroup'] = group
    dirDict['Status'] = int(resQuery['Value'][0][3])
    dirDict['Permissions'] = int(resQuery['Value'][0][4])
    dirDict['CreationTime'] = resQuery['Value'][0][5]
    dirDict['ModificationTime'] = resQuery['Value'][0][6]

    return S_OK(dirDict)

#####################################################################
  def __setDirectoryParameter(self,path,pname,pvalue):
    """ Set a numerical directory parameter
    """
    result = self.__getDirID(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    req = "UPDATE FC_DirectoryInfo SET %s=%d WHERE DirID=%d" % (pname,pvalue,dirID)    
    result = self._update(req)
    return result

#####################################################################
  def __setDirectoryUid(self,path,uid):
    """ Set the directory owner
    """
    return self.__setDirectoryParameter(path,'UID',uid)

#####################################################################
  def __setDirectoryGid(self,path,gid):
    """ Set the directory group
    """
    return self.__setDirectoryParameter(path,'GID',gid)

#####################################################################
  def setDirectoryOwner(self,path,owner):
    """ Set the directory owner
    """

    result = self.__getDirID(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    result = self.findUser(owner)
    uid = result['Value']
    result = self.__setDirectoryUid(dirID,uid)
    return result
  
#####################################################################
  def changeDirectoryOwner(self,paths,s_uid=0,s_gid=0):
    """ Bulk setting of the directory owner
    """  
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,owner in arguments.items():
      result = self.setDirectoryOwner(path,owner)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True
        
    return S_OK({'Successful':successful,'Failed':failed})      

#####################################################################
  def setDirectoryGroup(self,path,gname):
    """ Set the directory owner
    """

    result = self.__getDirID(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    result = self.findGroup(gname)
    gid = result['Value']
    result = self.__setDirectoryGid(dirID,gid)
    return result
  
#####################################################################
  def changeDirectoryGroup(self,paths,s_uid=0,s_gid=0):
    """ Bulk setting of the directory owner
    """  
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,group in arguments.items():
      result = self.setDirectoryGroup(path,group)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True
        
    return S_OK({'Successful':successful,'Failed':failed})        

#####################################################################
  def setDirectoryMode(self,path,mode):
    """ set the directory mask
    """
    return self.__setDirectoryParameter(path,'Mode',mode)
  
#####################################################################
  def changeDirectoryMode(self,paths,s_uid=0,s_gid=0):
    """ Bulk setting of the directory owner
    """  
    result = self.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,mode in arguments.items():
      result = self.setDirectoryMode(path,mode)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True
        
    return S_OK({'Successful':successful,'Failed':failed})     

  #####################################################################
  def setDirectoryStatus(self,path,status):
    """ set the directory mask
    """
    return self.__setDirectoryParameter(path,'Status',status)

  def getPathPermissions(self, lfns, credDict):
    """ Get permissions for the given user/group to manipulate the given lfns 
    """
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    paths = result['Value'].keys()
    
    successful = {}
    failed = {}
    for path in paths:
      result = self.getDirectoryPermissions(path,credDict)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']
        
    return S_OK({'Successful':successful,'Failed':failed}) 
  
  #####################################################################
  def getDirectoryPermissions(self,path,credDict):
    """ Get permissions for the given user/group to manipulate the given directory 
    """ 
    
    resultDict = {}
    if self.globalReadAccess:
      resultDict['Read'] = True
      
    resultDict['Write'] = True
    resultDict['Execute'] = True
    return S_OK(resultDict)
  
  def __getFilesInDirectory(self,dirID):
    """ Get file IDs for the given directory
    """
    
    req = "SELECT FileID FROM FC_Files WHERE DirID=%d" % dirID
    result = self._query(req)
    if not result['OK']:
      return result
    
    fileList = [ row[0] for row in result['Value'] ]
    return S_OK(fileList)
    
  
  def __getDirectoryContents(self,path,details=False):
    """ Get contents of a given directory
    """
    
    result = self.dtree.findDir(path)
    if not result['OK']:
      return result
    directoryID = result['Value']
    
    directories = {}
    files = {}
    links = {}
    result = self.dtree.getChildren(path)
    if not result['OK']:
      return result
    
    # Get subdirectories
    dirIDList = result['Value']
    for dirID in dirIDList:
      result = self.dtree.getDirectoryName(dirID)
      if not result['OK']:
        return result
      dirName = result['Value']
      if details:
        result = self.getDirectoryParameters(dirID)
        if not result['OK']:
          directories[dirName] = False
        else:
          directories[dirName] = result['Value']
      else:    
        directories[dirName] = True
                  
    # Get files
    result = self.__getFilesInDirectory(directoryID)
    if not result['OK']:
      return result
    fileIDList = result['Value']
    for fileID in fileIDList:
      result = self.getFileLFN(fileID)
      if not result['OK']:
        return result
      fname = os.path.basename(result['Value'])
      if details:
        result = self.getFileInfo(fileID)
        if not result['OK']:
          files[fname] = False
        else:
          files[fname] = result['Value']  
      else:  
        files[fname] = True
      
    # Get links                
    pass
               
    pathDict = {'Files': files,'SubDirs':directories,'Links':links}    
    return S_OK(pathDict)           

  def listDirectory(self,lfns,credDict,verbose=False):
    """ Get the directory listing
    """
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    paths = arguments.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.__getDirectoryContents(path,details=verbose)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']
        
    return S_OK({'Successful':successful,'Failed':failed})      
    

#####################################################################
#
#  File and replica related methods
#
#####################################################################

  def findFile(self,lfns):
    """ Find file ID if it exists for the given list of LFNs
    """
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    lfnsDict = result['Value']
    
    dirDict = {}
    for lfn in lfnsDict:
      dirPath = os.path.dirname(lfn)
      if not dirPath in dirDict:
        dirDict[dirPath] = [os.path.basename(lfn)]
      else:
        dirDict[dirPath] += [os.path.basename(lfn)]
    
    resultDict = {}
    count = 0     
    successful = {}
    failed = {}
    for dirPath in dirDict:
      start = time.time()
      result = self.dtree.findDir(dirPath)
      if not result['OK']:
        for fileName in dirDict[dirPath]:
          failed[dirPath+'/'+fileName] = result['Message']
      
      dirID = result['Value']      
      #count += 1
      #print dirPath,dirID,count
      
      if not dirID:
        for fileName in dirDict[dirPath]:
          failed[dirPath+'/'+fileName] = "File not found"
        continue  
    
      if len(dirDict[dirPath]) == 1:
        fileName = dirDict[dirPath][0]
        req = "SELECT FileID,FileName from FC_Files WHERE DirID=%d and FileName='%s'" % (dirID,fileName)            
        result = self._query(req)
        if not result['OK']:
          failed[dirPath+'/'+fileName] = result['Message']
      
        if not result['Value']:
          failed[dirPath+'/'+fileName] = "File not found"
        else:
          successful[dirPath+'/'+fileName] = result['Value'][0][0]  
        
      else:  
        fileString = "'"+"','".join(dirDict[dirPath])+"'"        
        req = "SELECT FileID,FileName from FC_Files WHERE DirID=%d and FileName in (%s)" % (dirID,fileString)            
        result = self._query(req)
        if not result['OK']:
          for fileName in dirDict[dirPath]:
            failed[dirPath+'/'+fileName] = result['Message']
      
        if not result['Value']:
          for fileName in dirDict[dirPath]:
            failed[dirPath+'/'+fileName] = "File not found"
              
        for row in result['Value']:
          fileID = row[0]
          fileName = row[1]
          successful[dirPath+"/"+fileName] = fileID
            
    return S_OK({"Successful":successful,"Failed":failed})
  
  def exists(self,lfn):
    """ Check if a given LFN exists
    """
    result = self.findFile(lfn)    
    if not result['OK']:
      return result  
    if not result['Value']:
      result = S_ERROR('File look up failed')
      return result
    
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][lfn]
      result = S_OK(fileID)
      result['Exists'] = True
    else:
      result = S_OK()
      result['Exists'] = False
        
    return result
  
  def existsLFNs(self, lfns, credDict):
    """ Check if the LFNs exists
    """
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    paths = result['Value'].keys()
    
    successful = {}
    failed = {}
    for path in paths:
      result = self.exists(path)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']
        
    return S_OK({'Successful':successful,'Failed':failed}) 

  def getLFNSize(self, lfns, credDict):
    """ Return Size of LFN
    """
    result = self.findFile(lfns)
    if not result['OK']:
      return result
    successful = dict( result['Value']['Successful'] )
    failed = result['Value']['Failed']
    for lfn,fileID in result['Value']['Successful'].items():
      req = "SELECT Size FROM FC_FileInfo WHERE FileID=%d" % fileID
      result = self._query(req)
      if not result['OK']:
        successful.pop(lfn)
        failed.append[lfn] = result['Message']
      elif not result['Value']:
        successful.pop(lfn)
        failed.append[lfn] = 'File not Found'
      else:        
        successful[lfn] = result['Value'][0][0]
      
    return S_OK({'Successful':successful,'Failed':failed}) 

  def getLFNMetadata(self, lfns, credDict):
    """ Return Size, of LFN
    """
    result = self.findFile(lfns)
    if not result['OK']:
      return result
    print result
    successful = dict( result['Value']['Successful'] )
    failed = result['Value']['Failed']
    for lfn,fileID in result['Value']['Successful'].items():
      req = "SELECT Size,CheckSum,CheckSumType FROM FC_FileInfo WHERE FileID=%d" % fileID
      result = self._query(req)
      if not result['OK']:
        successful.pop(lfn)
        failed[lfn] = result['Message']
      elif not result['Value']:
        successful.pop(lfn)
        failed[lfn] = 'File not Found'
      else:
        successful[lfn] = {}
        successful[lfn]['Size'] = result['Value'][0][0]
        successful[lfn]['CheckSumValue'] = result['Value'][0][1]
        successful[lfn]['CheckSumType'] = result['Value'][0][2]
      
    return S_OK({'Successful':successful,'Failed':failed}) 

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
    result = self.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    return S_OK(dirPath+'/'+fname)
  
  def __addLFN(self,lfn,dirID=0):
    """ Create new LFN entry
    """
    
    if DEBUG:
      print "addLFN",lfn,dirID
    
    if not dirID:
      dirPath = os.path.dirname(lfn)
      result = self.dtree.findDir(dirPath)      
      if not result['OK']:
        return result
      dID = result['Value']
      if not dID:
        return S_ERROR('Directory %s: not found' % dirPath)
    else:
      dID = dirID  
    
    fileName = os.path.basename(lfn)    
    result = self._insert('FC_Files',['DirID','FileName'],[dID,fileName])    
    if not result['OK']:
      return result
      
    return S_OK(result['lastRowId'])
  
  def __getGuid(self,fileID):
    """ Get GUID of the given file
    """
    
    req = "SELECT GUID FROM FC_GUID_to_File WHERE FileID=%d" % fileID
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    return S_OK(result['Value'][0][0])
    
  def addFile(self,lfns,credDict):
    """ Add files to the catalog
    """  

    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for lfn,info in arguments.items():
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
    
  def __addFile(self,lfn,credDict,pfn='',size=0,se='',guid='',checksum='',checksumtype=''):
    """Add (register) a file to the catalog. The file is specified by its
       logical file name lfn, physical replica pfn, size, storage element se
       and global unique identifier guid
    """

    start = time.time()
    result = self.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    ( uid, gid ) = result['Value']

    # check directory permissions
    lfnDir = os.path.dirname(lfn)
    result = self.getDirectoryPermissions(lfnDir,credDict)
    if not result['OK']:
      return result
    permDict = result['Value']
    if not permDict['Write']:
      return S_ERROR('Permission denied')

    # Check if the lfn already exists
    fileID = 0
    resExists = self.exists(lfn)        
    if not resExists['OK']:
      return resExists
    if resExists['Exists']:
      fileID = resExists['Value']
      # Check file GUID
      result = self.__getGuid(fileID)      
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR('%s: file info not found' % lfn)
      eguid = result['Value']
      if eguid != guid and guid:
        return S_ERROR('GUID mismatch')  
      if DEBUG:
        print "addFile existence checks %.4f" % (time.time()-start)
        start = time.time()  
      return S_OK(eguid)
      
    # Check if the GUID already exists   
    if guid:
      resGuid = self.existsGuid(guid)
      if resGuid['OK']:
        if resGuid['Exists']:
          gfileID = result['Value']
          if fileID and fileID != gfileID:
            return S_ERROR('GUID already exists for another file, consider making a link')
      else:
        return S_ERROR('Failed to check the GUID existence')
      
    if DEBUG:
      print "addFile initial checks %.4f" % (time.time()-start)
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
      result = self.makeDirectories(directory,credDict)
      if not result['OK']:
        return result
      dirID = result['Value']
      if not dirID:
        return S_ERROR('Failed to create (or find) the file directory')
      
      if DEBUG:
        print "addFile made directories %.4f" % (time.time()-start)
        start = time.time()
  
      # Create the file record
      result = self.__addLFN(lfn,dirID)      
      if not result['OK']:
        return result
      fileID = result['Value']
      
      if DEBUG:  
        print "addFile added LFN %.4f" % (time.time()-start)
        start = time.time()      
      req = "INSERT INTO FC_FileInfo (FileID,Size,CheckSum,CheckSumType,UID,GID,CreationDate," 
      req = req + "ModificationDate,Mode,Status) VALUES "
      req = req + "(%d,%d,'%s','%s',%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,0)" % \
            (fileID,size,checksum,checksumtype,uid,gid,self.umask)                        
      resAdd = self._update(req)            
      if resAdd['OK']:
        req = "INSERT INTO FC_GUID_to_File (GUID,FileID) VALUES ('%s','%s')" % (fileGUID,fileID)        
        resGuid = self._update(req)
        if resGuid['OK']:
          result = S_OK()
          result['GUID'] = fileGUID
        else:
          req = "DELETE FROM FC_FileInfo WHERE FileID=%d" % fileID
          resDel = self._update(req)      
          req = "DELETE FROM FC_Files WHERE FileID=%d" % fileID
          resDel = self._update(req)  
          result = S_ERROR('Failed to register the file guid')
      else:
        req = "DELETE FROM FC_Files WHERE FileID=%d" % fileID
        resDel = self._update(req)      
        result = S_ERROR('Failed to add file info')
      if not result['OK']:
        return result  
      
    if DEBUG:  
      print "addFile added File Info %.4f" % (time.time()-start)
      start = time.time()  

    if se:
      result = self.__addReplica(fileID,se,pfn)
      if not result['OK']:
        return result

    return result
  
  def getFileLFN(self,fileID):
    """ Get file LFN for the given fileID
    """
    req = "SELECT DirID,FileName from FC_Files WHERE FileID=%d" % fileID
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    dirID = result['Value'][0][0]
    fname = result['Value'][0][1]
    
    result = self.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    lfn = dirPath+'/'+fname
    
    return S_OK(lfn)
  
  def removeFile(self,lfns,credDict):
    """ Bulk file removal method
    """
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    
    files = arguments.keys()
    result = self.findFile(files)     
    fileDict = result['Value']
    if not fileDict["Successful"]:
      return S_OK(fileDict)
    
    failed = fileDict['Failed']
    lfnDict = {}
    for lfn,id in fileDict['Successful'].items():
      if id:
        lfnDict[id] = lfn
    fileIDString = ','.join([str(id) for id in lfnDict.keys()])
    for table in ['FC_Files','FC_FileInfo','FC_GUID_to_File']:
      req = "DELETE FROM %s WHERE FileID in (%s)" % (table,fileIDString)
      result = self._update(req)
      if not result['OK']:
        failed.update(fileDict["Successful"])
        break
      if table == "FC_GUID_to_File":
        successful = fileDict["Successful"]
        
    return S_OK({'Successful':successful,'Failed':failed})    
  
  def setFileOwner(self,fileID,owner):
    """ Set the file owner
    """
    
    result = self.findUser(owner)
    if not result['OK']:
      return result
    userID = result['Value']
    
    req = 'UPDATE FC_FileInfo SET UID=%d WHERE FileID=%d' % (int(userID),int(fileID))
    result = self._update(req) 
    return result
  
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
        result = self.setFileOwner(lfnDict[lfn],owner)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})
    
  def setFileGroup(self,fileID,group):
    """ Set the file owner
    """
    
    result = self.findGroup(group)
    if not result['OK']:
      return result
    groupID = result['Value']
    
    req = 'UPDATE FC_FileInfo SET GID=%d WHERE FileID=%d' % (int(groupID),int(fileID))    
    result = self._update(req) 
    return result
  
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
        result = self.setFileGroup(lfnDict[lfn],group)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})
  
  def setFileMode(self,fileID,mode):
    """ Set the file owner
    """
    
    req = 'UPDATE FC_FileInfo SET Mode=%d WHERE FileID=%d' % (int(mode),int(fileID))
    result = self._update(req) 
    return result
  
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
        result = self.setFileMode(lfnDict[lfn],mode)
        if result['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = result['Message']
      else:
        failed[lfn] = 'Path not found'
             
    return S_OK({'Successful':successful,'Failed':failed})    
  
#####################################################################
  def getFileInfo(self,fileID):
    """ Get file information for the given file ID
    """ 
    
    req = "SELECT FileID,Size,CheckSum,CheckSumType,UID,GID,CreationDate,ModificationDate,Mode,Status"
    req += " FROM FC_FileInfo WHERE FileID=%d" % fileID
    result = self._query(req)
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
      resGet = self.getUserName(uid)
      if resGet['OK']:
        owner = resGet['Value'] 
    resultDict['Owner'] = owner
    gid = int(result['Value'][0][5])
    resultDict['GID'] = gid
    group = 'unknown'
    if gid == 0:
      group = 'root'
    else:  
      resGet = self.getGroupName(gid)      
      if resGet['OK']:
        group = resGet['Value']  
    resultDict['OwnerGroup'] = group
    
    resultDict['CreationTime'] = result['Value'][0][6]
    resultDict['ModificationTime'] = result['Value'][0][7]
    resultDict['Permissions'] = result['Value'][0][8]
    resultDict['Status'] = result['Value'][0][9]
    
    req = "SELECT DirID,FileName from FC_Files WHERE FileID=%d" % fileID
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('File %d not found' % fileID)
    resultDict['DirID'] = result['Value'][0][0]
    resultDict['FileName'] = result['Value'][0][1]
    
    # ToDo: Number of links to be evaluated
    resultDict['NumberOfLinks'] = 0
    
    dirID = resultDict['DirID']
    result = self.dtree.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %d not found' % dirID)
    dirPath = result['Value']
    resultDict['LFN'] = dirPath+'/'+resultDict['FileName']      
    return S_OK(resultDict)  

#####################################################################
  def existsGuid(self,guid):
    """ Check the existence of the guid
    """
    fileID = 0
    query = "SELECT FileID FROM FC_GUID_to_File WHERE GUID='%s'" % guid
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        fileID = resQuery['Value'][0][0]
    else:
      return S_ERROR('GUID existence check failed')

    if fileID:
      result = S_OK(fileID)
      result['Exists'] = True
    else:
      result = S_OK(0)
      result['Exists'] = False

    return result
  
##################################################################### 
  def isFile(self,lfns,s_uid=0,s_gid=0):
    """ Check for the existence of files
    """
    
    #result = self.findUser(s_uid)
    #if not result['OK']:
    #  return result
    #uid = result['Value']
    #result = self.findGroup(s_gid)
    #if not result['OK']:
    #  return result
    #gid = result['Value']
    
    result = self.findFile(lfns)
    return result
  
  def changePathOwner(self,paths,credDict):
    """ Change the owner for the given paths
    """
    result = self.getUserAndGroupID(credDict)
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
    result = self.getUserAndGroupID(credDict)
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

#########################################################################
#
#  Replica related methods
#    
  
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
      result = self.findSE(se)
      if not result['OK']:
        return result
      seID = result['Value']
    result = self._insert('FC_Replicas',['FileID','SEID'],[fileID,seID])
    if not result['OK']:
      return result
    repID = result['lastRowId']
    req = "INSERT INTO FC_ReplicaInfo (RepID,RepType,Status,CreationDate,ModificationDate,PFN) VALUES "
    req += "(%d,'%s',%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s')" % (repID,rtype,0,pfn)    
    result = self._update(req)    
    if not result['OK']:
      result = self.__deleteReplica(repID)
      return S_ERROR('Failed to add replica info')
    
    return S_OK(repID)
  
  def __deleteReplica(self,repID):
    """ Delete replica specified by repID
    """
    
    success = True
    message = ''
    req = "DELETE FROM FC_Replicas WHERE RepID=%d" % repID
    result = self._update(req)
    if not result['OK']:
      success = False
      message = result['Message']
    req = "DELETE FROM FC_ReplicaInfo WHERE RepID=%d" % repID
    result = self._update(req)
    if not result['OK']:
      success = False  
      message = result['Message']
      
    if success:
      return S_OK()
    else:
      return S_ERROR(message)  
    
  def __existsReplica(self,fileID,se):
    """ Check if a replica already exists
    """    
    
    seID = se
    if type(se) in StringTypes:
      result = self.findSE(se)
      if not result['OK']:
        return result
      seID = result['Value']      
    req = "SELECT RepID FROM FC_Replicas WHERE FileID=%d AND SEID=%d" % (fileID,seID)
    result = self._query(req)
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

  def __checkLFNPFNConvention(self,lfn,pfn,se):
    """ Check that the PFN corresponds to the LFN-PFN convention
    """
    # Check if the PFN corresponds to the LFN convention
    if pfn == lfn:
      return S_OK()
    lfn_pfn = True   # flag that the lfn is contained in the pfn
    if (len(pfn)<len(lfn)) or (pfn[-len(lfn):] != lfn) :
      return S_ERROR('PFN does not correspond to the LFN convention')

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
  def addReplica(self,lfns,credDict):
    """ Add replica pfn in storage element se for the file specified by its lfn
        to the catalog. Pass optionally guid for extra verification
    """
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    successful = {}
    failed = {}
    for lfn,info in arguments.items():
      for key in [ 'PFN', 'SE' ]:
        if not key in info:
          return S_ERROR( 'Missing "%s" for LFN:%s' % ( key, lfn ) )
      pfn = info['PFN']
      se = info['SE']
      # check directory permissions
      lfnDir = os.path.dirname(lfn)
      result = self.getDirectoryPermissions(lfnDir,credDict)
      if not result['OK']:
        failed[lfn] = result['Message']
        continue
      permDict = result['Value']
      if not permDict['Write']:
        failed[lfn] = 'Permission denied'
        continue
  
      # Check if the lfn already exists
      fileID = 0
      resExists = self.exists(lfn)
      if not resExists['OK']:
        failed[lfn] = resExists['Message']
        continue
      if resExists['Exists']:
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
        if self.LFN_PFN_convention:
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
  def getReplicas(self,lfns,credDict):
    """ Get Replicas for the given LFNs
    """
    
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    files = arguments.keys()
    
    start = time.time()
    
    result = self.findFile(files) 
    
    print "findFiles", time.time()-start
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
    req = "SELECT FileID, SEID FROM FC_Replicas WHERE FileID in (%s)" % fileIDString    
    result = self._query(req)
    if not result['OK']:
      for id,lfn in lfnDict.items():
        failed[lfn] = result["Message"]
    
    if not result['Value']:
      for id,lfn in lfnDict.items():
        failed[lfn] = 'No replicas found'
    
    for row in result['Value']:
      lfn = lfnDict[int(row[0])]             
      seID = row[1]
      resSE = self.getSEDefinition(seID)     
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
    
    print "findReps", time.time()-start
    
    return S_OK({'Successful':successful,'Failed':failed})

  def removeReplica(self,lfns,credDict):
    """ Bulk replica removal method
    """
    result = checkArgumentFormat(lfns)
    if not result['OK']:
      return result
    arguments = result['Value']
    
    files = arguments.keys()
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
    
    for lfn,info in arguments.items():
      se = info['SE']
      result = self.findSE(se)
      if not result['OK']:
        failed[lfn] = result['Message']
        continue
      seID = result['Value']
      req = "DELETE FROM FC_Replicas WHERE FileID=%d and SEID=%d" % (lfnDict[lfn],seID)    
      result = self._update(req)
      if not result['OK']:
        failed[lfn] = result['Message']
      successful[lfn]=True  
      
    return S_OK({'Successful':successful,'Failed':failed})
    
