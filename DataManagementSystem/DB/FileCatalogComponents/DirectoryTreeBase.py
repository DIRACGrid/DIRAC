########################################################################
# $Id: FileCatalogDB.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################
""" DIRAC DirectoryTree base class """

__RCSID__ = "$Id: FileCatalogDB.py 22623 2010-03-09 19:54:25Z acsmith $"

import re, os, sys, md5, random
import string, time, datetime
import threading
from types import *

from DIRAC                                  import S_OK, S_ERROR

DEBUG = 0
     
#############################################################################
class DirectoryTreeBase:

  def __init__(self, database=None):
    self.db = database
    
  def setDatabase(self,database):
    self.db = database  

  def makeDirectory(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary
       containing all the parameters of the newly created directory
    """

    if path[0] != '/':
      return S_ERROR('Not an absolute path')

    result = self.findDir(path)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'])

    if path == '/':
      # Create the root directory
      l_uid = 0
      l_gid = 0
    else:
      result = self.db.ugManager.getUserAndGroupID(credDict)
      if not result['OK']:
        return result
      ( l_uid, l_gid ) = result['Value']

    dirDict = {}
    result = self.makeDir(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
    req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % (dirID,l_uid,l_gid,self.db.umask,status)            
    result = self.db._update(req)            
    if result['OK']:
      resGet = self.getDirectoryParameters(dirID)            
      if resGet['OK']:
        dirDict = resGet['Value']

    if not dirDict:
      result = self.removeDir(path)
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
    result = result['Value']
    if result['Exists']:
      return S_OK(result['DirID']) 

    if path == '/':
      result = self.makeDirectory(path,credDict)
      return result

    parentDir = os.path.dirname(path)
    result = self.existsDir(parentDir)
    if not result['OK']:
      return result
    result = result['Value']
    if result['Exists']:
      result = self.makeDirectory(path,credDict)
    else:
      result = self.makeDirectories(parentDir,credDict)
      if not result['OK']:
        return result
      result = self.makeDirectory(path,credDict)

    return result

#####################################################################
  def exists(self,lfns):
    successful = {}
    failed = {}
    for lfn in lfns:
      res = self.findDir(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      if not res['Value']:
        successful[lfn] = False
      else:
        successful[lfn] = True
    return S_OK({'Successful':successful,'Failed':failed})

  def existsDir(self,path):
    """ Check the existence of the directory path
    """
    result = self.findDir(path)
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
  def isDirectory(self,paths):
    """ Checking for existence of directories
    """
    dirs = paths.keys()
    successful = {}
    failed = {}
    for dir in dirs:
      result = self.existsDir(dir)
      if not result['OK']:
        failed[dir] = result['Message']
      elif result['Value']['Exists']:
        successful[dir] = True
      else: 
        successful[dir] = False  
          
    return S_OK({'Successful':successful,'Failed':failed})
  
  #####################################################################
  def createDirectory(self,dirs,credDict):
    """ Checking for existence of directories
    """
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
  def removeDirectory(self,dirs,force=False):
    """Remove an empty directory from the catalog """
    successful = {}
    failed = {}
    for dir in dirs:
      result = self.removeDir(dir)
      if not result['OK']:
        failed[dir] = result['Message']
      else: 
        successful[dir] = True  
    return S_OK({'Successful':successful,'Failed':failed}) 

#####################################################################
  def __getDirID(self,path):
    """ Get directory ID from the given path or already evaluated ID
    """

    if type(path) in StringTypes:
      result = self.findDir(path)
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
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      return resQuery

    if not resQuery['Value']:
      return S_ERROR('Directory not found')

    dirDict = {}
    dirDict['DirID'] = int(resQuery['Value'][0][0])
    uid = int(resQuery['Value'][0][1])
    dirDict['UID'] = uid
    owner = 'unknown'
    result = self.db.ugManager.getUserName(uid)
    if result['OK']:
      owner = result['Value'] 
    dirDict['Owner'] = owner
    gid = int(resQuery['Value'][0][2])
    dirDict['GID'] = int(resQuery['Value'][0][2])
    group = 'unknown'
    result = self.db.ugManager.getGroupName(gid)
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
    result = self.db._update(req)
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
    paths = lfns.keys()
    
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
    if self.db.globalReadAccess:
      resultDict['Read'] = True
      
    resultDict['Write'] = True
    resultDict['Execute'] = True
    return S_OK(resultDict)
  
  def __getFilesInDirectory(self,dirID):
    """ Get file IDs for the given directory
    """
    req = "SELECT FileID,DirID,FileName FROM FC_Files WHERE DirID=%d" % dirID
    result = self.db._query(req)
    if not result['OK']:
      return result
    fileList = [ row[0] for row in result['Value'] ]
    path = '/some/path'
    return S_OK(fileList)
  
  def __getDirectoryContents(self,path,details=False):
    """ Get contents of a given directory
    """
    
    result = self.findDir(path)
    if not result['OK']:
      return result
    directoryID = result['Value']
    
    directories = {}
    files = {}
    links = {}
    result = self.getChildren(path)
    if not result['OK']:
      return result
    
    # Get subdirectories
    dirIDList = result['Value']
    for dirID in dirIDList:
      result = self.getDirectoryName(dirID)
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

    res = self.db.fileManager.getFilesInDirectory(directoryID,path,verbose=details)
    if not res['OK']:
      return res
    files = res['Value']
    pathDict = {'Files': files,'SubDirs':directories,'Links':links}    
    return S_OK(pathDict)           

  def listDirectory(self,lfns,verbose=False):
    """ Get the directory listing
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.__getDirectoryContents(path,details=verbose)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']
        
    return S_OK({'Successful':successful,'Failed':failed})      
  
  def getDirectorySize(self,lfns,credDict):
    """ Get the total size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.findDir(path)
      if not result['OK']:
        failed[path] = "Directory not found"
        continue
      if not result['Value']:
        failed[path] = "Directory not found"
        continue
      dirID = result['Value']
      result = self.getSubdirectoriesByID(dirID)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        dirList = result['Value'].keys()
        dirList.append(dirID)
        dirString = ','.join([ str(x) for x in dirList ])
        req = "SELECT SUM(I.Size) FROM FC_FileInfo as I, FC_Files as F WHERE I.FileID=F.FileID AND F.DirID IN (%s)" % dirString
        result = self.db._query(req)
        if not result['OK']:
          failed[path] = result['Message']
        elif not result['Value']:
          successful[path] = 0
        else:
          successful[path] = int(result['Value'][0][0])
          
    return S_OK({'Successful':successful,'Failed':failed})          
