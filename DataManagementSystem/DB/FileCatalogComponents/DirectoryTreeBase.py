########################################################################
# $Id: FileCatalogDB.py 22623 2010-03-09 19:54:25Z acsmith $
########################################################################
""" DIRAC DirectoryTree base class """

__RCSID__ = "$Id: FileCatalogDB.py 22623 2010-03-09 19:54:25Z acsmith $"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities  import * 
from DIRAC                                                          import S_OK, S_ERROR, gLogger
import string, time, datetime,threading, re, os, sys, md5, random
from types import *
import stat

DEBUG = 0
     
#############################################################################
class DirectoryTreeBase:

  def __init__(self, database=None):
    self.db = database
    self.lock = threading.Lock()
    self.treeTable = ''
    
  def getTreeTable(self):
    """ Get the string of the Directory Tree type
    """  
    return self.treeTable
    
  def setDatabase(self,database):
    self.db = database  

  def makeDirectory(self,path,credDict,status=0):
    return self.makeDirectory_andrei(path,credDict,status)
    
  def makeDirectory_andrew(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary containing all the parameters of the newly created directory """
    if path[0] != '/':
      return S_ERROR('Not an absolute path')
    # Strip off the trailing slash if necessary
    if len(path) > 1 and path[-1] == '/':
      path = path[:-1]
    
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
    if result['NewDirectory']:
      req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
      req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % (dirID,l_uid,l_gid,self.db.umask,status)            
      result = self.db._update(req)            
      if result['OK']:
        resGet = self.getDirectoryParameters(dirID)            
        if resGet['OK']:
          dirDict = resGet['Value']
    else:
      return S_OK(dirID)

    if not dirDict:
      self.removeDir(path)
      return S_ERROR('Failed to create directory %s' % path)
    return S_OK(dirID)

  def makeDirectory_andrei(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary
       containing all the parameters of the newly created directory
    """
    if path[0] != '/':
      return S_ERROR('Not an absolute path')
    # Strip off the trailing slash if necessary
    if len(path) > 1 and path[-1] == '/':
      path = path[:-1]
    
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
    if result['NewDirectory']:
      req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
      req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % (dirID,l_uid,l_gid,self.db.umask,status)            
      result = self.db._update(req)            
      if result['OK']:
        resGet = self.getDirectoryParameters(dirID)            
        if resGet['OK']:
          dirDict = resGet['Value']
    else:
      return S_OK(dirID)

    if not dirDict:
      self.removeDir(path)
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
    dirDict['Mode'] = int(resQuery['Value'][0][4])
    dirDict['CreationDate'] = resQuery['Value'][0][5]
    dirDict['ModificationDate'] = resQuery['Value'][0][6]

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
    result = self.db.ugManager.findUser(owner)
    uid = result['Value']
    result = self.__setDirectoryUid(dirID,uid)
    return result
  
#####################################################################
  def changeDirectoryOwner(self,paths,s_uid=0,s_gid=0):
    """ Bulk setting of the directory owner
    """  
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,dict in arguments.items():
      owner = dict['Owner']
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
    result = self.db.ugManager.findGroup(gname)
    gid = result['Value']
    result = self.__setDirectoryGid(dirID,gid)
    return result
  
#####################################################################
  def changeDirectoryGroup(self,paths,s_uid=0,s_gid=0):
    """ Bulk setting of the directory owner
    """  
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,dict in arguments.items():
      group = dict['Group']
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
    result = self.db.ugManager.findUser(s_uid)
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup(s_gid)
    if not result['OK']:
      return result
    gid = result['Value']
    
    result = checkArgumentFormat(paths)
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,dict in arguments.items():
      mode = dict['Mode']
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
    successful = {}
    failed = {}
    for path in lfns:
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
    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid,gid = result['Value']
    
    result = self.getDirectoryParameters(path)
    if not result['OK']:
      if "not found" in result['Message']:
        # If the directory does not exist, check the nearest parent for the permissions
        pDir = os.path.dirname(path)
        result = self.getDirectoryPermissions(pDir,credDict)
        return result
      else:  
        return result 
    
    dUid = result['Value']['UID']
    dGid = result['Value']['GID']
    mode = result['Value']['Mode']

    owner = uid == dUid
    group = gid == dGid

    resultDict = {}
    if self.db.globalReadAccess:
      resultDict['Read'] = True
    else:
      resultDict['Read'] = (owner and mode&stat.S_IRUSR>0) or (group and mode&stat.S_IRGRP>0) or mode&stat.S_IROTH>0
    resultDict['Write'] = (owner and mode&stat.S_IWUSR>0) or (group and mode&stat.S_IWGRP>0) or mode&stat.S_IWOTH>0
    resultDict['Execute'] = (owner and mode&stat.S_IXUSR>0) or (group and mode&stat.S_IXGRP>0) or mode&stat.S_IXOTH>0
    return S_OK(resultDict)
  
  def getFilesInDirectory(self,dirID,credDict):
    """ Get file IDs for the given directory
    """
    dirs = dirID
    if type(dirID) != ListType:
      dirs = [dirID]
      
    dirListString = ','.join( [ str(dir) for dir in dirs ] )

    req = "SELECT FileID,DirID,FileName FROM FC_Files WHERE DirID IN ( %s )" % dirListString
    result = self.db._query(req)
    return result
 
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
      result = self.getDirectoryPath(dirID)
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
  
  def getDirectorySize(self,lfns,longOutput=False):
    """ Get the total size of the requested directories. If long flag
        is True, get also physical size per Storage Element
    """
    
    resultLogical = self._getDirectoryLogicalSize(lfns)
    if not resultLogical['OK']:
      return resultLogical
    
    resultDict = resultLogical['Value']
    if not resultDict['Successful']:
      return resultLogical
    
    if longOutput:
      # Continue with only successful directories
      resultPhysical = self._getDirectoryPhysicalSize(resultDict['Successful'])
      if not resultPhysical['OK']:
        result = S_OK(resultDict)
        result['Message'] = "Failed to get the physical size on storage"
        return result     
      for lfn in resultPhysical['Value']['Successful']:
        resultDict['Successful'][lfn]['PhysicalSize'] = resultPhysical['Value']['Successful'][lfn]
        
    return S_OK(resultDict)            
  
  def _getDirectoryLogicalSize(self,lfns):
    """ Get the total "logical" size of the requested directories
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
        req = "SELECT SUM(Size) FROM FC_Files WHERE DirID IN (%s)" % dirString      
        result = self.db._query(req)       
        if not result['OK']:
          failed[path] = result['Message']
        elif not result['Value']:
          successful[path] = {"LogicalSize":0}
        elif result['Value'][0][0]:
          successful[path] = {"LogicalSize":int(result['Value'][0][0])}
        else:
          successful[path] = {"LogicalSize":0}
          
    return S_OK({'Successful':successful,'Failed':failed})  
  
  def _getDirectoryPhysicalSize(self,lfns):
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
        req = "SELECT SUM(Size) FROM FC_Files WHERE DirID IN (%s)" % dirString
        req = "SELECT SUM(F.Size),S.SEName from FC_Files as F, FC_Replicas as R, FC_StorageElements as S "
        req += "WHERE R.SEID=S.SEID AND F.FileID=R.FileID AND F.DirID IN (%s) " % dirString
        req += "GROUP BY S.SEID"        
        result = self.db._query(req)        
        if not result['OK']:
          failed[path] = result['Message']
        elif not result['Value']:
          successful[path] = {}
        elif result['Value'][0][0]:
          seDict = {}
          total = 0
          for size,seName in result['Value']:
            seDict[seName] = int(size)
            total += size
          seDict['Total'] = int(total)  
          successful[path] = seDict
        else:
          successful[path] = {} 
          
    return S_OK({'Successful':successful,'Failed':failed}) 