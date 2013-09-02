########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/DB/FileCatalogComponents/UserAndGroupManager.py $
########################################################################
__RCSID__ = "$Id$"

""" DIRAC FileCatalog component representing a flat directory tree """

import os, types,stat
# import time
from DIRAC                                                                     import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryTreeBase     import DirectoryTreeBase
from DIRAC.Core.Utilities.List                                                 import stringListToString,intListToString

class DirectoryFlatTree(DirectoryTreeBase):

  _tables = {}
  _tables["DirectoryInfo"] = { "Fields": {
                                           "DirID": "INTEGER AUTO_INCREMENT",
                                           "Parent": "INTEGER NOT NULL",
                                           "Status": "SMALLINT UNSIGNED NOT NULL DEFAULT 0",
                                           "DirName": "VARCHAR(1024) NOT NULL",
                                           "CreationDate": "DATETIME",
                                           "ModificationDate": "DATETIME",
                                           "UID": "CHAR(8) NOT NULL",
                                           "GID": "CHAR(8) NOT NULL",
                                           "Mode": "SMALLINT UNSIGNED NOT NULL DEFAULT 775"
                                         },
                               "PrimaryKey": "DirID",
                               "Indexes": {
                                            "Parent": ["Parent"],
                                            "Status": ["Status"],
                                            "DirName": ["DirName"]
                                          }
                             }

  def __init__(self,database=None):
    DirectoryTreeBase.__init__(self,database)
    self.treeTable = 'DirectoryInfo'

  def getDirectoryCounters(self):
    req = "SELECT COUNT(*) FROM DirectoryInfo"
    res = self.db._query(req)
    if not res['OK']:
      return res
    return S_OK({'DirectoryInfo':res['Value'][0][0]})

  def _findDirectories(self,paths,metadata=[]):
    """ Find file ID if it exists for the given list of LFNs """
    #startTime = time.time()
    successful = {}
    failed = {}
    req = "SELECT DirName,DirID" 
    if metadata:
      req = "%s,%s" % (req,intListToString(metadata)) 
    req = "%s FROM DirectoryInfo WHERE DirName IN (%s)" % (req,stringListToString(paths))
    res = self.db._query(req)
    if not res['OK']:
      return res
    for tuple_ in res['Value']:
      dirName = tuple_[0]
      dirID = tuple_[1]
      metaDict = {'DirID':dirID}
      metaDict.update(dict(zip(metadata,tuple_[2:])))
      successful[dirName] = metaDict
    for path in paths:
      if not successful.has_key(path):
        failed[path] = 'No such file or directory'
    return S_OK({"Successful":successful,"Failed":failed})

  def __findDirs(self,paths,metadata=['DirName']):
    dirs = {}
    req = "SELECT DirID,%s FROM DirectoryInfo WHERE DirName IN (%s)" % (intListToString(metadata),stringListToString(paths))
    res = self.db._query(req)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(dirs)
    for tuple_ in res['Value']:
      dirID = tuple_[0]
      dirs[dirID] = dict(zip(metadata,tuple_[1:]))
    return S_OK(dirs)

  def getPathPermissions(self,paths,credDict):
    """ Get the permissions for the supplied paths """
    res = self.db.ugManager.getUserAndGroupID(credDict)
    if not res['OK']:
      return res
    uid,gid = res['Value']
    res = self._findDirectories(paths,metadata=['Mode','UID','GID'])
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

  def findDir(self,path):
    res = self.__findDirs([path])
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(0)
    return S_OK(res['Value'].keys()[0])
  
  def removeDir(self,path):
    """ Remove directory """
    res = self.findDir(path)
    if not res['OK']:
      return res   
    if not res['Value']:
      return S_OK()
    dirID = res['Value']
    req = "DELETE FROM DirectoryInfo WHERE DirID=%d" % dirID
    return self.db._update(req)


 
    
  def makeDirectory(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary containing all the parameters of the newly created directory """

    if path[0] != '/':
      return S_ERROR('Not an absolute path')

    result = self.findDir(path)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'])

    result = self.db.ugManager.getUserAndGroupID(credDict)
    if not result['OK']:
      return result
    uid,gid = result['Value']

    res = self.getParent(path)
    if not res['OK']:
      return res
    parentID = res['Value']
    
    req = "INSERT INTO DirectoryInfo (Parent,Status,DirName,UID,GID,Mode,CreationDate,ModificationDate)\
    VALUES (%d,%d,'%s',%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP());" % (parentID,status,path,uid,gid,self.db.umask)
    result = self.db._update(req)
    if not result['OK']:
      self.removeDir(path)
      return S_ERROR('Failed to create directory %s' % path)
    return S_OK(result['lastRowId'])

  def makeDir(self,path):
    result = self.findDir(path)
    if not result['OK']:
      return result
    dirID = result['Value']
    if dirID:
      return S_OK(dirID)     
    names = ['DirName']
    values = [path]
    result = self.db._insert('DirectoryInfo',names,values)
    if not result['OK']:
      return result
    return S_OK(result['lastRowId'])

  def existsDir(self,path):
    """ Check the existence of a directory at the specified path
    """
    result = self.findDir(path)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK({"Exists":False})
    else:
      return S_OK({"Exists":True,"DirID":result['Value']})  
    
  def getParent(self,path):
    """ Get the parent ID of the given directory """
    return self.findDir(os.path.dirname(path))
  
  def getParentID(self,dirID):
    """ Get the ID of the parent of a directory specified by ID
    """
    if dirID == 0:
      return S_ERROR('Root directory ID given')
    req = "SELECT Parent FROM DirectoryInfo WHERE DirID=%d" % dirID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('No parent found')
    return S_OK(result['Value'][0][0])

  def getDirectoryPath(self,dirID):
    """ Get directory name by directory ID """
    req = "SELECT DirName FROM DirectoryInfo WHERE DirID=%d" % int(dirID)
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory with id %d not found' % int(dirID) )
    return S_OK(result['Value'][0][0])

  def getDirectoryName(self,dirID):
    """ Get directory name by directory ID """
    result = self.getDirectoryPath(dirID)
    if not result['OK']:
      return result
    return S_OK(os.path.basename(result['Value']))

  def getPathIDs(self,path):
    """ Get IDs of all the directories in the parent hierarchy """    
    elements = path.split('/')
    pelements = []
    dPath = ''
    for el in elements[1:]:
      dPath += '/'+el
      pelements.append(dPath)
      
    pathString = [ "'"+p+"'" for p in pelements ]
    req = "SELECT DirID FROM DirectoryInfo WHERE DirName in (%s) ORDER BY DirID" % ','.join(pathString) 
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Directory %s not found' % path)
    return S_OK([ x[0] for x in result['Value'] ])

  def getChildren(self,path):
    """ Get child directory IDs for the given directory  """  
    if type(path) in types.StringTypes:
      result = self.findDir(path)
      if not result['OK']:
        return result
      dirID = result['Value']
    else:
      dirID = path
    req = "SELECT DirID FROM DirectoryInfo WHERE Parent=%d" % dirID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK([])
    return S_OK([ x[0] for x in result['Value'] ])
