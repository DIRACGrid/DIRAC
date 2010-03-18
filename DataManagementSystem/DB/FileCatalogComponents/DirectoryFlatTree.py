########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/DB/FileCatalogComponents/UserAndGroupManager.py $
########################################################################
__RCSID__ = "$Id: DirectoryFlatTree.py 23183 2010-03-16 13:12:11Z acsmith $"

""" DIRAC FileCatalog component representing a flat directory tree """

import time, os, types
from DIRAC                                                                     import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryTreeBase     import DirectoryTreeBase
from DIRAC.Core.Utilities.List                                                 import stringListToString,intListToString

class DirectoryFlatTree(DirectoryTreeBase):

  def getDirectoryCounters(self):
    req = "SELECT COUNT(*) FROM DirectoryInfo"
    res = self.db._query(req)
    if not res['OK']:
      return res
    return S_OK({'DirectoryInfo':res['Value'][0][0]})

  def __findDirs(self,paths,metadata=['DirName']):
    dirs = {}
    req = "SELECT DirID,%s FROM DirectoryInfo WHERE DirName IN (%s)" % (intListToString(metadata),stringListToString(paths))
    res = self.db._query(req)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK(dirs)
    for tuple in res['Value']:
      dirID = tuple[0]
      dirs[dirID] = dict(zip(metadata,tuple[1:]))
    return S_OK(dirs)

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
    VALUES (%d,%d,'%s','%s','%s',%d,UTC_TIMESTAMP(),UTC_TIMESTAMP());" % (parentID,status,path,uid,gid,self.db.umask)
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
    req = "SELECT DirID FROM DirectoryInfo WHERE DirName in (%s) ORDER BY DirID" % pathString
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
