########################################################################
# $Id: FileCatalogDB.py,v 1.2 2008/02/25 23:25:43 atsareg Exp $
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

__RCSID__ = "$Id: FileCatalogDB.py,v 1.2 2008/02/25 23:25:43 atsareg Exp $"

import re, os, sys
import string, time, datetime
import threading
from types import *

from DIRAC                      import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB         import DB
from DIRAC.Core.Utilities.Pfn   import pfnparse, pfnunparse

#############################################################################
class FileCatalogDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,'FileCatalogDB','DataManagement/FileCatalogDB',maxQueueSize)

    # In memory storage of the directory parameters
    self.directories = {}

    # In memory storage of the SE definitions
    self.ses = {}

    # Operational flags
    self.LFN_PFN_convention = True

#####################################################################
#
#  Directories related methods
#
#####################################################################
  def mkdir(self,path):
    """Create a new directory. The return value is the dictionary
       containing all the parameters of the newly created directory
    """

    if path[0] != '/':
      return S_ERROR('Not an absolute path')

    result = self.getDirectoryParams(path)
    if result['OK']:
      return S_OK()

    if path == '/':
      parent_directory = ''
    else:
      parent_directory = os.path.dirname(path)


    dirDict = {}
    if not parent_directory:
      # Create the root directory
      status,guid,error,pythonerror = exeCommand('uuidgen')
      req = "INSERT INTO FC_Directories (DirPath,Name,ParentID,GUID,UID,GID,"
      req = req + "CreateDate,ModifyDate,AccessDate,Umask,MinACL) Values "
      req = req + "('/','',-1,'%s',0,0,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),0777,0775)" % guid
      result = self._update(req)
      if result['OK']:
        resGet = self.getDirectory('/')
        if resGet['OK']:
          dirDict = resGet['Value']
        else:
          return S_ERROR('Failed to create root directory')
      else:
        return S_ERROR('Failed to create root directory')
      parent_id = -1

    else:
      # Create a new directory
      resGet = self.getDirectoryParams(parent_directory)
      if not resGet['OK']:
        if resGet['Message'] == 'Directory not found':
          result = self.makeDir(parent_directory)
          if result['OK']:
            resGet = self.getDirectoryParams(parent_directory)
          else:
            return S_ERROR("Failed to create directory "+parent_directory)

      parent_id = parentDict['DirID']
      gid = parentDict['GID']
      uid = parentDict['UID']
      umask = parentDict['Umask']
      acl = parentDict['MinACL']
      status,guid,error,pythonerror = exeCommand('uuidgen')
      req = "INSERT INTO FC_Directories (DirPath,ParentID,GUID,UID,GID,"
      req = req + "CreateDate,ModifyDate,AccessDate,Umask,MinACL) Values "
      req = req + "('%s',%d,'%s',%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % \
            (path,parent_id,guid,uid,gid,umask,acl)
      result = self._update(req)

      print "---------",result
      if not result['OK']:
        return result

      result = self.getDirectory(path)
      dirDict = result['Value']

    dirid = dirDict['DirID']

    req = """

CREATE TABLE F_%d (
  FileID INTEGER NOT NULL auto_increment,
  LFN VARCHAR(255) NOT NULL,
  FileName VARCHAR(127) NOT NULL,
  Size INTEGER NOT NULL,
  CheckSum VARCHAR(127) NOT NULL default '00000000-0000-0000-0000-000000000000',
  Type ENUM('File','Directory','Link') NOT NULL default 'File',
  GUID char(36),
  UID INTEGER NOT NULL default 0,
  GID INTEGER NOT NULL default 0,
  CreateDate DATETIME,
  ModifyDate DATETIME,
  AccessDate DATETIME,
  MinACL INTEGER(12),
  PRIMARY KEY (FileID,FileName)
)
""" % (dirid,)

    print req
    result = self._update(req)
    if not result['OK']:
      return S_ERROR('Failed to create directory '+path)

    # Add an entry into the parent directory
    if parent_id > 0 :
      dname = os.path.basename(path)
      date = dirDict['CreateDate'].strftime("%Y-%m-%dT %H:%M:%S")
      result = self.__addNode(parent_id,dname,guid,ntype='Directory',
                              uid=uid,gid=gid,acl=acl,
                              cdate=date,mdate=date,adate=date)
      if not result['OK']:
        result = S_ERROR('Failed to create an entry in the parent directory')
        result['Value'] = dirDict
        return result

    return S_OK(dirDict)

#####################################################################
  def __addNode(self,dirID,name,guid,ntype='File',lfn='',size=0,checksum='',
                uid=0,gid=0,acl=0775,cdate='',mdate='',adate=''):
    """ Add a new node to a directory
    """

    now = datetime.datetime.now().strftime("%Y-%m-%dT %H:%M:%S")
    c_date = cdate
    if not cdate:
      c_date = now
    m_date = mdate
    if not mdate:
      m_date = now
    a_date = adate
    if not adate:
      a_date = now

    req = "INSERT INTO F_%d (LFN,FileName,Size,CheckSum,Type,GUID,UID,GID," % dirID
    req = req + "CreateDate,ModifyDate,AccessDate,MinACL) VALUES "
    req = req + "('%s','%s',%d,'%s','%s','%s',%d,%d,'%s','%s','%s',%d)" % \
          (lfn,name,size,checksum,ntype,guid,uid,gid,c_date,m_date,a_date,acl)

    result = self._update(req)
    return result

#####################################################################
  def makedirs(self,path):
    """Make all the directories recursively in the path. The return value
       is the dictionary containing all the parameters of the newly created
       directory
    """

    parentDir = os.path.dirname(path)
    res = self.existsDir(path)

    if res['Status'] != "OK":
      result = S_ERROR('makedirs failed for directory '+path+": "+res['Message'])
      return result
    if res['Exists']:
      return S_OK()

    res = self.existsDir(parentDir)

    if res['Exists']:
      result = self.makeDir(path)
    else:
      result = self.makeDirs(parentDir)
      result = self.makeDir(path)

    return result

#####################################################################
  def existsDir(self,path):
    """ Check the existence of the directory path
    """

    query = "SELECT DirID FROM FC_Directories WHERE DirPath='%s'" % path
    resQuery = self._query(query)
    if not resQuery['OK']:
      return S_ERROR('Directory existence check failed')
    if not resQuery['Value']:
      result = S_OK()
      result['Exists'] = False
      return result

    result = S_OK(int(resQuery['Value'][0][0]))
    result['Exists'] = True

    return result

#####################################################################
  def rmdir(self,dirname,force=False):
    """Remove an empty directory from the catalog
    """

    pass

#####################################################################
  def getDirectoryParams(self,path):
    """ Get the given directory parameters
    """

    # Use _getfields() instead A.T.

    query = "SELECT DirID,GUID,UID,GID,ParentID,Umask,MinACL,"
    query = query + "CreateDate,ModifyDate,AccessDate from FC_Directories"
    query = query + " WHERE DirPath='%s'" % path
    resQuery = self._query(query)
    if not resQuery['OK']:
      return S_ERROR('Failed to query directory')

    if not resQuery['Value']:
      return S_ERROR('Directory not found')

    print resQuery

    dirDict = {}
    dirDict['DirID'] = int(resQuery['Value'][0][0])
    dirDict['GUID'] = resQuery['Value'][0][1]
    dirDict['UID'] = int(resQuery['Value'][0][2])
    dirDict['GID'] = int(resQuery['Value'][0][3])
    dirDict['ParentID'] = int(resQuery['Value'][0][4])
    if resQuery['Value'][0][5]:
      dirDict['Umask'] = int(resQuery['Value'][0][5])
    else:
      dirDict['Umask'] = 0777
    if resQuery['Value'][0][6]:
      dirDict['MinACL'] = int(resQuery['Value'][0][6])
    else:
      dirDict['MinACL'] = 0775
    dirDict['CreateDate'] = resQuery['Value'][0][7]
    dirDict['ModifyDate'] = resQuery['Value'][0][8]
    dirDict['AccessDate'] = resQuery['Value'][0][9]

    return S_OK(dirDict)

#####################################################################
  def __setDirectoryUid(self,dirid,uid):
    """ Set the directory owner
    """

    req = "UPDATE FC_Directories SET UID=%d WHERE DirID=%d" % (dirid,uid)
    result = self._update(req)
    return result

#####################################################################
  def __setDirectoryGid(self,dirid,gid):
    """ Set the directory group
    """

    req = "UPDATE FC_Directories SET GID=%d WHERE DirID=%d" % (dirid,gid)
    result = self._update(req)
    return result

#####################################################################
  def __getDirectoryId(self,path):
    """ Get ID for a directory specified by its path
    """

    query = "SELECT DirID from FC_Directories WHERE DirPath='%s'" % path
    resQuery = self._query(query)
    if resQuery['OK']:
      return S_OK(int(resQuery['Value'][0][0]))
    else:
      return resQuery

#####################################################################
  def setDirectoryOwner(self,path,owner):
    """ Set the directory owner
    """

    result = self.__getDirectoryId(path)
    dirid = result['Value']
    result = self.getUidByName(owner)
    uid = result['Value']
    result = self.__setDirectoryUid(dirid,uid)
    return result

#####################################################################
  def setDirectoryGroup(self,path,gname):
    """ Set the directory owner
    """

    result = self.__getDirectoryId(path)
    dirid = result['Value']
    result = self.getGid(gname)
    gid = result['Value']
    result = self.__setDirectoryGid(dirid,gid)
    return result

#####################################################################
  def setDirectoryMask(self,path,mask):
    """ set the directory mask
    """

    result = self.__getDirectoryId(path)
    dirid = result['Value']
    req = "UPDATE FC_Direcories SET Umask=%d WHERE DirID=%d" % (mask,dirid)

#####################################################################
  def setDirectoryMinACL(self,path,acl):
    """ set the directory mask
    """

    result = self.__getDirectoryId(path)
    dirid = result['Value']
    req = "UPDATE FC_Direcories SET MinACL=%d WHERE DirID=%d" % (acl,dirid)


#####################################################################
#
#  User related methods
#
#####################################################################
  def addUser(self,name,dn):
    """ Add a new user with a nickname 'name' and DN 'dn'
    """

    user_dn_id = -1
    query = "SELECT UID from FC_Users WHERE UserDN='%s'" % dn
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        user_dn_id = int(resQuery['Value'][0][0])

    user_name_id = -1
    query = "SELECT UID from FC_Users WHERE NickName='%s'" % name
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        user_name_id = int(resQuery['Value'][0][0])

    if user_dn_id >= 0 and user_name_id >= 0:
      if user_dn_id == user_name_id:
        # User already exists
        return S_OK(user_dn_id)
      else:
        # Conflict in user name-DN
        return S_ERROR('Conflict between the user name and DN')
    if user_dn_id >= 0:
      if user_dn_id != user_name_id:
        return S_ERROR('User with the given DN already exists')
    if user_name_id >= 0:
      if user_dn_id != user_name_id:
        return S_ERROR('User with the given name already exists')

    query = "SELECT max(UID) FROM FC_Users"
    resQuery = self._query(query)
    print resQuery
    if resQuery['OK']:
      if resQuery['Value']:
        if resQuery['Value'][0][0]:
          uid = int(resQuery['Value'][0][0])+1
        else:
          uid = 1


    req = "INSERT INTO FC_Users (UID,NickName,UserDN) Values (%d,'%s','%s')" % (uid,name,dn)
    resUpdate = self._update(req)
    if resUpdate['OK']:
      return S_OK(uid)
    else:
      return resUpdate

#####################################################################
  def deleteUser(self,name):
    """ Delete a user specified by its nickname
    """

    req = "DELETE FROM FC_Users WHERE Nickname='%s'" % name
    resUpdate = self._update(req)
    if resUpdate['OK']:
      return S_OK()
    else:
      return resUpdate

#####################################################################
  def getUsers(self):
    """ Get the current user IDs and DNs
    """

    resDict = {}
    query = "SELECT Nickname, UID, UserDN from FC_Users"
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        for name,uid,dn in resQuery['Value']:
          resDict[name] = (name,uid,dn)
      else:
        return S_ERROR('No users defined')

    return S_OK(resDict)

#####################################################################
  def getUidByName(self,user):
    """ Get ID for a user specified by his name
    """

    query = "SELECT UID from FC_Users WHERE NickName='%s'" % user
    resQuery = self._query(query)
    if resQuery['OK']:
      return S_OK(resQuery['Value'][0][0])
    else:
      return resQuery

#####################################################################
  def getUidByDN(self,user):
    """ Get ID for a user specified by his DN
    """

    query = "SELECT UID from FC_Users WHERE UserDN='%s'" % user
    resQuery = self._query(query)
    if resQuery['OK']:
      return S_OK(resQuery['Value'][0][0])
    else:
      return resQuery

#####################################################################
#
#  Group related methods
#
#####################################################################
  def addGroup(self,gname,gid=0):
    """ Add a new group with a name 'name'
    """

    group_id = 0
    query = "SELECT GID from FC_Groups WHERE GroupName='%s'" % gname
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        group_id = int(resQuery['Value'][0][0])

    if group_id:
      result = S_OK(group_id)
      result['Message'] = "Group "+gname+" already exists"
      return result

    group_name = ''
    if gid:
      query = "SELECT GroupName from FC_Groups WHERE GID=%d" % gid
      resQuery = self._query(query)
      if resQuery['OK']:
        if resQuery['Value']:
          group_name = int(resQuery['Value'][0][0])
    if group_name:
      return S_ERROR('Group with GID '+str(gid)+' already exists: '+group_name)

    if not gid:
      query = "SELECT max(GID) FROM FC_Groups"
      resQuery = self._query(query)
      if resQuery['OK']:
        if resQuery['Value']:
          if resQuery['Value'][0][0]:
            gid = int(resQuery['Value'][0][0])+1
          else:
            gid = 1

    req = "INSERT INTO FC_Groups (GID,GroupName) Values (%d,'%s')" % (gid,gname)
    print req
    resUpdate = self._update(req)
    if resUpdate['OK']:
      return S_OK(gid)
    else:
      return resUpdate

#####################################################################
  def deleteGroup(self,gname):
    """ Delete a group specified by its name
    """

    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % gname
    resUpdate = self._update(req)
    if resUpdate['OK']:
      return S_OK()
    else:
      return resUpdate

#####################################################################
  def getGroups(self):
    """ Get the current group IDs and names
    """

    resDict = {}
    query = "SELECT GID, GroupName from FC_Groups"
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        for gid,gname in resQuery['Value']:
          resDict[gname] = gid
      else:
        return S_ERROR('No groups defined')

    return S_OK(resDict)

#####################################################################
  def getGid(self,group):
    """ Get ID for a group specified by its name
    """

    query = "SELECT GID from FC_Groups WHERE GroupName='%s'" % group
    resQuery = self._query(query)
    if resQuery['OK']:
      return S_OK(resQuery['Value'][0][0])
    else:
      return resQuery

#####################################################################
#
#  File and replica related methods
#
#####################################################################
  def addFile(self,lfn,pfn='',size=0,se='',guid='',checksum='',uid=0,gid=0):
    """Add (register) a file to the catalog. The file is specified by its
       logical file name lfn, physical replica pfn, size, storage element se
       and global unique identifier guid
    """

    # Check if the lfn already exists
    resExists = self.exists(lfn)
    if resExists['OK']:
      if resExists['Exists']:
        resExists['Message'] = "File already exists"
        return resExists

    dirID = 0
    directory = os.path.dirname(lfn)
    resExists = self.getDirectory(directory)
    if resExists['OK']:
      dirDict = resExists['Value']
    elif resExists['Message'] == 'Directory not found':
      resMkDir = self.makedirs(directory)
      if not resMkDir['OK']:
        return S_ERROR('Failed to create the file directory')
#      if not resExists['Exists']:
#        resMkDir = self.makeDirs(directory)
#        if not resMkDir['OK']:
#          return S_ERROR('Failed to create the file directory')
#        else:
#          dirID = resMkDir['Value']
      else:
        dirDict = resMkDir['Value']
    else:
      return S_ERROR('Failed to get the file directory')


    dirID = dirDict['DirID']
    if not dirID:
      return S_ERROR('Failed to create (or find) the file directory')

    gguid = guid
    if not gguid:
      status,gguid,error,pythonerror = exeCommand('uuidgen')
    else:
      resGuid = self.existsGuid(gguid)
      if resGuid['OK']:
        if resGuid['Exists']:
          return S_ERROR('The specified GUID already exists')
      else:
        return S_ERROR('Failed to check the GUID existence')

    fname = os.path.basename(lfn)
    req = "INSERT INTO F_%d (LFN,FileName,Size,CheckSum,GUID,UID,GID,CreateDate," % dirID
    req = req + "ModifyDate,AccessDate,MinACL) VALUES ('%s','%s',%d,'%s','%s',%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d)" % \
          (lfn,fname,size,checksum,gguid,uid,gid,dirDict['MinACL'])
    resAdd = self._update(req)
    if resAdd['OK']:
      req = "INSERT INTO FC_GUID_to_LFN (GUID,LFN) VALUES ('%s','%s')" % (gguid,lfn)
      resGuid = self._update(req)
      if resGuid['OK']:
        result = S_OK()
        result['GUID'] = gguid
      else:
        req = "DELETE FROM F_%d WHERE GUID='%s'" % (dirID,gguid)
        resDel = self._update(req)
        result = S_ERROR('Failed to register the file guid')
    else:
      result = resAdd

    return result

#####################################################################
  def exists(self,lfn):
    """ Check if the file lfn exists already in the catalog
    """

    directory = os.path.dirname(lfn)
    resExists = self.existsDir(directory)
    if resExists['OK']:
      if resExists['Exists']:
        dirID = resExists['Value']
      else:
        result = S_OK()
        result['Exists'] = False
        return result
    else:
      return resExists

    guid = ''
    fname = os.path.basename(lfn)
    query = "SELECT GUID, FileID FROM F_%d WHERE FileName='%s'" % (dirID,fname)
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        guid = resQuery['Value'][0][0]
        fileID = resQuery['Value'][0][1]
    else:
      return S_ERROR('File existence check failed')

    result = S_OK()
    if guid:
      result['Exists'] = True
      result['GUID'] = guid
      result['DirID'] = dirID
      result['FileID'] = fileID
    else:
      result['Exists'] = False

    return result

#####################################################################
  def existsGuid(self,guid):
    """ Check the existence of the guid
    """

    lfn = ''
    query = "SELECT LFN FROM FC_GUID_to_LFN WHERE GUID='%s'" % guid
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        lfn = resQuery['Value'][0][0]
    else:
      return S_ERROR('GUID existence check failed')

    if lfn:
      result = S_OK()
      result['Exists'] = True
      result['LFN'] = lfn
    else:
      result = S_OK()
      result['Exists'] = False

    return result

#####################################################################
#
#  Storage Element related methods
#
#####################################################################


#####################################################################
  def addPfn(self,lfn,pfn='',se='',guid=''):
    """ Add replica pfn in storage element se for the file specified by its lfn
        to the catalog. Pass optionally guid for extra verification
    """

    # Check that the given LFN exists
    result = self.exists(lfn)
    if not result['OK']:
      return S_ERROR('Failed to check existence of the file '+lfn)

    if not result['Exists']:
      return S_ERROR('File %s does not exist' % lfn )

    fileID = result['FileID']
    dirID  = result['DirID']

    # Check if the SE name is known
    seID = 0
    query = "SELECT SEID FROM FC_StorageElement WHERE SEName='%s'"
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        seID = int(resQuery['Value'][0][0])
      else:
        return S_ERROR('Unknown SE %s' % se)
    else:
      return S_ERROR('Unknown SE %s' % se)

    # Check that the replica does not yet exist
    query = "SELECT RepID,PFN FROM FC_Replicas WHERE DirID=%d AND FileID=%d AND SEID=%d" % \
            (dirID,fileID,seID)
    resQuery = self._query(query)
    repID = 0
    if resQuery['OK']:
      if resQuery['Value']:
        repID = int(resQuery['Value'][0][0])
        old_pfn = resQuery['Value'][0][1]
    else:
      return S_ERROR('Failed to check the replica existence %s' % pfn)

    if repID:
      # Replica already exists. Check if the new replica is the same
      if not pfn:
        result = S_OK(repID)
        result['Message'] = 'Replica already exists'



    seDict = self.__getSEDefinition(seID)

    if pfn:
      # Check if the PFN corresponds to the LFN convention
      ind = pfn.find(lfn)
      lfn_pfn = True   # flag that the lfn is contained in the pfn
      if ind == -1:
        if self.LFN_PFN_convention:
          return S_ERROR('PFN does not correspond to the LFN convention')
        else:
          lfn_pfn = False

      # Check if the pfn corresponds to the SE definition
      pfnDict = pfnparse(pfn)
      protocol = pfnDict['Protocol']
      if protocol not in seDict.keys():
        return S_ERROR('Unknown protocol %s for SE %s' % (protocol,se))

      pfnpath = pfnDict['Path']
      seAccessDict = seDict[protocol]
      sepath = seAccessDict['Path']
      ind = pfnpath.find(sepath)
      if ind == -1:
        return S_ERROR('The given PFN %s does not correspond to the %s SE definition' % \
                       (pfn,se))

      # Check the full LFN-PFN-SE convention
      lfn_pfn_se = True
      if lfn_pfn:
        seAccessDict['Path'] = sepath + '/' + lfn
        check_pfn = pfnunparse(seAccessDict)
        if check_pfn != pfn:
          if self.LFN_PFN_convention:
            return S_ERROR('PFN does not correspond to the LFN convention')
          else:
            lfn_pfn_se = False

      if lfn_pfn_se:
        # The PFN is fully convention compliant. Store the minimalistic definition
        pass

      if not full_pfn:
      # The LFN is contained in the PFN. Check that the PFN is valid with
      # respect to the SE definition
        pass

      else:
      # The PFN
        pass

    else:
      pass


