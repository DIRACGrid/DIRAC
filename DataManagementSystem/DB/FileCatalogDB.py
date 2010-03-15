########################################################################
# $Id$
########################################################################
""" DIRAC FileCatalog Database """

__RCSID__ = "$Id$"

import re, os, sys, md5, random
import string, time, datetime
import threading
from types import *

from DIRAC                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                     import DB
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse
from DIRAC.Core.Utilities.List              import intListToString,stringListToString

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SEManager             import SEManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.UserAndGroupManager   import UserAndGroupManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryMetadata     import DirectoryMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectorySimpleTree   import DirectorySimpleTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryNodeTree     import DirectoryNodeTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryLevelTree    import DirectoryLevelTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryFlatTree     import DirectoryFlatTree
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities             import * 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager       import NoSecurityManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager           import FileManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SimpleFileManager     import SimpleFileManager


DEBUG = 0
     
#############################################################################
class FileCatalogDB(DB, DirectoryMetadata):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'FileCatalogDB','DataManagement/FileCatalogDB',maxQueueSize)

    # In memory storage of the directory parameters
    self.directories = {}
    self.users = {}
    self.groups = {}
    self.umask = 0775

    # User/group manager
    self.ugManager = UserAndGroupManager()
    self.ugManager.setDatabase(self)
    self.users = {}
    self.ugManager.setUsers(self.users)
    self.groups = {}
    self.ugManager.setGroups(self.groups)

    # SEManager 
    self.seDefinitions = {}
    self.seUpdatePeriod = 600
    self.seManager = SEManager()
    self.seManager.setDatabase(self)
    self.seManager.setSEDefinitions(self.seDefinitions)
    self.seManager.setUpdatePeriod(self.seUpdatePeriod)

    # Directory Tree instance
    self.dtree = DirectoryFlatTree()
    self.dtree.setDatabase(self)
    self.dtree.setUmask(self.umask)
    self.dtree.setUserGroupManager(self.ugManager)

    # File Manager instance
    self.fileManager = SimpleFileManager(self.dtree,self.seManager,self.ugManager)
    self.fileManager.setDatabase(self)
    self.fileManager.setUmask(self.umask)
    self.lfnConvention = False
    self.fileManager.setLFNConvention(self.lfnConvention)
    self.resolvePFN = False
    self.fileManager.setResolvePFN(self.resolvePFN)
    
    # So that the directory manager knows of the file manager
    self.dtree.setFileManager(self.fileManager)

    # Security module instance
    self.globalReadAccess = True
    self.securityManager = NoSecurityManager(self.dtree,self.fileManager,globalReadAccess=self.globalReadAccess)

  def setUmask(self,umask):
    self.umask = umask
    self.fileManager.setUmask(self.umask)
    self.dtree.setUmask(self.umask)


  ########################################################################
  #
  #  User/groups based write methods
  #

  def addUser(self,userName,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.addUser(userName)

  def deleteUser(self,userName,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.deleteUser(userName)

  def addGroup(self,groupName,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.addGroup(userName)
  
  def deleteGroup(self,groupName,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.deleteGroup(userName)
  
  ########################################################################
  #
  #  User/groups based write methods
  #

  def getUsers(self,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.getUsers(userName)

  def getGroups(self,credDict):
    res = self._checkAdminPermission()
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.getGroups(userName)

  ########################################################################
  #
  #  Path based read methods
  #

  def exists(self, lfns, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.exists(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    notExist = []
    for lfn in res['Value']['Successful'].keys():
      if not successful[lfn]:
        notExist.append(lfn)
        successful.pop(lfn)
    if notExist:
      res = self.dtree.exists(notExist)
      if not res['OK']:
        return res    
      failed.update(res['Value']['Failed'])
      successful.update(res['Value']['Successful'])
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  File based write methods
  #

  def addFile(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.addFile(res['Value']['Successful'],credDict)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def removeFile(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.removeFile(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def addReplica(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.addReplica(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def removeReplica(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.removeReplica(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def setReplicaStatus(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setReplicaStatus(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def setReplicaHost(self, lfns, credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setReplicaHost(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  File based read methods
  #

  def isFile(self, lfns, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.isFile(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def getFileSize(self, lfns, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getFileSize(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def getFileMetadata(self, lfns, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getFileMetadata(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def getReplicas(self, lfns, allStatus, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getReplicas(res['Value']['Successful'],allStatus=allStatus)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def getReplicaStatus(self, lfns, credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getReplicaStatus(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  Directory based write methods
  #

  def createDirectory(self,lfns,credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.createDirectory(res['Value']['Successful'],credDict)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def removeDirectory(self,lfns,credDict):
    res = self._checkPathPermissions('write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.removeDirectory(res['Value']['Successful'],credDict)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  Directory based read methods
  #

  def listDirectory(self,lfns,credDict,verbose=False):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.listDirectory(res['Value']['Successful'],verbose=verbose)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def isDirectory(self,lfns,credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.isDirectory(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def getDirectoryReplicas(self,lfns,allStatus,credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.getDirectoryReplicas(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def getDirectorySize(self,lfns,credDict):
    res = self._checkPathPermissions('read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.getDirectorySize(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  Security based methods
  #

  def _checkAdminPermission(self,credDict):
    return self.securityManager.hasAdminAccess(credDict)

  def _checkPathPermissions(self,opType,lfns,credDict):
    res = checkArgumentDict(lfns)
    if not res['OK']:
      return res
    lfns = res['Value']
    res = self.securityManager.hasAccess(opType,lfns.keys(),credDict)
    if not res['OK']:
      return res
    # Do not consider those paths for which we failed to determine access
    failed = res['Value']['Failed']
    for lfn in failed.keys():
      lfns.pop(lfn)
    # Do not consider those paths for which access is denied
    for lfn,access in res['Value']['Successful'].items():
      if not access:
        failed[lfn] = 'Permission denied'
        lfns.pop(lfn)
    return S_OK( {'Successful':lfns,'Failed':failed} )
