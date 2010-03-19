########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

""" FileCatalogHandler is a simple Replica and Metadata Catalog service  """

from DIRAC.Core.DISET.RequestHandler                import RequestHandler
from DIRAC                                          import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogDB    import FileCatalogDB

import time,os
from types import *

# This is a global instance of the DataIntegrityDB class
fcDB = False

def initializeFileCatalogHandler(serviceInfo):
  global fcDB
  fcDB = FileCatalogDB()
  serviceCS = serviceInfo ['serviceSectionPath']

  databaseConfig = {}

  # Obtain the plugins to be used for DB interaction
  databaseConfig['UserGroupManager'] = gConfig.getValue('%s/%s' % (serviceCS,'UserGroupManager'),'UserAndGroupManagerDB')
  databaseConfig['SEManager'] = gConfig.getValue('%s/%s' % (serviceCS,'SEManager'),'SEManagerDB')
  databaseConfig['SecurityManager'] = gConfig.getValue('%s/%s' % (serviceCS,'SecurityManager'),'NoSecurityManager')
  databaseConfig['DirectoryManager'] = gConfig.getValue('%s/%s' % (serviceCS,'DirectoryManager'),'DirectoryLevelTree')
  databaseConfig['FileManager'] = gConfig.getValue('%s/%s' % (serviceCS,'FileManager'),'FileManager')

  # Obtain some general configuration of the database

  # If true this option ensures that all GUIDs associated to files are unique
  databaseConfig['UniqueGUID'] = gConfig.getValue('%s/%s' % (serviceCS,'UniqueGUID'),False)
  # If true this option allows global read access to all files/directories
  databaseConfig['GlobalRead'] = gConfig.getValue('%s/%s' % (serviceCS,'GlobalRead'),True)
  # If true this option will ensure that all replicas being registered conform to the LFN->PFN convention
  databaseConfig['LFNPFNConvention'] = gConfig.getValue('%s/%s' % (serviceCS,'LFNPFNConvention'),True)
  # If true this option not store PFNs in the replica table but rather resolve it at read time
  databaseConfig['ResolvePFN'] = gConfig.getValue('%s/%s' % (serviceCS,'ResolvePFN'),True)
  # Default umask
  databaseConfig['DefaultUmask'] = gConfig.getValue('%s/%s' % (serviceCS,'DefaultUmask'),0775)
  
  fcDB.setConfig(databaseConfig)
  return S_OK()

class FileCatalogHandler(RequestHandler):
  
  ########################################################################
  # Path operations (not updated)
  #  
  types_changePathOwner = [[ListType,DictType]+list(StringTypes)]
  def export_changePathOwner(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathOwner(lfns,self.getRemoteCredentials())
  
  types_changePathGroup = [[ListType,DictType]+list(StringTypes)]
  def export_changePathGroup(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathGroup(lfns,self.getRemoteCredentials())
  
  types_changePathMode = [[ListType,DictType]+list(StringTypes)]
  def export_changePathMode(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathMode(lfns,self.getRemoteCredentials())

  ########################################################################
  # ACL Operations
  #
  types_getPathPermissions = [[ListType,DictType]+list(StringTypes)]
  def export_getPathPermissions(self, lfns):
    """ Determine the ACL information for a supplied path
    """
    return fcDB.getPathPermissions(lfns,self.getRemoteCredentials())

  ###################################################################
  #
  #  isOK
  #

  types_isOK = []
  def export_isOK(self):
    """ returns S_OK if DB is connected
    """
    if fcDB and fcDB._connected:
      return S_OK()
    return S_ERROR( 'Server not connected to DB' )
  
  ###################################################################
  #
  #  User/Group write operations
  #

  types_addUser = [StringTypes]
  def export_addUser(self,userName):
    """ Add a new user to the File Catalog """
    return fcDB.addUser(userName,self.getRemoteCredentials())
  
  types_deleteUser = [StringTypes]
  def export_deleteUser(self,userName):
    """ Delete user from the File Catalog """
    return fcDB.deleteUser(userName,self.getRemoteCredentials())
  
  types_addGroup = [StringTypes]
  def export_addGroup(self,groupName):
    """ Add a new group to the File Catalog """
    return fcDB.addGroup(groupName,self.getRemoteCredentials())
  
  types_deleteGroup = [StringTypes]
  def export_deleteGroup(self,groupName):
    """ Delete group from the File Catalog """
    return fcDB.deleteGroup(groupName,self.getRemoteCredentials())
  
  ###################################################################
  #
  #  User/Group read operations
  #

  types_getUsers = []
  def export_getUsers(self):
    """ Get all the users defined in the File Catalog """
    return fcDB.getUsers(self.getRemoteCredentials())
  
  types_getGroups = []
  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog """
    return fcDB.getGroups(self.getRemoteCredentials())

  ########################################################################
  #
  # Path read operations
  #

  types_exists = [[ListType,DictType]+list(StringTypes)]
  def export_exists(self, lfns):
    """ Check whether the supplied paths exists """
    return fcDB.exists(lfns,self.getRemoteCredentials())

  ########################################################################
  #
  # File write operations
  #

  types_addFile = [[ListType,DictType]+list(StringTypes)]
  def export_addFile(self,lfns):
    """ Register supplied files """
    return fcDB.addFile(lfns,self.getRemoteCredentials())
  
  types_removeFile = [[ListType,DictType]+list(StringTypes)]
  def export_removeFile(self,lfns):
    """ Remove the supplied lfns """
    return fcDB.removeFile(lfns,self.getRemoteCredentials())
  
  types_addReplica = [[ListType,DictType]+list(StringTypes)]
  def export_addReplica(self,lfns):
    """ Register supplied replicas """
    return fcDB.addReplica(lfns,self.getRemoteCredentials())

  types_removeReplica = [[ListType,DictType]+list(StringTypes)]
  def export_removeReplica(self,lfns):
    """ Remove the supplied replicas """
    return fcDB.removeReplica(lfns,self.getRemoteCredentials())

  types_setReplicaStatus = [[ListType,DictType]+list(StringTypes)]
  def export_setReplicaStatus(self,lfns):
    """ Set the status for the supplied replicas """
    return fcDB.setReplicaStatus(lfns,self.getRemoteCredentials())

  types_setReplicaHost = [[ListType,DictType]+list(StringTypes)]
  def export_setReplicaHost(self,lfns):
    """ Change the registered SE for the supplied replicas """
    return fcDB.setReplicaHost(lfns,self.getRemoteCredentials())

  ########################################################################
  #
  # File read operations
  #

  types_isFile = [[ListType,DictType]+list(StringTypes)]
  def export_isFile(self,lfns):
    """ Check whether the supplied lfns are files """
    return fcDB.isFile(lfns,self.getRemoteCredentials())
  
  types_getFileSize = [[ListType,DictType]+list(StringTypes)]
  def export_getFileSize(self,lfns):
    """ Get the size associated to supplied lfns """
    return fcDB.getFileSize(lfns,self.getRemoteCredentials())  
  
  types_getFileMetadata = [[ListType,DictType]+list(StringTypes)]
  def export_getFileMetadata(self,lfns):
    """ Get the metadata associated to supplied lfns """
    return fcDB.getFileMetadata(lfns,self.getRemoteCredentials())  
  
  types_getReplicas = [[ListType,DictType]+list(StringTypes)]
  def export_getReplicas(self,lfns,allStatus=False):
    """ Get replicas for supplied lfns """
    return fcDB.getReplicas(lfns,allStatus,self.getRemoteCredentials())
  
  types_getReplicaStatus = [[ListType,DictType]+list(StringTypes)]
  def export_getReplicaStatus(self,lfns):
    """ Get the status for the supplied replicas """
    return fcDB.getReplicaStatus(lfns,self.getRemoteCredentials())

  
  ########################################################################
  #
  # Directory write operations
  #

  types_createDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_createDirectory(self,lfns):
    """ Create the supplied directories """
    return fcDB.createDirectory(lfns,self.getRemoteCredentials())

  types_removeDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_removeDirectory(self,lfns):
    """ Remove the supplied directories """
    return fcDB.removeDirectory(lfns,self.getRemoteCredentials())

  ########################################################################
  #
  # Directory read operations
  #

  types_listDirectory = [[ListType,DictType]+list(StringTypes),BooleanType]
  def export_listDirectory(self,lfns,verbose):
    """ List the contents of supplied directories """
    return fcDB.listDirectory(lfns,self.getRemoteCredentials(),verbose=verbose)
  
  types_isDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_isDirectory(self,lfns):
    """ Determine whether supplied path is a directory """
    return fcDB.isDirectory(lfns,self.getRemoteCredentials())
  
  types_getDirectorySize = [[ListType,DictType]+list(StringTypes)]
  def export_getDirectorySize(self,lfns):
    """ Get the size of the supplied directory """
    return fcDB.getDirectorySize(lfns,self.getRemoteCredentials())

  types_getDirectoryReplicas = [[ListType,DictType]+list(StringTypes)]
  def export_getDirectoryReplicas(self,lfns,allStatus=False):
    """ Get the replicas for file in the supplied directory """
    return fcDB.getDirectoryReplicas(lfns,allStatus,self.getRemoteCredentials())

  ########################################################################
  #
  # Adinistrative database operations
  #

  types_getCatalogContents = []
  def export_getCatalogContents(self):
    """ Get the number of registered directories, files and replicas in various tables """
    return fcDB.getCatalogContents(self.getRemoteCredentials())

  ########################################################################
  # Metadata Catalog Operations
  #

  types_addMetadataField = [ StringTypes, StringTypes ]
  def export_addMetadataField(self, fieldName, fieldType ):
    """ Add a new metadata field of the given type
    """
    return fcDB.addMetadataField( fieldName, fieldType, self.getRemoteCredentials() )

  types_deleteMetadataField = [ StringTypes ]
  def export_deleteMetadataField(self, fieldName ):
    """ Delete the metadata field 
    """
    return fcDB.deleteMetadataField( fieldName, self.getRemoteCredentials() )
  
  types_getMetadataFields = [ ]
  def export_getMetadataFields(self):
    """ Get all the metadata fields
    """
    return fcDB.getMetadataFields(self.getRemoteCredentials())
  
  types_setMetadata = [ StringTypes, StringTypes ]
  def export_setMetadata(self, path, fieldName, fieldValue ):
    """ Set metadata parameter for the given path
    """
    return fcDB.setMetadata( path, fieldName, fieldValue, self.getRemoteCredentials() )
  
  types_getDirectoryMetadata = [ StringTypes ]
  def export_getDirectoryMetadata(self,path):
    """ Get all the metadata valid for the given directory path
    """
    return fcDB.getDirectoryMetadata(path, self.getRemoteCredentials())
  
  types_findDirectoriesByMetadata = [ DictType ]
  def export_findDirectoriesByMetadata(self,metaDict):
    """ Find all the directories satisfying the given metadata set
    """
    return fcDB.findDirectoriesByMetadata(metaDict, self.getRemoteCredentials())
  
  types_findFilesByMetadata = [ DictType ]
  def export_findFilesByMetadata(self,metaDict):
    """ Find all the files satisfying the given metadata set
    """
    return fcDB.findFilesByMetadata(metaDict, self.getRemoteCredentials())
  
  types_getCompatibleMetadata = [ DictType ]
  def export_getCompatibleMetadata(self,metaDict):
    """ Get metadata values compatible with the given metadata subset
    """
    return fcDB.getCompatibleMetadata(metaDict, self.getRemoteCredentials())
