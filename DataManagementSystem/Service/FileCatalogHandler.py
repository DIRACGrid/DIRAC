########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

""" FileCatalogHandler is a simple Replica and Metadata Catalog service 
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
import time,os
# This is a global instance of the DataIntegrityDB class
fcDB = False

def initializeFileCatalogHandler(serviceInfo):

  global fcDB
  fcDB = FileCatalogDB()
  credDict = { 'properties': Properties.FC_MANAGEMENT }
  fcDB.registerUsersAndGroupsFromCS( credDict )
  gThreadScheduler.addPeriodicTask( 6 * 60 * 60,  fcDB.registerUsersAndGroupsFromCS, ( credDict ) )
  return S_OK()

class FileCatalogHandler(RequestHandler):
  
  ###################################################################
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
  #  User and Group operations
  #
  types_addUser = [StringTypes]
  def export_addUser(self,userName):
    """ Add a new user to the File Catalog
    """
    return fcDB.addUser(userName,self.getRemoteCredentials())
  
  types_deleteUser = [StringTypes]
  def export_deleteUser(self,userName):
    """ Delete user from the File Catalog
    """
    return fcDB.deleteUser(userName,self.getRemoteCredentials())
  
  types_getUsers = []
  def export_getUsers(self):
    """ Get all the users defined in the File Catalog
    """
    return fcDB.getUsers(self.getRemoteCredentials())
  
  types_addGroup = [StringTypes]
  def export_addGroup(self,groupName):
    """ Add a new group to the File Catalog
    """
    return fcDB.addGroup(groupName,self.getRemoteCredentials())
  
  types_deleteGroup = [StringTypes]
  def export_deleteGroup(self,groupName):
    """ Delete group from the File Catalog
    """
    return fcDB.deleteGroup(groupName,self.getRemoteCredentials())
  
  types_getGroups = []
  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog
    """
    return fcDB.getGroups(self.getRemoteCredentials())
  
  ########################################################################
  # File operations
  #
  types_addFile = [[ListType,DictType]+list(StringTypes)]
  def export_addFile(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.addFile(lfns,self.getRemoteCredentials())
  
  types_removeFile = [[ListType,DictType]+list(StringTypes)]
  def export_removeFile(self,lfns):
    """ Remove files for the given list of LFNs
    """
    return fcDB.removeFile(lfns,self.getRemoteCredentials())
  
  types_isFile = [[ListType,DictType]+list(StringTypes)]
  def export_isFile(self,lfns):
    """ Check if the given LFNs are files registered in the catalog
    """
    return fcDB.isFile(lfns,self.getRemoteCredentials())
  
  types_getFileSize = [[ListType,DictType]+list(StringTypes)]
  def export_getFileSize(self,lfns):
    """ Check if the given LFNs are files registered in the catalog
    """
    return fcDB.getLFNSize(lfns,self.getRemoteCredentials())  
  
  types_getFileMetadata = [[ListType,DictType]+list(StringTypes)]
  def export_getFileMetadata(self,lfns):
    """ Check if the given LFNs are files registered in the catalog
    """
    return fcDB.getLFNMetadata(lfns,self.getRemoteCredentials())  
  
  types_addReplica = [[ListType,DictType]+list(StringTypes)]
  def export_addReplica(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.addReplica(lfns,self.getRemoteCredentials())
  
  types_getReplicas = [[ListType,DictType]+list(StringTypes)]
  def export_getReplicas(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.getReplicas(lfns,self.getRemoteCredentials())
  
  types_removeReplica = [[ListType,DictType]+list(StringTypes)]
  def export_removeReplica(self,lfns):
    """ Remove replicas for the given list of LFNs
    """
    return fcDB.removeReplica(lfns,self.getRemoteCredentials())
  
  ########################################################################
  # Directory operations
  #
  types_listDirectory = [[ListType,DictType]+list(StringTypes),BooleanType]
  def export_listDirectory(self,lfns,verbose):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.listDirectory(lfns,self.getRemoteCredentials(),verbose=verbose)
  
  types_isDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_isDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.isDirectory(lfns,self.getRemoteCredentials())
  
  types_createDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_createDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.createDirectory(lfns,self.getRemoteCredentials())

  types_exists = [[ListType,DictType]+list(StringTypes)]
  def export_exists(self, lfns):
    """ Check if the path exists
    """
    return fcDB.existsLFNs(lfns,self.getRemoteCredentials())

  ########################################################################
  # Path operations
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
