########################################################################
# $HeadURL:  $
########################################################################
__RCSID__   = "$Id: $"

""" FileCatalogHandler is a simple Replica and Metadata Catalog service 
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB
import time,os
# This is a global instance of the DataIntegrityDB class
fcDB = False

def initializeFileCatalogHandler(serviceInfo):

  global fcDB
  fcDB = FileCatalogDB()
  return S_OK()

class FileCatalogHandler(RequestHandler):
  
  ###################################################################
  #  User and Group operations
  #
  types_addUser = [StringTypes]
  def export_addUser(self,userName):
    """ Add a new user name to the File Catalog
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.addUser(userName,user,group)
  
  types_deleteUser = [StringTypes]
  def export_deleteUser(self,userName):
    """ Delete user from the File Catalog
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.deleteUser(userName,user,group)
  
  types_getUsers = []
  def export_getUsers(self):
    """ Get all the users defined in the File Catalog
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.getUsers(user,group)
  
  types_addGroup = [StringTypes]
  def export_addGroup(self,groupName):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.addGroup(groupName,user,group)
  
  types_deleteGroup = [StringTypes]
  def export_deleteGroup(self,groupName):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.deleteGroup(groupName,user,group)
  
  types_getGroups = []
  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.getGroups(user,group)
  
  ########################################################################
  # File operations
  #
  types_addFile = [[ListType,DictType]+list(StringTypes)]
  def export_addFile(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.addFile(lfns,user,group)
  
  types_addReplica = [[ListType,DictType]+list(StringTypes)]
  def export_addReplica(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.addReplica(lfns,user,group)
  
  types_getReplicas = [[ListType,DictType]+list(StringTypes)]
  def export_getReplicas(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.getReplicas(lfns,user,group)
  
  ########################################################################
  # Directory operations
  #
  types_listDirectory = [[ListType,DictType]+list(StringTypes),BooleanType]
  def export_listDirectory(self,lfns,verbose):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.listDirectory(lfns,user,group,verbose=verbose)
  
  types_isDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_isDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.isDirectory(lfns,user,group)
  
  types_createDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_createDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.createDirectory(lfns,user,group)

  ########################################################################
  # Path operations
  #  
  types_changePathOwner = [[ListType,DictType]+list(StringTypes)]
  def export_changePathOwner(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.changePathOwner(lfns,user,group)
  
  types_changePathGroup = [[ListType,DictType]+list(StringTypes)]
  def export_changePathGroup(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.changePathGroup(lfns,user,group)
  
  types_changePathMode = [[ListType,DictType]+list(StringTypes)]
  def export_changePathMode(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.changePathMode(lfns,user,group)