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
  
  types_listDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_listDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.listDirectory(lfns,user,group)
  
  types_isDirectory = [[ListType,DictType]+list(StringTypes)]
  def export_isDirectory(self,lfns):
    """ Get replica info for the given list of LFNs
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    return fcDB.isDirectory(lfns,user,group)