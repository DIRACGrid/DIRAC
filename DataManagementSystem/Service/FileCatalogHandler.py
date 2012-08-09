########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

""" FileCatalogHandler is a simple Replica and Metadata Catalog service  """

from DIRAC.Core.DISET.RequestHandler                import RequestHandler
from DIRAC                                          import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogDB    import FileCatalogDB
from DIRAC.Core.Utilities.List                      import sortList

import time,os
from types import *

# This is a global instance of the DataIntegrityDB class
fcDB = False

def initializeFileCatalogHandler(serviceInfo):
  
  global fcDB
  
  serviceCS = serviceInfo ['serviceSectionPath']
  
  # Instantiate the requested database
  dbLocation = gConfig.getValue('%s/Database' % serviceCS,'DataManagement/FileCatalogDB')
  fcDB = FileCatalogDB(dbLocation)
  
  databaseConfig = {}
  # Obtain the plugins to be used for DB interaction
  gLogger.info("Initializing with FileCatalog with following managers:")
  defaultManagers = {  'UserGroupManager'  : 'UserAndGroupManagerDB',
                       'SEManager'         : 'SEManagerDB',
                       'SecurityManager'   : 'NoSecurityManager',
                       'DirectoryManager'  : 'DirectoryLevelTree',
                       'FileManager'       : 'FileManager',
                       'DirectoryMetadata' : 'DirectoryMetadata',
                       'FileMetadata'      : 'FileMetadata'}
  for configKey in sortList(defaultManagers.keys()):
    defaultValue = defaultManagers[configKey]
    configValue = gConfig.getValue('%s/%s' % (serviceCS,configKey),defaultValue)
    gLogger.info("%s : %s" % (str(configKey).ljust(20),str(configValue).ljust(20)))
    databaseConfig[configKey] = configValue

  # Obtain some general configuration of the database
  gLogger.info("Initializing the FileCatalog with the following configuration:")
  defaultConfig = { 'UniqueGUID'        : False,
                    'GlobalReadAccess'  : True,
                    'LFNPFNConvention'  : True,
                    'ResolvePFN'        : True,
                    'DefaultUmask'      : 0775,
                    'VisibleStatus'     : ['AprioriGood']}
  for configKey in sortList(defaultConfig.keys()):
    defaultValue = defaultConfig[configKey]
    configValue = gConfig.getValue('%s/%s' % (serviceCS,configKey),defaultValue)
    gLogger.info("%s : %s" % (str(configKey).ljust(20),str(configValue).ljust(20)))
    databaseConfig[configKey] = configValue
  res = fcDB.setConfig(databaseConfig)
  return res

class FileCatalogHandler(RequestHandler):
  
  ########################################################################
  # Path operations (not updated)
  #  
  types_changePathOwner = [[ListType,DictType]+list(StringTypes)]
  def export_changePathOwner(self,lfns,recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathOwner(lfns,self.getRemoteCredentials(),recursive)
  
  types_changePathGroup = [[ListType,DictType]+list(StringTypes)]
  def export_changePathGroup(self,lfns,recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathGroup(lfns,self.getRemoteCredentials(),recursive)
  
  types_changePathMode = [[ListType,DictType]+list(StringTypes)]
  def export_changePathMode(self,lfns,recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return fcDB.changePathMode(lfns,self.getRemoteCredentials(),recursive)

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
    
  types_addFileAncestors = [DictType]
  def export_addFileAncestors(self,lfns):
    """ Add file ancestor information for the given list of LFNs """
    return fcDB.addFileAncestors(lfns,self.getRemoteCredentials())  

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

  types_getFileAncestors = [ListType,[ListType,IntType,LongType]]
  def export_getFileAncestors(self,lfns,depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if type(dList) != ListType:
      dList = [depths]
    lfnDict = {}        
    for lfn in lfns:
      lfnDict[lfn] = True
    return fcDB.getFileAncestors(lfnDict,dList,self.getRemoteCredentials())
    
  types_getFileDescendents = [ListType,[ListType,IntType,LongType]]
  def export_getFileDescendents(self,lfns,depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if type(dList) != ListType:
      dList = [depths]      
    lfnDict = {}        
    for lfn in lfns:
      lfnDict[lfn] = True  
    return fcDB.getFileDescendents(lfnDict,dList,self.getRemoteCredentials())  
  
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
  def export_getDirectorySize(self,lfns,long=False):
    """ Get the size of the supplied directory """
    return fcDB.getDirectorySize(lfns,long,self.getRemoteCredentials())
  
  types_rebuildDirectoryUsage = []
  def export_rebuildDirectoryUsage(self):
    """ Rebuild DirectoryUsage table from scratch """
    return fcDB.rebuildDirectoryUsage()

  types_getDirectoryReplicas = [[ListType,DictType]+list(StringTypes)]
  def export_getDirectoryReplicas(self,lfns,allStatus=False):
    """ Get the replicas for file in the supplied directory """
    return fcDB.getDirectoryReplicas(lfns,allStatus,self.getRemoteCredentials())

  ########################################################################
  #
  # Administrative database operations
  #

  types_getCatalogCounters = []
  def export_getCatalogCounters(self):
    """ Get the number of registered directories, files and replicas in various tables """
    return fcDB.getCatalogCounters(self.getRemoteCredentials())

  ########################################################################
  # Metadata Catalog Operations
  #

  types_addMetadataField = [ StringTypes, StringTypes ]
  def export_addMetadataField(self, fieldName, fieldType, metaType='-d' ):
    """ Add a new metadata field of the given type
    """
    if metaType.lower() == "-d":
      return fcDB.dmeta.addMetadataField( fieldName, fieldType, self.getRemoteCredentials() )
    elif metaType.lower() == "-f":
      return fcDB.fmeta.addMetadataField( fieldName, fieldType, self.getRemoteCredentials() )
    else:
      return S_ERROR('Unknown metadata type %s' % metaType)

  types_deleteMetadataField = [ StringTypes ]
  def export_deleteMetadataField(self, fieldName ):
    """ Delete the metadata field 
    """
    result = fcDB.dmeta.deleteMetadataField( fieldName, self.getRemoteCredentials() )
    error = ''
    if not result['OK']:
      error = result['Message']
    result = fcDB.fmeta.deleteMetadataField( fieldName, self.getRemoteCredentials() )  
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"] 
        
    return result    
  
  types_getMetadataFields = [ ]
  def export_getMetadataFields(self):
    """ Get all the metadata fields
    """
    resultDir = fcDB.dmeta.getMetadataFields(self.getRemoteCredentials())
    if not resultDir['OK']:
      return resultDir
    resultFile = fcDB.fmeta.getFileMetadataFields(self.getRemoteCredentials())
    if not resultFile['OK']:
      return resultFile
    
    resultDict = {'DirectoryMetaFields':resultDir['Value'],'FileMetaFields':resultFile['Value']}
    return  S_OK(resultDict)

  types_setMetadata = [ StringTypes, DictType ]
  def export_setMetadata(self, path, metadatadict ):
    """ Set metadata parameter for the given path
    """
    return fcDB.setMetadata( path, metadatadict, self.getRemoteCredentials() )
  
  types_removeMetadata = [ StringTypes, ListType ]
  def export_removeMetadata(self, path, metadata ):
    """ Remove the specified metadata for the given path
    """
    return fcDB.removeMetadata( path, metadata, self.getRemoteCredentials() )
  
  types_getDirectoryMetadata = [ StringTypes ]
  def export_getDirectoryMetadata(self,path):
    """ Get all the metadata valid for the given directory path
    """
    return fcDB.dmeta.getDirectoryMetadata(path, self.getRemoteCredentials())
    
  types_getFileUserMetadata = [ StringTypes ]
  def export_getFileUserMetadata(self,path):
    """ Get all the metadata valid for the given file
    """
    return fcDB.fmeta.getFileUserMetadata(path, self.getRemoteCredentials())  
  
  types_findDirectoriesByMetadata = [ DictType ]
  def export_findDirectoriesByMetadata(self,metaDict,path='/'):
    """ Find all the directories satisfying the given metadata set
    """
    return fcDB.dmeta.findDirectoriesByMetadata(metaDict, path, self.getRemoteCredentials())
  
  types_findFilesByMetadata = [ DictType, StringTypes ]
  def export_findFilesByMetadata(self,metaDict,path='/'):
    """ Find all the files satisfying the given metadata set
    """
    return fcDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
  
  types_getCompatibleMetadata = [ DictType ]
  def export_getCompatibleMetadata(self,metaDict):
    """ Get metadata values compatible with the given metadata subset
    """
    return fcDB.dmeta.getCompatibleMetadata(metaDict, self.getRemoteCredentials())

  types_addMetadataSet = [ StringTypes, DictType ]
  def export_addMetadataSet(self,setName,setDict):
    """ Add a new metadata set
    """
    return fcDB.dmeta.addMetadataSet(setName,setDict, self.getRemoteCredentials())
  
  types_getMetadataSet = [ StringTypes, BooleanType ]
  def export_getMetadataSet(self,setName,expandFlag):
    """ Add a new metadata set
    """
    return fcDB.dmeta.getMetadataSet(setName,expandFlag, self.getRemoteCredentials())
