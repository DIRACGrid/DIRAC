########################################################################
# $Id$
########################################################################
""" DIRAC FileCatalog Database """

__RCSID__ = "$Id$"

from DIRAC                                                                     import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                        import DB
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryMetadata     import DirectoryMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileMetadata          import FileMetadata
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectorySimpleTree   import DirectorySimpleTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryNodeTree     import DirectoryNodeTree 
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryLevelTree    import DirectoryLevelTree
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryFlatTree     import DirectoryFlatTree
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManagerFlat       import FileManagerFlat
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager           import FileManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SEManager             import SEManagerCS,SEManagerDB
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager       import NoSecurityManager,DirectorySecurityManager,FullSecurityManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.UserAndGroupManager   import UserAndGroupManagerCS,UserAndGroupManagerDB
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DatasetManager        import DatasetManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities             import checkArgumentFormat

#############################################################################
class FileCatalogDB(DB):

  def __init__( self, databaseLocation='DataManagement/FileCatalogDB', maxQueueSize=10 ):
    """ Standard Constructor
    """
    
    # The database location can be specified in System/Database form or in just the Database name
    # in the DataManagement system 
    db = databaseLocation
    if db.find('/') == -1:
      db = 'DataManagement/' + db
    DB.__init__(self,'FileCatalogDB',db,maxQueueSize)

  def setConfig(self,databaseConfig):

    self.directories = {}
    # In memory storage of the various parameters
    self.users = {}
    self.uids = {}
    self.groups = {}
    self.gids = {}
    self.seNames = {}
    self.seids = {}
    self.seDefinitions = {}

    # Obtain some general configuration of the database
    self.uniqueGUID = databaseConfig['UniqueGUID']
    self.globalReadAccess = databaseConfig['GlobalReadAccess']
    self.lfnPfnConvention = databaseConfig['LFNPFNConvention']
    if self.lfnPfnConvention == "None":
      self.lfnPfnConvention = False
    self.resolvePfn = databaseConfig['ResolvePFN']
    self.umask = databaseConfig['DefaultUmask']
    self.validFileStatus = databaseConfig['ValidFileStatus']
    self.validReplicaStatus = databaseConfig['ValidReplicaStatus']
    self.visibleFileStatus = databaseConfig['VisibleFileStatus']
    self.visibleReplicaStatus = databaseConfig['VisibleReplicaStatus']

    try:
      # Obtain the plugins to be used for DB interaction
      self.ugManager = eval("%s(self)" % databaseConfig['UserGroupManager'])
      self.seManager = eval("%s(self)" % databaseConfig['SEManager'])
      self.securityManager = eval("%s(self)" % databaseConfig['SecurityManager'])
      self.dtree = eval("%s(self)" % databaseConfig['DirectoryManager'])
      self.fileManager = eval("%s(self)" % databaseConfig['FileManager'])
      self.datasetManager = eval("%s(self)" % databaseConfig['DatasetManager'])
      self.dmeta = eval("%s(self)" % databaseConfig['DirectoryMetadata'])
      self.fmeta = eval("%s(self)" % databaseConfig['FileMetadata'])
    except Exception, x:
      gLogger.fatal("Failed to create database objects",x)
      return S_ERROR("Failed to create database objects")

    return S_OK()
    
  def setUmask(self,umask):
    self.umask = umask

  ########################################################################
  #
  #  SE based write methods
  #
  
  def addSE(self,seName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.seManager.addSE(seName)
    
  def deleteSE(self,seName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.seManager.deleteSE(seName)

  ########################################################################
  #
  #  User/groups based write methods
  #

  def addUser(self,userName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.addUser(userName)

  def deleteUser(self,userName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.deleteUser(userName)

  def addGroup(self,groupName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.addGroup(groupName)
  
  def deleteGroup(self,groupName,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.deleteGroup(groupName)
  
  ########################################################################
  #
  #  User/groups based read methods
  #

  def getUsers(self,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.getUsers()

  def getGroups(self,credDict):
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    return self.ugManager.getGroups()

  ########################################################################
  #
  #  Path based read methods
  #

  def exists(self, lfns, credDict):
    res = self._checkPathPermissions('Read', lfns, credDict)
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
  
  def getPathPermissions(self, lfns, credDict):
    """ Get permissions for the given user/group to manipulate the given lfns 
    """
    res = checkArgumentFormat(lfns)
    if not res['OK']:
      return res
    lfns = res['Value']

    return self.securityManager.getPathPermissions( lfns.keys(), credDict )
  
  ########################################################################
  #
  #  Path based read methods
  #

  def changePathOwner(self, lfns, credDict, recursive=False):
    """ Change the owner of the given list of paths
    """
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.changePathOwner(res['Value']['Successful'],credDict, recursive)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']   
    return S_OK({'Successful':successful,'Failed':failed}) 
  
  def changePathGroup(self, lfns, credDict, recursive=False):
    """ Change the group of the given list of paths
    """
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.changePathGroup(res['Value']['Successful'],credDict, recursive)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']   
    return S_OK({'Successful':successful,'Failed':failed}) 

  def changePathMode(self, lfns, credDict, recursive=False):
    """ Change the mode of the given list of paths
    """
    res = self._checkPathPermissions('Write', lfns, credDict)    
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.changePathMode(res['Value']['Successful'],credDict, recursive)    
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']   
    return S_OK({'Successful':successful,'Failed':failed}) 

  ########################################################################
  #
  #  File based write methods
  #

  def addFile(self, lfns, credDict):
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.addFile(res['Value']['Successful'],credDict)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def setFileStatus(self, lfns, credDict):
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setFileStatus( res['Value']['Successful'], credDict )
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def removeFile(self, lfns, credDict):
    res = self._checkPathPermissions('Write', lfns, credDict)
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
    res = self._checkPathPermissions('Write', lfns, credDict)
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
    res = self._checkPathPermissions('Write', lfns, credDict)
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
    res = self._checkPathPermissions('Write', lfns, credDict)
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
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setReplicaHost(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  def setFileOwner(self,lfns,credDict):
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setFileOwner(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
  
  def setFileGroup(self,lfns,credDict):  
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setFileGroup(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
    
  def setFileMode(self,lfns,credDict):  
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.setFileMode(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )
    
  def addFileAncestors(self,lfns,credDict):
    """ Add ancestor information for the given LFNs
    """        
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.addFileAncestors(res['Value']['Successful'])
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
    res = self._checkPathPermissions('Read', lfns, credDict)
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
    res = self._checkPathPermissions('Read', lfns, credDict)
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
    res = self._checkPathPermissions('Read', lfns, credDict)
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
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getReplicas(res['Value']['Successful'],allStatus=allStatus)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful, 'Failed':failed, 'SEPrefixes': res['Value'].get( 'SEPrefixes', {} ) } )

  def getReplicaStatus(self, lfns, credDict):
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getReplicaStatus(res['Value']['Successful'])
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )  
    
  def getFileAncestors(self, lfns, depths, credDict):
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getFileAncestors(res['Value']['Successful'],depths)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )        
    
  def getFileDescendents(self, lfns, depths, credDict):
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.fileManager.getFileDescendents(res['Value']['Successful'],depths)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )     
  
  def getFileDetails( self, lfnList, credDict ):
    """ Get all the metadata for the given files
    """  
    connection = False
    result = self.fileManager._findFiles( lfnList, connection=connection )
    if not result['OK']:
      return result
    resultDict = {}
    fileIDDict = {}
    lfnDict = result['Value']['Successful']
    for lfn in lfnDict:
      fileIDDict[lfnDict[lfn]['FileID']] = lfn
      
    result = self.fileManager._getFileMetadataByID( fileIDDict.keys(), connection=connection )
    if not result['OK']:
      return result
    for fileID in result['Value']:
      resultDict[ fileIDDict[fileID] ] = result['Value'][fileID]
      
    result = self.fmeta._getFileUserMetadataByID( fileIDDict.keys(), credDict, connection=connection )
    if not result['OK']:
      return result
    for fileID in fileIDDict:
      resultDict[ fileIDDict[fileID] ].setdefault( 'Metadata', {} )
      if fileID in result['Value']:
        resultDict[ fileIDDict[fileID] ]['Metadata'] = result['Value'][fileID]    
      
    return S_OK(resultDict) 

  ########################################################################
  #
  #  Directory based Write methods
  #

  def createDirectory(self,lfns,credDict):
    res = self._checkPathPermissions('Write', lfns, credDict)
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
    res = self._checkPathPermissions('Write', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']
    if successful:
      res = self.dtree.removeDirectory(res['Value']['Successful'],credDict)
      if not res['OK']:
        return res
      failed.update(res['Value']['Failed'])
      successful = res['Value']['Successful']
      if not successful:
        return S_OK( {'Successful':successful,'Failed':failed} )
    else:
      return S_OK( {'Successful':successful,'Failed':failed} )
    
    # Remove the directory metadata now
    dirIdList = [ successful[p]['DirID'] for p in successful ]
    result = self.dmeta.removeMetadataForDirectory( dirIdList,credDict )
    if not result['OK']:
      return result
    failed.update(result['Value']['Failed'])
    successful = result['Value']['Successful']
    return S_OK( {'Successful':successful,'Failed':failed} )

  ########################################################################
  #
  #  Directory based read methods
  #

  def listDirectory(self,lfns,credDict,verbose=False):
    res = self._checkPathPermissions('Read', lfns, credDict)
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
    res = self._checkPathPermissions('Read', lfns, credDict)
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
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.getDirectoryReplicas(res['Value']['Successful'],allStatus)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    return S_OK( { 'Successful':successful, 'Failed':failed, 'SEPrefixes': res['Value'].get( 'SEPrefixes', {} )} )

  def getDirectorySize(self,lfns,longOutput,fromFiles,credDict):
    res = self._checkPathPermissions('Read', lfns, credDict)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    res = self.dtree.getDirectorySize(res['Value']['Successful'],longOutput,fromFiles)
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    queryTime = res['Value'].get('QueryTime',-1.)
    return S_OK( {'Successful':successful,'Failed':failed,'QueryTime':queryTime} )
  
  def rebuildDirectoryUsage(self):
    """ Rebuild DirectoryUsage table from scratch
    """
    
    result = self.dtree._rebuildDirectoryUsage()
    return result

  def repairCatalog( self, directoryFlag=True, credDict={} ):
    """ Repair catalog inconsistencies
    """
    result = S_OK()
    if directoryFlag:
      result = self.dtree.recoverOrphanDirectories( credDict )
      
    return result 
    
  #######################################################################
  #
  #  Catalog metadata methods
  #  
  
  def setMetadata(self, path, metadataDict, credDict):
    """ Add metadata to the given path
    """
    res = self._checkPathPermissions('Write', path, credDict)   
    if not res['OK']:
      return res
    if not res['Value']['Successful']:
      return S_ERROR('Permission denied')
    if not res['Value']['Successful'][path]:
      return S_ERROR('Permission denied') 
      
    result = self.dtree.isDirectory({path:True})
    if not result['OK']:
      return result
    if not result['Value']['Successful']:
      return S_ERROR('Failed to determine the path type')
    if result['Value']['Successful'][path]:
      # This is a directory
      return self.dmeta.setMetadata(path,metadataDict,credDict)
    else:
      # This is a file      
      return self.fmeta.setMetadata(path,metadataDict,credDict)      
    
  def setMetadataBulk( self, pathMetadataDict, credDict ):
    """  Add metadata for the given paths
    """  
    successful = {}
    failed = {}
    for path, metadataDict in pathMetadataDict.items():
      result = self.setMetadata( path, metadataDict, credDict )
      if result['OK']:
        successful[path] = True
      else:
        failed[path] = result['Message']
        
    return S_OK( { 'Successful': successful, 'Failed': failed } )      
    
  def removeMetadata(self, path, metadata, credDict):
    """ Add metadata to the given path
    """
    res = self._checkPathPermissions('Write', path, credDict)   
    if not res['OK']:
      return res
    if not res['Value']['Successful']:
      return S_ERROR('Permission denied')
    if not res['Value']['Successful'][path]:
      return S_ERROR('Permission denied') 
      
    result = self.dtree.isDirectory({path:True})
    if not result['OK']:
      return result
    if not result['Value']['Successful']:
      return S_ERROR('Failed to determine the path type')
    if result['Value']['Successful'][path]:
      # This is a directory
      return self.dmeta.removeMetadata(path,metadata,credDict)
    else:
      # This is a file      
      return self.fmeta.removeMetadata(path,metadata,credDict)                                  
    
  #######################################################################
  #
  #  Catalog admin methods
  #

  def getCatalogCounters(self,credDict):
    counterDict = {}
    res = self._checkAdminPermission(credDict)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Permission denied")
    #res = self.dtree.getDirectoryCounters()
    #if not res['OK']:
    #  return res
    #counterDict.update(res['Value'])
    res = self.fileManager.getFileCounters()
    if not res['OK']:
      return res
    counterDict.update(res['Value'])
    res = self.fileManager.getReplicaCounters() 
    if not res['OK']:
      return res
    counterDict.update(res['Value'])
    res = self.dtree.getDirectoryCounters() 
    if not res['OK']:
      return res
    counterDict.update(res['Value'])
    return S_OK(counterDict)

  ########################################################################
  #
  #  Security based methods
  #

  def _checkAdminPermission(self,credDict):
    return self.securityManager.hasAdminAccess(credDict)

  def _checkPathPermissions(self,operation,lfns,credDict):
    res = checkArgumentFormat(lfns)
    if not res['OK']:
      return res
    lfns = res['Value']
    res = self.securityManager.hasAccess(operation,lfns.keys(),credDict)   
    if not res['OK']:
      return res
    # Do not consider those paths for which we failed to determine access
    failed = res['Value']['Failed']
    for lfn in failed.keys():
      lfns.pop(lfn)
    # Do not consider those paths for which access is denied
    successful = {}
    for lfn,access in res['Value']['Successful'].items():
      if not access:
        failed[lfn] = 'Permission denied'
      else:  
        successful[lfn] = lfns[lfn]
    return S_OK( {'Successful':successful,'Failed':failed} )
