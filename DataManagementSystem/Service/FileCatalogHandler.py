########################################################################
# $HeadURL $
# File: FileCatalogHandler.py
########################################################################
""" 
:mod: FileCatalogHandler 
 
.. module: FileCatalogHandler
:synopsis: FileCatalogHandler is a simple Replica and Metadata Catalog service 
"""

__RCSID__ = "$Id$"

## imports
import os
from types import IntType, LongType, DictType, StringTypes, BooleanType, ListType
## from DIRAC
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB
from DIRAC.Core.Utilities.List import sortList

# This is a global instance of the FileCatalogDB class
gFileCatalogDB = None

def initializeFileCatalogHandler( serviceInfo ):
  """ handler initialisation """

  global gFileCatalogDB

  dbLocation = getServiceOption( serviceInfo, 'Database', 'DataManagement/FileCatalogDB' )
  gFileCatalogDB = FileCatalogDB( dbLocation )

  databaseConfig = {}
  # Obtain the plugins to be used for DB interaction
  gLogger.info( "Initializing with FileCatalog with following managers:" )
  defaultManagers = {  'UserGroupManager'  : 'UserAndGroupManagerDB',
                       'SEManager'         : 'SEManagerDB',
                       'SecurityManager'   : 'NoSecurityManager',
                       'DirectoryManager'  : 'DirectoryLevelTree',
                       'FileManager'       : 'FileManager',
                       'DirectoryMetadata' : 'DirectoryMetadata',
                       'FileMetadata'      : 'FileMetadata',
                       'DatasetManager'    : 'DatasetManager' }
  for configKey in sortList( defaultManagers.keys() ):
    defaultValue = defaultManagers[configKey]
    configValue = getServiceOption( serviceInfo, configKey, defaultValue )
    gLogger.info( "%-20s : %-20s" % ( str( configKey ), str( configValue ) ) )
    databaseConfig[configKey] = configValue

  # Obtain some general configuration of the database
  gLogger.info( "Initializing the FileCatalog with the following configuration:" )
  defaultConfig = { 'UniqueGUID'          : False,
                    'GlobalReadAccess'    : True,
                    'LFNPFNConvention'    : 'Strong',
                    'ResolvePFN'          : True,
                    'DefaultUmask'        : 0775,
                    'ValidFileStatus'     : ['AprioriGood','Trash','Removing','Probing'],
                    'ValidReplicaStatus'  : ['AprioriGood','Trash','Removing','Probing'],
                    'VisibleFileStatus'   : ['AprioriGood'],
                    'VisibleReplicaStatus': ['AprioriGood']}
  for configKey in sortList( defaultConfig.keys() ):
    defaultValue = defaultConfig[configKey]
    configValue = getServiceOption( serviceInfo, configKey, defaultValue )
    gLogger.info( "%-20s : %-20s" % ( str( configKey ), str( configValue ) ) )
    databaseConfig[configKey] = configValue
  res = gFileCatalogDB.setConfig( databaseConfig )
  return res

class FileCatalogHandler( RequestHandler ):
  """
  ..class:: FileCatalogHandler

  A simple Replica and Metadata Catalog service. 
  """

  ########################################################################
  # Path operations (not updated)
  #  
  types_changePathOwner = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_changePathOwner( self, lfns, recursive = False ):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathOwner( lfns, self.getRemoteCredentials(), recursive )

  types_changePathGroup = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_changePathGroup( self, lfns, recursive = False ):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathGroup( lfns, self.getRemoteCredentials(), recursive )

  types_changePathMode = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_changePathMode( self, lfns, recursive = False ):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathMode( lfns, self.getRemoteCredentials(), recursive )

  ########################################################################
  # ACL Operations
  #
  types_getPathPermissions = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_getPathPermissions( self, lfns ):
    """ Determine the ACL information for a supplied path
    """
    return gFileCatalogDB.getPathPermissions( lfns, self.getRemoteCredentials() )

  ###################################################################
  #
  #  isOK
  #

  types_isOK = []
  @staticmethod
  def export_isOK():
    """ returns S_OK if DB is connected
    """
    if gFileCatalogDB and gFileCatalogDB._connected:
      return S_OK()
    return S_ERROR( 'Server not connected to DB' )

  ###################################################################
  #
  #  User/Group write operations
  #

  types_addUser = [ StringTypes ]
  def export_addUser( self, userName ):
    """ Add a new user to the File Catalog """
    return gFileCatalogDB.addUser( userName, self.getRemoteCredentials() )

  types_deleteUser = [ StringTypes ]
  def export_deleteUser( self, userName ):
    """ Delete user from the File Catalog """
    return gFileCatalogDB.deleteUser( userName, self.getRemoteCredentials() )

  types_addGroup = [ StringTypes ]
  def export_addGroup( self, groupName ):
    """ Add a new group to the File Catalog """
    return gFileCatalogDB.addGroup( groupName, self.getRemoteCredentials() )

  types_deleteGroup = [ StringTypes ]
  def export_deleteGroup( self, groupName ):
    """ Delete group from the File Catalog """
    return gFileCatalogDB.deleteGroup( groupName, self.getRemoteCredentials() )

  ###################################################################
  #
  #  User/Group read operations
  #

  types_getUsers = []
  def export_getUsers( self ):
    """ Get all the users defined in the File Catalog """
    return gFileCatalogDB.getUsers( self.getRemoteCredentials() )

  types_getGroups = []
  def export_getGroups( self ):
    """ Get all the groups defined in the File Catalog """
    return gFileCatalogDB.getGroups( self.getRemoteCredentials() )

  ########################################################################
  #
  # Path read operations
  #

  types_exists = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_exists( self, lfns ):
    """ Check whether the supplied paths exists """
    return gFileCatalogDB.exists( lfns, self.getRemoteCredentials() )

  ########################################################################
  #
  # File write operations
  #

  types_addFile = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_addFile( self, lfns ):
    """ Register supplied files """
    return gFileCatalogDB.addFile( lfns, self.getRemoteCredentials() )

  types_removeFile = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_removeFile( self, lfns ):
    """ Remove the supplied lfns """
    return gFileCatalogDB.removeFile( lfns, self.getRemoteCredentials() )
  
  types_setFileStatus = [ DictType ]
  def export_setFileStatus( self, lfns ):
    """ Remove the supplied lfns """
    return gFileCatalogDB.setFileStatus( lfns, self.getRemoteCredentials() )

  types_addReplica = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_addReplica( self, lfns ):
    """ Register supplied replicas """
    return gFileCatalogDB.addReplica( lfns, self.getRemoteCredentials() )

  types_removeReplica = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_removeReplica( self, lfns ):
    """ Remove the supplied replicas """
    return gFileCatalogDB.removeReplica( lfns, self.getRemoteCredentials() )

  types_setReplicaStatus = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_setReplicaStatus( self, lfns ):
    """ Set the status for the supplied replicas """
    return gFileCatalogDB.setReplicaStatus( lfns, self.getRemoteCredentials() )

  types_setReplicaHost = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_setReplicaHost( self, lfns ):
    """ Change the registered SE for the supplied replicas """
    return gFileCatalogDB.setReplicaHost( lfns, self.getRemoteCredentials() )

  types_addFileAncestors = [ DictType ]
  def export_addFileAncestors( self, lfns ):
    """ Add file ancestor information for the given list of LFNs """
    return gFileCatalogDB.addFileAncestors( lfns, self.getRemoteCredentials() )

  ########################################################################
  #
  # File read operations
  #

  types_isFile = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_isFile( self, lfns ):
    """ Check whether the supplied lfns are files """
    return gFileCatalogDB.isFile( lfns, self.getRemoteCredentials() )

  types_getFileSize = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_getFileSize( self, lfns ):
    """ Get the size associated to supplied lfns """
    return gFileCatalogDB.getFileSize( lfns, self.getRemoteCredentials() )

  types_getFileMetadata = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_getFileMetadata( self, lfns ):
    """ Get the metadata associated to supplied lfns """
    return gFileCatalogDB.getFileMetadata( lfns, self.getRemoteCredentials() )

  types_getReplicas = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_getReplicas( self, lfns, allStatus = False ):
    """ Get replicas for supplied lfns """
    return gFileCatalogDB.getReplicas( lfns, allStatus, self.getRemoteCredentials() )

  types_getReplicaStatus = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_getReplicaStatus( self, lfns ):
    """ Get the status for the supplied replicas """
    return gFileCatalogDB.getReplicaStatus( lfns, self.getRemoteCredentials() )

  types_getFileAncestors = [ ListType, [ ListType, IntType, LongType ] ]
  def export_getFileAncestors( self, lfns, depths ):
    """ Get the status for the supplied replicas """
    dList = depths
    if type( dList ) != ListType:
      dList = [ depths ]
    lfnDict = dict.fromkeys( lfns, True )
    return gFileCatalogDB.getFileAncestors( lfnDict, dList, self.getRemoteCredentials() )

  types_getFileDescendents = [ ListType, [ ListType, IntType, LongType ] ]
  def export_getFileDescendents( self, lfns, depths ):
    """ Get the status for the supplied replicas """
    dList = depths
    if type( dList ) != ListType:
      dList = [depths]
    lfnDict = dict.fromkeys( lfns, True )
    return gFileCatalogDB.getFileDescendents( lfnDict, dList, self.getRemoteCredentials() )

  ########################################################################
  #
  # Directory write operations
  #

  types_createDirectory = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_createDirectory( self, lfns ):
    """ Create the supplied directories """
    return gFileCatalogDB.createDirectory( lfns, self.getRemoteCredentials() )

  types_removeDirectory = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_removeDirectory( self, lfns ):
    """ Remove the supplied directories """
    return gFileCatalogDB.removeDirectory( lfns, self.getRemoteCredentials() )

  ########################################################################
  #
  # Directory read operations
  #

  types_listDirectory = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_listDirectory( self, lfns, verbose ):
    """ List the contents of supplied directories """
    return gFileCatalogDB.listDirectory( lfns, self.getRemoteCredentials(), verbose = verbose )

  types_isDirectory = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_isDirectory( self, lfns ):
    """ Determine whether supplied path is a directory """
    return gFileCatalogDB.isDirectory( lfns, self.getRemoteCredentials() )

  types_getDirectorySize = [ [ ListType, DictType ] + list( StringTypes ), BooleanType, BooleanType ]
  def export_getDirectorySize( self, lfns, longOut = False, fromFiles = False ):
    """ Get the size of the supplied directory """
    return gFileCatalogDB.getDirectorySize( lfns, longOut, fromFiles, self.getRemoteCredentials() )

  types_getDirectoryReplicas = [ [ ListType, DictType ] + list( StringTypes ), BooleanType ]
  def export_getDirectoryReplicas( self, lfns, allStatus = False ):
    """ Get replicas for files in the supplied directory """
    return gFileCatalogDB.getDirectoryReplicas( lfns, allStatus, self.getRemoteCredentials() )

  ########################################################################
  #
  # Administrative database operations
  #

  types_getCatalogCounters = []
  def export_getCatalogCounters( self ):
    """ Get the number of registered directories, files and replicas in various tables """
    return gFileCatalogDB.getCatalogCounters( self.getRemoteCredentials() )

  types_rebuildDirectoryUsage = []
  @staticmethod
  def export_rebuildDirectoryUsage():
    """ Rebuild DirectoryUsage table from scratch """
    return gFileCatalogDB.rebuildDirectoryUsage()

  types_repairCatalog = []
  def export_repairCatalog( self ):
    """ Repair the catalog inconsistencies """
    return gFileCatalogDB.repairCatalog( self.getRemoteCredentials() )

  ########################################################################
  # Metadata Catalog Operations
  #

  types_addMetadataField = [ StringTypes, StringTypes ]
  def export_addMetadataField( self, fieldName, fieldType, metaType = '-d' ):
    """ Add a new metadata field of the given type
    """
    if metaType.lower() == "-d":
      return gFileCatalogDB.dmeta.addMetadataField( fieldName, fieldType, self.getRemoteCredentials() )
    elif metaType.lower() == "-f":
      return gFileCatalogDB.fmeta.addMetadataField( fieldName, fieldType, self.getRemoteCredentials() )
    else:
      return S_ERROR( 'Unknown metadata type %s' % metaType )

  types_deleteMetadataField = [ StringTypes ]
  def export_deleteMetadataField( self, fieldName ):
    """ Delete the metadata field 
    """
    result = gFileCatalogDB.dmeta.deleteMetadataField( fieldName, self.getRemoteCredentials() )
    error = ''
    if not result['OK']:
      error = result['Message']
    result = gFileCatalogDB.fmeta.deleteMetadataField( fieldName, self.getRemoteCredentials() )
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"]

    return result

  types_getMetadataFields = [ ]
  def export_getMetadataFields( self ):
    """ Get all the metadata fields
    """
    resultDir = gFileCatalogDB.dmeta.getMetadataFields( self.getRemoteCredentials() )
    if not resultDir['OK']:
      return resultDir
    resultFile = gFileCatalogDB.fmeta.getFileMetadataFields( self.getRemoteCredentials() )
    if not resultFile['OK']:
      return resultFile

    return S_OK( { 'DirectoryMetaFields' : resultDir['Value'],
                   'FileMetaFields' : resultFile['Value'] } )

  types_setMetadata = [ StringTypes, DictType ]
  def export_setMetadata( self, path, metadatadict ):
    """ Set metadata parameter for the given path
    """
    return gFileCatalogDB.setMetadata( path, metadatadict, self.getRemoteCredentials() )
  
  types_setMetadataBulk = [ DictType ]
  def export_setMetadataBulk( self, pathMetadataDict ):
    """ Set metadata parameter for the given path
    """
    return gFileCatalogDB.setMetadataBulk( pathMetadataDict, self.getRemoteCredentials() )

  types_removeMetadata = [ StringTypes, ListType ]
  def export_removeMetadata( self, path, metadata ):
    """ Remove the specified metadata for the given path
    """
    return gFileCatalogDB.removeMetadata( path, metadata, self.getRemoteCredentials() )

  types_getDirectoryMetadata = [ StringTypes ]
  def export_getDirectoryMetadata( self, path ):
    """ Get all the metadata valid for the given directory path
    """
    return gFileCatalogDB.dmeta.getDirectoryMetadata( path, self.getRemoteCredentials() )

  types_getFileUserMetadata = [ StringTypes ]
  def export_getFileUserMetadata( self, path ):
    """ Get all the metadata valid for the given file
    """
    return gFileCatalogDB.fmeta.getFileUserMetadata( path, self.getRemoteCredentials() )

  types_findDirectoriesByMetadata = [ DictType ]
  def export_findDirectoriesByMetadata( self, metaDict, path = '/' ):
    """ Find all the directories satisfying the given metadata set
    """
    return gFileCatalogDB.dmeta.findDirectoriesByMetadata ( metaDict, path, self.getRemoteCredentials() )

  types_findFilesByMetadata = [ DictType, StringTypes ]
  def export_findFilesByMetadata( self, metaDict, path = '/' ):
    """ Find all the files satisfying the given metadata set
    """
    return gFileCatalogDB.fmeta.findFilesByMetadata( metaDict, path, self.getRemoteCredentials() )

  types_getReplicasByMetadata = [ DictType, StringTypes, BooleanType ]
  def export_getReplicasByMetadata( self, metaDict, path = '/', allStatus = False ):
    """ Find all the files satisfying the given metadata set
    """
    return gFileCatalogDB.fileManager.getReplicasByMetadata( metaDict, 
                                                             path, 
                                                             allStatus, 
                                                             self.getRemoteCredentials() )

  types_findFilesByMetadataDetailed = [ DictType, StringTypes ]
  def export_findFilesByMetadataDetailed( self, metaDict, path = '/' ):
    """ Find all the files satisfying the given metadata set
    """
    result = gFileCatalogDB.fmeta.findFilesByMetadata( metaDict, path, self.getRemoteCredentials() )
    if not result['OK'] or not result['Value']:
      return result

    lfns = []
    for directory in result['Value']:
      for fname in result['Value'][directory]:
        lfns.append( os.path.join( directory, fname ) )
    return gFileCatalogDB.getFileDetails( lfns, self.getRemoteCredentials() )

  types_findFilesByMetadataWeb = [ DictType, StringTypes, [IntType, LongType], [IntType, LongType]]
  def export_findFilesByMetadataWeb( self, metaDict, path, startItem, maxItems ):
    """ Find files satisfying the given metadata set
    """
    result = gFileCatalogDB.dmeta.findFileIDsByMetadata( metaDict, path, self.getRemoteCredentials(), startItem, maxItems )
    if not result['OK'] or not result['Value']:
      return result

    fileIDs = result['Value']
    totalRecords = result['TotalRecords']

    result = gFileCatalogDB.fileManager._getFileLFNs( fileIDs )
    if not result['OK']:
      return result

    lfnsResultList = result['Value']['Successful'].values()
    resultDetails = gFileCatalogDB.getFileDetails( lfnsResultList, self.getRemoteCredentials() )
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK( {"TotalRecords":totalRecords, "Records":resultDetails['Value'] } )
    return result


  def findFilesByMetadataWeb( self, metaDict, path, startItem, maxItems ):
    """ Find all the files satisfying the given metadata set
    """
    result = gFileCatalogDB.fmeta.findFilesByMetadata( metaDict, path, self.getRemoteCredentials() )
    if not result['OK'] or not result['Value']:
      return result

    lfns = []
    for directory in result['Value']:
      for fname in result['Value'][directory]:
        lfns.append( os.path.join( directory, fname ) )

    start = startItem
    totalRecords = len( lfns )
    if start > totalRecords:
      return S_ERROR( 'Requested files out of existing range' )
    end = start + maxItems
    if end > totalRecords:
      end = totalRecords
    lfnsResultList = lfns[start:end]

    resultDetails = gFileCatalogDB.getFileDetails( lfnsResultList, self.getRemoteCredentials() )
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK( {"TotalRecords":totalRecords, "Records":resultDetails['Value'] } )
    return result

  types_getCompatibleMetadata = [ DictType, StringTypes ]
  def export_getCompatibleMetadata( self, metaDict, path = '/' ):
    """ Get metadata values compatible with the given metadata subset
    """
    return gFileCatalogDB.dmeta.getCompatibleMetadata( metaDict, path, self.getRemoteCredentials() )

  types_addMetadataSet = [ StringTypes, DictType ]
  def export_addMetadataSet( self, setName, setDict ):
    """ Add a new metadata set
    """
    return gFileCatalogDB.dmeta.addMetadataSet( setName, setDict, self.getRemoteCredentials() )

  types_getMetadataSet = [ StringTypes, BooleanType ]
  def export_getMetadataSet( self, setName, expandFlag ):
    """ Add a new metadata set
    """
    return gFileCatalogDB.dmeta.getMetadataSet( setName, expandFlag, self.getRemoteCredentials() )

  types_listMetadataSets = []
  def export_listMetadataSets(self):
    """ Get the list of metadata sets with their definitions
    """
    return gFileCatalogDB.dmeta.listMetadataSets(self.getRemoteCredentials())

#########################################################################################
#
#  Dataset manipulation methods
#
  types_addDataset = [ StringTypes, DictType ]
  def export_addDataset( self, datasetName, metaQuery ):
    """ Add a new dynamic dataset defined by its meta query
    """
    return gFileCatalogDB.datasetManager.addDataset( datasetName, metaQuery, self.getRemoteCredentials() )
  
  types_addDatasetAnnotation = [ DictType ]
  def export_addDatasetAnnotation( self, datasetDict ):
    """ Add annotation to an already created dataset
    """
    return gFileCatalogDB.datasetManager.addDatasetAnnotation( datasetDict, self.getRemoteCredentials() )
  
  types_removeDataset = [ StringTypes ]
  def export_removeDataset( self, datasetName ):
    """ Check the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.removeDataset( datasetName, self.getRemoteCredentials() )
  
  types_checkDataset = [ StringTypes ]
  def export_checkDataset( self, datasetName ):
    """ Check the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.checkDataset( datasetName, self.getRemoteCredentials() )
  
  types_updateDataset = [ StringTypes ]
  def export_updateDataset( self, datasetName ):
    """ Update the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.updateDataset( datasetName, self.getRemoteCredentials() )
  
  types_getDatasets = [ list( StringTypes ) + [ListType] ]
  def export_getDatasets( self, datasetName ):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return gFileCatalogDB.datasetManager.getDatasets( datasetName, self.getRemoteCredentials() )
  
  types_getDatasetParameters = [ StringTypes ]
  def export_getDatasetParameters( self, datasetName ):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return gFileCatalogDB.datasetManager.getDatasetParameters( datasetName, self.getRemoteCredentials() )
  
  types_getDatasetAnnotation = [ list( StringTypes ) + [ListType] ]
  def export_getDatasetAnnotation( self, datasetName ):
    """ Get annotation of the given datasets 
    """
    return gFileCatalogDB.datasetManager.getDatasetAnnotation( datasetName, self.getRemoteCredentials() )
  
  types_freezeDataset = [ StringTypes ]
  def export_freezeDataset( self, datasetName ):
    """ Freeze the contents of the dataset making it effectively static
    """
    return gFileCatalogDB.datasetManager.freezeDataset( datasetName, self.getRemoteCredentials() )
  
  types_releaseDataset = [ StringTypes ]
  def export_releaseDataset( self, datasetName ):
    """ Release the contents of the frozen dataset allowing changes in its contents
    """
    return gFileCatalogDB.datasetManager.releaseDataset( datasetName, self.getRemoteCredentials() )
  
  types_getDatasetFiles = [ StringTypes ]
  def export_getDatasetFiles( self, datasetName ):
    """ Get lfns in the given dataset
    """
    return gFileCatalogDB.datasetManager.getDatasetFiles( datasetName, self.getRemoteCredentials() )
