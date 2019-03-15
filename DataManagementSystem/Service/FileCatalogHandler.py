########################################################################
# File: FileCatalogHandler.py
########################################################################
"""
:mod: FileCatalogHandler

.. module: FileCatalogHandler
  :synopsis: FileCatalogHandler is a simple Replica and Metadata Catalog service

"""

__RCSID__ = "$Id$"

# imports
import six
import cStringIO
import csv
import os
from types import IntType, LongType, DictType, StringTypes, BooleanType, ListType
# from DIRAC
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB

# This is a global instance of the FileCatalogDB class
gFileCatalogDB = None


def initializeFileCatalogHandler(serviceInfo):
  """ handler initialisation """

  global gFileCatalogDB

  dbLocation = getServiceOption(serviceInfo, 'Database', 'DataManagement/FileCatalogDB')
  gFileCatalogDB = FileCatalogDB(dbLocation)

  databaseConfig = {}
  # Obtain the plugins to be used for DB interaction
  gLogger.info("Initializing with FileCatalog with following managers:")
  defaultManagers = {'UserGroupManager': 'UserAndGroupManagerDB',
                     'SEManager': 'SEManagerDB',
                     'SecurityManager': 'NoSecurityManager',
                     'DirectoryManager': 'DirectoryLevelTree',
                     'FileManager': 'FileManager',
                     'DirectoryMetadata': 'DirectoryMetadata',
                     'FileMetadata': 'FileMetadata',
                     'DatasetManager': 'DatasetManager'}
  for configKey in sorted(defaultManagers.keys()):
    defaultValue = defaultManagers[configKey]
    configValue = getServiceOption(serviceInfo, configKey, defaultValue)
    gLogger.info("%-20s : %-20s" % (str(configKey), str(configValue)))
    databaseConfig[configKey] = configValue

  # Obtain some general configuration of the database
  gLogger.info("Initializing the FileCatalog with the following configuration:")
  defaultConfig = {'UniqueGUID': False,
                   'GlobalReadAccess': True,
                   'LFNPFNConvention': 'Strong',
                   'ResolvePFN': True,
                   'DefaultUmask': 0o775,
                   'ValidFileStatus': ['AprioriGood', 'Trash', 'Removing', 'Probing'],
                   'ValidReplicaStatus': ['AprioriGood', 'Trash', 'Removing', 'Probing'],
                   'VisibleFileStatus': ['AprioriGood'],
                   'VisibleReplicaStatus': ['AprioriGood']}
  for configKey in sorted(defaultConfig.keys()):
    defaultValue = defaultConfig[configKey]
    configValue = getServiceOption(serviceInfo, configKey, defaultValue)
    gLogger.info("%-20s : %-20s" % (str(configKey), str(configValue)))
    databaseConfig[configKey] = configValue
  res = gFileCatalogDB.setConfig(databaseConfig)

  gMonitor.registerActivity("AddFile", "Amount of addFile calls",
                            "FileCatalogHandler", "calls/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("AddFileSuccessful", "Files successfully added",
                            "FileCatalogHandler", "files/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("AddFileFailed", "Files failed to add",
                            "FileCatalogHandler", "files/min", gMonitor.OP_SUM)

  gMonitor.registerActivity("RemoveFile", "Amount of removeFile calls",
                            "FileCatalogHandler", "calls/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("RemoveFileSuccessful", "Files successfully removed",
                            "FileCatalogHandler", "files/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("RemoveFileFailed", "Files failed to remove",
                            "FileCatalogHandler", "files/min", gMonitor.OP_SUM)

  gMonitor.registerActivity("AddReplica", "Amount of addReplica calls",
                            "FileCatalogHandler", "calls/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("AddReplicaSuccessful", "Replicas successfully added",
                            "FileCatalogHandler", "replicas/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("AddReplicaFailed", "Replicas failed to add",
                            "FileCatalogHandler", "replicas/min", gMonitor.OP_SUM)

  gMonitor.registerActivity("RemoveReplica", "Amount of removeReplica calls",
                            "FileCatalogHandler", "calls/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("RemoveReplicaSuccessful", "Replicas successfully removed",
                            "FileCatalogHandler", "replicas/min", gMonitor.OP_SUM)
  gMonitor.registerActivity("RemoveReplicaFailed", "Replicas failed to remove",
                            "FileCatalogHandler", "replicas/min", gMonitor.OP_SUM)

  gMonitor.registerActivity("ListDirectory", "Amount of listDirectory calls",
                            "FileCatalogHandler", "calls/min", gMonitor.OP_SUM)

  return res


class FileCatalogHandler(RequestHandler):
  """
  ..class:: FileCatalogHandler

  A simple Replica and Metadata Catalog service.
  """

  ########################################################################
  # Path operations (not updated)
  #
  types_changePathOwner = [[ListType, DictType] + list(StringTypes)]

  def export_changePathOwner(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathOwner(lfns, self.getRemoteCredentials(), recursive)

  types_changePathGroup = [[ListType, DictType] + list(StringTypes)]

  def export_changePathGroup(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathGroup(lfns, self.getRemoteCredentials(), recursive)

  types_changePathMode = [[ListType, DictType] + list(StringTypes)]

  def export_changePathMode(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return gFileCatalogDB.changePathMode(lfns, self.getRemoteCredentials(), recursive)

  ########################################################################
  # ACL Operations
  #
  types_getPathPermissions = [[ListType, DictType] + list(StringTypes)]

  def export_getPathPermissions(self, lfns):
    """ Determine the ACL information for a supplied path
    """
    return gFileCatalogDB.getPathPermissions(lfns, self.getRemoteCredentials())

  types_hasAccess = [[basestring, dict], [basestring, list, dict]]

  def export_hasAccess(self, paths, opType):
    """ Determine if the given op can be performed on the paths
        The OpType is all the operations exported

        The reason for the param types is backward compatibility. Between v6r14 and v6r15,
        the signature of hasAccess has changed, and the two parameters were swapped.
    """

    # The signature of v6r15 is (dict, str)
    # The signature of v6r14 is (str, [dict, str, list])
    # We swap the two params if the first attribute is a string
    if isinstance(paths, six.string_types):
      paths, opType = opType, paths

    return gFileCatalogDB.hasAccess(opType, paths, self.getRemoteCredentials())

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
    return S_ERROR('Server not connected to DB')

  ###################################################################
  #
  #  User/Group write operations
  #

  types_addUser = [StringTypes]

  def export_addUser(self, userName):
    """ Add a new user to the File Catalog """
    return gFileCatalogDB.addUser(userName, self.getRemoteCredentials())

  types_deleteUser = [StringTypes]

  def export_deleteUser(self, userName):
    """ Delete user from the File Catalog """
    return gFileCatalogDB.deleteUser(userName, self.getRemoteCredentials())

  types_addGroup = [StringTypes]

  def export_addGroup(self, groupName):
    """ Add a new group to the File Catalog """
    return gFileCatalogDB.addGroup(groupName, self.getRemoteCredentials())

  types_deleteGroup = [StringTypes]

  def export_deleteGroup(self, groupName):
    """ Delete group from the File Catalog """
    return gFileCatalogDB.deleteGroup(groupName, self.getRemoteCredentials())

  ###################################################################
  #
  #  User/Group read operations
  #

  types_getUsers = []

  def export_getUsers(self):
    """ Get all the users defined in the File Catalog """
    return gFileCatalogDB.getUsers(self.getRemoteCredentials())

  types_getGroups = []

  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog """
    return gFileCatalogDB.getGroups(self.getRemoteCredentials())

  ########################################################################
  #
  # Path read operations
  #

  types_exists = [[ListType, DictType] + list(StringTypes)]

  def export_exists(self, lfns):
    """ Check whether the supplied paths exists """
    return gFileCatalogDB.exists(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File write operations
  #

  types_addFile = [[ListType, DictType] + list(StringTypes)]

  def export_addFile(self, lfns):
    """ Register supplied files """
    gMonitor.addMark("AddFile", 1)
    res = gFileCatalogDB.addFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_removeFile = [[ListType, DictType] + list(StringTypes)]

  def export_removeFile(self, lfns):
    """ Remove the supplied lfns """
    gMonitor.addMark("RemoveFile", 1)
    res = gFileCatalogDB.removeFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_setFileStatus = [DictType]

  def export_setFileStatus(self, lfns):
    """ Remove the supplied lfns """
    return gFileCatalogDB.setFileStatus(lfns, self.getRemoteCredentials())

  types_addReplica = [[ListType, DictType] + list(StringTypes)]

  def export_addReplica(self, lfns):
    """ Register supplied replicas """
    gMonitor.addMark("AddReplica", 1)
    res = gFileCatalogDB.addReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_removeReplica = [[ListType, DictType] + list(StringTypes)]

  def export_removeReplica(self, lfns):
    """ Remove the supplied replicas """
    gMonitor.addMark("RemoveReplica", 1)
    res = gFileCatalogDB.removeReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_setReplicaStatus = [[ListType, DictType] + list(StringTypes)]

  def export_setReplicaStatus(self, lfns):
    """ Set the status for the supplied replicas """
    return gFileCatalogDB.setReplicaStatus(lfns, self.getRemoteCredentials())

  types_setReplicaHost = [[ListType, DictType] + list(StringTypes)]

  def export_setReplicaHost(self, lfns):
    """ Change the registered SE for the supplied replicas """
    return gFileCatalogDB.setReplicaHost(lfns, self.getRemoteCredentials())

  types_addFileAncestors = [DictType]

  def export_addFileAncestors(self, lfns):
    """ Add file ancestor information for the given list of LFNs """
    return gFileCatalogDB.addFileAncestors(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File read operations
  #

  types_isFile = [[ListType, DictType] + list(StringTypes)]

  def export_isFile(self, lfns):
    """ Check whether the supplied lfns are files """
    return gFileCatalogDB.isFile(lfns, self.getRemoteCredentials())

  types_getFileSize = [[ListType, DictType] + list(StringTypes)]

  def export_getFileSize(self, lfns):
    """ Get the size associated to supplied lfns """
    return gFileCatalogDB.getFileSize(lfns, self.getRemoteCredentials())

  types_getFileMetadata = [[ListType, DictType] + list(StringTypes)]

  def export_getFileMetadata(self, lfns):
    """ Get the metadata associated to supplied lfns """
    return gFileCatalogDB.getFileMetadata(lfns, self.getRemoteCredentials())

  types_getReplicas = [[ListType, DictType] + list(StringTypes), BooleanType]

  def export_getReplicas(self, lfns, allStatus=False):
    """ Get replicas for supplied lfns """
    return gFileCatalogDB.getReplicas(lfns, allStatus, self.getRemoteCredentials())

  types_getReplicaStatus = [[ListType, DictType] + list(StringTypes)]

  def export_getReplicaStatus(self, lfns):
    """ Get the status for the supplied replicas """
    return gFileCatalogDB.getReplicaStatus(lfns, self.getRemoteCredentials())

  types_getFileAncestors = [[ListType, DictType], [ListType, IntType, LongType]]

  def export_getFileAncestors(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, ListType):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return gFileCatalogDB.getFileAncestors(lfnDict, dList, self.getRemoteCredentials())

  types_getFileDescendents = [[ListType, DictType], [ListType, IntType, LongType]]

  def export_getFileDescendents(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, ListType):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return gFileCatalogDB.getFileDescendents(lfnDict, dList, self.getRemoteCredentials())

  types_getLFNForGUID = [[ListType, DictType] + list(StringTypes)]

  def export_getLFNForGUID(self, guids):
    """Get the matching lfns for given guids"""
    return gFileCatalogDB.getLFNForGUID(guids, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory write operations
  #

  types_createDirectory = [[ListType, DictType] + list(StringTypes)]

  def export_createDirectory(self, lfns):
    """ Create the supplied directories """
    return gFileCatalogDB.createDirectory(lfns, self.getRemoteCredentials())

  types_removeDirectory = [[ListType, DictType] + list(StringTypes)]

  def export_removeDirectory(self, lfns):
    """ Remove the supplied directories """
    return gFileCatalogDB.removeDirectory(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory read operations
  #

  types_listDirectory = [[ListType, DictType] + list(StringTypes), BooleanType]

  def export_listDirectory(self, lfns, verbose):
    """ List the contents of supplied directories """
    gMonitor.addMark('ListDirectory', 1)
    return gFileCatalogDB.listDirectory(lfns, self.getRemoteCredentials(), verbose=verbose)

  types_isDirectory = [[ListType, DictType] + list(StringTypes)]

  def export_isDirectory(self, lfns):
    """ Determine whether supplied path is a directory """
    return gFileCatalogDB.isDirectory(lfns, self.getRemoteCredentials())

  types_getDirectoryMetadata = [[ListType, DictType] + list(StringTypes)]

  def export_getDirectoryMetadata(self, lfns):
    """ Get the size of the supplied directory """
    return gFileCatalogDB.getDirectoryMetadata(lfns, self.getRemoteCredentials())

  types_getDirectorySize = [[ListType, DictType] + list(StringTypes)]

  def export_getDirectorySize(self, lfns, longOut=False, fromFiles=False):
    """ Get the size of the supplied directory """
    return gFileCatalogDB.getDirectorySize(lfns, longOut, fromFiles, self.getRemoteCredentials())

  types_getDirectoryReplicas = [[ListType, DictType] + list(StringTypes), BooleanType]

  def export_getDirectoryReplicas(self, lfns, allStatus=False):
    """ Get replicas for files in the supplied directory """
    return gFileCatalogDB.getDirectoryReplicas(lfns, allStatus, self.getRemoteCredentials())

  ########################################################################
  #
  # Administrative database operations
  #

  types_getCatalogCounters = []

  def export_getCatalogCounters(self):
    """ Get the number of registered directories, files and replicas in various tables """
    return gFileCatalogDB.getCatalogCounters(self.getRemoteCredentials())

  types_rebuildDirectoryUsage = []

  @staticmethod
  def export_rebuildDirectoryUsage():
    """ Rebuild DirectoryUsage table from scratch """
    return gFileCatalogDB.rebuildDirectoryUsage()

  types_repairCatalog = []

  def export_repairCatalog(self):
    """ Repair the catalog inconsistencies """
    return gFileCatalogDB.repairCatalog(self.getRemoteCredentials())

  ########################################################################
  # Metadata Catalog Operations
  #

  types_addMetadataField = [StringTypes, StringTypes]

  def export_addMetadataField(self, fieldName, fieldType, metaType='-d'):
    """ Add a new metadata field of the given type
    """
    if metaType.lower() == "-d":
      return gFileCatalogDB.dmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    elif metaType.lower() == "-f":
      return gFileCatalogDB.fmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    else:
      return S_ERROR('Unknown metadata type %s' % metaType)

  types_deleteMetadataField = [StringTypes]

  def export_deleteMetadataField(self, fieldName):
    """ Delete the metadata field
    """
    result = gFileCatalogDB.dmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    error = ''
    if not result['OK']:
      error = result['Message']
    result = gFileCatalogDB.fmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"]

    return result

  types_getMetadataFields = []

  def export_getMetadataFields(self):
    """ Get all the metadata fields.
    """
    resultDir = gFileCatalogDB.dmeta.getMetadataFields(self.getRemoteCredentials(), stripVO=True)
    if not resultDir['OK']:
      return resultDir

    resultFile = gFileCatalogDB.fmeta.getFileMetadataFields(self.getRemoteCredentials(), stripVO=True)
    if not resultFile['OK']:
      return resultFile

    return S_OK({'DirectoryMetaFields': resultDir['Value'],
                 'FileMetaFields': resultFile['Value']})

  types_setMetadata = [StringTypes, DictType]

  def export_setMetadata(self, path, metadatadict):
    """ Set metadata parameter for the given path
    """
    return gFileCatalogDB.setMetadata(path, metadatadict, self.getRemoteCredentials())

  types_setMetadataBulk = [DictType]

  def export_setMetadataBulk(self, pathMetadataDict):
    """ Set metadata parameter for the given path
    """
    return gFileCatalogDB.setMetadataBulk(pathMetadataDict, self.getRemoteCredentials())

  types_removeMetadata = [DictType]

  def export_removeMetadata(self, pathMetadataDict):
    """ Remove the specified metadata for the given path
    """
    return gFileCatalogDB.removeMetadata(pathMetadataDict, self.getRemoteCredentials())

  types_getDirectoryUserMetadata = [StringTypes]

  def export_getDirectoryUserMetadata(self, path):
    """ Get all the metadata valid for the given directory path.
    """
    return gFileCatalogDB.dmeta.getDirectoryMetadata(path, self.getRemoteCredentials(), stripVO=True)
    # code below breaks client-side checks

  types_getFileUserMetadata = [StringTypes]

  def export_getFileUserMetadata(self, path):
    """ Get all the metadata valid for the given file.
    """
    return gFileCatalogDB.fmeta.getFileUserMetadata(path, self.getRemoteCredentials(), stripVO=True)

  types_findDirectoriesByMetadata = [DictType]

  def export_findDirectoriesByMetadata(self, metaDict, path='/'):
    """ Find all the directories satisfying the given metadata set
    """
    return gFileCatalogDB.dmeta.findDirectoriesByMetadata(
        metaDict, path, self.getRemoteCredentials())

  types_findFilesByMetadata = [DictType, StringTypes]

  def export_findFilesByMetadata(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK']:
      return result
    lfns = result['Value'].values()
    return S_OK(lfns)

  types_getReplicasByMetadata = [DictType, StringTypes, BooleanType]

  def export_getReplicasByMetadata(self, metaDict, path='/', allStatus=False):
    """ Find all the files satisfying the given metadata set
    """
    return gFileCatalogDB.fileManager.getReplicasByMetadata(metaDict,
                                                            path,
                                                            allStatus,
                                                            self.getRemoteCredentials())

  types_findFilesByMetadataDetailed = [DictType, StringTypes]

  def export_findFilesByMetadataDetailed(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK'] or not result['Value']:
      return result

    lfns = result['Value'].values()
    return gFileCatalogDB.getFileDetails(lfns, self.getRemoteCredentials())

  types_findFilesByMetadataWeb = [DictType, StringTypes, [IntType, LongType], [IntType, LongType]]

  def export_findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find files satisfying the given metadata set
    """
    result = gFileCatalogDB.dmeta.findFileIDsByMetadata(
        metaDict, path, self.getRemoteCredentials(), startItem, maxItems)
    if not result['OK'] or not result['Value']:
      return result

    fileIDs = result['Value']
    totalRecords = result['TotalRecords']

    result = gFileCatalogDB.fileManager._getFileLFNs(fileIDs)
    if not result['OK']:
      return result

    lfnsResultList = result['Value']['Successful'].values()
    resultDetails = gFileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  def findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find all the files satisfying the given metadata set
    """
    result = gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK'] or not result['Value']:
      return result

    lfns = []
    for directory in result['Value']:
      for fname in result['Value'][directory]:
        lfns.append(os.path.join(directory, fname))

    start = startItem
    totalRecords = len(lfns)
    if start > totalRecords:
      return S_ERROR('Requested files out of existing range')
    end = start + maxItems
    if end > totalRecords:
      end = totalRecords
    lfnsResultList = lfns[start:end]

    resultDetails = gFileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  types_getCompatibleMetadata = [DictType, StringTypes]

  def export_getCompatibleMetadata(self, metaDict, path='/'):
    """ Get metadata values compatible with the given metadata subset
    """
    return gFileCatalogDB.dmeta.getCompatibleMetadata(metaDict, path, self.getRemoteCredentials())

  types_addMetadataSet = [StringTypes, DictType]

  def export_addMetadataSet(self, setName, setDict):
    """ Add a new metadata set
    """
    return gFileCatalogDB.dmeta.addMetadataSet(setName, setDict, self.getRemoteCredentials())

  types_getMetadataSet = [StringTypes, BooleanType]

  def export_getMetadataSet(self, setName, expandFlag):
    """ Add a new metadata set
    """
    return gFileCatalogDB.dmeta.getMetadataSet(setName, expandFlag, self.getRemoteCredentials())

#########################################################################################
#
#  Dataset manipulation methods
#
  types_addDataset = [DictType]

  def export_addDataset(self, datasets):
    """ Add a new dynamic dataset defined by its meta query
    """
    return gFileCatalogDB.datasetManager.addDataset(datasets, self.getRemoteCredentials())

  types_addDatasetAnnotation = [DictType]

  def export_addDatasetAnnotation(self, datasetDict):
    """ Add annotation to an already created dataset
    """
    return gFileCatalogDB.datasetManager.addDatasetAnnotation(
        datasetDict, self.getRemoteCredentials())

  types_removeDataset = [DictType]

  def export_removeDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.removeDataset(datasets, self.getRemoteCredentials())

  types_checkDataset = [DictType]

  def export_checkDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.checkDataset(datasets, self.getRemoteCredentials())

  types_updateDataset = [DictType]

  def export_updateDataset(self, datasets):
    """ Update the given dynamic dataset for changes since its definition
    """
    return gFileCatalogDB.datasetManager.updateDataset(datasets, self.getRemoteCredentials())

  types_getDatasets = [DictType]

  def export_getDatasets(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return gFileCatalogDB.datasetManager.getDatasets(datasets, self.getRemoteCredentials())

  types_getDatasetParameters = [DictType]

  def export_getDatasetParameters(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return gFileCatalogDB.datasetManager.getDatasetParameters(datasets, self.getRemoteCredentials())

  types_getDatasetAnnotation = [DictType]

  def export_getDatasetAnnotation(self, datasets):
    """ Get annotation of the given datasets
    """
    return gFileCatalogDB.datasetManager.getDatasetAnnotation(datasets, self.getRemoteCredentials())

  types_freezeDataset = [DictType]

  def export_freezeDataset(self, datasets):
    """ Freeze the contents of the dataset making it effectively static
    """
    return gFileCatalogDB.datasetManager.freezeDataset(datasets, self.getRemoteCredentials())

  types_releaseDataset = [DictType]

  def export_releaseDataset(self, datasets):
    """ Release the contents of the frozen dataset allowing changes in its contents
    """
    return gFileCatalogDB.datasetManager.releaseDataset(datasets, self.getRemoteCredentials())

  types_getDatasetFiles = [DictType]

  def export_getDatasetFiles(self, datasets):
    """ Get lfns in the given dataset
    """
    return gFileCatalogDB.datasetManager.getDatasetFiles(datasets, self.getRemoteCredentials())

  def getSEDump(self, seName):
    """
         Return all the files at a given SE, together with checksum and size

        :param seName: name of the StorageElement

        :returns: S_OK with list of tuples (lfn, checksum, size)
    """
    return gFileCatalogDB.getSEDump(seName)['Value']

  def transfer_toClient(self, seName, token, fileHelper):
    """ This method used to transfer the SEDump to the client,
        formated as CSV with '|' separation

        :param seName: name of the se to dump

        :returns: the result of the FileHelper


    """

    retVal = self.getSEDump(seName)

    try:
      csvOutput = cStringIO.StringIO()
      writer = csv.writer(csvOutput, delimiter='|')
      for lfn in retVal:
        writer.writerow(lfn)

      csvOutput.seek(0)

      ret = fileHelper.DataSourceToNetwork(csvOutput)
      return ret

    except Exception as e:
      gLogger.exception("Exception while sending seDump", repr(e))
      return S_ERROR("Exception while sendind seDump: %s" % repr(e))
    finally:
      csvOutput.close()
