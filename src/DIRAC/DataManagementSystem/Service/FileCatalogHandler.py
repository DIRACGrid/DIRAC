"""
FileCatalogHandler is a simple Replica and Metadata Catalog service
in the DIRAC framework
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from six import StringIO
import csv
import os

from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB


class FileCataloghandlerMixin(object):
  """
  A simple Replica and Metadata Catalog service.
  """

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ Handler  initialization """

    dbLocation = getServiceOption(serviceInfo, 'Database', 'DataManagement/FileCatalogDB')
    cls.fileCatalogDB = FileCatalogDB(dbLocation)

    databaseConfig = {}
    # Obtain the plugins to be used for DB interaction
    cls.log.info("Initializing with FileCatalog with following managers:")
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
      cls.log.info("%-20s : %-20s" % (str(configKey), str(configValue)))
      databaseConfig[configKey] = configValue

    # Obtain some general configuration of the database
    cls.log.info("Initializing the FileCatalog with the following configuration:")
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
      cls.log.info("%-20s : %-20s" % (str(configKey), str(configValue)))
      databaseConfig[configKey] = configValue
    res = cls.fileCatalogDB.setConfig(databaseConfig)

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

  ########################################################################
  # Path operations (not updated)
  #
  types_changePathOwner = [[list, dict] + list(six.string_types)]

  def export_changePathOwner(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.fileCatalogDB.changePathOwner(lfns, self.getRemoteCredentials(), recursive)

  types_changePathGroup = [[list, dict] + list(six.string_types)]

  def export_changePathGroup(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.fileCatalogDB.changePathGroup(lfns, self.getRemoteCredentials(), recursive)

  types_changePathMode = [[list, dict] + list(six.string_types)]

  def export_changePathMode(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.fileCatalogDB.changePathMode(lfns, self.getRemoteCredentials(), recursive)

  ########################################################################
  # ACL Operations
  #
  types_getPathPermissions = [[list, dict] + list(six.string_types)]

  def export_getPathPermissions(self, lfns):
    """ Determine the ACL information for a supplied path
    """
    return self.fileCatalogDB.getPathPermissions(lfns, self.getRemoteCredentials())

  types_hasAccess = [[six.string_types, dict], [six.string_types, list, dict]]

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

    return self.fileCatalogDB.hasAccess(opType, paths, self.getRemoteCredentials())

  ###################################################################
  #
  #  isOK
  #

  types_isOK = []

  @classmethod
  def export_isOK(cls):
    """ returns S_OK if DB is connected
    """
    if cls.fileCatalogDB and cls.fileCatalogDB._connected:
      return S_OK()
    return S_ERROR('Server not connected to DB')

  ###################################################################
  #
  #  User/Group write operations
  #

  types_addUser = [six.string_types]

  def export_addUser(self, userName):
    """ Add a new user to the File Catalog """
    return self.fileCatalogDB.addUser(userName, self.getRemoteCredentials())

  types_deleteUser = [six.string_types]

  def export_deleteUser(self, userName):
    """ Delete user from the File Catalog """
    return self.fileCatalogDB.deleteUser(userName, self.getRemoteCredentials())

  types_addGroup = [six.string_types]

  def export_addGroup(self, groupName):
    """ Add a new group to the File Catalog """
    return self.fileCatalogDB.addGroup(groupName, self.getRemoteCredentials())

  types_deleteGroup = [six.string_types]

  def export_deleteGroup(self, groupName):
    """ Delete group from the File Catalog """
    return self.fileCatalogDB.deleteGroup(groupName, self.getRemoteCredentials())

  ###################################################################
  #
  #  User/Group read operations
  #

  types_getUsers = []

  def export_getUsers(self):
    """ Get all the users defined in the File Catalog """
    return self.fileCatalogDB.getUsers(self.getRemoteCredentials())

  types_getGroups = []

  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog """
    return self.fileCatalogDB.getGroups(self.getRemoteCredentials())

  ########################################################################
  #
  # Path read operations
  #

  types_exists = [[list, dict] + list(six.string_types)]

  def export_exists(self, lfns):
    """ Check whether the supplied paths exists """
    return self.fileCatalogDB.exists(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File write operations
  #

  types_addFile = [[list, dict] + list(six.string_types)]

  def export_addFile(self, lfns):
    """ Register supplied files """
    gMonitor.addMark("AddFile", 1)
    res = self.fileCatalogDB.addFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_removeFile = [[list, dict] + list(six.string_types)]

  def export_removeFile(self, lfns):
    """ Remove the supplied lfns """
    gMonitor.addMark("RemoveFile", 1)
    res = self.fileCatalogDB.removeFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_setFileStatus = [dict]

  def export_setFileStatus(self, lfns):
    """ Remove the supplied lfns """
    return self.fileCatalogDB.setFileStatus(lfns, self.getRemoteCredentials())

  types_addReplica = [[list, dict] + list(six.string_types)]

  def export_addReplica(self, lfns):
    """ Register supplied replicas """
    gMonitor.addMark("AddReplica", 1)
    res = self.fileCatalogDB.addReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_removeReplica = [[list, dict] + list(six.string_types)]

  def export_removeReplica(self, lfns):
    """ Remove the supplied replicas """
    gMonitor.addMark("RemoveReplica", 1)
    res = self.fileCatalogDB.removeReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  types_setReplicaStatus = [[list, dict] + list(six.string_types)]

  def export_setReplicaStatus(self, lfns):
    """ Set the status for the supplied replicas """
    return self.fileCatalogDB.setReplicaStatus(lfns, self.getRemoteCredentials())

  types_setReplicaHost = [[list, dict] + list(six.string_types)]

  def export_setReplicaHost(self, lfns):
    """ Change the registered SE for the supplied replicas """
    return self.fileCatalogDB.setReplicaHost(lfns, self.getRemoteCredentials())

  types_addFileAncestors = [dict]

  def export_addFileAncestors(self, lfns):
    """ Add file ancestor information for the given list of LFNs """
    return self.fileCatalogDB.addFileAncestors(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File read operations
  #

  types_isFile = [[list, dict] + list(six.string_types)]

  def export_isFile(self, lfns):
    """ Check whether the supplied lfns are files """
    return self.fileCatalogDB.isFile(lfns, self.getRemoteCredentials())

  types_getFileSize = [[list, dict] + list(six.string_types)]

  def export_getFileSize(self, lfns):
    """ Get the size associated to supplied lfns """
    return self.fileCatalogDB.getFileSize(lfns, self.getRemoteCredentials())

  types_getFileMetadata = [[list, dict] + list(six.string_types)]

  def export_getFileMetadata(self, lfns):
    """ Get the metadata associated to supplied lfns """
    return self.fileCatalogDB.getFileMetadata(lfns, self.getRemoteCredentials())

  types_getReplicas = [[list, dict] + list(six.string_types), bool]

  def export_getReplicas(self, lfns, allStatus=False):
    """ Get replicas for supplied lfns """
    return self.fileCatalogDB.getReplicas(lfns, allStatus, self.getRemoteCredentials())

  types_getReplicaStatus = [[list, dict] + list(six.string_types)]

  def export_getReplicaStatus(self, lfns):
    """ Get the status for the supplied replicas """
    return self.fileCatalogDB.getReplicaStatus(lfns, self.getRemoteCredentials())

  types_getFileAncestors = [[list, dict], (list,) + six.integer_types]

  def export_getFileAncestors(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, list):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return self.fileCatalogDB.getFileAncestors(lfnDict, dList, self.getRemoteCredentials())

  types_getFileDescendents = [[list, dict], (list,) + six.integer_types]

  def export_getFileDescendents(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, list):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return self.fileCatalogDB.getFileDescendents(lfnDict, dList, self.getRemoteCredentials())

  types_getLFNForGUID = [[list, dict] + list(six.string_types)]

  def export_getLFNForGUID(self, guids):
    """Get the matching lfns for given guids"""
    return self.fileCatalogDB.getLFNForGUID(guids, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory write operations
  #

  types_createDirectory = [[list, dict] + list(six.string_types)]

  def export_createDirectory(self, lfns):
    """ Create the supplied directories """
    return self.fileCatalogDB.createDirectory(lfns, self.getRemoteCredentials())

  types_removeDirectory = [[list, dict] + list(six.string_types)]

  def export_removeDirectory(self, lfns):
    """ Remove the supplied directories """
    return self.fileCatalogDB.removeDirectory(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory read operations
  #

  types_listDirectory = [[list, dict] + list(six.string_types), bool]

  def export_listDirectory(self, lfns, verbose):
    """ List the contents of supplied directories """
    gMonitor.addMark('ListDirectory', 1)
    return self.fileCatalogDB.listDirectory(lfns, self.getRemoteCredentials(), verbose=verbose)

  types_isDirectory = [[list, dict] + list(six.string_types)]

  def export_isDirectory(self, lfns):
    """ Determine whether supplied path is a directory """
    return self.fileCatalogDB.isDirectory(lfns, self.getRemoteCredentials())

  types_getDirectoryMetadata = [[list, dict] + list(six.string_types)]

  def export_getDirectoryMetadata(self, lfns):
    """ Get the size of the supplied directory """
    return self.fileCatalogDB.getDirectoryMetadata(lfns, self.getRemoteCredentials())

  types_getDirectorySize = [[list, dict] + list(six.string_types)]

  def export_getDirectorySize(self, lfns, longOut=False, fromFiles=False, recursiveSum=True):
    """ Get the size of the supplied directory """
    return self.fileCatalogDB.getDirectorySize(lfns, longOut, fromFiles, recursiveSum, self.getRemoteCredentials())

  types_getDirectoryReplicas = [[list, dict] + list(six.string_types), bool]

  def export_getDirectoryReplicas(self, lfns, allStatus=False):
    """ Get replicas for files in the supplied directory """
    return self.fileCatalogDB.getDirectoryReplicas(lfns, allStatus, self.getRemoteCredentials())

  ########################################################################
  #
  # Administrative database operations
  #

  types_getCatalogCounters = []

  def export_getCatalogCounters(self):
    """ Get the number of registered directories, files and replicas in various tables """
    return self.fileCatalogDB.getCatalogCounters(self.getRemoteCredentials())

  types_rebuildDirectoryUsage = []

  def export_rebuildDirectoryUsage(self):
    """ Rebuild DirectoryUsage table from scratch """
    return self.fileCatalogDB.rebuildDirectoryUsage()

  types_repairCatalog = []

  def export_repairCatalog(self):
    """ Repair the catalog inconsistencies """
    return self.fileCatalogDB.repairCatalog(self.getRemoteCredentials())

  ########################################################################
  # Metadata Catalog Operations
  #

  types_addMetadataField = [six.string_types, six.string_types, six.string_types]

  def export_addMetadataField(self, fieldName, fieldType, metaType='-d'):
    """ Add a new metadata field of the given type
    """
    if metaType.lower() == "-d":
      return self.fileCatalogDB.dmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    elif metaType.lower() == "-f":
      return self.fileCatalogDB.fmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    else:
      return S_ERROR('Unknown metadata type %s' % metaType)

  types_deleteMetadataField = [six.string_types]

  def export_deleteMetadataField(self, fieldName):
    """ Delete the metadata field
    """
    result = self.fileCatalogDB.dmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    error = ''
    if not result['OK']:
      error = result['Message']
    result = self.fileCatalogDB.fmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"]

    return result

  types_getMetadataFields = []

  def export_getMetadataFields(self):
    """ Get all the metadata fields
    """
    resultDir = self.fileCatalogDB.dmeta.getMetadataFields(self.getRemoteCredentials())
    if not resultDir['OK']:
      return resultDir
    resultFile = self.fileCatalogDB.fmeta.getFileMetadataFields(self.getRemoteCredentials())
    if not resultFile['OK']:
      return resultFile

    return S_OK({'DirectoryMetaFields': resultDir['Value'],
                 'FileMetaFields': resultFile['Value']})

  types_setMetadata = [six.string_types, dict]

  def export_setMetadata(self, path, metadatadict):
    """ Set metadata parameter for the given path
    """
    return self.fileCatalogDB.setMetadata(path, metadatadict, self.getRemoteCredentials())

  types_setMetadataBulk = [dict]

  def export_setMetadataBulk(self, pathMetadataDict):
    """ Set metadata parameter for the given path
    """
    return self.fileCatalogDB.setMetadataBulk(pathMetadataDict, self.getRemoteCredentials())

  types_removeMetadata = [dict]

  def export_removeMetadata(self, pathMetadataDict):
    """ Remove the specified metadata for the given path
    """
    return self.fileCatalogDB.removeMetadata(pathMetadataDict, self.getRemoteCredentials())

  types_getDirectoryUserMetadata = [six.string_types]

  def export_getDirectoryUserMetadata(self, path):
    """ Get all the metadata valid for the given directory path
    """
    return self.fileCatalogDB.dmeta.getDirectoryMetadata(path, self.getRemoteCredentials())

  types_getFileUserMetadata = [six.string_types]

  def export_getFileUserMetadata(self, path):
    """ Get all the metadata valid for the given file
    """
    return self.fileCatalogDB.fmeta.getFileUserMetadata(path, self.getRemoteCredentials())

  types_findDirectoriesByMetadata = [dict]

  def export_findDirectoriesByMetadata(self, metaDict, path='/'):
    """ Find all the directories satisfying the given metadata set
    """
    return self.fileCatalogDB.dmeta.findDirectoriesByMetadata(
        metaDict, path, self.getRemoteCredentials())

  types_findFilesByMetadata = [dict, six.string_types]

  def export_findFilesByMetadata(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = self.fileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK']:
      return result
    lfns = list(result['Value'].values())
    return S_OK(lfns)

  types_getReplicasByMetadata = [dict, six.string_types, bool]

  def export_getReplicasByMetadata(self, metaDict, path='/', allStatus=False):
    """ Find all the files satisfying the given metadata set
    """
    return self.fileCatalogDB.fileManager.getReplicasByMetadata(metaDict,
                                                                path,
                                                                allStatus,
                                                                self.getRemoteCredentials())

  types_findFilesByMetadataDetailed = [dict, six.string_types]

  def export_findFilesByMetadataDetailed(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = self.fileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK'] or not result['Value']:
      return result

    lfns = list(result['Value'].values())
    return self.fileCatalogDB.getFileDetails(lfns, self.getRemoteCredentials())

  types_findFilesByMetadataWeb = [dict, six.string_types, six.integer_types, six.integer_types]

  def export_findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find files satisfying the given metadata set
    """
    result = self.fileCatalogDB.dmeta.findFileIDsByMetadata(
        metaDict, path, self.getRemoteCredentials(), startItem, maxItems)
    if not result['OK'] or not result['Value']:
      return result

    fileIDs = result['Value']
    totalRecords = result['TotalRecords']

    result = self.fileCatalogDB.fileManager._getFileLFNs(fileIDs)
    if not result['OK']:
      return result

    lfnsResultList = list(result['Value']['Successful'].values())
    resultDetails = self.fileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  def findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find all the files satisfying the given metadata set
    """
    result = self.fileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
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

    resultDetails = self.fileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  types_getCompatibleMetadata = [dict, six.string_types]

  def export_getCompatibleMetadata(self, metaDict, path='/'):
    """ Get metadata values compatible with the given metadata subset
    """
    return self.fileCatalogDB.dmeta.getCompatibleMetadata(metaDict, path, self.getRemoteCredentials())

  types_addMetadataSet = [six.string_types, dict]

  def export_addMetadataSet(self, setName, setDict):
    """ Add a new metadata set
    """
    return self.fileCatalogDB.dmeta.addMetadataSet(setName, setDict, self.getRemoteCredentials())

  types_getMetadataSet = [six.string_types, bool]

  def export_getMetadataSet(self, setName, expandFlag):
    """ Add a new metadata set
    """
    return self.fileCatalogDB.dmeta.getMetadataSet(setName, expandFlag, self.getRemoteCredentials())

#########################################################################################
#
#  Dataset manipulation methods
#
  types_addDataset = [dict]

  def export_addDataset(self, datasets):
    """ Add a new dynamic dataset defined by its meta query
    """
    return self.fileCatalogDB.datasetManager.addDataset(datasets, self.getRemoteCredentials())

  types_addDatasetAnnotation = [dict]

  def export_addDatasetAnnotation(self, datasetDict):
    """ Add annotation to an already created dataset
    """
    return self.fileCatalogDB.datasetManager.addDatasetAnnotation(
        datasetDict, self.getRemoteCredentials())

  types_removeDataset = [dict]

  def export_removeDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return self.fileCatalogDB.datasetManager.removeDataset(datasets, self.getRemoteCredentials())

  types_checkDataset = [dict]

  def export_checkDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return self.fileCatalogDB.datasetManager.checkDataset(datasets, self.getRemoteCredentials())

  types_updateDataset = [dict]

  def export_updateDataset(self, datasets):
    """ Update the given dynamic dataset for changes since its definition
    """
    return self.fileCatalogDB.datasetManager.updateDataset(datasets, self.getRemoteCredentials())

  types_getDatasets = [dict]

  def export_getDatasets(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return self.fileCatalogDB.datasetManager.getDatasets(datasets, self.getRemoteCredentials())

  types_getDatasetParameters = [dict]

  def export_getDatasetParameters(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return self.fileCatalogDB.datasetManager.getDatasetParameters(datasets, self.getRemoteCredentials())

  types_getDatasetAnnotation = [dict]

  def export_getDatasetAnnotation(self, datasets):
    """ Get annotation of the given datasets
    """
    return self.fileCatalogDB.datasetManager.getDatasetAnnotation(datasets, self.getRemoteCredentials())

  types_freezeDataset = [dict]

  def export_freezeDataset(self, datasets):
    """ Freeze the contents of the dataset making it effectively static
    """
    return self.fileCatalogDB.datasetManager.freezeDataset(datasets, self.getRemoteCredentials())

  types_releaseDataset = [dict]

  def export_releaseDataset(self, datasets):
    """ Release the contents of the frozen dataset allowing changes in its contents
    """
    return self.fileCatalogDB.datasetManager.releaseDataset(datasets, self.getRemoteCredentials())

  types_getDatasetFiles = [dict]

  def export_getDatasetFiles(self, datasets):
    """ Get lfns in the given dataset
    """
    return self.fileCatalogDB.datasetManager.getDatasetFiles(datasets, self.getRemoteCredentials())

  def getSEDump(self, seName):
    """
         Return all the files at a given SE, together with checksum and size

        :param seName: name of the StorageElement

        :returns: S_OK with list of tuples (lfn, checksum, size)
    """
    return self.fileCatalogDB.getSEDump(seName)['Value']


class FileCatalogHandler(FileCataloghandlerMixin, RequestHandler):

  def transfer_toClient(self, seName, token, fileHelper):
    """ This method used to transfer the SEDump to the client,
        formated as CSV with '|' separation

        :param seName: name of the se to dump

        :returns: the result of the FileHelper


    """

    retVal = self.getSEDump(seName)

    try:
      csvOutput = StringIO()
      writer = csv.writer(csvOutput, delimiter='|')
      for lfn in retVal:
        writer.writerow(lfn)

      csvOutput.seek(0)

      ret = fileHelper.DataSourceToNetwork(csvOutput)
      return ret

    except Exception as e:
      self.log.exception("Exception while sending seDump", repr(e))
      return S_ERROR("Exception while sending seDump: %s" % repr(e))
    finally:
      csvOutput.close()
