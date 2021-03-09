########################################################################
# File: FileCatalogHandler.py
########################################################################
"""
:mod: FileCatalogHandler

.. module: FileCatalogHandler

:synopsis: FileCatalogHandler is a simple Replica and Metadata Catalog service

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# imports
import six
import csv
import os

from io import BytesIO
# from DIRAC
from DIRAC.Core.DISET.RequestHandler import getServiceOption

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB

from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService


class TornadoFileCatalogHandler(TornadoService):
  """
  ..class:: FileCatalogHandler

  A simple Replica and Metadata Catalog service.
  """

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ handler initialisation """

    dbLocation = getServiceOption(serviceInfo, 'Database', 'DataManagement/FileCatalogDB')
    cls.gFileCatalogDB = FileCatalogDB(dbLocation)

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
    res = cls.gFileCatalogDB.setConfig(databaseConfig)

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
  def export_changePathOwner(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.gFileCatalogDB.changePathOwner(lfns, self.getRemoteCredentials(), recursive)

  def export_changePathGroup(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.gFileCatalogDB.changePathGroup(lfns, self.getRemoteCredentials(), recursive)

  def export_changePathMode(self, lfns, recursive=False):
    """ Get replica info for the given list of LFNs
    """
    return self.gFileCatalogDB.changePathMode(lfns, self.getRemoteCredentials(), recursive)

  ########################################################################
  # ACL Operations
  #
  def export_getPathPermissions(self, lfns):
    """ Determine the ACL information for a supplied path
    """
    return self.gFileCatalogDB.getPathPermissions(lfns, self.getRemoteCredentials())

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

    return self.gFileCatalogDB.hasAccess(opType, paths, self.getRemoteCredentials())

  ###################################################################
  #
  #  isOK
  #

  @classmethod
  def export_isOK(cls):
    """ returns S_OK if DB is connected
    """
    if cls.gFileCatalogDB and cls.gFileCatalogDB._connected:
      return S_OK()
    return S_ERROR('Server not connected to DB')

  ###################################################################
  #
  #  User/Group write operations
  #

  def export_addUser(self, userName):
    """ Add a new user to the File Catalog """
    return self.gFileCatalogDB.addUser(userName, self.getRemoteCredentials())

  def export_deleteUser(self, userName):
    """ Delete user from the File Catalog """
    return self.gFileCatalogDB.deleteUser(userName, self.getRemoteCredentials())

  def export_addGroup(self, groupName):
    """ Add a new group to the File Catalog """
    return self.gFileCatalogDB.addGroup(groupName, self.getRemoteCredentials())

  def export_deleteGroup(self, groupName):
    """ Delete group from the File Catalog """
    return self.gFileCatalogDB.deleteGroup(groupName, self.getRemoteCredentials())

  ###################################################################
  #
  #  User/Group read operations
  #

  def export_getUsers(self):
    """ Get all the users defined in the File Catalog """
    return self.gFileCatalogDB.getUsers(self.getRemoteCredentials())

  def export_getGroups(self):
    """ Get all the groups defined in the File Catalog """
    return self.gFileCatalogDB.getGroups(self.getRemoteCredentials())

  ########################################################################
  #
  # Path read operations
  #

  def export_exists(self, lfns):
    """ Check whether the supplied paths exists """
    return self.gFileCatalogDB.exists(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File write operations
  #

  def export_addFile(self, lfns):
    """ Register supplied files """
    gMonitor.addMark("AddFile", 1)
    res = self.gFileCatalogDB.addFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  def export_removeFile(self, lfns):
    """ Remove the supplied lfns """
    gMonitor.addMark("RemoveFile", 1)
    res = self.gFileCatalogDB.removeFile(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveFileSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveFileFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  def export_setFileStatus(self, lfns):
    """ Remove the supplied lfns """
    return self.gFileCatalogDB.setFileStatus(lfns, self.getRemoteCredentials())

  def export_addReplica(self, lfns):
    """ Register supplied replicas """
    gMonitor.addMark("AddReplica", 1)
    res = self.gFileCatalogDB.addReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("AddReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("AddReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  def export_removeReplica(self, lfns):
    """ Remove the supplied replicas """
    gMonitor.addMark("RemoveReplica", 1)
    res = self.gFileCatalogDB.removeReplica(lfns, self.getRemoteCredentials())
    if res['OK']:
      gMonitor.addMark("RemoveReplicaSuccessful", len(res.get('Value', {}).get('Successful', [])))
      gMonitor.addMark("RemoveReplicaFailed", len(res.get('Value', {}).get('Failed', [])))

    return res

  def export_setReplicaStatus(self, lfns):
    """ Set the status for the supplied replicas """
    return self.gFileCatalogDB.setReplicaStatus(lfns, self.getRemoteCredentials())

  def export_setReplicaHost(self, lfns):
    """ Change the registered SE for the supplied replicas """
    return self.gFileCatalogDB.setReplicaHost(lfns, self.getRemoteCredentials())

  def export_addFileAncestors(self, lfns):
    """ Add file ancestor information for the given list of LFNs """
    return self.gFileCatalogDB.addFileAncestors(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # File read operations
  #

  def export_isFile(self, lfns):
    """ Check whether the supplied lfns are files """
    return self.gFileCatalogDB.isFile(lfns, self.getRemoteCredentials())

  def export_getFileSize(self, lfns):
    """ Get the size associated to supplied lfns """
    return self.gFileCatalogDB.getFileSize(lfns, self.getRemoteCredentials())

  def export_getFileMetadata(self, lfns):
    """ Get the metadata associated to supplied lfns """
    return self.gFileCatalogDB.getFileMetadata(lfns, self.getRemoteCredentials())

  def export_getReplicas(self, lfns, allStatus=False):
    """ Get replicas for supplied lfns """
    return self.gFileCatalogDB.getReplicas(lfns, allStatus, self.getRemoteCredentials())

  def export_getReplicaStatus(self, lfns):
    """ Get the status for the supplied replicas """
    return self.gFileCatalogDB.getReplicaStatus(lfns, self.getRemoteCredentials())

  def export_getFileAncestors(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, list):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return self.gFileCatalogDB.getFileAncestors(lfnDict, dList, self.getRemoteCredentials())

  def export_getFileDescendents(self, lfns, depths):
    """ Get the status for the supplied replicas """
    dList = depths
    if not isinstance(dList, list):
      dList = [depths]
    lfnDict = dict.fromkeys(lfns, True)
    return self.gFileCatalogDB.getFileDescendents(lfnDict, dList, self.getRemoteCredentials())

  def export_getLFNForGUID(self, guids):
    """Get the matching lfns for given guids"""
    return self.gFileCatalogDB.getLFNForGUID(guids, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory write operations
  #

  def export_createDirectory(self, lfns):
    """ Create the supplied directories """
    return self.gFileCatalogDB.createDirectory(lfns, self.getRemoteCredentials())

  def export_removeDirectory(self, lfns):
    """ Remove the supplied directories """
    return self.gFileCatalogDB.removeDirectory(lfns, self.getRemoteCredentials())

  ########################################################################
  #
  # Directory read operations
  #

  def export_listDirectory(self, lfns, verbose):
    """ List the contents of supplied directories """
    gMonitor.addMark('ListDirectory', 1)
    return self.gFileCatalogDB.listDirectory(lfns, self.getRemoteCredentials(), verbose=verbose)

  def export_isDirectory(self, lfns):
    """ Determine whether supplied path is a directory """
    return self.gFileCatalogDB.isDirectory(lfns, self.getRemoteCredentials())

  def export_getDirectoryMetadata(self, lfns):
    """ Get the size of the supplied directory """
    return self.gFileCatalogDB.getDirectoryMetadata(lfns, self.getRemoteCredentials())

  def export_getDirectorySize(self, lfns, longOut=False, fromFiles=False, recursiveSum=True):
    """ Get the size of the supplied directory """
    return self.gFileCatalogDB.getDirectorySize(lfns, longOut, fromFiles, recursiveSum, self.getRemoteCredentials())

  def export_getDirectoryReplicas(self, lfns, allStatus=False):
    """ Get replicas for files in the supplied directory """
    return self.gFileCatalogDB.getDirectoryReplicas(lfns, allStatus, self.getRemoteCredentials())

  ########################################################################
  #
  # Administrative database operations
  #

  def export_getCatalogCounters(self):
    """ Get the number of registered directories, files and replicas in various tables """
    return self.gFileCatalogDB.getCatalogCounters(self.getRemoteCredentials())

  @staticmethod
  def export_rebuildDirectoryUsage(self):
    """ Rebuild DirectoryUsage table from scratch """
    return self.gFileCatalogDB.rebuildDirectoryUsage()

  def export_repairCatalog(self):
    """ Repair the catalog inconsistencies """
    return self.gFileCatalogDB.repairCatalog(self.getRemoteCredentials())

  ########################################################################
  # Metadata Catalog Operations
  #

  def export_addMetadataField(self, fieldName, fieldType, metaType='-d'):
    """ Add a new metadata field of the given type
    """
    if metaType.lower() == "-d":
      return self.gFileCatalogDB.dmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    elif metaType.lower() == "-f":
      return self.gFileCatalogDB.fmeta.addMetadataField(
          fieldName, fieldType, self.getRemoteCredentials())
    else:
      return S_ERROR('Unknown metadata type %s' % metaType)

  def export_deleteMetadataField(self, fieldName):
    """ Delete the metadata field
    """
    result = self.gFileCatalogDB.dmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    error = ''
    if not result['OK']:
      error = result['Message']
    result = self.gFileCatalogDB.fmeta.deleteMetadataField(fieldName, self.getRemoteCredentials())
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"]

    return result

  def export_getMetadataFields(self):
    """ Get all the metadata fields
    """
    resultDir = self.gFileCatalogDB.dmeta.getMetadataFields(self.getRemoteCredentials())
    if not resultDir['OK']:
      return resultDir
    resultFile = self.gFileCatalogDB.fmeta.getFileMetadataFields(self.getRemoteCredentials())
    if not resultFile['OK']:
      return resultFile

    return S_OK({'DirectoryMetaFields': resultDir['Value'],
                 'FileMetaFields': resultFile['Value']})

  def export_setMetadata(self, path, metadatadict):
    """ Set metadata parameter for the given path
    """
    return self.gFileCatalogDB.setMetadata(path, metadatadict, self.getRemoteCredentials())

  def export_setMetadataBulk(self, pathMetadataDict):
    """ Set metadata parameter for the given path
    """
    return self.gFileCatalogDB.setMetadataBulk(pathMetadataDict, self.getRemoteCredentials())

  def export_removeMetadata(self, pathMetadataDict):
    """ Remove the specified metadata for the given path
    """
    return self.gFileCatalogDB.removeMetadata(pathMetadataDict, self.getRemoteCredentials())

  def export_getDirectoryUserMetadata(self, path):
    """ Get all the metadata valid for the given directory path
    """
    return self.gFileCatalogDB.dmeta.getDirectoryMetadata(path, self.getRemoteCredentials())

  def export_getFileUserMetadata(self, path):
    """ Get all the metadata valid for the given file
    """
    return self.gFileCatalogDB.fmeta.getFileUserMetadata(path, self.getRemoteCredentials())

  def export_findDirectoriesByMetadata(self, metaDict, path='/'):
    """ Find all the directories satisfying the given metadata set
    """
    return self.gFileCatalogDB.dmeta.findDirectoriesByMetadata(
        metaDict, path, self.getRemoteCredentials())

  def export_findFilesByMetadata(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = self.gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK']:
      return result
    lfns = result['Value'].values()
    return S_OK(lfns)

  def export_getReplicasByMetadata(self, metaDict, path='/', allStatus=False):
    """ Find all the files satisfying the given metadata set
    """
    return self.gFileCatalogDB.fileManager.getReplicasByMetadata(metaDict,
                                                                 path,
                                                                 allStatus,
                                                                 self.getRemoteCredentials())

  def export_findFilesByMetadataDetailed(self, metaDict, path='/'):
    """ Find all the files satisfying the given metadata set
    """
    result = self.gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
    if not result['OK'] or not result['Value']:
      return result

    lfns = result['Value'].values()
    return self.gFileCatalogDB.getFileDetails(lfns, self.getRemoteCredentials())

  def export_findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find files satisfying the given metadata set
    """
    result = self.gFileCatalogDB.dmeta.findFileIDsByMetadata(
        metaDict, path, self.getRemoteCredentials(), startItem, maxItems)
    if not result['OK'] or not result['Value']:
      return result

    fileIDs = result['Value']
    totalRecords = result['TotalRecords']

    result = self.gFileCatalogDB.fileManager._getFileLFNs(fileIDs)
    if not result['OK']:
      return result

    lfnsResultList = result['Value']['Successful'].values()
    resultDetails = self.gFileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  def findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems):
    """ Find all the files satisfying the given metadata set
    """
    result = self.gFileCatalogDB.fmeta.findFilesByMetadata(metaDict, path, self.getRemoteCredentials())
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

    resultDetails = self.gFileCatalogDB.getFileDetails(lfnsResultList, self.getRemoteCredentials())
    if not resultDetails['OK']:
      return resultDetails

    result = S_OK({"TotalRecords": totalRecords, "Records": resultDetails['Value']})
    return result

  def export_getCompatibleMetadata(self, metaDict, path='/'):
    """ Get metadata values compatible with the given metadata subset
    """
    return self.gFileCatalogDB.dmeta.getCompatibleMetadata(metaDict, path, self.getRemoteCredentials())

  def export_addMetadataSet(self, setName, setDict):
    """ Add a new metadata set
    """
    return self.gFileCatalogDB.dmeta.addMetadataSet(setName, setDict, self.getRemoteCredentials())

  def export_getMetadataSet(self, setName, expandFlag):
    """ Add a new metadata set
    """
    return self.gFileCatalogDB.dmeta.getMetadataSet(setName, expandFlag, self.getRemoteCredentials())

#########################################################################################
#
#  Dataset manipulation methods
#

  def export_addDataset(self, datasets):
    """ Add a new dynamic dataset defined by its meta query
    """
    return self.gFileCatalogDB.datasetManager.addDataset(datasets, self.getRemoteCredentials())

  def export_addDatasetAnnotation(self, datasetDict):
    """ Add annotation to an already created dataset
    """
    return self.gFileCatalogDB.datasetManager.addDatasetAnnotation(
        datasetDict, self.getRemoteCredentials())

  def export_removeDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return self.gFileCatalogDB.datasetManager.removeDataset(datasets, self.getRemoteCredentials())

  def export_checkDataset(self, datasets):
    """ Check the given dynamic dataset for changes since its definition
    """
    return self.gFileCatalogDB.datasetManager.checkDataset(datasets, self.getRemoteCredentials())

  def export_updateDataset(self, datasets):
    """ Update the given dynamic dataset for changes since its definition
    """
    return self.gFileCatalogDB.datasetManager.updateDataset(datasets, self.getRemoteCredentials())

  def export_getDatasets(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return self.gFileCatalogDB.datasetManager.getDatasets(datasets, self.getRemoteCredentials())

  def export_getDatasetParameters(self, datasets):
    """ Get parameters of the given dynamic dataset as they are stored in the database
    """
    return self.gFileCatalogDB.datasetManager.getDatasetParameters(datasets, self.getRemoteCredentials())

  def export_getDatasetAnnotation(self, datasets):
    """ Get annotation of the given datasets
    """
    return self.gFileCatalogDB.datasetManager.getDatasetAnnotation(datasets, self.getRemoteCredentials())

  def export_freezeDataset(self, datasets):
    """ Freeze the contents of the dataset making it effectively static
    """
    return self.gFileCatalogDB.datasetManager.freezeDataset(datasets, self.getRemoteCredentials())

  def export_releaseDataset(self, datasets):
    """ Release the contents of the frozen dataset allowing changes in its contents
    """
    return self.gFileCatalogDB.datasetManager.releaseDataset(datasets, self.getRemoteCredentials())

  def export_getDatasetFiles(self, datasets):
    """ Get lfns in the given dataset
    """
    return self.gFileCatalogDB.datasetManager.getDatasetFiles(datasets, self.getRemoteCredentials())

  def getSEDump(self, seName):
    """
         Return all the files at a given SE, together with checksum and size

        :param seName: name of the StorageElement

        :returns: S_OK with list of tuples (lfn, checksum, size)
    """
    return self.gFileCatalogDB.getSEDump(seName)['Value']

  def export_streamToClient(self, seName):
    """ This method used to transfer the SEDump to the client,
        formated as CSV with '|' separation

        :param seName: name of the se to dump

        :returns: the result of the FileHelper


    """

    retVal = self.getSEDump(seName)

    try:
      csvOutput = BytesIO()
      writer = csv.writer(csvOutput, delimiter='|')
      for lfn in retVal:
        writer.writerow(lfn)

      # csvOutput.seek(0)
      ret = csvOutput.getvalue()
      return ret

    except Exception as e:
      gLogger.exception("Exception while sending seDump", repr(e))
      return S_ERROR("Exception while sendind seDump: %s" % repr(e))
    finally:
      csvOutput.close()
