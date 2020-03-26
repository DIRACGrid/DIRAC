"""
:mod: IRODSStorageElementHandler

.. module: IRODSStorageElementHandler
  :synopsis: IRODSStorageElementHandler is the implementation of a simple StorageElement
  service with the iRods SE as a backend

The following methods are available in the Service interface

getMetadata() - get file metadata
listDirectory() - get directory listing
remove() - remove one file
removeDirectory() - remove on directory recursively
removeFileList() - remove files in the list
getAdminInfo() - get administration information about the SE status

The handler implements also the DISET data transfer calls
toClient(), fromClient(), bulkToClient(), bulkFromClient
which support single file, directory and file list upload and download

The class can be used as the basis for more advanced StorageElement implementations

"""

# imports
import os
import stat
import re
import six
# from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

from DIRAC.Resources.Storage.StorageBase import StorageBase

from irods import rcConnect, rcDisconnect, clientLoginWithPassword  # pylint: disable=import-error
from irods import irodsCollection, irodsOpen, getResources  # pylint: disable=import-error

__RCSID__ = "$Id$"


"""
iRods Glossary:

Server: An iRODS Server is software that interacts with the access protocol of a
specific storage system; enables storing and sharing data distributed geographically
and across administrative domains.

Resource: A resource, or storage resource, is a software/hardware system that stores
digital data. Currently iRODS can use a Unix file system as a resource. As other
iRODS drivers are written, additional types of resources will be included. iRODS
clients can operate on local or remote data stored on different types of resources,
through a common interface.

Zone: An iRODS Zone is an independent iRODS system consisting of an iCAT-enabled server,
optional additional distributed iRODS Servers (which can reach hundreds, worldwide) and
clients. Each Zone has a unique name. An iRODS Zone can interoperate with other Zones
in what is called Federation

Collection: All Data Objects stored in an iRODS/iCat system are stored in some Collection,
which is a logical name for that set of data objects. A Collection can have sub-collections,
and hence provides a hierarchical structure. An iRODS/iCAT Collection is like a directory
in a Unix file system (or Folder in Windows), but is not limited to a single device or
partition. A Collection is logical or so that the data objects can span separate and
heterogeneous storage devices (i.e. is infrastructure and administrative domain independent).
Each Data Object in a Collection or sub-collection must have a unique name in that Collection.

Data Object: A Data Object is a single a "stream-of-bytes" entity that can be uniquely
identified, basically a file stored in iRODS. It is given a Unique Internal Identifier in
iRODS (allowing a global name space), and is associated with a Collection.

Collection is a directory
DataObject is a file
"""


# TODO: Should it be SE per resource or per iRods server?
IRODS_HOST = None
IRODS_PORT = None
IRODS_USER = None
IRODS_ZONE = None
BASE_PATH = ""
IRODS_HOME = ""
MAX_STORAGE_SIZE = 2048
IRODS_RESOURCE = None


def initializeIRODSStorageElementHandler(serviceInfo):
  """ Initialize Storage Element global settings
"""

  global IRODS_HOST
  global IRODS_PORT
  global IRODS_ZONE
  global BASE_PATH
  global IRODS_HOME
  global IRODS_RESOURCE

  cfgPath = serviceInfo['serviceSectionPath']

  IRODS_HOST = gConfig.getValue("%s/iRodsServer" % cfgPath, IRODS_HOST)
  if not IRODS_HOST:
    gLogger.error('Failed to get iRods server host')
    return S_ERROR('Failed to get iRods server host')

  IRODS_PORT = gConfig.getValue("%s/iRodsPort" % cfgPath, IRODS_PORT)
  try:
    IRODS_PORT = int(IRODS_PORT)
  except ValueError:
    pass
  if not IRODS_PORT:
    gLogger.error('Failed to get iRods server port')
    return S_ERROR('Failed to get iRods server port')

  IRODS_ZONE = gConfig.getValue("%s/iRodsZone" % cfgPath, IRODS_ZONE)
  if not IRODS_ZONE:
    gLogger.error('Failed to get iRods zone')
    return S_ERROR('Failed to get iRods zone')

  IRODS_HOME = gConfig.getValue("%s/iRodsHome" % cfgPath, IRODS_HOME)
  if not IRODS_HOME:
    gLogger.error('Failed to get the base path')
    return S_ERROR('Failed to get the base path')

  IRODS_RESOURCE = gConfig.getValue("%s/iRodsResource" % cfgPath, IRODS_RESOURCE)

  gLogger.info('Starting iRods Storage Element')
  gLogger.info('iRods server: %s' % IRODS_HOST)
  gLogger.info('iRods port: %s' % IRODS_PORT)
  gLogger.info('iRods zone: %s' % IRODS_ZONE)
  gLogger.info('iRods home: %s' % IRODS_HOME)
  gLogger.info('iRods resource: %s' % IRODS_RESOURCE)
  return S_OK()


class IRODSStorageElementHandler(RequestHandler):
  """
.. class:: StorageElementHandler

"""

  def __checkForDiskSpace(self, path, size):
    """ Check if iRods resource has enough space
    """
    # dsize = ( getDiskSpace( dpath ) - 1 ) * 1024 * 1024
    # maxStorageSizeBytes = MAX_STORAGE_SIZE * 1024 * 1024
    # return ( min( dsize, maxStorageSizeBytes ) > size )

    return True

  def __resolveFileID(self, fileID, userDict):
    """ get path to file for a given :fileID: """

    port = self.getCSOption('Port', '')
    if not port:
      return ''

    if ":%s" % port in fileID:
      loc = fileID.find(":%s" % port)
      if loc >= 0:
        fileID = fileID[loc + len(":%s" % port):]

    serviceName = self.serviceInfoDict['serviceName']
    loc = fileID.find(serviceName)
    if loc >= 0:
      fileID = fileID[loc + len(serviceName):]

    loc = fileID.find('?=')
    if loc >= 0:
      fileID = fileID[loc + 2:]

    # Leave only one / in front
    while fileID and fileID[0] == '/':
      fileID = fileID[1:]
    fileID = '/' + fileID

    # For user data strip off the HOME part as iRods has its own user home
    if fileID.startswith(userDict['DIRACHome']):
      fileID = fileID.replace(userDict['DIRACHome'], '')

    return fileID

  def __getFileStat(self, path):
    """
    Get the file stat information
    """

    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(path, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    file_path = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % file_path)

    resultDict = {}

    irodsFile = irodsOpen(conn, file_path, "r")
    if irodsFile:
      resultDict['Exists'] = True
      resultDict['File'] = True
      resultDict['Directory'] = False
      resultDict['Type'] = "File"
      resultDict['Size'] = irodsFile.getSize()
      resultDict['TimeStamps'] = (irodsFile.getCreateTs(), irodsFile.getModifyTs(), irodsFile.getCreateTs())
      resultDict['Cached'] = 1
      resultDict['Migrated'] = 0
      resultDict['Lost'] = 0
      resultDict['Unavailable'] = 0
      resultDict['Mode'] = 0o755
      resultDict = StorageBase._addCommonMetadata(resultDict)
      return S_OK(resultDict)
    else:
      coll = irodsCollection(conn, file_path)
      if coll:
        resultDict['Exists'] = True
        resultDict['Type'] = "Directory"
        resultDict['File'] = False
        resultDict['Directory'] = True
        resultDict['Size'] = 0
        resultDict['TimeStamps'] = (0, 0, 0)
        resultDict['Cached'] = 1
        resultDict['Migrated'] = 0
        resultDict['Lost'] = 0
        resultDict['Unavailable'] = 0
        resultDict['Mode'] = 0o755
        resultDict = StorageBase._addCommonMetadata(resultDict)
        return S_OK(resultDict)
      else:
        return S_ERROR('Path does not exist')

  types_exists = [list(six.string_types)]

  def export_exists(self, fileID):
    """ Check existence of the fileID """
    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(fileID, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    file_path = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % file_path)

    irodsFile = irodsOpen(conn, file_path, "r")
    if irodsFile:
      return S_OK(True)
    else:
      coll = irodsCollection(conn, file_path)
      if coll:
        return S_OK(True)
    return S_OK(False)

  types_getMetadata = [list(six.string_types)]

  def export_getMetadata(self, fileID):
    """
    Get metadata for the file or directory specified by fileID
    """
    return self.__getFileStat(fileID)

  types_createDirectory = [list(six.string_types)]

  def export_createDirectory(self, dir_path):
    """
    Creates the directory on the storage
    """
    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    coll = irodsCollection(conn, irodsHome)

    dir_path = self.__resolveFileID(dir_path, userDict)

    path = dir_path.split("/")

    if len(path) > 0:
      coll = self.__changeCollection(coll, path)
      if not coll:
        return S_ERROR('Failed to create directory')
    return S_OK()

  types_listDirectory = [list(six.string_types), list(six.string_types)]

  def export_listDirectory(self, dir_path, mode):
    """
    Return the dir_path directory listing
    """
    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(dir_path, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    irodsPath = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % irodsPath)

    is_file = False
    irodsFile = irodsOpen(conn, irodsPath, "r")
    if not irodsFile:
      is_file = True
    else:
      irodsDir = os.path.dirname(irodsPath)
      coll = irodsCollection(conn, irodsDir)
      if not coll:
        return S_ERROR('Directory not found')
      objects = coll.getObjects()
      fileList = [x[0] for x in objects]
      dirList = coll.getSubCollections()

    resultDict = {}
    if mode == 'l':
      if is_file:
        result = self.__getFileStat(dir_path)
        if result['OK']:
          resultDict[dir_path] = result['Value']
          return S_OK(resultDict)
        else:
          return S_ERROR('Failed to get the file stat info')
      else:
        failed_list = []
        one_OK = False
        for fname in dirList + fileList:
          result = self.__getFileStat(dir_path + '/' + fname)
          if result['OK']:
            resultDict[fname] = result['Value']
            one_OK = True
          else:
            failed_list.append(fname)
        if failed_list:
          if one_OK:
            result = S_ERROR('Failed partially to get the file stat info')
          else:
            result = S_ERROR('Failed to get the file stat info')
          result['FailedList'] = failed_list
          result['Value'] = resultDict
        else:
          result = S_OK(resultDict)

        return result
    else:
      return S_OK(dirList)

  def __changeCollection(self, coll, path):
    if not len(path) > 0:
      return coll
    name = path.pop(0)
    if not len(name) > 0:
      return self.__changeCollection(coll, path)
    gLogger.info('Check subcollection: %s' % name)
    subs = coll.getSubCollections()
    if name not in subs:
      gLogger.info('Create subcollection: %s' % name)
      coll.createCollection(name)
    gLogger.info('Open subcollection: %s' % name)
    coll.openCollection(name)
    return self.__changeCollection(coll, path)

  def transfer_fromClient(self, fileID, token, fileSize, fileHelper):
    """
    Method to receive file from clients.
    fileID is the local file name in the SE.
    fileSize can be Xbytes or -1 if unknown.
    token is used for access rights confirmation.
    """

    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    coll = irodsCollection(conn, irodsHome)

    if not self.__checkForDiskSpace(IRODS_HOME, fileSize):
      rcDisconnect(conn)
      return S_ERROR('Not enough disk space')

    file_path = self.__resolveFileID(fileID, userDict)

    path = file_path.split("/")
    file_ = path.pop()

    if len(path) > 0:
      coll = self.__changeCollection(coll, path)

    file_path = irodsHome + file_path

    try:
      if IRODS_RESOURCE:
        irodsFile = coll.create(file_, IRODS_RESOURCE)
      else:
        irodsFile = coll.create(file_, "w")
    except Exception as error:
      rcDisconnect(conn)
      return S_ERROR("Cannot open to write destination file %s: %s" % (file_path, str(error)))

    if "NoCheckSum" in token:
      fileHelper.disableCheckSum()

    result = fileHelper.networkToDataSink(irodsFile, maxFileSize=(MAX_STORAGE_SIZE * 1024 * 1024))
    irodsFile.close()
    rcDisconnect(conn)
    if not result['OK']:
      return result
    return result

  def transfer_toClient(self, fileID, token, fileHelper):
    """ Method to send files to clients.
fileID is the local file name in the SE.
token is used for access rights confirmation.
"""

    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(fileID, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    file_path = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % file_path)

    irodsFile = irodsOpen(conn, file_path, "r")
    if not irodsFile:
      rcDisconnect(conn)
      gLogger.error("Failed to get file object")
      return S_ERROR("Failed to get file object")

    result = fileHelper.DataSourceToNetwork(irodsFile)
    irodsFile.close()

    rcDisconnect(conn)
    if not result["OK"]:
      gLogger.error("Failed to get file " + fileID)
      return S_ERROR("Failed to get file " + fileID)
    else:
      return result

  def transfer_bulkFromClient(self, fileID, token, ignoredSize, fileHelper):
    """ Receive files packed into a tar archive by the fileHelper logic.
token is used for access rights confirmation.
"""
    if not self.__checkForDiskSpace(BASE_PATH, 10 * 1024 * 1024):
      return S_ERROR('Less than 10MB remaining')
    dirName = fileID.replace('.bz2', '').replace('.tar', '')
    dir_path = self.__resolveFileID(dirName, {})
    res = fileHelper.networkToBulk(dir_path)
    if not res['OK']:
      gLogger.error('Failed to receive network to bulk.', res['Message'])
      return res
    if not os.path.exists(dir_path):
      return S_ERROR('Failed to receive data')
    try:
      os.chmod(dir_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    except Exception as error:
      gLogger.exception('Could not set permissions of destination directory.', dir_path, error)
    return S_OK()

  def transfer_bulkToClient(self, fileId, token, fileHelper):
    """ Send directories and files specified in the fileID.
The fileID string can be a single directory name or a list of
colon (:) separated file/directory names.
token is used for access rights confirmation.
"""
    tmpList = fileId.split(':')
    tmpList = [os.path.join(BASE_PATH, x) for x in tmpList]
    strippedFiles = []
    compress = False
    for fileID in tmpList:
      if re.search('.bz2', fileID):
        fileID = fileID.replace('.bz2', '')
        compress = True
      fileID = fileID.replace('.tar', '')
      strippedFiles.append(self.__resolveFileID(fileID, {}))
    res = fileHelper.bulkToNetwork(strippedFiles, compress=compress)
    if not res['OK']:
      gLogger.error('Failed to send bulk to network', res['Message'])
    return res

  types_remove = [list(six.string_types), list(six.string_types)]

  def export_remove(self, fileID, token):
    """ Remove fileID from the storage. token is used for access rights confirmation. """
    return self.__removeFile(fileID, token)

  def __confirmToken(self, token, path, mode):
    """ Confirm the access rights for the path in a given mode
    """
    # Not yet implemented
    return True

  def __removeFile(self, fileID, token):
    """ Remove one file with fileID name from the storage
"""
    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(fileID, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    file_path = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % file_path)

    irodsFile = irodsOpen(conn, file_path, "r")
    if not irodsFile:
      rcDisconnect(conn)
      gLogger.error("Failed to get file object")
      return S_ERROR("Failed to get file object")

    if self.__confirmToken(token, fileID, 'x'):
      try:
        status = irodsFile.delete()
        if status == 0:
          return S_OK()
        else:
          return S_ERROR('Failed to delete file with status code %d' % status)
      except OSError as error:
        if str(error).find('No such file') >= 0:
          # File does not exist anyway
          return S_OK()
        else:
          return S_ERROR('Failed to remove file %s with exception %s' % (fileID, error))
    else:
      return S_ERROR('File removal %s not authorized' % fileID)

  types_getDirectorySize = [list(six.string_types)]

  def export_getDirectorySize(self, fileID):
    """ Get the size occupied by the given directory
"""
    dir_path = self.__resolveFileID(fileID, {})
    if os.path.exists(dir_path):
      try:
        space = self.__getDirectorySize(dir_path)
        return S_OK(space)
      except Exception as error:
        gLogger.exception("Exception while getting size of directory", dir_path, error)
        return S_ERROR("Exception while getting size of directory")
    else:
      return S_ERROR("Directory does not exists")

  def __getDirectorySize(self, dir_path):
    """ dummy implementation to make pylint happy"""
    raise NotImplementedError("IRODSStorageElement__getDirectorySize is not implemented ")

  types_removeDirectory = [list(six.string_types), list(six.string_types)]

  def export_removeDirectory(self, fileID, token):
    """ Remove the given directory from the storage
"""

    conn, error, userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    file_path = self.__resolveFileID(fileID, userDict)
    irodsHome = userDict.get('iRodsHome', IRODS_HOME)
    file_path = irodsHome + file_path
    gLogger.debug("file_path to read: %s" % file_path)

    coll = irodsCollection(conn, file_path)
    if not coll:
      return S_ERROR('Directory not found')
    objects = coll.getObjects()
    if objects:
      return S_ERROR('Directory is not empty')
    coll.delete()

    return S_ERROR()

  types_removeFileList = [list, list(six.string_types)]

  def export_removeFileList(self, fileList, token):
    """ Remove files in the given list
"""
    failed_list = []
    partial_success = False
    for fileItem in fileList:
      result = self.__removeFile(fileItem, token)
      if not result['OK']:
        failed_list.append(fileItem)
      else:
        partial_success = True

    if not failed_list:
      return S_OK()
    else:
      if partial_success:
        result = S_ERROR('Bulk file removal partially failed')
        result['FailedList'] = failed_list
      else:
        result = S_ERROR('Bulk file removal failed')
      return result

###################################################################

  def export_getResourceInfo(self, resource=None):
    """ Send the storage element resource information
"""

    conn, error, _userDict = self.__irodsClient()
    if not conn:
      return S_ERROR(error)

    storageDict = {}
    for resource in getResources(conn):
      name = resource.getName()
      if resource and resource != name:
        continue
      storageDict[name] = {}
      storageDict[name]["Id"] = resource.getId()
      storageDict[name]["AvailableSpace"] = resource.getFreeSpace()
      storageDict[name]["Zone"] = resource.getZone()
      storageDict[name]["Type"] = resource.getTypeName()
      storageDict[name]["Class"] = resource.getClassName()
      storageDict[name]["Host"] = resource.getHost()
      storageDict[name]["Path"] = resource.getPath()
      storageDict[name]["Free Space TS"] = resource.getFreeSpaceTs()
      storageDict[name]["Info"] = resource.getInfo()
      storageDict[name]["Comment"] = resource.getComment()
    rcDisconnect(conn)

    return S_OK(storageDict)

  def __getUserDetails(self):
    """ Get details on user account
    """
    credentials = self.getRemoteCredentials()
    if credentials:
      diracUser = credentials.get("username")
      diracGroup = credentials.get("group")
    if not (diracUser and diracGroup):
      return S_ERROR('Failed to get DIRAC user name and/or group')
    vo = getVOForGroup(diracGroup)

    diracHome = ''
    if vo:
      diracHome = '/%s/user/%s/%s' % (vo, diracUser[0], diracUser)

    cfgPath = self.serviceInfoDict['serviceSectionPath']
    gLogger.debug("cfgPath: %s" % cfgPath)
    irodsUser = gConfig.getValue("%s/UserCredentials/%s/iRodsUser" % (cfgPath, diracUser), diracUser)
    irodsHome = gConfig.getValue("%s/UserCredentials/%s/iRodsHome" % (cfgPath, diracUser), '')
    irodsGroup = gConfig.getValue("%s/UserCredentials/%s/iRodsGroup" % (cfgPath, diracUser), '')
    irodsPassword = gConfig.getValue("%s/UserCredentials/%s/iRodsPassword" % (cfgPath, diracUser), '')

    resultDict = {}
    resultDict['DIRACUser'] = diracUser
    resultDict['DIRACGroup'] = diracGroup
    resultDict['DIRACHome'] = diracHome
    resultDict['iRodsUser'] = irodsUser
    resultDict['iRodsGroup'] = irodsGroup
    resultDict['iRodsHome'] = irodsHome
    resultDict['iRodsPassword'] = irodsPassword

    return S_OK(resultDict)

  def __irodsClient(self, user=None):
    """ Get the iRods client
    """
    global IRODS_USER

    userDict = {}
    result = self.__getUserDetails()
    if not result['OK']:
      return False, "Failed to get iRods user info", userDict

    userDict = result['Value']

    IRODS_USER = userDict['iRodsUser']

    if not IRODS_USER:
      return False, "Failed to get iRods user", userDict
    gLogger.debug("iRods user: %s" % IRODS_USER)

    password = userDict['iRodsPassword']
    if not password:
      return False, "Failed to get iRods user/password", userDict

    conn, errMsg = rcConnect(IRODS_HOST, IRODS_PORT, IRODS_USER, IRODS_ZONE)
    status = clientLoginWithPassword(conn, password)

    if not status == 0:
      return False, "Failed to authenticate user '%s'" % IRODS_USER, userDict

    return conn, errMsg, userDict
