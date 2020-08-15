########################################################################
# File: StorageElementHandler.py
########################################################################
"""
:mod: StorageElementHandler

.. module: StorageElementHandler
  :synopsis: StorageElementHandler is the implementation of a simple StorageElement
  service in the DISET framework

The handler implements also the DISET data transfer calls
toClient(), fromClient(), bulkToClient(), bulkFromClient
which support single file, directory and file list upload and download

The class can be used as the basis for more advanced StorageElement implementations

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN StorageElement
  :end-before: ##END
  :dedent: 2
  :caption: StorageElementHandler options

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# imports
import os
import shutil
import stat
import re
import errno
import shlex
import six

# from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import mkDir, convertSizeUnits
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.Os import getDirectorySize
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.Adler import fileAdler

from DIRAC.Resources.Storage.StorageBase import StorageBase


BASE_PATH = "/"  # This is not a constant and it will be set in initializeStorageElementHandler() function
MAX_STORAGE_SIZE = 0  # This is in bytes, but in MB in the CS
USE_TOKENS = False


def getDiskSpace(path, total=False):
  """
    Returns disk usage of the given path, in Bytes.
    If total is set to true, the total disk space will be returned instead.
  """

  try:
    st = os.statvfs(path)

    if total:
      # return total space
      queriedSize = st.f_blocks
    else:
      # return free space
      queriedSize = st.f_bavail

    result = float(queriedSize * st.f_frsize)

  except OSError as e:
    return S_ERROR(errno.EIO, "Error while getting the available disk space: %s" % repr(e))

  return S_OK(round(result, 4))


def getTotalDiskSpace():
  """  Returns the total maximum volume of the SE storage in B. The total volume
       can be limited either by the amount of the available disk space or by the
       MAX_STORAGE_SIZE value.

       :return: S_OK/S_ERROR, Value is the max total volume of the SE storage in B
  """

  global BASE_PATH
  global MAX_STORAGE_SIZE

  result = getDiskSpace(BASE_PATH, total=True)
  if not result['OK']:
    return result
  totalSpace = result['Value']
  maxTotalSpace = min(totalSpace, MAX_STORAGE_SIZE) if MAX_STORAGE_SIZE else totalSpace
  return S_OK(maxTotalSpace)


def getFreeDiskSpace():
  """ Returns the free disk space still available for writing taking into account
      the total available disk space and the MAX_STORAGE_SIZE limitation

      :return: S_OK/S_ERROR, Value is the free space of the SE storage in B
  """

  global MAX_STORAGE_SIZE
  global BASE_PATH

  result = getDiskSpace(BASE_PATH)  # free
  if not result['OK']:
    return result
  if not MAX_STORAGE_SIZE:
    return result

  totalFreeSpace = result['Value']
  result = getDiskSpace(BASE_PATH, total=True)  # total
  if not result['OK']:
    return result
  totalSpace = result['Value']
  totalOccupiedSpace = totalSpace - totalFreeSpace
  freeSpace = min(totalSpace, MAX_STORAGE_SIZE) - totalOccupiedSpace
  return S_OK(freeSpace if freeSpace > 0 else 0)


def initializeStorageElementHandler(serviceInfo):
  """  Initialize Storage Element global settings
  """

  global BASE_PATH
  global USE_TOKENS
  global MAX_STORAGE_SIZE

  BASE_PATH = getServiceOption(serviceInfo, "BasePath", '')
  if not BASE_PATH:
    gLogger.error('Failed to get the base path')
    return S_ERROR('Failed to get the base path')
  mkDir(BASE_PATH)

  USE_TOKENS = getServiceOption(serviceInfo, "UseTokens", USE_TOKENS)
  MAX_STORAGE_SIZE = convertSizeUnits(getServiceOption(serviceInfo, "MaxStorageSize", MAX_STORAGE_SIZE), 'MB', 'B')

  gLogger.info('Starting DIRAC Storage Element')
  gLogger.info('Base Path: %s' % BASE_PATH)
  gLogger.info('Max size: %d Bytes' % MAX_STORAGE_SIZE)
  gLogger.info('Use access control tokens: ' + str(USE_TOKENS))
  return S_OK()


class StorageElementHandler(RequestHandler):
  """
  .. class:: StorageElementHandler

  """

  def __confirmToken(self, token, path, mode):
    """ Confirm the access rights for the path in a given mode
    """
    # Not yet implemented
    return True

  @staticmethod
  def __checkForDiskSpace(size):
    """ Check if the StorageElement can accommodate 'size' volume of data

        :param int size: size of a new file to store in the StorageElement
        :return: S_OK/S_ERROR, Value is boolean flag True if enough space is available
    """
    result = getFreeDiskSpace()
    if not result['OK']:
      return result
    freeSpace = result['Value']
    return S_OK(freeSpace > size)

  def __resolveFileID(self, fileID):
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

    if fileID.find(BASE_PATH) == 0:
      return fileID
    while fileID and fileID[0] == '/':
      fileID = fileID[1:]
    return os.path.join(BASE_PATH, fileID)

  @staticmethod
  def __getFileStat(path):
    """ Get the file stat information
    """
    resultDict = {}
    try:
      statTuple = os.stat(path)
    except OSError as x:
      if str(x).find('No such file') >= 0:
        resultDict['Exists'] = False
        return S_OK(resultDict)
      return S_ERROR('Failed to get metadata for %s' % path)

    resultDict['Exists'] = True
    mode = statTuple[stat.ST_MODE]
    resultDict['Type'] = "File"
    resultDict['File'] = True
    resultDict['Directory'] = False
    if stat.S_ISDIR(mode):
      resultDict['Type'] = "Directory"
      resultDict['File'] = False
    resultDict['Directory'] = True
    resultDict['Size'] = statTuple[stat.ST_SIZE]
    resultDict['TimeStamps'] = (statTuple[stat.ST_ATIME], statTuple[stat.ST_MTIME], statTuple[stat.ST_CTIME])
    resultDict['Cached'] = 1
    resultDict['Migrated'] = 0
    resultDict['Lost'] = 0
    resultDict['Unavailable'] = 0
    resultDict['Mode'] = stat.S_IMODE(mode)

    if resultDict['File']:
      cks = fileAdler(path)
      resultDict['Checksum'] = cks

    resultDict = StorageBase._addCommonMetadata(resultDict)

    return S_OK(resultDict)

  types_exists = [six.string_types]

  def export_exists(self, fileID):
    """ Check existence of the fileID """
    if os.path.exists(self.__resolveFileID(fileID)):
      return S_OK(True)
    return S_OK(False)

  types_getMetadata = [six.string_types]

  def export_getMetadata(self, fileID):
    """ Get metadata for the file or directory specified by fileID
    """
    return self.__getFileStat(self.__resolveFileID(fileID))

  types_getFreeDiskSpace = []

  @staticmethod
  def export_getFreeDiskSpace():
    """ Get the free disk space of the storage element

        :return: S_OK/S_ERROR, Value is the free space on the SE storage in B
    """
    return getFreeDiskSpace()

  types_getTotalDiskSpace = []

  @staticmethod
  def export_getTotalDiskSpace():
    """ Get the total disk space of the storage element

        :return: S_OK/S_ERROR, Value is the max total volume of the SE storage in B
    """
    return getTotalDiskSpace()

  types_createDirectory = [six.string_types]

  def export_createDirectory(self, dir_path):
    """ Creates the directory on the storage
    """
    path = self.__resolveFileID(dir_path)
    gLogger.info("StorageElementHandler.createDirectory: Attempting to create %s." % path)
    if os.path.exists(path):
      if os.path.isfile(path):
        errStr = "Supplied path exists and is a file"
        gLogger.error("StorageElementHandler.createDirectory: %s." % errStr, path)
        return S_ERROR(errStr)
      gLogger.info("StorageElementHandler.createDirectory: %s already exists." % path)
      return S_OK()
    # Need to think about permissions.
    try:
      mkDir(path)
      return S_OK()
    except Exception as x:
      errStr = "Exception creating directory."
      gLogger.error("StorageElementHandler.createDirectory: %s" % errStr, repr(x))
      return S_ERROR(errStr)

  types_listDirectory = [six.string_types, six.string_types]

  def export_listDirectory(self, dir_path, mode):
    """ Return the dir_path directory listing
    """
    is_file = False
    path = self.__resolveFileID(dir_path)
    if not os.path.exists(path):
      return S_ERROR('Directory %s does not exist' % dir_path)
    elif os.path.isfile(path):
      fname = os.path.basename(path)
      is_file = True
    else:
      dirList = os.listdir(path)

    resultDict = {}
    if mode == 'l':
      if is_file:
        result = self.__getFileStat(fname)
        if result['OK']:
          resultDict[fname] = result['Value']
          return S_OK(resultDict)
        return S_ERROR('Failed to get the file stat info')
      else:
        failed_list = []
        one_OK = False
        for fname in dirList:
          result = self.__getFileStat(path + '/' + fname)
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

  def transfer_fromClient(self, fileID, token, fileSize, fileHelper):
    """ Method to receive file from clients.
        fileID is the local file name in the SE.
        fileSize can be Xbytes or -1 if unknown.
        token is used for access rights confirmation.
    """
    result = self.__checkForDiskSpace(fileSize)
    if not result['OK']:
      return S_ERROR('Failed to get available free space')
    elif not result['Value']:
      return S_ERROR('Not enough disk space')
    file_path = self.__resolveFileID(fileID)

    if "NoCheckSum" in token:
      fileHelper.disableCheckSum()

    try:
      mkDir(os.path.dirname(file_path))
      with open(file_path, "wb") as fd:
        return fileHelper.networkToDataSink(fd, maxFileSize=(MAX_STORAGE_SIZE))
    except Exception as error:
      return S_ERROR("Cannot open to write destination file %s: %s" % (file_path, str(error)))

  def transfer_toClient(self, fileID, token, fileHelper):
    """ Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
    """
    file_path = self.__resolveFileID(fileID)
    if "NoCheckSum" in token:
      fileHelper.disableCheckSum()
    result = fileHelper.getFileDescriptor(file_path, 'r')
    if not result['OK']:
      result = fileHelper.sendEOF()
      # check if the file does not really exist
      if not os.path.exists(file_path):
        return S_ERROR('File %s does not exist' % os.path.basename(file_path))
      return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.FDToNetwork(fileDescriptor)
    fileHelper.oFile.close()
    if not result['OK']:
      return S_ERROR('Failed to get file ' + fileID)
    return result

  def transfer_bulkFromClient(self, fileID, token, _ignoredSize, fileHelper):
    """ Receive files packed into a tar archive by the fileHelper logic.
        token is used for access rights confirmation.
    """
    result = self.__checkForDiskSpace(10)
    if not result['OK']:
      return S_ERROR('Failed to get available free space')
    elif not result['Value']:
      return S_ERROR('Not enough disk space')
    dirName = fileID.replace('.bz2', '').replace('.tar', '')
    dir_path = self.__resolveFileID(dirName)
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
    fileInput = self.__resolveFileID(fileId)
    tmpList = fileInput.split(':')
    tmpList = [os.path.join(BASE_PATH, x) for x in tmpList]
    strippedFiles = []
    compress = False
    for fileID in tmpList:
      if re.search('.bz2', fileID):
        fileID = fileID.replace('.bz2', '')
        compress = True
      fileID = fileID.replace('.tar', '')
      strippedFiles.append(self.__resolveFileID(fileID))
    res = fileHelper.bulkToNetwork(strippedFiles, compress=compress)
    if not res['OK']:
      gLogger.error('Failed to send bulk to network', res['Message'])
    return res

  types_remove = [six.string_types, six.string_types]

  def export_remove(self, fileID, token):
    """ Remove fileID from the storage. token is used for access rights confirmation. """
    return self.__removeFile(self.__resolveFileID(fileID), token)

  def __removeFile(self, fileID, token):
    """ Remove one file with fileID name from the storage
    """
    filename = self.__resolveFileID(fileID)
    if self.__confirmToken(token, fileID, 'x'):
      try:
        os.remove(filename)
        return S_OK()
      except OSError as error:
        if str(error).find('No such file') >= 0:
          # File does not exist anyway
          return S_OK()
        return S_ERROR('Failed to remove file %s' % fileID)
    else:
      return S_ERROR('File removal %s not authorized' % fileID)

  types_getDirectorySize = [six.string_types]

  def export_getDirectorySize(self, fileID):
    """ Get the size occupied by the given directory
    """
    dir_path = self.__resolveFileID(fileID)
    if os.path.exists(dir_path):
      try:
        space = self.__getDirectorySize(dir_path)
        return S_OK(space)
      except Exception as error:
        gLogger.exception("Exception while getting size of directory", dir_path, error)
        return S_ERROR("Exception while getting size of directory")
    else:
      return S_ERROR("Directory does not exists")

  types_removeDirectory = [six.string_types, six.string_types]

  def export_removeDirectory(self, fileID, token):
    """ Remove the given directory from the storage
    """

    dir_path = self.__resolveFileID(fileID)
    if not self.__confirmToken(token, fileID, 'x'):
      return S_ERROR('Directory removal %s not authorized' % fileID)
    else:
      if not os.path.exists(dir_path):
        return S_OK()
      else:
        try:
          shutil.rmtree(dir_path)
          return S_OK()
        except Exception as error:
          gLogger.error("Failed to remove directory", dir_path)
          gLogger.error(str(error))
          return S_ERROR("Failed to remove directory %s" % dir_path)

  types_removeFileList = [list, six.string_types]

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

  types_getAdminInfo = []

  @staticmethod
  def export_getAdminInfo():
    """ Send the storage element administration information
    """
    storageDict = {}
    storageDict['BasePath'] = BASE_PATH
    storageDict['MaxCapacity'] = MAX_STORAGE_SIZE
    used_space = getDirectorySize(BASE_PATH)
    stats = os.statvfs(BASE_PATH)
    available_space = convertSizeUnits(stats.f_bsize * stats.f_bavail, 'B', 'MB')
    allowed_space = convertSizeUnits(MAX_STORAGE_SIZE, 'B', 'MB') - used_space
    actual_space = min(available_space, allowed_space)
    storageDict['AvailableSpace'] = actual_space
    storageDict['UsedSpace'] = used_space
    return S_OK(storageDict)

  @staticmethod
  def __getDirectorySize(path):
    """ Get the total size of the given directory in bytes
    """
    comm = "du -sb %s" % path
    result = systemCall(10, shlex.split(comm))
    if not result['OK'] or result['Value'][0]:
      return 0
    output = result['Value'][1]
    size = int(output.split()[0])
    return size
