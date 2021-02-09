""" :mod: GFAL2_StorageBase

.. module: python

:synopsis: GFAL2 class from StorageElement using gfal2. Other modules can inherit from this use the gfal2 methods.


:TODO: When we are totally migrated to python 3, we can explicitely remove the str cast
  of all the ctx calls. They were added because we would receive unicode from DIRAC https servers
  and boost python does not play well with that.

Environment Variables:

* DIRAC_GFAL_GRIDFTP_SESSION_REUSE: This should be exported
  and set to true in server bashrc files for efficiency reasons.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=arguments-differ

# # imports
from past.builtins import long
import six
import os
import datetime
import errno
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
    S_IRWXG, S_IRWXU, S_IRWXO

import gfal2  # pylint: disable=import-error

# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse


# # RCSID
__RCSID__ = "$Id$"


class GFAL2_StorageBase(StorageBase):
  """ .. class:: GFAL2_StorageBase

  SRM v2 interface to StorageElement using gfal2
  """

  def __init__(self, storageName, parameters):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param dict parameters: storage parameters
    """

    StorageBase.__init__(self, storageName, parameters)

    self.log = gLogger.getSubLogger("GFAL2_StorageBase", True)

    # Some storages have problem to compute the checksum with xroot while getting the files
    # This allows to disable the checksum calculation while transferring, and then we compare the
    # file size
    self.disableTransferChecksum = True if (parameters.get('DisableChecksum') == 'True') else False

    # Different levels or verbosity:
    # gfal2.verbose_level.normal,
    # gfal2.verbose_level.verbose,
    # gfal2.verbose_level.debug,
    # gfal2.verbose_level.trace

    dlevel = self.log.getLevel()
    if dlevel == 'DEBUG':
      gLogger.enableLogsFromExternalLibs()
      gfal2.set_verbose(gfal2.verbose_level.trace)

    self.isok = True

    # # gfal2 API
    self.ctx = gfal2.creat_context()

    # by default turn off BDII checks
    self.ctx.set_opt_boolean("BDII", "ENABLE", False)

    # session reuse should only be done on servers
    self.ctx.set_opt_boolean("GRIDFTP PLUGIN", "SESSION_REUSE",
                             os.environ.get("DIRAC_GFAL_GRIDFTP_SESSION_REUSE", "no").lower() in ["true", "yes"])

    # Enable IPV6 for gsiftp
    self.ctx.set_opt_boolean("GRIDFTP PLUGIN", "IPV6", True)

    # spaceToken used for copying from and to the storage element
    self.spaceToken = parameters.get('SpaceToken', '')
    # stageTimeout, default timeout to try and stage/pin a file
    self.stageTimeout = gConfig.getValue('/Resources/StorageElements/StageTimeout', 12 * 60 * 60)
    # gfal2Timeout, amount of time it takes until an operation times out
    self.gfal2Timeout = gConfig.getValue("/Resources/StorageElements/GFAL_Timeout", 100)
    # set the gfal2 default protocols, e.g. used when trying to retrieve transport url
    self.defaultLocalProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols', [])

    # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue("/Resources/StorageElements/ChecksumType", '0')

    if self.checksumType == '0':
      self.checksumType = None

    self.log.debug('GFAL2_StorageBase: using %s checksum' % self.checksumType)

    self.voName = None
    ret = getProxyInfo(disableVOMS=True)
    if ret['OK'] and 'group' in ret['Value']:
      self.voName = getVOForGroup(ret['Value']['group'])

    self.MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10  # 10 MB ???
    self.MIN_BANDWIDTH = 0.5 * (1024 * 1024)  # 0.5 MB/s ???

    # This is the list of extended metadata to query the server for.
    # It is used by getSingleMetadata.
    # If set to None, No extended metadata will be queried
    # If the list is empty, all of them will be queried
    self._defaultExtendedAttributes = []

  def exists(self, path):
    """ Check if the path exists on the storage

    :param self: self reference
    :param str path: path or list of paths to be checked
    :returns: Failed dictionary: {pfn : error message}
              Successful dictionary: {pfn : bool}
              S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.exists: Checking the existence of %s path(s)" % len(urls))

    successful = {}
    failed = {}

    for url in urls:
      res = self.__singleExists(url)

      if res['OK']:
        successful[url] = res['Value']
      else:  # something went wrong with the query
        failed[url] = res['Message']

    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def _estimateTransferTimeout(self, fileSize):
    """ Dark magic to estimate the timeout for a transfer
        The values are set empirically and seem to work fine.
        They were evaluated with gfal1 and SRM.

        :param fileSize: size of the file in bytes we want to transfer

        :return: timeout in seconds
    """

    return int(fileSize / self.MIN_BANDWIDTH * 4 + 310)

  def __singleExists(self, path):
    """ Check if :path: exists on the storage

    :param self: self reference
    :param str: path to be checked (srm://...)
    :returns: S_OK ( boolean exists ) a boolean whether it exists or not
              S_ERROR( errStr ) there is a problem with getting the information
    """
    log = self.log.getSubLogger("GFAL2_StorageBase._singleExists")
    log.debug("Determining whether %s exists or not" % path)

    try:
      self.ctx.stat(str(path))  # If path doesn't exist this will raise an error - otherwise path exists
      log.debug("path exists")
      return S_OK(True)
    except gfal2.GError as e:
      if e.code == errno.ENOENT:
        log.debug("Path does not exist")
        return S_OK(False)
      else:
        log.debug("GFAL2_StorageBase.__singleExists: %s" % repr(e))
        return S_ERROR(e.code, e.message)

  def isFile(self, path):
    """ Check if the path provided is a file or not

    :param self: self reference
    :param str: path or list of paths to be checked ( 'srm://...')
    :returns: Failed dict: {path : error message}
              Successful dict: {path : bool}
              S_ERROR in case of argument problems

    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.isFile: checking whether %s path(s) are file(s)." % len(urls))

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleFile(url)

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __isSingleFile(self, path):
    """ Checking if :path: exists and is a file

    :param self: self reference
    :param str path: single path on the storage (srm://...)

    :returns: S_ERROR if there is a fatal error
              S_OK ( boolean) if it is a file or not
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__isSingleFile")
    log.debug("Determining whether %s is a file or not." % path)

    try:
      statInfo = self.ctx.stat(str(path))
      # instead of return S_OK( S_ISDIR( statInfo.st_mode ) ) we use if/else. So we can use the log.
      if S_ISREG(statInfo.st_mode):
        log.debug("Path is a file")
        return S_OK(True)
      else:
        log.debug("Path is not a file")
        return S_OK(False)
    except gfal2.GError as e:
      log.debug("GFAL2_StorageBase.__isSingleFile: %s" % repr(e))
      return S_ERROR(e.code, e.message)

  def putFile(self, path, sourceSize=0):
    """ Put a copy of a local file or a file on another srm storage to a directory on the
        physical storage.

        :param path: dictionary { lfn (srm://...) : localFile }
        :param sourceSize: size of the file in byte. Mandatory for third party copy (WHY ???)
                             Also, this parameter makes it essentially a non bulk operation for
                             third party copy, unless all files have the same size...
        :returns: Successful dict: { path : size }
                  Failed dict: { path : error message }
                  S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}

    for dest_url, src_file in urls.items():
      if not src_file:
        errStr = "GFAL2_StorageBase.putFile: Source file not set. Argument must be a dictionary \
                                             (or a list of a dictionary) {url : local path}"
        self.log.debug(errStr)
        return S_ERROR(errStr)
      res = self.__putSingleFile(src_file, dest_url, sourceSize)

      if res['OK']:
        successful[dest_url] = res['Value']
      else:
        failed[dest_url] = res['Message']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __putSingleFile(self, src_file, dest_url, sourceSize):
    """Put a copy of the local file to the current directory on the
       physical storage

       For gfal2 version 2.7.8 and lower the environment variable GLOBUS_THREAD_MODEL has to be
       set to 'pthread' otherwise dirac-dms-add-file will go in a deadlock. This is fixed with gfal2 2.8.

       :param str src_file: local file to copy
       :param str dest_file: pfn (srm://...)
       :param int sourceSize: size of the source file
       :returns: S_OK( fileSize ) if everything went fine, S_ERROR otherwise
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__putSingleFile")
    log.debug("trying to upload %s to %s" % (src_file, dest_url))

    # in case src_file is not set (this is also done in putFile but some methods directly access this method,
    # so that's why it's checked here one more time
    if not src_file:
      errStr = 'no source defined, please check argument format { destination : localfile }'
      return S_ERROR(errStr)

    # check whether the source is local or on another storage
    if any(src_file.startswith(protocol + ':') for protocol in self.protocolParameters['InputProtocols']):
      src_url = src_file
      if not sourceSize:
        errStr = "For file replication the source file size in bytes must be provided."
        log.debug(errStr, src_file)
        return S_ERROR(errno.EINVAL, errStr)

    # file is local so we can set the protocol and determine source size accordingly
    else:
      if not os.path.isfile(src_file):
        errStr = "The local source file does not exist or is a directory"
        log.debug(errStr, src_file)
        return S_ERROR(errno.ENOENT, errStr)
      if not src_file.startswith('file://'):
        src_url = 'file://%s' % os.path.abspath(src_file)
      sourceSize = getSize(src_file)
      if sourceSize == -1:
        errStr = "Failed to get file size"
        log.debug(errStr, src_file)
        return S_ERROR(DErrno.EFILESIZE, errStr)
      if sourceSize == 0:
        errStr = "Source file size is zero."
        log.debug(errStr, src_file)
        return S_ERROR(DErrno.EFILESIZE, errStr)

    # source is OK, creating the destination folder
    dest_path = os.path.dirname(dest_url)
    res = self._createSingleDirectory(dest_path)
    if not res['OK']:
      log.debug("Failed to create destination folder %s" % dest_path, res['Message'])
      return res

    # folder is created and file exists, setting known copy parameters
    params = self.ctx.transfer_parameters()
    params.timeout = self._estimateTransferTimeout(sourceSize)
    if sourceSize > self.MAX_SINGLE_STREAM_SIZE:
      params.nbstreams = 4
    else:
      params.nbstreams = 1
    params.overwrite = True  # old gfal removed old file first, gfal2 can just overwrite it with this flag set to True
    if self.spaceToken:
      params.dst_spacetoken = self.spaceToken

    if self.checksumType:
      params.set_checksum(gfal2.checksum_mode.both, self.checksumType, '')

    # Params set, copying file now
    try:
      self.ctx.filecopy(params, str(src_url), str(dest_url))
      if self.checksumType:
        # checksum check is done by gfal2
        return S_OK(sourceSize)
      # no checksum check, compare file sizes for verfication
      else:
        res = self.__getSingleFileSize(dest_url)
        # In case of failure, we set destSize to None
        # so that the cleaning of the file happens
        if not res['OK']:
          destSize = None
        else:
          destSize = res['Value']

        log.debug('destSize: %s, sourceSize: %s' % (destSize, sourceSize))
        if destSize == sourceSize:
          return S_OK(destSize)
        else:
          log.debug("Source and destination file size don't match.\
                                                                        Trying to remove destination file")
          res = self.__removeSingleFile(dest_url)
          if not res['OK']:
            log.debug("Failed to remove destination file", res['Message'])
            return res
          errStr = "Source and destination file size don't match. Removed destination file"
          log.debug(errStr, {sourceSize: destSize})
          return S_ERROR("%s srcSize: %s destSize: %s" % (errStr, sourceSize, destSize))
    except gfal2.GError as e:
      # ##
      # extended error message because otherwise we could only guess what the error could be when we copy
      # from another srm to our srm-SE '''
      errStr = "Exception while copying"
      detailMsg = "Failed to copy file %s to destination url %s: [%d] %s" % (src_file, dest_url, e.code, e.message)
      log.debug(errStr, detailMsg)
      return S_ERROR(e.code, detailMsg)

  def getFile(self, path, localPath=False):
    """ Make a local copy of storage :path:

    :param self: self reference
    :param str path: path (or list of paths) on storage (srm://...)
    :param localPath: destination folder. Default is from current directory
    :returns: Successful dict: {path : size}
              Failed dict: {path : errorMessage}
              S_ERROR in case of argument problems
    """

    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.getFile: Trying to download %s files." % len(urls))

    failed = {}
    successful = {}

    for src_url in urls:
      fileName = os.path.basename(src_url)
      dest_file = os.path.join(localPath if localPath else os.getcwd(), fileName)

      res = self._getSingleFile(src_url, dest_file, disableChecksum=self.disableTransferChecksum)

      if not res['OK']:
        failed[src_url] = res['Message']
      else:
        successful[src_url] = res['Value']

    return S_OK({'Failed': failed, 'Successful': successful})

  def _getSingleFile(self, src_url, dest_file, disableChecksum=False):
    """ Copy a storage file :src_url: to a local fs under :dest_file:

    :param self: self reference
    :param str src_url: SE url that is to be copied (srm://...)
    :param str dest_file: local fs path
    :param bool disableChecksum: There are problems with xroot comparing checksums after
                                 copying a file so with this parameter we can disable checksum
                                 checks for xroot
    :returns: S_ERROR( errStr ) in case of an error
              S_OK( size of file ) if copying is successful
    """

    log = self.log.getSubLogger("GFAL2_StorageBase._getSingleFile")

    log.info("Trying to download %s to %s" % (src_url, dest_file))
    if disableChecksum:
      log.warn("checksum calculation disabled for transfers!")

    destDir = os.path.dirname(dest_file)

    if not os.path.exists(destDir):
      log.debug("Local directory does not yet exist. Creating it", destDir)
      try:
        os.makedirs(destDir)
      except OSError as error:
        errStr = "Error while creating the destination folder"
        log.exception(errStr, lException=error)
        return S_ERROR("%s: %s" % (errStr, repr(error)))

    res = self.__getSingleFileSize(src_url)
    if not res['OK']:
      log.debug("Error while determining file size", res['Message'])
      return res

    remoteSize = res['Value']

    # Set gfal2 copy parameters
    # folder is created and file exists, setting known copy parameters
    params = self.ctx.transfer_parameters()
    params.timeout = self._estimateTransferTimeout(remoteSize)
    if remoteSize > self.MAX_SINGLE_STREAM_SIZE:
      params.nbstreams = 4
    else:
      params.nbstreams = 1
    params.overwrite = True  # old gfal removed old file first, gfal2 can just overwrite it with this flag set to True
    if self.spaceToken:
      params.src_spacetoken = self.spaceToken

    useChecksum = bool(self.checksumType and not disableChecksum)
    if useChecksum:
      # params.set_user_defined_checksum(self.checksumType, '')
      params.set_checksum(gfal2.checksum_mode.both, self.checksumType, '')

    # Params set, copying file now
    try:
      # gfal2 needs a protocol to copy local which is 'file:'
      if not dest_file.startswith('file://'):
        dest = 'file://%s' % os.path.abspath(dest_file)
      self.ctx.filecopy(params, str(src_url), str(dest))
      if useChecksum:
        # gfal2 did a checksum check, so we should be good
        return S_OK(remoteSize)
      else:
        # No checksum check was done so we compare file sizes
        destSize = getSize(dest_file)
        if destSize == remoteSize:
          return S_OK(destSize)
        else:
          errStr = "File sizes don't match. Something went wrong. Removing local file %s" % dest_file
          log.debug(errStr, {remoteSize: destSize})
          if os.path.exists(dest_file):
            os.remove(dest_file)
          return S_ERROR(errStr)
    except gfal2.GError as e:
      errStr = 'Could not copy %s to %s, [%d] %s' % (src_url, dest, e.code, e.message)
      log.debug(errStr)
      return S_ERROR(e.code, errStr)

  def removeFile(self, path):
    """ Physically remove the file specified by path

    A non existing file will be considered as successfully removed

    :param str path: path (or list of paths) on storage (srm://...)
    :returns: Successful dict {path : True}
               Failed dict {path : error message}
               S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.removeFile: Attempting to remove %s files" % len(urls))

    failed = {}
    successful = {}

    for url in urls:
      res = self.__removeSingleFile(url)

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __removeSingleFile(self, path):
    """ Physically remove the file specified by path

    :param str path: path on storage (srm://...)
    :returns: S_OK( True )  if the removal was successful (also if file didn't exist in the first place)
              S_ERROR( errStr ) if there was a problem removing the file
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__removeSingleFile")
    log.debug("Attempting to remove single file %s" % path)
    path = str(path)
    try:
      status = self.ctx.unlink(str(path))
      if status == 0:
        log.debug("File successfully removed")
        return S_OK(True)
      elif status < 0:
        errStr = 'return status < 0. Error occurred.'
        return S_ERROR(errStr)
    except gfal2.GError as e:
      # file doesn't exist so operation was successful
      if e.code == errno.ENOENT:
        log.debug("File does not exist.")
        return S_OK(True)
      elif e.code == errno.EISDIR:
        log.debug("Path is a directory.")
        return S_ERROR(errno.EISDIR, errStr)
      else:
        errStr = "Failed to remove file."
        log.debug("Failed to remove file: [%d] %s" % (e.code, e.message))
        return S_ERROR(e.code, repr(e))

  def getFileSize(self, path):
    """Get the physical size of the given file

      :param self: self reference
      :param path: path (or list of path) on storage (pfn : srm://...)
      :returns: Successful dict {path : size}
             Failed dict {path : error message }
             S_ERROR in case of argument problems
    """

    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.getFileSize: Trying to determine file size of %s files" % len(urls))

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleFileSize(url)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __getSingleFileSize(self, path):
    """ Get the physical size of the given file

    :param self: self reference
    :param path: single path on the storage (srm://...)
    :returns: S_OK( filesize ) when successfully determined filesize
              S_ERROR( errStr ) filesize could not be determined
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__getSingleFileSize")
    log.debug("Determining file size of %s" % path)
    path = str(path)
    try:
      statInfo = self.ctx.stat(str(path))  # keeps info like size, mode.

      # If it is not a file
      if not S_ISREG(statInfo.st_mode):
        errStr = "Path is not a file"
        self.log.debug(errStr)
        return S_ERROR(errno.EISDIR, errStr)

      self.log.debug("File size successfully determined %s" % statInfo.st_size)
      return S_OK(long(statInfo.st_size))
    except gfal2.GError as e:
      errStr = "Failed to determine file size."
      self.log.debug(errStr, repr(e))
      return S_ERROR(e.code, "%s: %s" % (errStr, repr(e)))

  def getFileMetadata(self, path):
    """ Get metadata associated to the file(s)

    :param self: self reference
    :param str path: path (or list of paths) on the storage (srm://...)
    :returns: successful dict { path : metadata }
             failed dict { path : error message }
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug('GFAL2_StorageBase.getFileMetadata: trying to read metadata for %s paths' % len(urls))

    failed = {}
    successful = {}

    for url in urls:
      res = self._getSingleFileMetadata(url)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK({'Failed': failed, 'Successful': successful})

  def _getSingleFileMetadata(self, path):
    """  Fetch the metadata associated to the file
      :param self: self reference
      :param path: path (only 1) on storage (srm://...)
      :returns:
          S_OK (MetadataDict) if we could get the metadata
          S_ERROR (errorMsg) if there was a problem getting the metadata or if it is not a file
    """
    self.log.debug('GFAL2_StorageBase._getSingleFileMetadata: trying to read metadata for %s' % path)

    res = self.__getSingleMetadata(path)

    if not res['OK']:
      return res

    metaDict = res['Value']

    if not metaDict['File']:
      errStr = "GFAL2_StorageBase._getSingleFileMetadata: supplied path is not a file"
      self.log.debug(errStr, path)
      return S_ERROR(errno.EISDIR, errStr)

    return S_OK(metaDict)

  def _updateMetadataDict(self, _metadataDict, _attributeDict):
    """ Updating the metadata dictionary with protocol specific attributes
      Dummy implementation
    :param self: self reference
    :param dict: metadataDict we want add the specific attributes to
    :param dict: attributeDict contains the special attributes

    """

    return

  def __getSingleMetadata(self, path):
    """  Fetches the metadata of a single file or directory via gfal2.stat
         and getExtendedAttributes

      :param self: self reference
      :param path: path (only 1) on storage (srm://...)
      :returns:
          S_OK ( MetadataDict ) if we could get the metadata
          S_ERROR ( errorMsg ) if there was a problem getting the metadata
    """
    log = self.log.getSubLogger('GFAL2_StorageBase.__getSingleMetadata')
    log.debug('Reading metadata for %s' % path)

    try:
      statInfo = self.ctx.stat(str(path))
    except gfal2.GError as e:
      errStr = "Failed to retrieve metadata"
      self.log.debug(errStr, repr(e))
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))

    metadataDict = self.__parseStatInfoFromApiOutput(statInfo)

    if metadataDict['File'] and self.checksumType:
      res = self.__getChecksum(path, self.checksumType)
      if not res['OK']:
        log.warn("Could not get checksum:%s" % res['Message'])
      metadataDict['Checksum'] = res.get('Value', '')

    metadataDict = self._addCommonMetadata(metadataDict)

    if self._defaultExtendedAttributes is not None:
      res = self._getExtendedAttributes(path, attributes=self._defaultExtendedAttributes)
      if not res['OK']:
        log.warn("Could not get extended attributes: %s" % res['Message'])
      else:
        attributeDict = res['Value']
        # add extended attributes to the dict if available
        self._updateMetadataDict(metadataDict, attributeDict)

    return S_OK(metadataDict)

  def prestageFile(self, path, lifetime=86400):
    """ Issue prestage request for file(s)

    :param self: self reference
    :param str path: path or list of paths to be prestaged
    :param int lifetime: prestage lifetime in seconds (default 24h)

    :return: succesful dict { url : token }
            failed dict { url : message }
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug('GFAL2_StorageBase.prestageFile: Attempting to issue stage requests for %s file(s).' % len(urls))

    failed = {}
    successful = {}
    for url in urls:
      res = self.__prestageSingleFile(url, lifetime)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']
    return S_OK({'Failed': failed, 'Successful': successful})

  def __prestageSingleFile(self, path, lifetime):
    """ Issue prestage for single file

    :param self: self reference
    :param str path: path to be prestaged
    :param int lifetime: prestage lifetime in seconds (default 24h)

    :return: S_ structure
                            S_OK( token ) ) if status >= 0 (0 - staging is pending, 1 - file is pinned)
                            S_ERROR( errMsg ) ) in case of an error: status -1
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__prestageSingleFile")
    log.debug("Attempting to issue stage request for single file: %s" % path)

    try:
      (status, token) = self.ctx.bring_online(str(path), lifetime, self.stageTimeout, True)
      log.debug("Staging issued - Status: %s" % status)
      if status >= 0:
        return S_OK(token)
      else:
        return S_ERROR('An error occured while issuing prestaging.')
    except gfal2.GError as e:
      errStr = "Error occured while prestaging file"
      log.debug(errStr, "%s %s" % (path, repr(e)))
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))

  def prestageFileStatus(self, path):
    """ Checking the staging status of file(s) on the storage

    :param self: self reference
    :param dict path: dict { url : token }
    :return: succesful dict { url : bool }
            failed dict { url : message }
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug('GFAL2_StorageBase.prestageFileStatus: Checking the staging status for %s file(s).' % len(urls))

    failed = {}
    successful = {}
    for path, token in urls.items():

      res = self.__prestageSingleFileStatus(path, token)
      if not res['OK']:
        failed[path] = res['Message']
      else:
        successful[path] = res['Value']
    return S_OK({'Failed': failed, 'Successful': successful})

  def __prestageSingleFileStatus(self, path, token):
    """ Check prestage status for single file

    :param self: self reference
    :param str path: path to be checked
    :param str token: token of the file

    :return: S_ structure
                            S_OK( True ) if file is staged
                            S_OK( False ) if file is not staged yet
                            S_ERROR( errMsg ) ) in case of an error: status -1
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__prestageSingleFileStatus")
    log.debug("Checking prestage file status for %s" % path)
    # also allow int as token - converting them to strings
    if not isinstance(token, six.string_types):
      token = str(token)

    try:
      self.ctx.set_opt_boolean("BDII", "ENABLE", True)
      status = self.ctx.bring_online_poll(str(path), str(token))

      if status == 0:
        log.debug("File not staged")
        return S_OK(False)
      elif status == 1:
        log.debug("File is staged")
        return S_OK(True)
      else:
        return S_ERROR('An error occured while checking prestage status.')
    except gfal2.GError as e:
      if e.code == errno.EAGAIN:
        log.debug("File not staged")
        return S_OK(False)
      elif e.code == errno.ETIMEDOUT:
        errStr = 'Polling request timed out'
        log.debug(errStr)
        return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))
      else:
        errStr = "Error occured while polling for prestaging file"
        log.debug(errStr, "%s %s" % (path, repr(e)))
        return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))
    finally:
      self.ctx.set_opt_boolean("BDII", "ENABLE", False)

  def pinFile(self, path, lifetime=86400):
    """ Pin a staged file

    :param self: self reference
    :param str path: path of list of paths to be pinned
    :param int lifetime: pinning time in seconds (default 24h)

    :return: successful dict {url : token},
             failed dict {url : message}
             S_ERROR in case of argument problems
    """

    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug('GFAL2_StorageBase.pinFile: Attempting to pin %s file(s).' % len(urls))
    failed = {}
    successful = {}
    for url in urls:
      res = self.__pinSingleFile(url, lifetime)
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']
    return S_OK({'Failed': failed, 'Successful': successful})

  def __pinSingleFile(self, path, lifetime):
    """ Pin a single staged file

    :param self: self reference
    :param str path: path to be pinned
    :param int lifetime: pinning lifetime in seconds (default 24h)

    :return:  S_OK( token ) ) if status >= 0 (0 - staging is pending, 1 - file is pinned). EAGAIN is also considered pending
              S_ERROR( errMsg ) ) in case of an error: status -1
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__pinSingleFile")
    log.debug("Attempting to issue pinning request for single file: %s" % path)

    try:
      self.ctx.set_opt_boolean("BDII", "ENABLE", True)
      status, token = self.ctx.bring_online(str(path), lifetime, self.stageTimeout, True)
      log.debug("Pinning issued - Status: %s" % status)
      if status >= 0:
        return S_OK(token)
      else:
        return S_ERROR('An error occured while issuing pinning.')
    except gfal2.GError as e:
      errStr = "GFAL2_StorageBase.__pinSingleFile: Error occured while pinning file"
      log.debug(errStr, "%s %s" % (path, repr(e)))
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))
    finally:
      self.ctx.set_opt_boolean("BDII", "ENABLE", False)

  def releaseFile(self, path):
    """ Release a pinned file

    :param self: self reference
    :param str path: PFN path { pfn : token } - pfn can be an empty string, then all files that have that same token get released.
                     Just as you can pass an empty token string and a directory as pfn which then releases all the files in the directory
                     an its subdirectories

    :return: successful dict {url : token},
             failed dict {url : message}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.releaseFile: Attempting to release %s file(s)." % len(urls))

    failed = {}
    successful = {}
    for path, token in urls.items():
      res = self.__releaseSingleFile(path, token)

      if not res['OK']:
        failed[path] = res['Message']
      else:
        successful[path] = res['Value']
    return S_OK({'Failed': failed, 'Successful': successful})

  def __releaseSingleFile(self, path, token):
    """ release a single pinned file

    :param self: self reference
    :param str path: path to the file to be released
    :token str token: token belonging to the path

    :returns: S_OK( token ) when releasing was successful, S_ERROR( errMessage ) in case of an error
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__releaseSingleFile")
    log.debug("Attempting to release single file: %s" % path)
    if not isinstance(token, six.string_types):
      token = str(token)
    try:
      self.ctx.set_opt_boolean("BDII", "ENABLE", True)
      status = self.ctx.release(str(path), token)
      if status >= 0:
        return S_OK(token)
      else:
        errStr = "Error occured: Return status < 0"
        log.debug(errStr, "path %s token %s" % (path, token))
        return S_ERROR(errStr)
    except gfal2.GError as e:
      errStr = "Error occured while releasing file"
      self.log.debug(errStr, "%s %s" % (path, repr(e)))
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))
    finally:
      self.ctx.set_opt_boolean("BDII", "ENABLE", False)

  def __getChecksum(self, path, checksumType=None):
    """ Calculate the checksum (ADLER32 by default) of a file on the storage

    :param self: self reference
    :param str path: path to single file on storage (srm://...)
    :returns: S_OK( checksum ) if checksum could be calculated
              S_ERROR( errMsg ) if something failed
    """
    log = self.log.getSubLogger(GFAL2_StorageBase.__getChecksum)
    log.debug('Trying to calculate checksum of file %s' % path)

    if not checksumType:
      errStr = "No checksum type set by the storage element. Can't retrieve checksum"
      log.debug(errStr, path)
      return S_ERROR(errStr)

    res = self.__isSingleFile(path)
    if not res['OK']:
      return res

    try:
      log.debug("using %s checksum" % checksumType)
      fileChecksum = self.ctx.checksum(str(path), checksumType)
      return S_OK(fileChecksum)

    except gfal2.GError as e:
      errStr = 'Failed to calculate checksum.'
      log.debug(errStr, repr(e))
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))

  def __parseStatInfoFromApiOutput(self, statInfo):
    """ Fill the metaDict with the information obtained with gfal2.stat()

        returns metaDict with following keys:

        st_dev: ID of device containing file
        st_ino: file serial number
        st_mode: mode of file
        st_nlink: number of links to the file
        st_uid: user ID of file
        st_gid: group ID of file
        st_size: file size in bytes
        st_atime: time of last access
        st_mtime: time of last modification
        st_ctime: time of last status chage
        File (bool): whether object is a file or not
        Directory (bool): whether object is a directory or not
    """
    metaDict = {}
    # to identify whether statInfo are from file or directory
    metaDict['File'] = S_ISREG(statInfo.st_mode)
    metaDict['Directory'] = S_ISDIR(statInfo.st_mode)

    if metaDict['File']:
      metaDict['FileSerialNumber'] = statInfo.st_ino
      metaDict['Mode'] = statInfo.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)
      metaDict['Links'] = statInfo.st_nlink
      metaDict['UserID'] = statInfo.st_uid
      metaDict['GroupID'] = statInfo.st_gid
      metaDict['Size'] = long(statInfo.st_size)
      metaDict['LastAccess'] = self.__convertTime(statInfo.st_atime) if statInfo.st_atime else 'Never'
      metaDict['ModTime'] = self.__convertTime(statInfo.st_mtime) if statInfo.st_mtime else 'Never'
      metaDict['StatusChange'] = self.__convertTime(statInfo.st_ctime) if statInfo.st_ctime else 'Never'
      metaDict['Executable'] = bool(statInfo.st_mode & S_IXUSR)
      metaDict['Readable'] = bool(statInfo.st_mode & S_IRUSR)
      metaDict['Writeable'] = bool(statInfo.st_mode & S_IWUSR)
    elif metaDict['Directory']:
      metaDict['Mode'] = statInfo.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)

    return metaDict

  @staticmethod
  def __convertTime(time):
    """ Converts unix time to proper time format

    :param self: self reference
    :param time: unix time
    :return: Date in following format: 2014-10-29 14:32:10
    """
    return datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

  def createDirectory(self, path):
    """ Create directory on the storage

    :param self: self reference
    :param str path: path to be created on the storage (pfn : srm://...)
    :returns: Successful dict {path : True }
             Failed dict     {path : error message }
             S_ERROR in case of argument problems
    """
    urls = checkArgumentFormat(path)
    if not urls['OK']:
      return urls
    urls = urls['Value']

    successful = {}
    failed = {}
    self.log.debug("createDirectory: Attempting to create %s directories." % len(urls))
    for url in urls:
      res = self._createSingleDirectory(url)
      if res['OK']:
        successful[url] = True
      else:
        failed[url] = res['Message']
    return S_OK({'Failed': failed, 'Successful': successful})

  def _createSingleDirectory(self, path):
    """ Create directory :path: on the storage
    if no exception is caught the creation was successful. Also if the
    directory already exists we return S_OK().

    :param self: self reference
    :param str path: path to be created (srm://...)

    :returns: S_OK() if creation was successful or directory already exists
             S_ERROR() in case of an error during creation
    """

    log = self.log.getSubLogger("GFAL2_StorageBase._createSingleDirectory")
    try:
      log.debug("Creating %s" % path)
      status = self.ctx.mkdir_rec(str(path), 755)
      if status >= 0:
        log.debug('Successfully created directory')
        return S_OK()
      else:
        errStr = 'Failled to create directory. Status return > 0.'
        log.debug(errStr, status)
        return S_ERROR(errStr)
    except gfal2.GError as e:
      # error: directory already exists
      # Explanations for ECOMM:
      # Because of the way the new DPM DOME flavor works
      # and the poor error handling of Globus works, we might
      # encounter ECOMM when creating an existing directory
      # This will be fixed in the future versions of DPM,
      # but in the meantime, we catch it ourselves.
      if e.code in (errno.EEXIST, errno.ECOMM):
        log.debug("Directory already exists")
        return S_OK()
      # any other error: failed to create directory
      else:
        errStr = "Failed to create directory."
        log.debug(errStr, repr(e))
        return S_ERROR(e.code, repr(e))

  def isDirectory(self, path):
    """ check if the path provided is a directory or not

    :param self: self reference
    :param str: path or list of paths to be checked ( 'srm://...')
    :returns: dict 'Failed' : failed, 'Successful' : succesful
             S_ERROR in case of argument problems

    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.isDirectory: checking whether %s path(s) are directory(ies)." % len(urls))

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleDirectory(url)
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def __isSingleDirectory(self, path):
    """ Checking if :path: exists and is a directory

    :param self: self reference
    :param str path: single path on the storage (srm://...)

    :returns: S_OK ( boolean) if it is a directory or not
              S_ERROR ( errStr ) when there was a problem getting the info
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__isSingleDirectory")
    log.debug("Determining whether %s is a directory or not." % path)
    try:
      statInfo = self.ctx.stat(str(path))
      # instead of return S_OK( S_ISDIR( statInfo.st_mode ) ) we use if/else. So we can use the log.
      if S_ISDIR(statInfo.st_mode):
        log.debug("Path is a directory")
        return S_OK(True)
      else:
        log.debug("Path is not a directory")
        return S_OK(False)
    except gfal2.GError as e:
      errStr = "Failed to determine if path %s is a directory." % path
      log.debug(errStr, repr(e))
      return S_ERROR(e.code, repr(e))

  def listDirectory(self, path):
    """ List the content of the path provided

    :param str path: single or list of paths (srm://...)
    :return: failed  dict {path : message }
            successful dict { path :  {'SubDirs' : subDirs, 'Files' : files} }.
            They keys are the paths, the values are the dictionary 'SubDirs' and 'Files'.
            Each are dictionaries with path as key and metadata as values
            S_ERROR in case of argument problems
    """

    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.listDirectory: Attempting to list %s directories" % len(urls))
    res = self.isDirectory(urls)
    if not res['OK']:
      return res
    successful = {}
    failed = res['Value']['Failed']

    directories = []

    for url, isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories.append(url)
      else:
        errStr = "GFAL2_StorageBase.listDirectory: path is not a directory"
        gLogger.error(errStr, url)
        failed[url] = errStr

    for directory in directories:
      res = self.__listSingleDirectory(directory)
      if not res['OK']:
        failed[directory] = res['Message']
      else:
        successful[directory] = res['Value']

    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def __listSingleDirectory(self, path, internalCall=False):
    """ List the content of the single directory provided

    :param self: self reference
    :param str path: single path on storage (srm://...)
    :param bool internalCall: if we call this method from another internal method we want
                              to work with the full pfn. Used for __getSingleDirectory and
                              __removeSingleDirectory
    :returns: S_ERROR( errStr ) if there is an error
             S_OK( dictionary ): Key: SubDirs and Files
                                 The values of the Files are dictionaries with filename as key and metadata as value
                                 The values of SubDirs are just the dirnames as key and True as value
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__listSingleDirectory")
    log.debug("Attempting to list content of %s" % path)

    try:
      listing = self.ctx.listdir(str(path))

    except gfal2.GError as e:
      errStr = 'Could not list directory content.'
      log.debug(errStr, e.message)
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))

    files = {}
    subDirs = {}

    res = pfnparse(path, srmSpecific=self.srmSpecificParse)
    if not res['OK']:
      return res
    pathDict = res['Value']

    for entry in listing:

      nextEntry = dict(pathDict)
      nextEntry['FileName'] = os.path.join(pathDict['FileName'], entry)
      res = pfnunparse(nextEntry, srmSpecific=self.srmSpecificParse)
      if not res['OK']:
        log.debug("Cannot generate url for next entry", res)
        continue

      nextUrl = res['Value']

      res = self.__getSingleMetadata(nextUrl)
      if res['OK']:
        metadataDict = res['Value']
        if internalCall:
          subPathLFN = nextUrl
        else:
          # If it is not an internal call, we return the LFN
          # We cannot use a simple replace because of the double slash
          # that might be at the start
          basePath = os.path.normpath(self.protocolParameters['Path'])
          startBase = nextEntry['Path'].find(basePath)
          lfnStart = nextEntry['Path'][startBase + len(basePath):]
          if not lfnStart:
            lfnStart = '/'
          subPathLFN = os.path.join(lfnStart, nextEntry['FileName'])

        if metadataDict['Directory']:
          subDirs[subPathLFN] = metadataDict
        elif metadataDict['File']:
          files[subPathLFN] = metadataDict
        else:
          log.debug("Found item which is neither file nor directory", nextUrl)
      else:
        log.debug("Could not stat content", "%s %s" % (nextUrl, res['Message']))

    return S_OK({'SubDirs': subDirs, 'Files': files})

  def getDirectory(self, path, localPath=False):
    """ get a directory from the SE to a local path with all its files and subdirectories

    :param str path: path (or list of paths) on the storage (srm://...)
    :param str localPath: local path where the content of the remote directory will be saved,
                            if not defined it takes current working directory.
    :return: successful and failed dictionaries. The keys are the paths,
             the values are dictionary {'Files': amount of files downloaded, 'Size' : amount of data downloaded}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    log = self.log.getSubLogger("GFAL2_StorageBase.getDirectory")
    log.debug("Attempting to get local copies of %s directories. %s" % (len(urls), urls))

    failed = {}
    successful = {}

    for src_dir in urls:
      res = pfnparse(src_dir, srmSpecific=self.srmSpecificParse)
      if not res['OK']:
        log.debug("cannot parse src_url", res)
        continue
      srcUrlDict = res['Value']
      dirName = srcUrlDict['FileName']

      dest_dir = os.path.join(localPath if localPath else os.getcwd(), dirName)

      res = self.__getSingleDirectory(src_dir, dest_dir)

      if res['OK']:
        if res['Value']['AllGot']:
          log.debug("Successfully got local copy of %s" % src_dir)
          successful[src_dir] = {'Files': res['Value']['Files'], 'Size': res['Value']['Size']}
        else:
          log.debug("Failed to get entire directory.", src_dir)
          failed[src_dir] = {'Files': res['Value']['Files'], 'Size': res['Value']['Size']}
      else:
        log.debug("Completely failed to get local copy of directory.", src_dir)
        failed[src_dir] = {'Files': 0, 'Size': 0}

    return S_OK({'Failed': failed, 'Successful': successful})

  def __getSingleDirectory(self, src_dir, dest_dir):
    """Download a single directory recursively
      :param self: self reference
      :param src_dir : remote directory to download (srm://...)
      :param dest_dir: local destination path
      :returns: S_ERROR if there is a fatal error
              S_OK if we could download something :
                            'AllGot': boolean of whether we could download everything
                            'Files': amount of files received
                            'Size': amount of data received
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__getSingleDirectory")
    log.debug("Attempting to download directory %s at %s" % (src_dir, dest_dir))

    filesReceived = 0
    sizeReceived = 0

    res = self.__isSingleDirectory(src_dir)
    if not res['OK']:
      log.debug("Failed to find the source directory: %s %s" % (res['Message'], src_dir))
      return res

    # res['Value'] is False if it's not a directory
    if not res['Value']:
      errStr = 'The path provided is not a directory'
      log.debug(errStr, src_dir)
      return S_ERROR(errno.ENOTDIR, errStr)

    if not os.path.exists(dest_dir):
      try:
        os.makedirs(dest_dir)
      except OSError as error:
        errStr = 'Error trying to create destination directory %s' % error
        log.exception(errStr, lException=error)
        return S_ERROR(errStr)

    # Get the remote directory contents
    res = self.__listSingleDirectory(src_dir, internalCall=True)
    if not res['OK']:
      return res

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    # Get all the files in the directory
    receivedAllFiles = True
    log.debug('Trying to download the %s files' % len(sFilesDict))
    for sFile in sFilesDict:
      # Getting the last filename
      res = pfnparse(sFile, srmSpecific=self.srmSpecificParse)
      if not res['OK']:
        log.debug("Cannot unparse target file. Skipping", res)
        receivedAllFiles = False
        continue
      filename = res['Value']['FileName']
      # Returns S_OK(fileSize) if successful
      res = self._getSingleFile(sFile, os.path.join(dest_dir, filename), disableChecksum=self.disableTransferChecksum)
      if res['OK']:
        filesReceived += 1
        sizeReceived += res['Value']
      else:
        receivedAllFiles = False

    # recursion to get contents of sub directoryies
    receivedAllDirs = True
    log.debug('Trying to recursively download the %s directories' % len(subDirsDict))
    for subDir in subDirsDict:
      # Getting the last filename
      res = pfnparse(subDir, srmSpecific=self.srmSpecificParse)
      if not res['OK']:
        log.debug("Cannot unparse target dir. Skipping", res)
        receivedAllDirs = False
        continue
      subDirName = res['Value']['FileName']
      localPath = os.path.join(dest_dir, subDirName)
      res = self.__getSingleDirectory(subDir, localPath)

      if not res['OK']:
        receivedAllDirs = False
      else:
        if not res['Value']['AllGot']:
          receivedAllDirs = False
        filesReceived += res['Value']['Files']
        sizeReceived += res['Value']['Size']

    allGot = receivedAllDirs and receivedAllFiles

    resDict = {'AllGot': allGot, 'Files': filesReceived, 'Size': sizeReceived}
    return S_OK(resDict)

  def putDirectory(self, path):
    """ Puts one or more local directories to the physical storage together with all its files

    :param self: self reference
    :param str path: dictionary { srm://... (destination) : localdir (source dir) }
    :return: successful and failed dictionaries. The keys are the paths,
             the values are dictionary {'Files' : amount of files uploaded, 'Size' : amount of data upload }
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    log = self.log.getSubLogger("GFAL2_StorageBase.putDirectory")

    log.debug('Attempting to put %s directories to remote storage' % len(urls))

    successful = {}
    failed = {}
    for destDir, sourceDir in urls.items():
      if not sourceDir:
        errStr = 'No source directory set, make sure the input format is correct { dest. dir : source dir }'
        return S_ERROR(errStr)
      res = self.__putSingleDirectory(sourceDir, destDir)
      if res['OK']:
        if res['Value']['AllPut']:
          log.debug("Successfully put directory to remote storage: %s" % destDir)
          successful[destDir] = {'Files': res['Value']['Files'], 'Size': res['Value']['Size']}
        else:
          log.debug("Failed to put entire directory to remote storage.", destDir)
          failed[destDir] = {'Files': res['Value']['Files'], 'Size': res['Value']['Size']}
      else:
        log.debug("Completely failed to put directory to remote storage.", destDir)
        failed[destDir] = {"Files": 0, "Size": 0}
    return S_OK({"Failed": failed, "Successful": successful})

  def __putSingleDirectory(self, src_directory, dest_directory):
    """ puts one local directory to the physical storage together with all its files and subdirectories
        :param self: self reference
        :param src_directory : the local directory to copy
        :param dest_directory: pfn (srm://...) where to copy
        :returns: S_ERROR if there is a fatal error
                  S_OK if we could upload something :
                                    'AllPut': boolean of whether we could upload everything
                                    'Files': amount of files uploaded
                                    'Size': amount of data uploaded
    """
    log = self.log.getSubLogger("GFAL2_StorageBase.__putSingleDirectory")
    log.debug('Trying to upload %s to %s' % (src_directory, dest_directory))
    filesPut = 0
    sizePut = 0

    if not os.path.isdir(src_directory):
      errStr = 'The supplied source directory does not exist or is not a directory.'
      log.debug(errStr, src_directory)
      return S_ERROR(errno.ENOENT, errStr)

    contents = os.listdir(src_directory)
    allSuccessful = True
    directoryFiles = {}

    res = pfnparse(dest_directory, srmSpecific=self.srmSpecificParse)
    if not res['OK']:
      return res
    destDirParse = res['Value']
    for fileName in contents:
      localPath = os.path.join(src_directory, fileName)

      nextUrlDict = dict(destDirParse)
      nextUrlDict['FileName'] = os.path.join(destDirParse['FileName'], fileName)
      res = pfnunparse(nextUrlDict, srmSpecific=self.srmSpecificParse)
      if not res['OK']:
        log.debug("Cannot unparse next url dict. Skipping", res)
        allSuccessful = False
        continue

      remoteUrl = res['Value']

      # if localPath is not a directory put it to the files dict that needs to be uploaded
      if not os.path.isdir(localPath):
        directoryFiles[remoteUrl] = localPath
      # localPath is another folder, start recursion
      else:
        res = self.__putSingleDirectory(localPath, remoteUrl)
        if not res['OK']:
          log.debug('Failed to put directory to storage. Skipping', res['Message'])
          allSuccessful = False
        else:
          if not res['Value']['AllPut']:
            allSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile(directoryFiles)
      if not res['OK']:
        log.debug('Failed to put files to storage.', res['Message'])
        allSuccessful = False
      else:
        for fileSize in res['Value']['Successful'].values():
          filesPut += 1
          sizePut += fileSize
        if res['Value']['Failed']:
          allSuccessful = False
    return S_OK({'AllPut': allSuccessful, 'Files': filesPut, 'Size': sizePut})

  def removeDirectory(self, path, recursive=False):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.

       :param path: single or list of path (srm://..)
       :param recursive: if True, we recursively delete the subdir
       :return: successful and failed dictionaries. The keys are the pathes,
                 the values are dictionary {'Files': amount of files deleted, 'Size': amount of data deleted}
                 S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    log = self.log.getSubLogger("GFAL2_StorageBase.removeDirectory")
    log.debug("Attempting to remove %s directories." % len(urls))

    successful = {}
    failed = {}

    for url in urls:
      res = self.__removeSingleDirectory(url, recursive)

      if res['OK']:
        if res['Value']['AllRemoved']:
          log.debug("Successfully removed %s" % url)
          successful[url] = {'FilesRemoved': res['Value']['FilesRemoved'], 'SizeRemoved': res['Value']['SizeRemoved']}
        else:
          log.debug("Failed to remove entire directory.", path)
          failed[url] = {'FilesRemoved': res['Value']['FilesRemoved'], 'SizeRemoved': res['Value']['SizeRemoved']}
      else:
        log.debug("Completely failed to remove directory.", url)
        failed[url] = res['Message']  # {'FilesRemoved':0, 'SizeRemoved':0}

    return S_OK({'Failed': failed, 'Successful': successful})

  def __removeSingleDirectory(self, path, recursive=False):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
       :param path: pfn (srm://...) of a directory to remove
       :param recursive : if True, we recursively delete the subdir
       :returns: S_ERROR if there is a fatal error
                  S_OK (statistics dictionary ) if we could upload something :
                                    'AllRemoved': boolean of whether we could delete everything
                                    'FilesRemoved': amount of files deleted
                                    'SizeRemoved': amount of data deleted
    """

    log = self.log.getSubLogger("GFAL2_StorageBase.__removeSingleDirectory")
    filesRemoved = 0
    sizeRemoved = 0

    # Check the remote directory exists

    res = self.__isSingleDirectory(path)

    if not res['OK']:
      return res

    # res['Value'] is True if it is a directory
    if not res['Value']:
      errStr = "The supplied path is not a directory."
      log.debug(errStr, path)
      return S_ERROR(errno.ENOTDIR, errStr)

    # Get the remote directory contents
    res = self.__listSingleDirectory(path, internalCall=True)
    if not res['OK']:
      return res

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    removedAllFiles = True
    removedAllDirs = True

    # if recursive, we call ourselves on all the subdirs
    if recursive:
      # Recursively remove the sub directories
      log.debug("Trying to recursively remove %s folder." % len(subDirsDict))
      for subDirUrl in subDirsDict:
        res = self.__removeSingleDirectory(subDirUrl, recursive)
        if not res['OK']:
          log.debug("Recursive removal failed", res)
          removedAllDirs = False
        else:
          if not res['Value']['AllRemoved']:
            removedAllDirs = False
          filesRemoved += res['Value']['FilesRemoved']
          sizeRemoved += res['Value']['SizeRemoved']

    # Remove all the files in the directory
    log.debug("Trying to remove %s files." % len(sFilesDict))
    for sFile in sFilesDict:
      # Returns S__OK(Filesize) if it worked
      res = self.__removeSingleFile(sFile)

      if res['OK']:
        filesRemoved += 1
        sizeRemoved += sFilesDict[sFile]['Size']
      else:
        removedAllFiles = False

    # Check whether all the operations were successful
    allRemoved = removedAllDirs and removedAllFiles

    # Now we try to remove the directory itself
    # We do it only if :
    # If we wanted to remove recursively and everything was deleted
    # We didn't want to remove recursively but we deleted all the files and there are no subfolders

    if (recursive and allRemoved) or (not recursive and removedAllFiles and not subDirsDict):
      try:
        status = self.ctx.rmdir(str(path))
        if status < 0:
          errStr = "Error occured while removing directory. Status: %s" % status
          log.debug(errStr)
          allRemoved = False
      except gfal2.GError as e:
        # How would that be possible...
        if e.code == errno.ENOENT:
          errStr = 'Files does not exist'
          log.debug(errStr)
        else:
          errStr = 'Failed to remove directory %s' % path
          log.debug(errStr)
          allRemoved = False

    resDict = {'AllRemoved': allRemoved, 'FilesRemoved': filesRemoved, 'SizeRemoved': sizeRemoved}
    return S_OK(resDict)

  def getDirectorySize(self, path):
    """ Get the size of the directory on the storage

      .. warning:: it is not recursive

      :param self: self reference
      :param str path: path or list of paths on storage (srm://...)
      :returns: list of successful and failed dictionaries, both indexed by the path

                  * In the failed, the value is the error message
                  * In the successful the values are dictionaries:

                      * Files : amount of files in the dir
                      * Size : summed up size of all files
                      * subDirs : amount of sub dirs

                  * S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug('GFAL2_StorageBase.getDirectorySize: Attempting to get size of %s directories' % len(urls))

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectorySize(url)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __getSingleDirectorySize(self, path):
    """ Get the size of the directory on the storage
      CAUTION : the size is not recursive, and does not go into subfolders
      :param self: self reference
      :param path: path (single) on storage (srm://...)
      :return: S_ERROR in case of problem
                S_OK (Dictionary) Files : amount of files in the directory
                                  Size : summed up size of files
                                  subDirs : amount of sub directories
    """

    self.log.debug("GFAL2_StorageBase.__getSingleDirectorySize: Attempting to get the size of directory %s" % path)

    res = self.__listSingleDirectory(path)
    if not res['OK']:
      return res

    directorySize = 0
    directoryFiles = 0
    # itervalues returns a list of values of the dictionnary
    for fileDict in res['Value']['Files'].values():
      directorySize += fileDict['Size']
      directoryFiles += 1

    self.log.debug("GFAL2_StorageBase.__getSingleDirectorySize: Successfully obtained size of %s." % path)
    subDirectories = len(res['Value']['SubDirs'])
    return S_OK({'Files': directoryFiles, 'Size': directorySize, 'SubDirs': subDirectories})

  def getDirectoryMetadata(self, path):
    """ Get metadata for the directory(ies) provided

    :param self: self reference
    :param str path: path (or list of paths) on storage (srm://...)
    :returns: Successful dict {path : metadata}
               Failed dict {path : errStr}
               S_ERROR in case of argument problems
    """
    res = checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug("GFAL2_StorageBase.getDirectoryMetadata: Attempting to fetch metadata.")

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectoryMetadata(url)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK({'Failed': failed, 'Successful': successful})

  def __getSingleDirectoryMetadata(self, path):
    """ Fetch the metadata of the provided path

    :param self: self reference
    :param str path: path (only 1) on the storage (srm://...)
    :returns: S_OK( metadataDict ) if we could get the metadata
              S_ERROR( errStr )if there was a problem getting the metadata or path isn't a directory
    """
    self.log.debug("GFAL2_StorageBase.__getSingleDirectoryMetadata: Fetching metadata of directory %s." % path)

    res = self.__getSingleMetadata(path)

    if not res['OK']:
      return res

    metadataDict = res['Value']

    if not metadataDict['Directory']:
      errStr = "GFAL2_StorageBase.__getSingleDirectoryMetadata: Provided path is not a directory."
      self.log.debug(errStr, path)
      return S_ERROR(errno.ENOTDIR, errStr)

    return S_OK(metadataDict)

  def _getExtendedAttributes(self, path, attributes=None):
    """ Get all the available extended attributes of path

    :param self: self reference
    :param str path: path of which we want extended attributes
    :param str list attributes: list of extended attributes we want to receive
    :return: S_OK( attributeDict ) if successful.
            Where the keys of the dict are the attributes and values the respective values
    """

    log = self.log.getSubLogger("GFAL2_StorageBase._getExtendedAttributes")

    log.debug("Checking %s attributes for %s" % (attributes, path))
    attributeDict = {}
    # get all the extended attributes from path
    try:
      if not attributes:
        attributes = self.ctx.listxattr(str(path))

      # get all the respective values of the extended attributes of path
      for attribute in attributes:
        log.debug("Fetching %s" % attribute)
        attributeDict[attribute] = self.ctx.getxattr(str(path), attribute)
      return S_OK(attributeDict)
    # simple error messages, the method that is calling them adds the source of error.
    except gfal2.GError as e:
      errStr = 'Something went wrong while checking for extended attributes.'
      log.debug(errStr, e.message)
      return S_ERROR(e.code, "%s %s" % (errStr, repr(e)))
