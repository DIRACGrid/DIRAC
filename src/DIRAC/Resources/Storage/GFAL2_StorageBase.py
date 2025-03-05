"""

:mod: GFAL2_StorageBase

.. module: python

:synopsis: GFAL2 class from StorageElement using gfal2. Other modules can inherit from this use the gfal2 methods.


Environment Variables:

* DIRAC_GFAL_GRIDFTP_ENABLE_IPV6: this should be exported and set
  to false on pure ipv4 nodes because of the globus bug

* DIRAC_GFAL_GRIDFTP_SESSION_REUSE: This should be exported
  and set to true in server bashrc files for efficiency reasons.
"""

# pylint: disable=arguments-differ

# # imports
import os
import datetime
import errno
from contextlib import contextmanager
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, S_IRWXG, S_IRWXU, S_IRWXO
from urllib import parse

from collections.abc import Iterator
from resource import getrlimit, setrlimit, RLIMIT_NPROC
from typing import cast, Literal, Union, Any, Optional

import gfal2  # pylint: disable=import-error

# # from DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse

# MacOS does not know ECOMM...
try:
    ECOMM = errno.ECOMM
except AttributeError:
    ECOMM = 70


# These two values are used to estimate the timeout
# They are totally empirical and dates from
# SRM and lcg libraries. Maybe we re-evaluate one day :-)
MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10  # 10MB
MIN_BANDWIDTH = 0.5 * (1024 * 1024)  # 0.5 MB/s


@contextmanager
def setGfalSetting(
    ctx: gfal2.Gfal2Context, pluginName: str, optionName: str, optionValue: Union[str, bool, int]
) -> Iterator[None]:
    """This contect manager allows to define gfal2 plugin options.
    The parameters are those required by the ``set_opt_*`` methods of the
    Gfal2 context

    For example:

    .. code-block :: python

        with setGfalSetting(ctx, "HTTP PLUGIN", "OPERATION_TIMEOUT", 30):
            ctx.unlink(path)

    :param ctx: Gfal2 context
    :param str pluginName: Name of the plugin concerned
    :param str optionName: name of the option
    :param optionValue: value of the option

    """

    if isinstance(optionValue, bool):
        _setter = ctx.set_opt_boolean
        _getter = ctx.get_opt_boolean
    elif isinstance(optionValue, int):
        _setter = ctx.set_opt_integer
        _getter = ctx.get_opt_integer
    elif isinstance(optionValue, str):
        _setter = ctx.set_opt_string
        _getter = ctx.get_opt_string
    elif isinstance(optionValue, list):
        _setter = ctx.set_opt_string_list
        _getter = ctx.get_opt_string_list
    else:
        raise NotImplementedError(f"Unknown option type {type(optionValue)}")

    try:
        # raises an error if setting does not exist
        old_value = _getter(pluginName, optionName)
    except gfal2.GError:
        old_value = None
    _setter(pluginName, optionName, optionValue)
    try:
        yield
    finally:
        if old_value is None:
            ctx.remove_opt(pluginName, optionName)
        else:
            _setter(pluginName, optionName, old_value)


class GFAL2_StorageBase(StorageBase):
    """.. class:: GFAL2_StorageBase

    This is the base class for all the gfal2 base protocol plugins
    """

    def __init__(self, storageName: str, parameters: dict[str, str]):
        """c'tor

        :param str storageName: SE name
        :param dict parameters: storage parameters
        """

        StorageBase.__init__(self, storageName, parameters)

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        # Some storages have problem to compute the checksum with xroot while getting the files
        # This allows to disable the checksum calculation while transferring, and then we compare the
        # file size
        self.disableTransferChecksum = True if (parameters.get("DisableChecksum") == "True") else False

        # Different levels or verbosity:
        # gfal2.verbose_level.normal,
        # gfal2.verbose_level.verbose,
        # gfal2.verbose_level.debug,
        # gfal2.verbose_level.trace

        dlevel = self.log.getLevel()
        if dlevel == "DEBUG":
            gfal2.set_verbose(gfal2.verbose_level.trace)

        # # gfal2 API
        # Workaround for https://github.com/xrootd/xrootd/issues/2396
        # xrootd internaly sets nproc, so we save the limit and reset it
        # just after creating the context
        saved_limit = getrlimit(RLIMIT_NPROC)
        self.ctx = gfal2.creat_context()
        new_limit = getrlimit(RLIMIT_NPROC)
        if saved_limit != new_limit:
            setrlimit(RLIMIT_NPROC, saved_limit)

        # by default turn off BDII checks
        self.ctx.set_opt_boolean("BDII", "ENABLE", False)

        # session reuse should only be done on servers
        self.ctx.set_opt_boolean(
            "GRIDFTP PLUGIN",
            "SESSION_REUSE",
            os.environ.get("DIRAC_GFAL_GRIDFTP_SESSION_REUSE", "no").lower() in ["true", "yes"],
        )

        # Enable IPV6 for gsiftp
        self.ctx.set_opt_boolean(
            "GRIDFTP PLUGIN",
            "IPV6",
            os.environ.get("DIRAC_GFAL_GRIDFTP_ENABLE_IPV6", "true").lower() not in ["false", "no"],
        )

        # Disable retrieving the bearer token for every operations.
        # It is only useful for TPC
        self.ctx.set_opt_boolean("HTTP PLUGIN", "RETRIEVE_BEARER_TOKEN", False)

        # spaceToken used for copying from and to the storage element
        self.spaceToken = parameters.get("SpaceToken", "")
        # stageTimeout, default timeout to try and stage/pin a file
        self.stageTimeout = gConfig.getValue("/Resources/StorageElements/StageTimeout", 12 * 60 * 60)
        # gfal2Timeout, amount of time it takes until an operation times out
        self.gfal2Timeout = gConfig.getValue("/Resources/StorageElements/GFAL_Timeout", 100)

        # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
        self.checksumType = gConfig.getValue("/Resources/StorageElements/ChecksumType", "0")

        if self.checksumType == "0":
            self.checksumType = None

        self.log.debug(f"GFAL2_StorageBase: using {self.checksumType} checksum")

        # This is the list of extended metadata to query the server for.
        # It is used by getSingleMetadata.
        # If set to None, No extended metadata will be queried
        # If the list is empty, all of them will be queried
        self._defaultExtendedAttributes: Union[list[str], None] = []

    def _estimateTransferTimeout(self, fileSize: int) -> int:
        """Dark magic to estimate the timeout for a transfer
        The values are set empirically and seem to work fine.
        They were evaluated with gfal1 and SRM.

        :param fileSize: size of the file in bytes we want to transfer

        :return: timeout in seconds
        """

        return int(fileSize / MIN_BANDWIDTH * 4 + 310)

    @convertToReturnValue
    def exists(self, path):
        """Check if the path exists on the storage

        :param str path: path or list of paths to be checked
        :returns: Failed dictionary: {pfn : error message}
                  Successful dictionary: {pfn : bool}
                  S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.exists: Checking the existence of {len(urls)} path(s)")

        successful = {}
        failed = {}

        for url in urls:
            try:
                successful[url] = self.__singleExists(url)
            except Exception as e:
                failed[url] = repr(e)

        resDict = {"Failed": failed, "Successful": successful}
        return resDict

    def __singleExists(self, path: str) -> bool:
        """Check if :path: exists on the storage

        :param str: path to be checked (srm://...)
        :returns: a boolean whether it exists or not
                   there is a problem with getting the information

        :raises:
            gfal2.GError: gfal problem
        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._singleExists")
        log.debug(f"Determining whether {path} exists or not")

        try:
            self.ctx.stat(path)  # If path doesn't exist this will raise an error - otherwise path exists
            log.debug("path exists")
            return True
        except gfal2.GError as e:
            if e.code == errno.ENOENT:
                log.debug("Path does not exist")
                return False
            else:
                raise

    @convertToReturnValue
    def isFile(self, path):
        """Check if the path provided is a file or not

        :param str: path or list of paths to be checked ( 'srm://...')
        :returns: Failed dict: {path : error message}
                  Successful dict: {path : bool}
                  S_ERROR in case of argument problems

        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.isFile: checking whether {len(urls)} path(s) are file(s).")

        successful = {}
        failed = {}

        for url in urls:
            try:
                successful[url] = self._isSingleFile(url)
            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _isSingleFile(self, path: str) -> bool:
        """Checking if :path: exists and is a file

        :param str path: single path on the storage (srm://...)

        :returns: boolean

        :raises:
            gfal2.GError: gfal problem

        """

        statInfo = self.ctx.stat(path)
        return S_ISREG(statInfo.st_mode)

    @convertToReturnValue
    def putFile(self, path, sourceSize: int = 0):
        """Put a copy of a local file or a file on another srm storage to a directory on the
        physical storage.

        :param path: dictionary { lfn (srm://...) : localFile }
        :param sourceSize: size of the file in byte. Mandatory for third party copy (WHY ???)
                             Also, this parameter makes it essentially a non bulk operation for
                             third party copy, unless all files have the same size...
        :returns: Successful dict: { path : size }
                  Failed dict: { path : error message }
                  S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        failed = {}
        successful = {}

        # In principle we only need the bearer token when doing TPC, however it's a
        # bit cumbersome to test, so we always re-enable it when uploading
        with setGfalSetting(self.ctx, "HTTP PLUGIN", "RETRIEVE_BEARER_TOKEN", True):
            for dest_url, src_file in urls.items():
                if not src_file:
                    errStr = "GFAL2_StorageBase.putFile: Source file not set. Argument must be a dictionary \
                                                (or a list of a dictionary) {url : local path}"
                    self.log.debug(errStr)
                    failed[dest_url] = errStr
                    continue

                try:
                    successful[dest_url] = self._putSingleFile(src_file, dest_url, sourceSize)
                except (gfal2.GError, ValueError, RuntimeError) as e:
                    detailMsg = f"Failed to copy {src_file} to {dest_url}: {repr(e)}"
                    self.log.debug("Exception while copying", detailMsg)
                    failed[dest_url] = detailMsg

        return {"Failed": failed, "Successful": successful}

    # CHRIS TODO:
    # if we remove the sourceSize parameter, and accept that if there
    # is no checksum enabled we do not compare the sizes,
    # we can go much faster...
    def _putSingleFile(self, src_file: str, dest_url: str, sourceSize: int) -> int:
        """Put a copy of the local file to the current directory on the
        physical storage

        :param str src_file: local file to copy
        :param str dest_file: pfn (srm://...)
        :param int sourceSize: size of the source file
        :returns: fileSize

        :raises:
            gfal2.GError: gfal problem
            ValueError:
                * input protocol can't be understood
                * missing sourceSize parameter for TPC
            RuntimeError: if file sizes don't match and checksum validation is not enabled
        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._putSingleFile")
        log.debug(f"trying to upload {src_file} to {dest_url}")

        # check whether the source is local or on another storage
        srcProtocol = parse.urlparse(src_file).scheme
        # file is local so we can set the protocol
        if not srcProtocol:
            src_url = f"file://{os.path.abspath(src_file)}"
            if not sourceSize:
                sourceSize = getSize(src_file)
                if sourceSize <= 0:
                    raise OSError("Can't get local file size")

        # This is triggered when the source protocol is not a protocol we can
        # take as source.
        # It should not happen, as the DataManager should filter that.
        elif srcProtocol not in self.protocolParameters["InputProtocols"]:
            raise ValueError(f"{srcProtocol} is not a suitable input protocol")

        # If we do a TPC, we want the file size to be specified
        else:
            src_url = src_file
            if not sourceSize:
                raise ValueError("sourceSize argument is mandatory for TPC copy")

        params = self.ctx.transfer_parameters()
        params.create_parent = True
        params.timeout = self._estimateTransferTimeout(sourceSize)
        if sourceSize > MAX_SINGLE_STREAM_SIZE:
            params.nbstreams = 4
        else:
            params.nbstreams = 1
        params.overwrite = True
        if self.spaceToken:
            params.dst_spacetoken = self.spaceToken

        if self.checksumType:
            params.set_checksum(gfal2.checksum_mode.both, self.checksumType, "")

        # Params set, copying file now
        self.ctx.filecopy(params, src_url, dest_url)
        if self.checksumType:
            # checksum check is done by gfal2
            return sourceSize

        # no checksum check, compare file sizes for verfication
        try:
            destSize = self._getSingleFileSize(dest_url)
            log.debug(f"destSize: {destSize}, sourceSize: {sourceSize}")
            if destSize == sourceSize:
                return destSize
            log.debug(
                "Source and destination file size don't match.\
                                                            Trying to remove destination file"
            )
            raise RuntimeError(
                f"Source and destination file size don't match ({sourceSize} vs {destSize}). Removed destination file"
            )
        except Exception:
            self._removeSingleFile(dest_url)
            raise

    @convertToReturnValue
    def getFile(self, path, localPath=False):
        """Make a local copy of storage :path:

        :param str path: path (or list of paths) on storage (srm://...)
        :param localPath: destination folder. Default is from current directory
        :returns: Successful dict: {path : size}
                  Failed dict: {path : errorMessage}
                  S_ERROR in case of argument problems
        """

        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.getFile: Trying to download {len(urls)} files.")

        failed = {}
        successful = {}

        for src_url in urls:
            fileName = os.path.basename(src_url)
            dest_file = os.path.join(localPath if localPath else os.getcwd(), fileName)

            try:
                successful[src_url] = self._getSingleFile(
                    src_url, dest_file, disableChecksum=self.disableTransferChecksum
                )
            except (gfal2.GError, RuntimeError, OSError) as e:
                failed[src_url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    # TODO CHRIS: ignore size check if checksum not enabled
    def _getSingleFile(self, src_url: str, dest_file: str, disableChecksum: bool = False) -> int:
        """Copy a storage file :src_url: to a local fs under :dest_file:

        :param str src_url: SE url that is to be copied (srm://...)
        :param str dest_file: local fs path
        :param bool disableChecksum: There are problems with xroot comparing checksums after
                                     copying a file so with this parameter we can disable checksum
                                     checks for xroot
        :returns: size of file if copying is successful

        :raises:
            gfal2.GError gfal problem
            RuntimeError: local and remote size different after copy
            TypeError: problem checking remote size
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._getSingleFile")

        log.info(f"Trying to download {src_url} to {dest_file}")
        if disableChecksum:
            log.warn("checksum calculation disabled for transfers!")

        sourceSize = self._getSingleFileSize(src_url)

        # Set gfal2 copy parameters
        # folder is created and file exists, setting known copy parameters
        params = self.ctx.transfer_parameters()
        params.timeout = self._estimateTransferTimeout(sourceSize)
        if sourceSize > MAX_SINGLE_STREAM_SIZE:
            params.nbstreams = 4
        else:
            params.nbstreams = 1
        params.create_parent = True
        params.overwrite = True
        if self.spaceToken:
            params.src_spacetoken = self.spaceToken

        useChecksum = bool(self.checksumType and not disableChecksum)
        if useChecksum:
            params.set_checksum(gfal2.checksum_mode.both, self.checksumType, "")

        # gfal2 needs a protocol to copy local which is 'file:'
        if not dest_file.startswith("file://"):
            dest = f"file://{os.path.abspath(dest_file)}"
        else:
            dest = dest_file

        # We can remove the context manager when https://its.cern.ch/jira/browse/DMC-1371
        # is solved
        # The problem is gfal2 not respecting the parameter timeout
        with setGfalSetting(self.ctx, "HTTP PLUGIN", "OPERATION_TIMEOUT", params.timeout):
            self.ctx.filecopy(params, src_url, dest)

        if useChecksum:
            # gfal2 did a checksum check, so we should be good
            return sourceSize

        # No checksum check was done so we compare file sizes
        localSize = getSize(dest_file)
        if localSize == sourceSize:
            return localSize

        errStr = f"File sizes don't match. Something went wrong. Removing local file {dest_file}"
        log.debug(errStr, {sourceSize: localSize})
        try:
            os.remove(dest_file)
        except Exception:
            pass
        raise RuntimeError(f"Remote and local filesizes don't match: {sourceSize} vs {localSize}")

    @convertToReturnValue
    def removeFile(self, path):
        """Physically remove the file specified by path

        A non existing file will be considered as successfully removed

        :param str path: path (or list of paths) on storage (srm://...)
        :returns: Successful dict {path : True}
                   Failed dict {path : error message}
                   S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.removeFile: Attempting to remove {len(urls)} files")

        failed = {}
        successful = {}

        for url in urls:
            try:
                successful[url] = self._removeSingleFile(url)
            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _removeSingleFile(self, path: str) -> Literal[True]:
        """Physically remove the file specified by path

        :param str path: path on storage (srm://...)
        :returns:  True  if the removal was successful (also if file didn't exist in the first place)

        :raises:
            gfal2.GError: gfal problem
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._removeSingleFile")
        log.debug(f"Attempting to remove single file {path}")
        try:
            self.ctx.unlink(path)
            log.debug("File successfully removed")
            return True
        except gfal2.GError as e:
            # file doesn't exist so operation was successful
            if e.code == errno.ENOENT:
                log.debug("File does not exist.")
                return True
            raise

    @convertToReturnValue
    def getFileSize(self, path):
        """Get the physical size of the given file

        :param path: path (or list of path) on storage (pfn : srm://...)
        :returns: Successful dict {path : size}
               Failed dict {path : error message }
               S_ERROR in case of argument problems
        """

        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.getFileSize: Trying to determine file size of {len(urls)} files")

        failed = {}
        successful = {}

        for url in urls:
            try:
                successful[url] = self._getSingleFileSize(url)
            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _getSingleFileSize(self, path: str) -> int:
        """Get the physical size of the given file

        :param path: single path on the storage (srm://...)
        :returns: filesize when successfully determined filesize

        :raises:
            gfal2.GError gfal2 problem
            TypeError: path is not a file
        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._getSingleFileSize")
        log.debug(f"Determining file size of {path}")

        statInfo = self.ctx.stat(path)  # keeps info like size, mode.

        # If it is not a file
        if not S_ISREG(statInfo.st_mode):
            raise TypeError("Path is not a file")

        return int(statInfo.st_size)

    @convertToReturnValue
    def getFileMetadata(self, path):
        """Get metadata associated to the file(s)

        :param str path: path (or list of paths) on the storage (srm://...)
        :returns: successful dict { path : metadata }
                 failed dict { path : error message }
                 S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.getFileMetadata: trying to read metadata for {len(urls)} paths")

        failed = {}
        successful = {}

        for url in urls:
            try:
                successful[url] = self._getSingleFileMetadata(url)

            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _getSingleFileMetadata(self, path: str) -> dict[str, str]:
        """Fetch the metadata associated to the file
        :param path: path (only 1) on storage (srm://...)
        :returns:MetadataDict) if we could get the metadata


        :raises:
            gfal2.GError for gfal error
            TypeError if the path is not a file
        """
        self.log.debug(f"GFAL2_StorageBase._getSingleFileMetadata: trying to read metadata for {path}")

        metaDict = self._getSingleMetadata(path)

        if not metaDict["File"]:
            errStr = "GFAL2_StorageBase._getSingleFileMetadata: supplied path is not a file"
            self.log.debug(errStr, path)
            raise TypeError(errno.EISDIR, errStr)

        return metaDict

    def _updateMetadataDict(self, _metadataDict: dict[str, Any], _attributeDict: dict[str, Any]) -> None:
        """Updating the metadata dictionary with protocol specific attributes
          Dummy implementation
        :param dict: metadataDict we want add the specific attributes to
        :param dict: attributeDict contains the special attributes

        """

    def _getSingleMetadata(self, path: str) -> dict[str, Any]:
        """Fetches the metadata of a single file or directory via gfal2.stat
           and getExtendedAttributes

        :param path: path (only 1) on storage (srm://...)
        :returns: MetadataDict if we could get the metadata

        :raises:
            gfal2.GError: gfal problem
        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._getSingleMetadata")
        log.debug(f"Reading metadata for {path}")

        statInfo = self.ctx.stat(path)

        metadataDict = self.__parseStatInfoFromApiOutput(statInfo)
        if metadataDict["File"] and self.checksumType:
            try:
                metadataDict["Checksum"] = self.ctx.checksum(path, self.checksumType)
            except gfal2.GError as e:
                log.warn("Could not get checksum", repr(e))

        metadataDict = self._addCommonMetadata(metadataDict)

        if self._defaultExtendedAttributes is not None:
            try:
                # add extended attributes to the dict if available
                attributeDict = self._getExtendedAttributes(path, attributes=self._defaultExtendedAttributes)
                self._updateMetadataDict(metadataDict, attributeDict)
            except gfal2.GError as e:
                log.warn("Could not get extended attributes", repr(e))

        return metadataDict

    @convertToReturnValue
    def prestageFile(self, path, lifetime: int = 86400):
        """Issue prestage request for file(s)

        :param str path: path or list of paths to be prestaged
        :param int lifetime: prestage lifetime in seconds (default 24h)

        :return: succesful dict { url : token }
                failed dict { url : message }
                S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.prestageFile: Attempting to issue stage requests for {len(urls)} file(s).")

        failed = {}
        successful = {}
        for url in urls:
            try:
                successful[url] = self._prestageSingleFile(url, lifetime)
            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _prestageSingleFile(self, path: str, lifetime: int) -> str:
        """Issue prestage for single file

        :param str path: path to be prestaged
        :param int lifetime: prestage lifetime in seconds (default 24h)

        :return: token if status >= 0 (0 - staging is pending, 1 - file is pinned)

        :raises:
            gfal2.GError gfal problems
        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._prestageSingleFile")
        log.debug(f"Attempting to issue stage request for single file: {path}")

        status, token = self.ctx.bring_online(path, lifetime, self.stageTimeout, True)
        log.debug(f"Staging issued - Status: {status}")
        return cast(str, token)

    @convertToReturnValue
    def prestageFileStatus(self, path):
        """Checking the staging status of file(s) on the storage

        :param dict path: dict { url : token }
        :return: succesful dict { url : bool }
                failed dict { url : message }
                S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.prestageFileStatus: Checking the staging status for {len(urls)} file(s).")

        failed = {}
        successful = {}
        for url, token in urls.items():
            try:
                successful[url] = self._prestageSingleFileStatus(url, token)
            except Exception as e:
                failed[url] = repr(e)
        return {"Failed": failed, "Successful": successful}

    def _prestageSingleFileStatus(self, path: str, token: str) -> bool:
        """Check prestage status for single file

        :param str path: path to be checked
        :param str token: token of the file

        :return: bool whether the file is staged or not

        :raises:
            gfal2.GError gfal problem
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._prestageSingleFileStatus")
        log.debug(f"Checking prestage file status for {path}")
        # also allow int as token - converting them to strings
        token = str(token)

        with setGfalSetting(self.ctx, "BDII", "ENABLE", True):
            # 0: not staged
            # 1: staged
            status = self.ctx.bring_online_poll(path, token)

            isStaged = bool(status)
            log.debug(f"File staged: {isStaged}")
            return isStaged

    @convertToReturnValue
    def releaseFile(self, path):
        """Release a pinned file

        :param str path: PFN path { pfn : token } - pfn can be an empty string,
                         then all files that have that same token get released.
                         Just as you can pass an empty token string and a directory
                         as pfn which then releases all the files in the directory
                         an its subdirectories

        :return: successful dict {url : token},
                 failed dict {url : message}
                 S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.releaseFile: Attempting to release {len(urls)} file(s).")

        failed = {}
        successful = {}
        with setGfalSetting(self.ctx, "BDII", "ENABLE", True):
            for url, token in urls.items():
                # token could be an int
                token = str(token)
                try:
                    self.ctx.release(url, token)
                    successful[url] = token
                except gfal2.GError as e:
                    failed[url] = f"Error occured while releasing file {repr(e)}"

        return {"Failed": failed, "Successful": successful}

    def __parseStatInfoFromApiOutput(self, statInfo) -> dict[str, Any]:
        """Fill the metaDict with the information obtained with gfal2.stat()

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
        metaDict: dict[str, Any] = {}
        # to identify whether statInfo are from file or directory
        metaDict["File"] = S_ISREG(statInfo.st_mode)
        metaDict["Directory"] = S_ISDIR(statInfo.st_mode)

        if metaDict["File"]:
            metaDict["FileSerialNumber"] = statInfo.st_ino
            metaDict["Mode"] = statInfo.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)
            metaDict["Links"] = statInfo.st_nlink
            metaDict["UserID"] = statInfo.st_uid
            metaDict["GroupID"] = statInfo.st_gid
            metaDict["Size"] = int(statInfo.st_size)
            metaDict["LastAccess"] = self.__convertTime(statInfo.st_atime) if statInfo.st_atime else "Never"
            metaDict["ModTime"] = self.__convertTime(statInfo.st_mtime) if statInfo.st_mtime else "Never"
            metaDict["StatusChange"] = self.__convertTime(statInfo.st_ctime) if statInfo.st_ctime else "Never"
            metaDict["Executable"] = bool(statInfo.st_mode & S_IXUSR)
            metaDict["Readable"] = bool(statInfo.st_mode & S_IRUSR)
            metaDict["Writeable"] = bool(statInfo.st_mode & S_IWUSR)
        elif metaDict["Directory"]:
            metaDict["Mode"] = statInfo.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)

        return metaDict

    @staticmethod
    def __convertTime(time: int) -> str:
        """Converts unix time to proper time format

        :param time: unix time
        :return: Date in following format: 2014-10-29 14:32:10
        """
        return datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S")

    @convertToReturnValue
    def createDirectory(self, path):
        """Create directory on the storage

        :param str path: path to be created on the storage (pfn : srm://...)
        :returns: Successful dict {path : True }
                 Failed dict     {path : error message }
                 S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        successful = {}
        failed = {}
        self.log.debug(f"createDirectory: Attempting to create {len(urls)} directories.")
        for url in urls:
            try:
                successful[url] = self._createSingleDirectory(url)
            except Exception as e:
                failed[url] = f"Failed to create directory: {repr(e)}"

        return {"Failed": failed, "Successful": successful}

    def _createSingleDirectory(self, path: str) -> Literal[True]:
        """Create directory :path: on the storage
        if no exception is caught the creation was successful. Also if the
        directory already exists we consider it a success.

        :param str path: path to be created (srm://...)

        :returns: return True in case of success

        :raises:
            gfal2.GError for gfal error
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._createSingleDirectory")
        try:
            log.debug(f"Creating {path}")
            self.ctx.mkdir_rec(path, 0o755)

            log.debug("Successfully created directory")
            return True

        except gfal2.GError as e:
            # error: directory already exists
            # Explanations for ECOMM:
            # Because of the way the new DPM DOME flavor works
            # and the poor error handling of Globus works, we might
            # encounter ECOMM when creating an existing directory
            # This will be fixed in the future versions of DPM,
            # but in the meantime, we catch it ourselves.
            if e.code in (errno.EEXIST, ECOMM):
                log.debug("Directory already exists")
                return True
            # any other error: failed to create directory
            raise

    @convertToReturnValue
    def isDirectory(self, path):
        """check if the path provided is a directory or not

        :param str: path or list of paths to be checked ( 'srm://...')
        :returns: dict 'Failed' : failed, 'Successful' : succesful
                 S_ERROR in case of argument problems

        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.isDirectory: checking whether {len(urls)} path(s) are directory(ies).")

        successful = {}
        failed = {}

        for url in urls:
            try:
                successful[url] = self._isSingleDirectory(url)
            except Exception as e:
                failed[url] = f"Failed to determine if path is a directory {repr(e)}"

        return {"Failed": failed, "Successful": successful}

    def _isSingleDirectory(self, path: str) -> bool:
        """Checking if :path: exists and is a directory

        :param str path: single path on the storage (srm://...)

        :returns: boolean if it is a directory or not

        :raises:
            gfal2.GError in case of gfal problem
        """

        return S_ISDIR(self.ctx.stat(path).st_mode)

    @convertToReturnValue
    def listDirectory(self, path):
        """List the content of the path provided

        TODO: add an option if we want or not the metadata, as it is an expensive operation ?

        :param str path: single or list of paths (srm://...)
        :return: failed  dict {path : message }
                successful dict { path :  {'SubDirs' : subDirs, 'Files' : files} }.
                They keys are the paths, the values are the dictionary 'SubDirs' and 'Files'.
                Each are dictionaries with path as key and metadata as values
                S_ERROR in case of argument problems
        """

        urls = returnValueOrRaise(checkArgumentFormat(path))

        successful = {}
        failed = {}

        for directory in urls:
            try:
                successful[directory] = self._listSingleDirectory(directory)
            except Exception as e:
                failed[directory] = f"Failed to list directory: {repr(e)}"

        return {"Failed": failed, "Successful": successful}

    def _listSingleDirectory(self, path: str, internalCall: bool = False) -> dict[str, dict[str, Any]]:
        """List the content of the single directory provided

        :param str path: single path on storage (srm://...)
        :param bool internalCall: if we call this method from another internal method we want
                                  to work with the full pfn. Used for _getSingleDirectory and
                                  _removeSingleDirectory
        :returns:dictionary:
                         Key: SubDirs and Files
                            The values of the Files/SubDirs are dictionaries with filename as key and metadata as value

        :raises:
            gfal2.GError: for Gfal issues
            SErrorException: for pfn unparsing errors

        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._listSingleDirectory")
        log.debug(f"Attempting to list content of {path}")

        listing = self.ctx.listdir(path)

        files = {}
        subDirs = {}

        pathDict = returnValueOrRaise(pfnparse(path, srmSpecific=self.srmSpecificParse))

        # If it is not an internal call
        # We find the LFN of the current directory, and we will just append
        # the filename. Thus we return an LFN to the user.
        # We cannot use a simple replace because of the double slash
        # that might be at the start
        lfnStart = "/"
        if not internalCall:
            basePath = os.path.normpath(self.protocolParameters["Path"])
            startBase = pathDict["Path"].find(basePath)
            lfnStart = pathDict["Path"][startBase + len(basePath) :]
            if not lfnStart:
                lfnStart = "/"

        for entry in listing:
            nextEntry = dict(pathDict)
            nextEntry["FileName"] = os.path.join(pathDict["FileName"], entry)
            nextUrl = returnValueOrRaise(pfnunparse(nextEntry, srmSpecific=self.srmSpecificParse))

            try:
                metadataDict = self._getSingleMetadata(nextUrl)

                # If we are using _listSingleDirectory from another method
                # we want to get the full URL
                # Otherwise, we want the LFN
                if internalCall:
                    subPathLFN = nextUrl
                else:
                    # If it is not an internal call, we return the LFN
                    subPathLFN = os.path.join(lfnStart, nextEntry["FileName"])

                if metadataDict["Directory"]:
                    subDirs[subPathLFN] = metadataDict
                elif metadataDict["File"]:
                    files[subPathLFN] = metadataDict
                else:
                    log.debug("Found item which is neither file nor directory", nextUrl)
            except Exception as e:
                log.debug("Could not stat content", f"{nextUrl} {e}")

        return {"SubDirs": subDirs, "Files": files}

    @convertToReturnValue
    def getDirectory(self, path, localPath: Union[str, None] = None):
        """get a directory from the SE to a local path with all its files and subdirectories

        :param str path: path (or list of paths) on the storage (srm://...)
        :param str localPath: local path where the content of the remote directory will be saved,
                                if not defined it takes current working directory.
        :return: successful and failed dictionaries. The keys are the paths,
                 the values are dictionary {'Files': amount of files downloaded, 'Size' : amount of data downloaded}
                 S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        log = self.log.getLocalSubLogger("GFAL2_StorageBase.getDirectory")
        log.debug(f"Attempting to get local copies of {len(urls)} directories. {urls}")

        failed = {}
        successful = {}

        for src_dir in urls:
            res = pfnparse(src_dir, srmSpecific=self.srmSpecificParse)
            if not res["OK"]:
                log.debug("cannot parse src_url", res)
                continue

            srcUrlDict = res["Value"]
            dirName = srcUrlDict["FileName"]

            dest_dir = os.path.join(localPath if localPath else os.getcwd(), dirName)

            try:
                copyResult = self._getSingleDirectory(src_dir, dest_dir)

                if copyResult["AllGot"]:
                    log.debug(f"Successfully got local copy of {src_dir}")
                    successful[src_dir] = {"Files": copyResult["Files"], "Size": copyResult["Size"]}
                else:
                    log.debug("Failed to get entire directory.", src_dir)
                    failed[src_dir] = {"Files": copyResult["Files"], "Size": copyResult["Size"]}
            except Exception as e:
                log.error("Completely failed to get local copy of directory.", f"{src_dir}, {repr(e)}")
                failed[src_dir] = {"Files": 0, "Size": 0}

        return {"Failed": failed, "Successful": successful}

    def _getSingleDirectory(self, src_dir: str, dest_dir: str) -> dict[str, Union[bool, int]]:
        """Download a single directory recursively
        :param src_dir : remote directory to download (srm://...)
        :param dest_dir: local destination path
        :returns: if we could download something :
                              'AllGot': boolean of whether we could download everything
                              'Files': amount of files received
                              'Size': amount of data received

        :raises:
            gfal2.GError fpr gfal2 problem
            OSError in case the local directory can't be created
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._getSingleDirectory")
        log.debug(f"Attempting to download directory {src_dir} at {dest_dir}")

        filesReceived = 0
        sizeReceived = 0

        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        # Get the remote directory contents
        dirListing = self._listSingleDirectory(src_dir, internalCall=True)

        sFilesDict = dirListing["Files"]
        subDirsDict = dirListing["SubDirs"]

        # Get all the files in the directory
        receivedAllFiles = True
        log.debug(f"Trying to download the {len(sFilesDict)} files")
        for sFile in sFilesDict:
            # Getting the last filename
            res = pfnparse(sFile, srmSpecific=self.srmSpecificParse)

            if not res["OK"]:
                log.debug("Cannot unparse target file. Skipping", res)
                receivedAllFiles = False
                continue

            filename = res["Value"]["FileName"]

            try:
                sizeReceived += self._getSingleFile(
                    sFile, os.path.join(dest_dir, filename), disableChecksum=self.disableTransferChecksum
                )

                filesReceived += 1

            except (gfal2.GError, OSError, RuntimeError):
                receivedAllFiles = False

        # recursion to get contents of sub directoryies
        receivedAllDirs = True

        log.debug(f"Trying to recursively download the {len(subDirsDict)} directories")
        for subDir in subDirsDict:
            # Getting the last filename
            res = pfnparse(subDir, srmSpecific=self.srmSpecificParse)

            if not res["OK"]:
                log.debug("Cannot unparse target dir. Skipping", res)
                receivedAllDirs = False
                continue

            subDirName = res["Value"]["FileName"]
            localPath = os.path.join(dest_dir, subDirName)
            try:
                copyResult = self._getSingleDirectory(subDir, localPath)
                if not copyResult["AllGot"]:
                    receivedAllDirs = False
                filesReceived += copyResult["Files"]
                sizeReceived += copyResult["Size"]

            except Exception:
                receivedAllDirs = False

        allGot = receivedAllDirs and receivedAllFiles

        return {"AllGot": allGot, "Files": filesReceived, "Size": sizeReceived}

    @convertToReturnValue
    def putDirectory(self, path):
        """Puts one or more local directories to the physical storage together with all its files

        :param str path: dictionary { srm://... (destination) : localdir (source dir) }
        :return: successful and failed dictionaries. The keys are the paths,
                 the values are dictionary {'Files' : amount of files uploaded, 'Size' : amount of data upload }
                 S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        log = self.log.getLocalSubLogger("GFAL2_StorageBase.putDirectory")

        log.debug(f"Attempting to put {len(urls)} directories to remote storage")

        successful = {}
        failed = {}
        for destDir, sourceDir in urls.items():
            if not sourceDir:
                errStr = "No source directory set, make sure the input format is correct { dest. dir : source dir }"
                raise Exception(errStr)
            try:
                uploadRes = self._putSingleDirectory(sourceDir, destDir)
                if uploadRes["AllPut"]:
                    log.debug("Successfully put directory to remote storage", destDir)
                    successful[destDir] = uploadRes
                else:
                    log.debug("Failed to put entire directory to remote storage.", destDir)
                    failed[destDir] = uploadRes
            except Exception as e:
                log.debug("Completely failed to put directory to remote storage.", f"{destDir}:{repr(e)}")
                failed[destDir] = {"Files": 0, "Size": 0}
        return {"Failed": failed, "Successful": successful}

    def _putSingleDirectory(self, src_directory: str, dest_directory: str) -> dict[str, Union[bool, int]]:
        """puts one local directory to the physical storage together with all its files and subdirectories
        :param src_directory : the local directory to copy
        :param dest_directory: pfn (srm://...) where to copy
        :returns: if we could upload something :
                                    'AllPut': boolean of whether we could upload everything
                                    'Files': amount of files uploaded
                                    'Size': amount of data uploaded

        :raises:
            gfal2.GError in case of gfal problem
            OSError if the source is not a directory
            SErrorException parsing issues

        """
        log = self.log.getLocalSubLogger("GFAL2_StorageBase._putSingleDirectory")
        log.debug(f"Trying to upload {src_directory} to {dest_directory}")
        filesPut = 0
        sizePut = 0

        if not os.path.isdir(src_directory):
            raise OSError("The supplied source directory does not exist or is not a directory.")

        destDirParse = returnValueOrRaise(pfnparse(dest_directory, srmSpecific=self.srmSpecificParse))

        # This is the remote directory in which we want to copy
        destRootDir = destDirParse["FileName"]

        allSuccessful = True

        # Build a dictionary {remoteURL : localPath}
        directoryFiles = {}

        for root, _, files in os.walk(src_directory):
            # relative path of the root with respect to the src_directory
            relDir = os.path.relpath(root, src_directory)

            for fileName in files:
                # That is the full path of the file localy
                localFilePath = os.path.join(root, fileName)

                # That is the path of the file remotely:
                # <destDir/subfolders we are in/filename>
                # we use normpath because relDir can be '.'
                remoteFilePath = os.path.normpath(os.path.join(destRootDir, relDir, fileName))

                # We do not need a copy of the pfnparse dict as we don't
                # need it anywhere further, so just keep reusing it
                destDirParse["FileName"] = remoteFilePath

                # Make it a URL
                res = pfnunparse(destDirParse, srmSpecific=self.srmSpecificParse)
                if not res["OK"]:
                    log.debug("Cannot unparse next url dict. Skipping", res)
                    allSuccessful = False
                    continue

                remoteUrl = res["Value"]

                directoryFiles[remoteUrl] = localFilePath

        if directoryFiles:
            res = self.putFile(directoryFiles)
            if not res["OK"]:
                log.debug("Failed to put files to storage.", res["Message"])
                allSuccessful = False
            else:
                for fileSize in res["Value"]["Successful"].values():
                    filesPut += 1
                    sizePut += fileSize
                if res["Value"]["Failed"]:
                    allSuccessful = False
        return {"AllPut": allSuccessful, "Files": filesPut, "Size": sizePut}

    @convertToReturnValue
    def removeDirectory(self, path, recursive: bool = False):
        """Remove a directory on the physical storage together with all its files and
        subdirectories.

        :param path: single or list of path (srm://..)
        :param recursive: if True, we recursively delete the subdir
        :return: successful and failed dictionaries. The keys are the pathes,
                  the values are dictionary {'Files': amount of files deleted, 'Size': amount of data deleted}
                  S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        log = self.log.getLocalSubLogger("GFAL2_StorageBase.removeDirectory")
        log.debug(f"Attempting to remove {len(urls)} directories.")

        successful = {}
        failed: dict[str, Union[dict[str, int], str]] = {}

        for url in urls:
            try:
                removalRes = self._removeSingleDirectory(url, recursive)

                if removalRes["AllRemoved"]:
                    log.debug(f"Successfully removed {url}")
                    successful[url] = {
                        "FilesRemoved": removalRes["FilesRemoved"],
                        "SizeRemoved": removalRes["SizeRemoved"],
                    }
                else:
                    log.debug("Failed to remove entire directory.", path)
                    failed[url] = {
                        "FilesRemoved": removalRes["FilesRemoved"],
                        "SizeRemoved": removalRes["SizeRemoved"],
                    }
            except Exception as e:
                log.debug("Completely failed to remove directory.", f"{url}:{repr(e)}")
                failed[url] = repr(e)  # {'FilesRemoved':0, 'SizeRemoved':0}

        return {"Failed": failed, "Successful": successful}

    def _removeSingleDirectory(self, path: str, recursive: bool = False) -> dict[str, Union[bool, int]]:
        """Remove a directory on the physical storage together with all its files and
        subdirectories.
        :param path: pfn (srm://...) of a directory to remove
        :param recursive : if True, we recursively delete the subdir
        :returns: statistics dictionary if we could upload something :
                                     'AllRemoved': boolean of whether we could delete everything
                                     'FilesRemoved': amount of files deleted
                                     'SizeRemoved': amount of data deleted
        """

        log = self.log.getLocalSubLogger("GFAL2_StorageBase._removeSingleDirectory")
        filesRemoved = 0
        sizeRemoved = 0

        # Get the remote directory contents
        dirListing = self._listSingleDirectory(path, internalCall=True)

        sFilesDict = dirListing["Files"]
        subDirsDict = dirListing["SubDirs"]

        removedAllFiles = True
        removedAllDirs = True

        # if recursive, we call ourselves on all the subdirs
        if recursive:
            # Recursively remove the sub directories
            log.debug(f"Trying to recursively remove {len(subDirsDict)} folder.")
            for subDirUrl in subDirsDict:
                try:
                    removeRes = self._removeSingleDirectory(subDirUrl, recursive)
                    if not removeRes["AllRemoved"]:
                        removedAllDirs = False
                    filesRemoved += removeRes["FilesRemoved"]
                    sizeRemoved += removeRes["SizeRemoved"]
                except Exception as e:
                    log.debug("Recursive removal failed", repr(e))
                    removedAllDirs = False

        # Remove all the files in the directory
        log.debug(f"Trying to remove {len(sFilesDict)} files.")

        for sFile, sFileMeta in sFilesDict.items():
            try:
                self._removeSingleFile(sFile)
                filesRemoved += 1
                sizeRemoved += sFileMeta["Size"]
            except gfal2.GError:
                removedAllFiles = False

        # Check whether all the operations were successful
        allRemoved = removedAllDirs and removedAllFiles

        # Now we try to remove the directory itself
        # We do it only if :
        # If we wanted to remove recursively and everything was deleted
        # We didn't want to remove recursively but we deleted all the files and there are no subfolders

        if (recursive and allRemoved) or (not recursive and removedAllFiles and not subDirsDict):
            try:
                self.ctx.rmdir(path)
            except gfal2.GError as e:
                log.debug(f"Failed to remove directory {path}: {repr(e)}")
                allRemoved = False

        return {"AllRemoved": allRemoved, "FilesRemoved": filesRemoved, "SizeRemoved": sizeRemoved}

    @convertToReturnValue
    def getDirectorySize(self, path):
        """Get the size of the directory on the storage

        .. warning:: it is not recursive

        :param str path: path or list of paths on storage (srm://...)
        :returns: list of successful and failed dictionaries, both indexed by the path

                    * In the failed, the value is the error message
                    * In the successful the values are dictionaries:

                        * Files : amount of files in the dir
                        * Size : summed up size of all files
                        * subDirs : amount of sub dirs

                    * S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug(f"GFAL2_StorageBase.getDirectorySize: Attempting to get size of {len(urls)} directories")

        failed = {}
        successful = {}

        for url in urls:
            try:
                successful[url] = self._getSingleDirectorySize(url)

            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _getSingleDirectorySize(self, path: str) -> dict[str, int]:
        """Get the size of the directory on the storage
        CAUTION : the size is not recursive, and does not go into subfolders
        :param path: path (single) on storage (srm://...)
        :return: Dictionary Files : amount of files in the directory
                            Size : summed up size of files
                            subDirs : amount of sub directories

        :raises:
            From _listSingleDirectory
                gfal2.GError: for Gfal issues
                SErrorException: for pfn unparsing errors
        """

        self.log.debug(f"GFAL2_StorageBase._getSingleDirectorySize: Attempting to get the size of directory {path}")

        dirListing = self._listSingleDirectory(path)

        directorySize = 0
        directoryFiles = 0
        # itervalues returns a list of values of the dictionary
        for fileDict in dirListing["Files"].values():
            directorySize += fileDict["Size"]
            directoryFiles += 1

        self.log.debug(f"GFAL2_StorageBase._getSingleDirectorySize: Successfully obtained size of {path}.")
        subDirectories = len(dirListing["SubDirs"])
        return {"Files": directoryFiles, "Size": directorySize, "SubDirs": subDirectories}

    @convertToReturnValue
    def getDirectoryMetadata(self, path):
        """Get metadata for the directory(ies) provided

        :param str path: path (or list of paths) on storage (srm://...)
        :returns: Successful dict {path : metadata}
                   Failed dict {path : errStr}
                   S_ERROR in case of argument problems
        """
        urls = returnValueOrRaise(checkArgumentFormat(path))

        self.log.debug("GFAL2_StorageBase.getDirectoryMetadata: Attempting to fetch metadata.")

        failed = {}
        successful = {}

        for url in urls:
            try:
                successful[url] = self._getSingleDirectoryMetadata(url)
            except Exception as e:
                failed[url] = repr(e)

        return {"Failed": failed, "Successful": successful}

    def _getSingleDirectoryMetadata(self, path: str):
        """Fetch the metadata of the provided path

        :param str path: path (only 1) on the storage (srm://...)
        :returns: metadataDict if we could get the metadata

        :raises:
            gfal2.GError for gfal error
            TypeError if the path is not a directory
        """
        self.log.debug(f"GFAL2_StorageBase._getSingleDirectoryMetadata: Fetching metadata of directory {path}.")

        metadataDict = self._getSingleMetadata(path)

        if not metadataDict["Directory"]:
            errStr = "GFAL2_StorageBase._getSingleDirectoryMetadata: Provided path is not a directory."
            self.log.debug(errStr, path)
            raise TypeError(errno.ENOTDIR, errStr)

        return metadataDict

    def _getExtendedAttributes(self, path: str, attributes: Optional[list[str]] = None) -> dict[str, str]:
        """Get all the available extended attributes of path

        :param str path: path of which we want extended attributes
        :param str list attributes: list of extended attributes we want to receive

        :return: {attribute name: attribute value}

        :raises:
            gfal2.GError for gfal issues
        """

        attributeDict = {}
        # get all the extended attributes from path
        if not attributes:
            attributes = cast(list[str], self.ctx.listxattr(path))

        # get all the respective values of the extended attributes of path
        for attribute in attributes:
            self.log.debug(f"Fetching {attribute}")
            attributeDict[attribute] = self.ctx.getxattr(path, attribute)
        return attributeDict
