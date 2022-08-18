"""
 This is the File StorageClass, only meant to be used localy
 """
import os
import shutil
import errno
import stat

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Adler import fileAdler


class FileStorage(StorageBase):
    """.. class:: FileStorage

    File storage, for local file management
    """

    def __init__(self, storageName, parameters):
        """c'tor

        :param self: self reference
        :param str storageName: SE name
        :param str protocol: protocol to use
        :param str rootdir: base path for vo files
        """

        # # init base class
        StorageBase.__init__(self, storageName, parameters)
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        #     self.log.setLevel( "DEBUG" )

        self.pluginName = "File"
        self.protocol = self.protocolParameters["Protocol"]

    def getURLBase(self, withWSUrl=False):
        return S_OK(self.basePath)

    def constructURLFromLFN(self, lfn, withWSUrl=False):
        """The URL of this file protocol is very special:
        it has no protocol in front, etc. So we overwrite
        the method
        """

        result = self.getURLBase(withWSUrl=withWSUrl)
        if not result["OK"]:
            return result
        urlBase = result["Value"]
        url = os.path.join(urlBase, lfn.lstrip("/"))
        return S_OK(url)

    def exists(self, path):
        """Check if the given path exists.

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: Failed dictionary: {pfn : errorMsg}
                Successful dictionary: {pfn : bool}
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        self.log.debug("FileStorage.exists: Checking the existence of %s path(s)" % len(urls))

        successful = {url: os.path.exists(url) for url in urls}

        resDict = {"Failed": {}, "Successful": successful}
        return S_OK(resDict)

    #############################################################
    #
    # These are the methods for file manipulation
    #

    def isFile(self, path):
        """Check if the given path exists and it is a file

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: Successful dict {path : boolean}
                  Failed dict {path : error message }
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.isFile: Determining whether %s paths are files." % len(urls))
        successful = {}
        failed = {}

        for url in urls:
            if os.path.exists(url):
                successful[url] = os.path.isfile(url)
            else:
                failed[url] = "No such file or directory"

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getFile(self, path, localPath=False):
        """make a local copy of a storage :path:

        :param self: self reference
        :param str path: path  on storage
        :param localPath: if not specified, self.cwd
        :returns: Successful dict {path : size}
                 Failed dict {path : error message }
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.getFile: Trying to download %s files." % len(urls))

        failed = {}
        successful = {}

        if not localPath:
            localPath = os.getcwd()

        for src_url in urls:
            try:
                fileName = os.path.basename(src_url)
                dest_url = os.path.join(localPath, fileName)
                shutil.copy2(src_url, dest_url)

                fileSize = os.path.getsize(dest_url)
                successful[src_url] = fileSize
            except OSError as ose:
                failed[src_url] = str(ose)

        return S_OK({"Failed": failed, "Successful": successful})

    def putFile(self, path, sourceSize=0):
        """Put a copy of the local file to the current directory on the
        physical storage

        :param path: dictionary {pfn  : localFile}
        :param sourceSize: compares the size of the local and remote.
                            You obviously run into trouble if you use a list of path...
                            If the size do not match, remove the remote file
        :returns: Successful dict {path : size}
                 Failed dict {path : error message }
        """

        if not isinstance(path, dict):
            return S_ERROR(
                "FileStorage.putFile: path argument must be a dictionary (or a list of dictionary) { url : local path}"
            )

        failed = {}
        successful = {}

        for dest_url, src_file in path.items():
            try:
                dirname = os.path.dirname(dest_url)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                shutil.copy2(src_file, dest_url)
                fileSize = os.path.getsize(dest_url)
                if sourceSize and (sourceSize != fileSize):
                    try:
                        os.unlink(dest_url)
                    except OSError as _ose:
                        pass
                    failed[dest_url] = "Source and destination file sizes do not match ({} vs {}).".format(
                        sourceSize,
                        fileSize,
                    )
                else:
                    successful[dest_url] = fileSize
            except OSError as ose:
                failed[dest_url] = str(ose)

        return S_OK({"Failed": failed, "Successful": successful})

    def removeFile(self, path):
        """Remove physically the file specified by its path

        A non existing file will be considered as successfully removed.

        :param path: path (or list of path) on storage
        :returns: Successful dict {path : True}
                 Failed dict {path : error message }
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("FileStorage.removeFile: Attempting to remove %s files." % len(urls))

        failed = {}
        successful = {}

        for url in urls:
            try:
                os.unlink(url)
                successful[url] = True
            except OSError as ose:
                # Removing a non existing file is a success
                if ose.errno == errno.ENOENT:
                    successful[url] = True
                else:
                    failed[url] = str(ose)
            except Exception as e:
                failed[url] = str(e)

        return S_OK({"Failed": failed, "Successful": successful})

    @staticmethod
    def __stat(path):
        """Issue a stat call and format it the dirac way, and add the checksum

        :param self: self reference
        :param path: path on the storage
        :returns: Successful S_OK(metadataDict) or S_ERROR
        """
        try:
            statInfo = os.stat(path)
            metadataDict = {}

            metadataDict["ModTime"] = statInfo.st_mtime
            metadataDict["Size"] = statInfo.st_size
            metadataDict["Mode"] = stat.S_IMODE(statInfo.st_mode)
            metadataDict["Directory"] = bool(stat.S_ISDIR(statInfo.st_mode))
            isFile = bool(stat.S_ISREG(statInfo.st_mode))
            metadataDict["File"] = isFile

            cks = ""
            if isFile:
                cks = fileAdler(path)

            metadataDict["Checksum"] = cks if cks else ""

            # FIXME: only here for compatibility with SRM until multi protocol is properly handled
            metadataDict["Cached"] = 1
            metadataDict["Migrated"] = 0
            metadataDict["Lost"] = 0
            metadataDict["Unavailable"] = 0

            metadataDict = FileStorage._addCommonMetadata(metadataDict)

        except OSError as ose:
            return S_ERROR(str(ose))

        return S_OK(metadataDict)

    def getFileMetadata(self, path):
        """Get metadata associated to the file(s)

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: Successful dict {path : metadata}
           Failed dict {path : error message }
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        failed = {}
        successful = {}
        for url in urls:
            res = self.__stat(url)
            if not res["OK"]:
                failed[url] = res["Message"]
            elif not res["Value"]["File"]:
                failed[url] = os.strerror(errno.EISDIR)
            else:
                successful[url] = res["Value"]

        return S_OK({"Failed": failed, "Successful": successful})

    def getFileSize(self, path):
        """Get the physical size of the given file

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: Successful dict {path : size}
               Failed dict {path : error message }
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        failed = {}
        successful = {}

        for url in urls:
            try:
                # We check the filesize first because if it does not exist
                # it raises an exception, while os.path.isfile just return False
                filesize = os.path.getsize(url)
                if os.path.isfile(url):
                    successful[url] = filesize
                else:
                    failed[url] = os.strerror(errno.EISDIR)
            except OSError as ose:
                failed[url] = str(ose)

        return S_OK({"Failed": failed, "Successful": successful})

    #############################################################
    #
    # These are the methods for directory manipulation
    #

    def isDirectory(self, path):
        """Check if the given path exists and it is a directory

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: Successful dict {path : boolean}
                  Failed dict {path : error message }
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        successful = {}
        failed = {}

        for url in urls:
            if os.path.exists(url):
                successful[url] = os.path.isdir(url)
            else:
                failed[url] = os.strerror(errno.ENOENT)

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    @staticmethod
    def __copyDirectory(src_dir, dest_dir):
        """Copy a whole tree, and return the number of files and the size copied

        :raise OSError:

        :param src_dir: source folder
        :param dest_dir: destination folder


        :return: dictionary with 'Files' and 'Size' as key
        """

        shutil.copytree(src_dir, dest_dir)
        nbOfFiles = 0
        totalSize = 0
        for root, _dirs, files in os.walk(src_dir):
            nbOfFiles += len(files)
            totalSize += sum(os.path.getsize(os.path.join(root, fn)) for fn in files)

        return {"Files": nbOfFiles, "Size": totalSize}

    def getDirectory(self, path, localPath=False):
        """Get locally a directory from the physical storage together with all its
        files and subdirectories.

        :param path: path (or list of path) on storage
        :param localPath: local path where to store what is downloaded
        :return: successful and failed dictionaries. The keys are the pathes,
                the values are dictionary {'Files': amount of files downloaded, 'Size': amount of data downloaded}
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.getDirectory: Attempting to get local copies of %s directories." % len(urls))

        failed = {}
        successful = {}

        if not localPath:
            localPath = os.getcwd()

        for src_dir in urls:
            try:
                dirName = os.path.basename(src_dir)
                dest_dir = f"{localPath}/{dirName}"
                successful[src_dir] = self.__copyDirectory(src_dir, dest_dir)

            except OSError:
                failed[src_dir] = {"Files": 0, "Size": 0}

        return S_OK({"Failed": failed, "Successful": successful})

    def putDirectory(self, path):
        """puts a or several local directory to the physical storage together with all its files and subdirectories

        :param self: self reference
        :param dict path: dictionary {url : local dir}
        :return: successful and failed dictionaries. The keys are the pathes,
             the values are dictionary {'Files': amount of files uploaded, 'Size': amount of data uploaded}
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        successful = {}
        failed = {}

        for destDir, sourceDir in urls.items():
            try:
                successful[destDir] = self.__copyDirectory(sourceDir, destDir)
            except OSError:
                failed[destDir] = {"Files": 0, "Size": 0}

        return S_OK({"Failed": failed, "Successful": successful})

    def createDirectory(self, path):
        """Make a/several new directory on the physical storage
            This method creates all the intermediate directory

        :param self: self reference
        :param str path: path (or list of path) on storage
        :returns: Successful dict {path : True}
             Failed dict {path : error message }
        """
        urls = checkArgumentFormat(path)
        if not urls["OK"]:
            return urls
        urls = urls["Value"]

        successful = {}
        failed = {}
        self.log.debug("FileStorage.createDirectory: Attempting to create %s directories." % len(urls))

        for url in urls:
            try:
                # Create all the path
                os.makedirs(url)
                successful[url] = True
            except OSError as ose:
                if ose.errno == errno.EEXIST:
                    successful[url] = True
                    continue
                failed[url] = str(ose)

        return S_OK({"Failed": failed, "Successful": successful})

    def removeDirectory(self, path, recursive=False):
        """Remove a directory on the physical storage together with all its files and
         subdirectories.

         :param path: single or list of path
         :param bool recursive: if True, we recursively delete the subdir
         :return: successful and failed dictionaries. The keys are the pathes,
               the values are dictionary {'Files': amount of files deleted, 'Size': amount of data deleted}

        .. Note:: It is known that if recursive is False, the removal of a non existing directory is successful,
              while it is failed for recursive = True. That's stupid, but well... I guess I have to keep the interface
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.removeDirectory: Attempting to remove %s directories." % len(urls))

        successful = {}
        failed = {}

        for url in urls:
            if recursive:
                nbOfFiles = 0
                totalSize = 0
                # Calculate the original size
                for root, _dirs, files in os.walk(url):
                    nbOfFiles += len(files)
                    totalSize += sum(os.path.getsize(os.path.join(root, fn)) for fn in files)
                try:
                    shutil.rmtree(url)
                    successful[url] = {"FilesRemoved": nbOfFiles, "SizeRemoved": totalSize}
                except OSError as ose:
                    # if the directory does not exist, then the numbers are already correct, no need to re do
                    # the walk
                    if ose.errno != errno.ENOENT:
                        # If we only removed partially, check how much was removed
                        leftFiles = 0
                        leftSize = 0
                        for root, _dirs, files in os.walk(url):
                            leftFiles += len(files)
                            leftSize += sum(os.path.getsize(os.path.join(root, fn)) for fn in files)
                        nbOfFiles -= leftFiles
                        totalSize -= leftSize
                    failed[url] = {"FilesRemoved": nbOfFiles, "SizeRemoved": totalSize}
            # If no recursive
            else:
                try:
                    # Delete all the files
                    for child in os.listdir(url):
                        fullpath = os.path.join(url, child)
                        if os.path.isfile(fullpath):
                            os.unlink(fullpath)
                    successful[url] = True
                except OSError as ose:
                    # If we get as exception that the directory does not exist
                    # (it can only be the directory), then success
                    if ose.errno == errno.ENOENT:
                        successful[url] = True
                    else:
                        failed[url] = str(ose)

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def listDirectory(self, path):
        """List the supplied path

         .. warning:: It is not recursive!

        :param path: single or list of url
        :return: successful and failed dictionaries. The keys are the pathes,
              the values are dictionary 'SubDirs' and 'Files'. Each are dictionaries with
              path as key and metadata as values (for Files only, SubDirs has just True as value)
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.listDirectory: Attempting to list %s directories." % len(urls))

        successful = {}
        failed = {}

        for url in urls:
            try:
                dirs = {}
                files = {}
                # os.listdir returns files and directories together
                for child in os.listdir(url):
                    fullpath = os.path.join(url, child)
                    lfnPath = os.path.join("/", os.path.relpath(fullpath, self.basePath))
                    res = self.__stat(fullpath)
                    if not res["OK"]:
                        failed[lfnPath] = res["Message"]
                        continue

                    if os.path.isfile(fullpath):
                        files[lfnPath] = res["Value"]
                    else:
                        dirs[lfnPath] = res["Value"]

                successful[url] = {"SubDirs": dirs, "Files": files}
            except OSError as ose:
                failed[url] = str(ose)

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getDirectoryMetadata(self, path):
        """Get metadata associated to the directory(ies)

        :param self: self reference
        :param path: url (or list of urls) on storage
        :returns: Successful dict {path : metadata}
                 Failed dict {path : error message }
        """

        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        failed = {}
        successful = {}
        for url in urls:
            res = self.__stat(url)
            if not res["OK"]:
                failed[url] = res["Message"]
            elif not res["Value"]["Directory"]:
                failed[url] = os.strerror(errno.ENOTDIR)
            else:
                successful[url] = res["Value"]

        return S_OK({"Failed": failed, "Successful": successful})

    def getDirectorySize(self, path):
        """Get the size of the directory on the storage

        .. warning:: the size is not recursive, and does not go into subfolders

        :param self: self reference
        :param path: path (or list of path) on storage
        :returns: list of successfull and failed dictionary, both indexed by the path

                  * In the failed, the value is the error message
                  * In the successful the values are dictionnaries :

                      * Files : amount of files in the directory
                      * Size : summed up size of files
                      * subDirs : amount of sub directories
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("FileStorage.getDirectorySize: Attempting to get size of %s directories." % len(urls))

        failed = {}
        successful = {}

        for url in urls:
            try:
                totSize = 0
                nbOfFiles = 0
                nbOfSubDirs = 0

                for filename in os.listdir(url):
                    fullPath = os.path.join(url, filename)

                    if os.path.isfile(fullPath):
                        nbOfFiles += 1
                        totSize += os.path.getsize(fullPath)
                    else:
                        nbOfSubDirs += 1

                successful[url] = {"Files": nbOfFiles, "Size": totSize, "SubDirs": nbOfSubDirs}
            except OSError as ose:
                failed[url] = str(ose)

        return S_OK({"Failed": failed, "Successful": successful})
