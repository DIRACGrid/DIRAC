""" This is the RFIO StorageClass
"""
import re
import os
import time


from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize


class RFIOStorage(StorageBase):

    _INPUT_PROTOCOLS = ["file", "rfio"]
    _OUTPUT_PROTOCOLS = ["rfio"]

    def __init__(self, storageName, parameters):

        StorageBase.__init__(self, storageName, parameters)
        self.spaceToken = self.protocolParameters["SpaceToken"]

        self.isok = True

        self.pluginName = "RFIO"

        self.timeout = 100
        self.long_timeout = 600

    #############################################################
    #
    # These are the methods for manipulating the client
    #

    def getName(self):
        """The name with which the storage was instantiated"""
        return S_OK(self.name)

    #############################################################
    #
    # These are the methods for file manipulation
    #

    def exists(self, path):
        """Check if the given path exists. The 'path' variable can be a string or a list of strings."""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.exists: Determining the existance of %s files." % len(urls))
        comm = "nsls -d"
        for url in urls:
            comm = f" {comm} {url}"
        res = shellCall(self.timeout, comm)
        successful = {}
        failed = {}
        if res["OK"]:
            returncode, stdout, stderr = res["Value"]
            if returncode in [0, 1]:
                for line in stdout.splitlines():
                    url = line.strip()
                    successful[url] = True
                for line in stderr.splitlines():
                    pfn, _ = line.split(": ")
                    url = pfn.strip()
                    successful[url] = False
            else:
                errStr = "RFIOStorage.exists: Completely failed to determine the existance files."
                gLogger.error(errStr, f"{self.name} {stderr}")
                return S_ERROR(errStr)
        else:
            errStr = "RFIOStorage.exists: Completely failed to determine the existance files."
            gLogger.error(errStr, "{} {}".format(self.name, res["Message"]))
            return S_ERROR(errStr)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def isFile(self, path):
        """Check if the given path exists and it is a file"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.isFile: Determining whether %s paths are files." % len(urls))
        successful = {}
        failed = {}
        comm = "nsls -ld"
        for url in urls:
            comm = f" {comm} {url}"
        res = shellCall(self.timeout, comm)
        if not res["OK"]:
            return res
        returncode, stdout, stderr = res["Value"]
        if returncode in [0, 1]:
            for line in stdout.splitlines():
                permissions, _subdirs, _owner, _group, _size, _month, _date, _timeYear, pfn = line.split()
                if permissions[0] != "d":
                    successful[pfn] = True
                else:
                    successful[pfn] = False
            for line in stderr.splitlines():
                pfn, error = line.split(": ")
                url = pfn.strip()
                failed[url] = error
        else:
            errStr = "RFIOStorage.isFile: Completely failed to determine whether path is file."
            gLogger.error(errStr, f"{self.name} {stderr}")
            return S_ERROR(errStr)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __getPathMetadata(self, urls):
        gLogger.debug("RFIOStorage.__getPathMetadata: Attempting to get metadata for %s paths." % (len(urls)))
        comm = "nsls -ld"
        for url in urls:
            comm = f" {comm} {url}"
        res = shellCall(self.timeout, comm)
        successful = {}
        failed = {}
        if not res["OK"]:
            errStr = "RFIOStorage.__getPathMetadata: Completely failed to get path metadata."
            gLogger.error(errStr, res["Message"])
            return S_ERROR(errStr)
        else:
            returncode, stdout, stderr = res["Value"]
            if returncode not in [0, 1]:
                errStr = "RFIOStorage.__getPathMetadata: failed to perform nsls."
                gLogger.error(errStr, stderr)
            else:
                for line in stdout.splitlines():
                    permissions, subdirs, owner, group, size, month, date, timeYear, pfn = line.split()
                    successful[pfn] = {}
                    if permissions[0] == "d":
                        successful[pfn]["Type"] = "Directory"
                    else:
                        successful[pfn]["Type"] = "File"
                    successful[pfn]["Mode"] = self.__permissionsToInt(permissions)
                    successful[pfn]["NbSubDirs"] = subdirs
                    successful[pfn]["Owner"] = owner
                    successful[pfn]["Group"] = group
                    successful[pfn]["Size"] = int(size)
                    successful[pfn]["Month"] = month
                    successful[pfn]["Date"] = date
                    successful[pfn]["Year"] = timeYear
                for line in stderr.splitlines():
                    pfn, error = line.split(": ")
                    url = pfn.strip()
                    failed[url] = error
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __permissionsToInt(self, permissions):
        mode = permissions[1:]
        return sum(pow(2, 8 - i) * int(mode[i] != "-") for i in range(0, 9))

    def __getFileMetadata(self, urls):
        gLogger.debug(
            "RFIOStorage.__getPathMetadata: Attempting to get additional metadata for %s files." % (len(urls))
        )
        # Check whether the files that exist are staged
        comm = "stager_qry -S %s" % self.spaceToken
        successful = {}
        for pfn in urls:
            successful[pfn] = {}
            comm = f"{comm} -M {pfn}"
        res = shellCall(self.timeout, comm)
        if not res["OK"]:
            errStr = "RFIOStorage.__getFileMetadata: Completely failed to get cached status."
            gLogger.error(errStr, res["Message"])
            return S_ERROR(errStr)
        else:
            _returncode, stdout, _stderr = res["Value"]
            for line in stdout.splitlines():
                pfn = line.split()[0]
                status = line.split()[-1]
                if status in ["STAGED", "CANBEMIGR"]:
                    successful[pfn]["Cached"] = True
        for pfn in urls:
            if "Cached" not in successful[pfn]:
                successful[pfn]["Cached"] = False

        # Now for the files that exist get the tape segment (i.e. whether they have been migrated) and related checksum
        comm = "nsls -lT --checksum"
        for pfn in urls:
            comm = f"{comm} {pfn}"
        res = shellCall(self.timeout, comm)
        if not res["OK"]:
            errStr = "RFIOStorage.__getFileMetadata: Completely failed to get migration status."
            gLogger.error(errStr, res["Message"])
            return S_ERROR(errStr)
        else:
            _returncode, stdout, _stderr = res["Value"]
            for line in stdout.splitlines():
                pfn = line.split()[-1]
                checksum = line.split()[-2]
                successful[pfn]["Migrated"] = True
                successful[pfn]["Checksum"] = checksum
        for pfn in urls:
            if "Migrated" not in successful[pfn]:
                successful[pfn]["Migrated"] = False

        # Update all the metadata with the common one
        for lfn in successful:
            successful[lfn] = self._addCommonMetadata(successful[lfn])

        resDict = {"Failed": {}, "Successful": successful}
        return S_OK(resDict)

    def getFile(self, path, localPath=False):
        """Get a local copy in the current directory of a physical file specified by its path"""
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        failed = {}
        successful = {}
        for src_url in urls.keys():
            fileName = os.path.basename(src_url)
            if localPath:
                dest_file = f"{localPath}/{fileName}"
            else:
                dest_file = f"{os.getcwd()}/{fileName}"
            res = self.__getFile(src_url, dest_file)
            if res["OK"]:
                successful[src_url] = res["Value"]
            else:
                failed[src_url] = res["Message"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __getFile(self, src_url, dest_file):
        """Get a local copy in the current directory of a physical file specified by its path"""
        if not os.path.exists(os.path.dirname(dest_file)):
            os.makedirs(os.path.dirname(dest_file))
        if os.path.exists(dest_file):
            gLogger.debug("RFIOStorage.getFile: Local file already exists %s. Removing..." % dest_file)
            os.remove(dest_file)
        res = self.__executeOperation(src_url, "getFileSize")
        if not res["OK"]:
            return S_ERROR(res["Message"])
        remoteSize = res["Value"]
        MIN_BANDWIDTH = 1024 * 100  # 100 KB/s
        timeout = int(remoteSize / MIN_BANDWIDTH + 300)
        gLogger.debug(f"RFIOStorage.getFile: Executing transfer of {src_url} to {dest_file}")
        comm = f"rfcp {src_url} {dest_file}"
        res = shellCall(timeout, comm)
        if res["OK"]:
            returncode, _stdout, stderr = res["Value"]
            if returncode == 0:
                gLogger.debug("RFIOStorage.__getFile: Got file from storage, performing post transfer check.")
                localSize = getSize(dest_file)
                if localSize == remoteSize:
                    gLogger.debug("RFIOStorage.getFile: Post transfer check successful.")
                    return S_OK(localSize)
                errorMessage = "RFIOStorage.__getFile: Source and destination file sizes do not match."
                gLogger.error(errorMessage, src_url)
            else:
                errStr = "RFIOStorage.__getFile: Failed to get local copy of file."
                gLogger.error(errStr, stderr)
                errorMessage = f"{errStr} {stderr}"
        else:
            errStr = "RFIOStorage.__getFile: Failed to get local copy of file."
            gLogger.error(errStr, res["Message"])
            errorMessage = "{} {}".format(errStr, res["Message"])
        if os.path.exists(dest_file):
            gLogger.debug("RFIOStorage.getFile: Removing local file %s." % dest_file)
            os.remove(dest_file)
        return S_ERROR(errorMessage)

    def putFile(self, path, sourceSize=0):
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        failed = {}
        successful = {}
        for dest_url, src_file in urls.items():
            res = self.__executeOperation(os.path.dirname(dest_url), "createDirectory")
            if not res["OK"]:
                failed[dest_url] = res["Message"]
            else:
                res = self.__putFile(src_file, dest_url, sourceSize)
                if res["OK"]:
                    successful[dest_url] = res["Value"]
                else:
                    failed[dest_url] = res["Message"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __putFile(self, src_file, dest_url, sourceSize):
        """Put a copy of the local file to the current directory on the physical storage"""
        # Pre-transfer check
        res = self.__executeOperation(dest_url, "exists")
        if not res["OK"]:
            gLogger.debug("RFIOStorage.__putFile: Failed to find pre-existance of destination file.")
            return res
        if res["Value"]:
            res = self.__executeOperation(dest_url, "removeFile")
            if not res["OK"]:
                gLogger.debug("RFIOStorage.__putFile: Failed to remove remote file %s." % dest_url)
            else:
                gLogger.debug("RFIOStorage.__putFile: Removed remote file %s." % dest_url)
        if not os.path.exists(src_file):
            errStr = "RFIOStorage.__putFile: The source local file does not exist."
            gLogger.error(errStr, src_file)
            return S_ERROR(errStr)
        sourceSize = getSize(src_file)
        if sourceSize == -1:
            errStr = "RFIOStorage.__putFile: Failed to get file size."
            gLogger.error(errStr, src_file)
            return S_ERROR(errStr)

        res = self.__getTransportURL(dest_url)
        if not res["OK"]:
            gLogger.debug("RFIOStorage.__putFile: Failed to get transport URL for file.")
            return res
        turl = res["Value"]

        MIN_BANDWIDTH = 1024 * 100  # 100 KB/s
        timeout = sourceSize / MIN_BANDWIDTH + 300
        gLogger.debug(f"RFIOStorage.putFile: Executing transfer of {src_file} to {turl}")
        comm = f"rfcp {src_file} '{turl}'"
        res = shellCall(timeout, comm)
        if res["OK"]:
            returncode, _stdout, stderr = res["Value"]
            if returncode == 0:
                gLogger.debug("RFIOStorage.putFile: Put file to storage, performing post transfer check.")
                res = self.__executeOperation(dest_url, "getFileSize")
                if res["OK"]:
                    destinationSize = res["Value"]
                    if sourceSize == destinationSize:
                        gLogger.debug("RFIOStorage.__putFile: Post transfer check successful.")
                        return S_OK(destinationSize)
                errorMessage = "RFIOStorage.__putFile: Source and destination file sizes do not match."
                gLogger.error(errorMessage, dest_url)
            else:
                errStr = "RFIOStorage.__putFile: Failed to put file to remote storage."
                gLogger.error(errStr, stderr)
                errorMessage = f"{errStr} {stderr}"
        else:
            errStr = "RFIOStorage.__putFile: Failed to put file to remote storage."
            gLogger.error(errStr, res["Message"])
            errorMessage = "{} {}".format(errStr, res["Message"])
        res = self.__executeOperation(dest_url, "removeFile")
        if res["OK"]:
            gLogger.debug("RFIOStorage.__putFile: Removed remote file remnant %s." % dest_url)
        else:
            gLogger.debug("RFIOStorage.__putFile: Unable to remove remote file remnant %s." % dest_url)
        return S_ERROR(errorMessage)

    def removeFile(self, path):
        """Remove physically the file specified by its path"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        successful = {}
        failed = {}
        listOfLists = breakListIntoChunks(urls, 100)
        for urls in listOfLists:
            gLogger.debug("RFIOStorage.removeFile: Attempting to remove %s files." % len(urls))
            comm = "stager_rm -S %s" % self.spaceToken
            for url in urls:
                comm = f"{comm} -M {url}"
            res = shellCall(100, comm)
            if res["OK"]:
                returncode, _stdout, stderr = res["Value"]
                if returncode in [0, 1]:
                    comm = "nsrm -f"
                    for url in urls:
                        comm = f"{comm} {url}"
                    res = shellCall(100, comm)
                    if res["OK"]:
                        returncode, _stdout, stderr = res["Value"]
                        if returncode in [0, 1]:
                            for pfn in urls:
                                successful[pfn] = True
                        else:
                            errStr = "RFIOStorage.removeFile. Completely failed to remove files from the nameserver."
                            gLogger.error(errStr, stderr)
                            for pfn in urls:
                                failed[pfn] = errStr
                    else:
                        errStr = "RFIOStorage.removeFile. Completely failed to remove files from the nameserver."
                        gLogger.error(errStr, res["Message"])
                        for pfn in urls:
                            failed[pfn] = errStr
                else:
                    errStr = "RFIOStorage.removeFile. Completely failed to remove files from the stager."
                    gLogger.error(errStr, stderr)
                    for pfn in urls:
                        failed[pfn] = errStr
            else:
                errStr = "RFIOStorage.removeFile. Completely failed to remove files from the stager."
                gLogger.error(errStr, res["Message"])
                for pfn in urls:
                    failed[pfn] = errStr
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getFileMetadata(self, path):
        """Get metadata associated to the file"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.getFileMetadata: Obtaining metadata for %s files." % len(urls))
        res = self.__getPathMetadata(urls)
        if not res["OK"]:
            return res
        failed = {}
        successful = {}
        for pfn, error in res["Value"]["Failed"].items():
            if error == "No such file or directory":
                failed[pfn] = "File does not exist"
            else:
                failed[pfn] = error
        files = []
        for pfn, pfnDict in res["Value"]["Successful"].items():
            if pfnDict["Type"] == "Directory":
                failed[pfn] = "Supplied path is not a file"
            else:
                successful[pfn] = res["Value"]["Successful"][pfn]
                files.append(pfn)
        if files:
            res = self.__getFileMetadata(files)
            if not res["OK"]:
                return res
            for pfn, pfnDict in res["Value"]["Successful"].items():
                successful[pfn].update(pfnDict)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getFileSize(self, path):
        """Get the physical size of the given file"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.getFileSize: Determining the sizes for  %s files." % len(urls))
        res = self.__getPathMetadata(urls)
        if not res["OK"]:
            return res
        failed = {}
        successful = {}
        for pfn, error in res["Value"]["Failed"].items():
            if error == "No such file or directory":
                failed[pfn] = "File does not exist"
            else:
                failed[pfn] = error
        for pfn, pfnDict in res["Value"]["Successful"].items():
            if pfnDict["Type"] == "Directory":
                failed[pfn] = "Supplied path is not a file"
            else:
                successful[pfn] = res["Value"]["Successful"][pfn]["Size"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def prestageFile(self, path):
        """Issue prestage request for file"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        userTag = f"{self.spaceToken}-{time.time()}"
        comm = f"stager_get -S {self.spaceToken} -U {userTag} "
        for url in urls:
            comm = f"{comm} -M {url}"
        res = shellCall(100, comm)
        successful = {}
        failed = {}
        if res["OK"]:
            returncode, stdout, stderr = res["Value"]
            if returncode in [0, 1]:
                for line in stdout.splitlines():
                    if re.search("SUBREQUEST_READY", line):
                        pfn, _status = line.split()
                        successful[pfn] = userTag
                    elif re.search("SUBREQUEST_FAILED", line):
                        pfn, _status, err = line.split(" ", 2)
                        failed[pfn] = err
            else:
                errStr = "RFIOStorage.prestageFile: Got unexpected return code from stager_get."
                gLogger.error(errStr, stderr)
                return S_ERROR(errStr)
        else:
            errStr = "RFIOStorage.prestageFile: Completely failed to issue stage requests."
            gLogger.error(errStr, res["Message"])
            return S_ERROR(errStr)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def prestageFileStatus(self, path):
        """Monitor the status of a prestage request"""
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        successful = {}
        failed = {}
        requestFiles = {}
        for url, requestID in urls.items():
            if requestID not in requestFiles:
                requestFiles[requestID] = []
            requestFiles[requestID].append(url)
        for requestID, urls in requestFiles.items():
            comm = f"stager_qry -S {self.spaceToken} -U {requestID} "
            res = shellCall(100, comm)
            if res["OK"]:
                returncode, stdout, stderr = res["Value"]
                if returncode in [0, 1]:
                    for line in stdout.splitlines():
                        pfn = line.split()[0]
                        status = line.split()[-1]
                        if status in ["STAGED", "CANBEMIGR"]:
                            successful[pfn] = True
                        else:
                            successful[pfn] = False
                else:
                    errStr = "RFIOStorage.prestageFileStatus: Got unexpected return code from stager_get."
                    gLogger.error(errStr, stderr)
                    return S_ERROR(errStr)
            else:
                errStr = "RFIOStorage.prestageFileStatus: Completely failed to obtain prestage status."
                gLogger.error(errStr, res["Message"])
                return S_ERROR(errStr)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getTransportURL(self, path, protocols=False):
        """Obtain the TURLs for the supplied path and protocols"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        successful = {}
        failed = {}
        res = self.exists(urls)
        if not res["OK"]:
            return res
        for path, exists in res["Value"]["Successful"].items():
            if not exists:
                failed[path] = "File does not exist"
            else:
                res = self.__getTransportURL(path)
                if not res["OK"]:
                    failed[path] = res["Message"]
                else:
                    successful[path] = res["Value"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __getTransportURL(self, path):
        try:
            if self.spaceToken:
                tURL = "{}://{}:{}/?svcClass={}&castorVersion=2&path={}".format(
                    self.protocolParameters["Protocol"],
                    self.protocolParameters["Host"],
                    self.protocolParameters["Port"],
                    self.spaceToken,
                    path,
                )
            else:
                tURL = "castor:%s" % (path)
            return S_OK(tURL)
        except Exception as x:
            errStr = "RFIOStorage.__getTransportURL: Exception while creating turl."
            gLogger.exception(errStr, self.name, x)
            return S_ERROR(errStr)

    #############################################################
    #
    # These are the methods for directory manipulation
    #

    def isDirectory(self, path):
        """Check if the given path exists and it is a directory"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.isDirectory: Determining whether %s paths are directories." % len(urls))
        res = self.__getPathMetadata(urls)
        if not res["OK"]:
            return res
        failed = {}
        successful = {}
        for pfn, error in res["Value"]["Failed"].items():
            if error == "No such file or directory":
                failed[pfn] = "Directory does not exist"
            else:
                failed[pfn] = error
        for pfn, pfnDict in res["Value"]["Successful"].items():
            if pfnDict["Type"] == "Directory":
                successful[pfn] = True
            else:
                successful[pfn] = False
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getDirectory(self, path, localPath=False):
        """Get locally a directory from the physical storage together with all its files and subdirectories."""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        successful = {}
        failed = {}
        gLogger.debug("RFIOStorage.getDirectory: Attempting to get local copies of %s directories." % len(urls))
        for src_directory in urls:
            dirName = os.path.basename(src_directory)
            if localPath:
                dest_dir = f"{localPath}/{dirName}"
            else:
                dest_dir = f"{os.getcwd()}/{dirName}"
            res = self.__getDir(src_directory, dest_dir)
            if res["OK"]:
                if res["Value"]["AllGot"]:
                    gLogger.debug("RFIOStorage.getDirectory: Successfully got local copy of %s" % src_directory)
                    successful[src_directory] = {"Files": res["Value"]["Files"], "Size": res["Value"]["Size"]}
                else:
                    gLogger.error("RFIOStorage.getDirectory: Failed to get entire directory.", src_directory)
                    failed[src_directory] = {"Files": res["Value"]["Files"], "Size": res["Value"]["Size"]}
            else:
                gLogger.error(
                    "RFIOStorage.getDirectory: Completely failed to get local copy of directory.", src_directory
                )
                failed[src_directory] = {"Files": 0, "Size": 0}
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __getDir(self, srcDirectory, destDirectory):
        """Black magic contained within..."""
        filesGot = 0
        sizeGot = 0

        # Check the remote directory exists
        res = self.isDirectory(srcDirectory)
        if not res["OK"]:
            errStr = "RFIOStorage.__getDir: Failed to find the supplied source directory."
            gLogger.error(errStr, srcDirectory)
            return S_ERROR(errStr)
        if srcDirectory not in res["Value"]["Successful"]:
            errStr = "RFIOStorage.__getDir: Failed to find the supplied source directory."
            gLogger.error(errStr, srcDirectory)
            return S_ERROR(errStr)
        if not res["Value"]["Successful"][srcDirectory]:
            errStr = "RFIOStorage.__getDir: The supplied source directory does not exist."
            gLogger.error(errStr, srcDirectory)
            return S_ERROR(errStr)

        # Check the local directory exists and create it if not
        if not os.path.exists(destDirectory):
            os.makedirs(destDirectory)

        # Get the remote directory contents
        res = self.listDirectory(srcDirectory)
        if not res["OK"]:
            errStr = "RFIOStorage.__getDir: Failed to list the source directory."
            gLogger.error(errStr, srcDirectory)
        if srcDirectory not in res["Value"]["Successful"]:
            errStr = "RFIOStorage.__getDir: Failed to list the source directory."
            gLogger.error(errStr, srcDirectory)

        surlsDict = res["Value"]["Successful"][srcDirectory]["Files"]
        subDirsDict = res["Value"]["Successful"][srcDirectory]["SubDirs"]

        # First get all the files in the directory
        gotFiles = True
        for surl in surlsDict.keys():
            surlGot = False
            fileSize = surlsDict[surl]["Size"]
            fileName = os.path.basename(surl)
            localPath = f"{destDirectory}/{fileName}"
            fileDict = {surl: localPath}
            res = self.getFile(fileDict)
            if res["OK"]:
                if surl in res["Value"]["Successful"]:
                    filesGot += 1
                    sizeGot += fileSize
                    surlGot = True
            if not surlGot:
                gotFiles = False

        # Then recursively get the sub directories
        subDirsGot = True
        for subDir in subDirsDict.keys():
            subDirName = os.path.basename(subDir)
            localPath = f"{destDirectory}/{subDirName}"
            dirSuccessful = False
            res = self.__getDir(subDir, localPath)
            if res["OK"]:
                if res["Value"]["AllGot"]:
                    dirSuccessful = True
                filesGot += res["Value"]["Files"]
                sizeGot += res["Value"]["Size"]
            if not dirSuccessful:
                subDirsGot = False

        # Check whether all the operations were successful
        allGot = bool(gotFiles and subDirsGot)
        resDict = {"AllGot": allGot, "Files": filesGot, "Size": sizeGot}
        return S_OK(resDict)

    def putDirectory(self, path):
        """Put a local directory to the physical storage together with all its files and subdirectories."""
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        successful = {}
        failed = {}
        gLogger.debug("RFIOStorage.putDirectory: Attemping to put %s directories to remote storage." % len(urls))
        for destDir, sourceDir in urls.items():
            res = self.__putDir(sourceDir, destDir)
            if res["OK"]:
                if res["Value"]["AllPut"]:
                    gLogger.debug(
                        "RFIOStorage.putDirectory: Successfully put directory to remote storage: %s" % destDir
                    )
                    successful[destDir] = {"Files": res["Value"]["Files"], "Size": res["Value"]["Size"]}
                else:
                    gLogger.error(
                        "RFIOStorage.putDirectory: Failed to put entire directory to remote storage.", destDir
                    )
                    failed[destDir] = {"Files": res["Value"]["Files"], "Size": res["Value"]["Size"]}
            else:
                gLogger.error(
                    "RFIOStorage.putDirectory: Completely failed to put directory to remote storage.", destDir
                )
                failed[destDir] = {"Files": 0, "Size": 0}
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __putDir(self, src_directory, dest_directory):
        """Black magic contained within..."""
        filesPut = 0
        sizePut = 0

        # Check the local directory exists
        if not os.path.isdir(src_directory):
            errStr = "RFIOStorage.__putDir: The supplied source directory does not exist."
            gLogger.error(errStr, src_directory)
            return S_ERROR(errStr)

        # Create the remote directory
        res = self.createDirectory(dest_directory)
        if not res["OK"]:
            errStr = "RFIOStorage.__putDir: Failed to create destination directory."
            gLogger.error(errStr, dest_directory)
            return S_ERROR(errStr)

        # Get the local directory contents
        contents = os.listdir(src_directory)
        allSuccessful = True
        for cFile in contents:
            pathSuccessful = False
            localPath = f"{src_directory}/{cFile}"
            remotePath = f"{dest_directory}/{cFile}"
            if os.path.isdir(localPath):
                res = self.__putDir(localPath, remotePath)
                if res["OK"]:
                    if res["Value"]["AllPut"]:
                        pathSuccessful = True
                    filesPut += res["Value"]["Files"]
                    sizePut += res["Value"]["Size"]
                else:
                    return S_ERROR("Failed to put directory")
            else:
                fileDict = {remotePath: localPath}
                res = self.putFile(fileDict)
                if res["OK"]:
                    if remotePath in res["Value"]["Successful"]:
                        filesPut += 1
                        sizePut += res["Value"]["Successful"][remotePath]
                        pathSuccessful = True
            if not pathSuccessful:
                allSuccessful = False
        resDict = {"AllPut": allSuccessful, "Files": filesPut, "Size": sizePut}
        return S_OK(resDict)

    def createDirectory(self, path):
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        successful = {}
        failed = {}
        gLogger.debug("RFIOStorage.createDirectory: Attempting to create %s directories." % len(urls))
        for url in urls:
            strippedUrl = url.rstrip("/")
            res = self.__makeDirs(strippedUrl)
            if res["OK"]:
                gLogger.debug("RFIOStorage.createDirectory: Successfully created directory on storage: %s" % url)
                successful[url] = True
            else:
                gLogger.error("RFIOStorage.createDirectory: Failed to create directory on storage.", url)
                failed[url] = res["Message"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __makeDir(self, path):
        # First create a local file that will be used as a directory place holder in storage name space
        comm = "nsmkdir -m 775 %s" % path
        res = shellCall(100, comm)
        if not res["OK"]:
            return res
        returncode, _stdout, stderr = res["Value"]
        if returncode not in [0]:
            return S_ERROR(stderr)
        return S_OK()

    def __makeDirs(self, path):
        """Black magic contained within...."""
        pDir = os.path.dirname(path)
        res = self.exists(path)
        if not res["OK"]:
            return res
        if res["OK"]:
            if path in res["Value"]["Successful"]:
                if res["Value"]["Successful"][path]:
                    return S_OK()
                else:
                    res = self.exists(pDir)
                    if res["OK"]:
                        if pDir in res["Value"]["Successful"]:
                            if res["Value"]["Successful"][pDir]:
                                res = self.__makeDir(path)
                            else:
                                res = self.__makeDirs(pDir)
                                res = self.__makeDir(path)
        return res

    def removeDirectory(self, path, recursive=False):
        """Remove a directory on the physical storage together with all its files and
        subdirectories.
        """
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.removeDirectory: Attempting to remove %s directories." % len(urls))
        successful = {}
        failed = {}
        for url in urls:
            comm = "nsrm -r %s" % url
            res = shellCall(100, comm)
            if res["OK"]:
                returncode, _stdout, stderr = res["Value"]
                if returncode == 0:
                    successful[url] = {"FilesRemoved": 0, "SizeRemoved": 0}
                elif returncode == 1:
                    successful[url] = {"FilesRemoved": 0, "SizeRemoved": 0}
                else:
                    failed[url] = stderr
            else:
                errStr = "RFIOStorage.removeDirectory: Completely failed to remove directory."
                gLogger.error(errStr, "{} {}".format(url, res["Message"]))
                failed[url] = res["Message"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def listDirectory(self, path):
        """List the supplied path. First checks whether the path is a directory then gets the contents."""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.listDirectory: Attempting to list %s directories." % len(urls))
        res = self.isDirectory(urls)
        if not res["OK"]:
            return res
        successful = {}
        failed = res["Value"]["Failed"]
        directories = []
        for url, isDirectory in res["Value"]["Successful"].items():
            if isDirectory:
                directories.append(url)
            else:
                errStr = "RFIOStorage.listDirectory: Directory does not exist."
                gLogger.error(errStr, url)
                failed[url] = errStr

        for directory in directories:
            comm = "nsls -l %s" % directory
            res = shellCall(self.timeout, comm)
            if res["OK"]:
                returncode, stdout, stderr = res["Value"]
                if not returncode == 0:
                    errStr = "RFIOStorage.listDirectory: Failed to list directory."
                    gLogger.error(errStr, f"{directory} {stderr}")
                    failed[directory] = errStr
                else:
                    subDirs = {}
                    files = {}
                    successful[directory] = {}
                    for line in stdout.splitlines():
                        permissions, _subdirs, _owner, _group, size, _month, _date, _timeYear, pfn = line.split()
                        if not pfn == "dirac_directory":
                            path = f"{directory}/{pfn}"
                            if permissions[0] == "d":
                                # If the subpath is a directory
                                subDirs[path] = True
                            elif permissions[0] == "m":
                                # In the case that the path is a migrated file
                                files[path] = {"Size": int(size), "Migrated": 1}
                            else:
                                # In the case that the path is not migrated file
                                files[path] = {"Size": int(size), "Migrated": 0}
                    successful[directory]["SubDirs"] = subDirs
                    successful[directory]["Files"] = files
            else:
                errStr = "RFIOStorage.listDirectory: Completely failed to list directory."
                gLogger.error(errStr, "{} {}".format(directory, res["Message"]))
                return S_ERROR(errStr)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getDirectoryMetadata(self, path):
        """Get the metadata for the directory"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.getDirectoryMetadata: Attempting to get metadata for %s directories." % len(urls))
        res = self.isDirectory(urls)
        if not res["OK"]:
            return res
        successful = {}
        failed = res["Value"]["Failed"]
        directories = []
        for url, isDirectory in res["Value"]["Successful"].items():
            if isDirectory:
                directories.append(url)
            else:
                errStr = "RFIOStorage.getDirectoryMetadata: Directory does not exist."
                gLogger.error(errStr, url)
                failed[url] = errStr
        res = self.__getPathMetadata(directories)
        if not res["OK"]:
            return res
        else:
            failed.update(res["Value"]["Failed"])
            successful = res["Value"]["Successful"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def getDirectorySize(self, path):
        """Get the size of the directory on the storage"""
        res = self.__checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        gLogger.debug("RFIOStorage.getDirectorySize: Attempting to get size of %s directories." % len(urls))
        res = self.listDirectory(urls)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        for directory, dirDict in res["Value"]["Successful"].items():
            directorySize = 0
            directoryFiles = 0
            filesDict = dirDict["Files"]
            for _fileURL, fileDict in filesDict.items():
                directorySize += fileDict["Size"]
                directoryFiles += 1
            gLogger.debug("RFIOStorage.getDirectorySize: Successfully obtained size of %s." % directory)
            successful[directory] = {"Files": directoryFiles, "Size": directorySize}
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def __checkArgumentFormat(self, path):
        """FIXME: Can be replaced by a generic checkArgumentFormat Utility"""
        if isinstance(path, str):
            urls = [path]
        elif isinstance(path, list):
            urls = path
        elif isinstance(path, dict):
            urls = list(path)
        else:
            return S_ERROR("RFIOStorage.__checkArgumentFormat: Supplied path is not of the correct format.")
        return S_OK(urls)

    def __executeOperation(self, url, method):
        """Executes the requested functionality with the supplied url"""
        fcn = None
        if hasattr(self, method) and callable(getattr(self, method)):
            fcn = getattr(self, method)
        if not fcn:
            return S_ERROR("Unable to invoke %s, it isn't a member funtion of RFIOStorage" % method)
        res = fcn(url)
        if not res["OK"]:
            return res
        elif url not in res["Value"]["Successful"]:
            return S_ERROR(res["Value"]["Failed"][url])
        return S_OK(res["Value"]["Successful"][url])
