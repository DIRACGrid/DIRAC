""" DIRAC DirectoryTree base class """
import errno
import time
import threading
import os
import stat

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import getIDSelectString

DEBUG = 0

#############################################################################


class DirectoryTreeBase:
    def __init__(self, database=None):
        self.db = database
        self.lock = threading.Lock()
        self.treeTable = ""

    ############################################################################
    #
    # THE FOLLOWING METHODS NEED TO ME IMPLEMENTED IN THE DERIVED CLASS
    #
    ############################################################################

    def findDir(self, path, connection=False):
        """Find directory ID for the given path"""
        return S_ERROR("To be implemented on derived class")

    def findDirs(self, paths, connection=False):
        """Find DirIDs for the given path list"""
        return S_ERROR("To be implemented on derived class")

    def makeDir(self, path):

        return S_ERROR("To be implemented on derived class")

    def removeDir(self, path):

        return S_ERROR("To be implemented on derived class")

    def getChildren(self, path, connection=False):
        return S_ERROR("To be implemented on derived class")

    def getDirectoryPath(self, dirID):
        """Get directory name by directory ID"""
        return S_ERROR("To be implemented on derived class")

    def countSubdirectories(self, dirId, includeParent=True):
        return S_ERROR("To be implemented on derived class")

    def getSubdirectoriesByID(self, dirID, requestString=False, includeParent=False):
        """Get all the subdirectories of the given directory at a given level"""
        return S_ERROR("To be implemented on derived class")

    ##########################################################################

    def _getConnection(self, connection):
        if connection:
            return connection
        res = self.db._getConnection()
        if res["OK"]:
            return res["Value"]
        gLogger.warn("Failed to get MySQL connection", res["Message"])
        return connection

    def getTreeTable(self):
        """Get the string of the Directory Tree type"""
        return self.treeTable

    def setDatabase(self, database):
        self.db = database

    def makeDirectory(self, path, credDict, status=0):
        """Create a new directory. The return value is the dictionary
        containing all the parameters of the newly created directory
        """
        if path[0] != "/":
            return S_ERROR("Not an absolute path")
        # Strip off the trailing slash if necessary
        if len(path) > 1 and path[-1] == "/":
            path = path[:-1]

        if path == "/":
            # Create the root directory
            l_uid = 0
            l_gid = 0
        else:
            result = self.db.ugManager.getUserAndGroupID(credDict)
            if not result["OK"]:
                return result
            (l_uid, l_gid) = result["Value"]

        dirDict = {}
        result = self.makeDir(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]
        if result["NewDirectory"]:
            req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
            req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % (
                dirID,
                l_uid,
                l_gid,
                self.db.umask,
                status,
            )
            result = self.db._update(req)
            if result["OK"]:
                resGet = self.getDirectoryParameters(dirID)
                if resGet["OK"]:
                    dirDict = resGet["Value"]
        else:
            return S_OK(dirID)

        if not dirDict:
            self.removeDir(path)
            return S_ERROR("Failed to create directory %s" % path)
        return S_OK(dirID)

    #####################################################################
    def makeDirectories(self, path, credDict):
        """Make all the directories recursively in the path. The return value
        is the dictionary containing all the parameters of the newly created
        directory
        """

        if not path or path[0] != "/":
            return S_ERROR("Not an absolute path")

        result = self.existsDir(path)
        if not result["OK"]:
            return result
        result = result["Value"]
        if result["Exists"]:
            return S_OK(result["DirID"])

        if path == "/":
            result = self.makeDirectory(path, credDict)
            return result

        parentDir = os.path.dirname(path)
        result = self.existsDir(parentDir)
        if not result["OK"]:
            return result
        result = result["Value"]
        if result["Exists"]:
            result = self.makeDirectory(path, credDict)
        else:
            result = self.makeDirectories(parentDir, credDict)
            if not result["OK"]:
                return result
            result = self.makeDirectory(path, credDict)

        return result

    #####################################################################
    def exists(self, lfns):
        successful = {}
        failed = {}
        for lfn in lfns:
            res = self.findDir(lfn)
            if not res["OK"]:
                failed[lfn] = res["Message"]
            if not res["Value"]:
                successful[lfn] = False
            else:
                successful[lfn] = lfn
        return S_OK({"Successful": successful, "Failed": failed})

    def existsDir(self, path):
        """Check the existence of the directory path"""
        result = self.findDir(path)
        if not result["OK"]:
            return result
        if result["Value"]:
            result = S_OK(int(result["Value"]))
            result["Exists"] = True
            result["DirID"] = result["Value"]
        else:
            result = S_OK(0)
            result["Exists"] = False

        return result

    #####################################################################
    def isDirectory(self, paths):
        """Checking for existence of directories"""
        successful = {}
        failed = {}
        for dir in paths:
            result = self.existsDir(dir)
            if not result["OK"]:
                failed[dir] = result["Message"]
            elif result["Value"]["Exists"]:
                successful[dir] = True
            else:
                successful[dir] = False

        return S_OK({"Successful": successful, "Failed": failed})

    #####################################################################
    def createDirectory(self, dirs, credDict):
        """Checking for existence of directories"""
        successful = {}
        failed = {}
        for dir in dirs:
            result = self.makeDirectories(dir, credDict)
            if not result["OK"]:
                failed[dir] = result["Message"]
            else:
                successful[dir] = True

        return S_OK({"Successful": successful, "Failed": failed})

    #####################################################################
    def isEmpty(self, path):
        """Find out if the given directory is empty"""
        # Check if there are subdirectories
        result = self.getChildren(path)
        if not result["OK"]:
            return result
        childIDs = result["Value"]
        if childIDs:
            return S_OK(False)

        # Check if there are files
        result = self.__getDirID(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]
        result = self.db.fileManager.getFilesInDirectory(dirID)
        if not result["OK"]:
            return result
        files = result["Value"]
        if files:
            return S_OK(False)

        return S_OK(True)

    #####################################################################
    def removeDirectory(self, dirs, force=False):
        """Remove an empty directory from the catalog"""
        successful = {}
        failed = {}

        # Check if requested directories exist in the catalog
        result = self.findDirs(dirs)
        if not result["OK"]:
            return result
        dirDict = result["Value"]
        for d in dirs:
            if d not in dirDict:
                successful[d] = "Directory does not exist"

        for dir in dirDict:
            result = self.isEmpty(dir)
            if not result["OK"]:
                return result
            if not result["Value"]:
                failed[dir] = "Failed to remove non-empty directory"
                continue
            result = self.removeDir(dir)
            if not result["OK"]:
                failed[dir] = result["Message"]
            else:
                successful[dir] = result
        return S_OK({"Successful": successful, "Failed": failed})

    #####################################################################
    def __getDirID(self, path):
        """Get directory ID from the given path or already evaluated ID"""

        if isinstance(path, str):
            result = self.findDir(path)
            if not result["OK"]:
                return result
            dirID = result["Value"]
            if not dirID:
                return S_ERROR("%s: not found" % str(path))
            return S_OK(dirID)
        else:
            return S_OK(path)

    #####################################################################
    def getDirectoryParameters(self, path):
        """Get the given directory parameters"""

        result = self.__getDirID(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]

        query = "SELECT DirID,UID,GID,Status,Mode,CreationDate,ModificationDate from FC_DirectoryInfo"
        query = query + " WHERE DirID=%d" % dirID
        resQuery = self.db._query(query)
        if not resQuery["OK"]:
            return resQuery

        if not resQuery["Value"]:
            return S_ERROR("Directory not found")
        dirDict = {}
        dirDict["DirID"] = int(resQuery["Value"][0][0])
        uid = int(resQuery["Value"][0][1])
        dirDict["UID"] = uid
        owner = "unknown"
        result = self.db.ugManager.getUserName(uid)
        if result["OK"]:
            owner = result["Value"]
        dirDict["Owner"] = owner
        gid = int(resQuery["Value"][0][2])
        dirDict["GID"] = int(resQuery["Value"][0][2])
        group = "unknown"
        result = self.db.ugManager.getGroupName(gid)
        if result["OK"]:
            group = result["Value"]
        dirDict["OwnerGroup"] = group
        dirDict["Status"] = int(resQuery["Value"][0][3])
        dirDict["Mode"] = int(resQuery["Value"][0][4])
        dirDict["CreationDate"] = resQuery["Value"][0][5]
        dirDict["ModificationDate"] = resQuery["Value"][0][6]

        return S_OK(dirDict)

    #####################################################################
    def _setDirectoryParameter(self, path, pname, pvalue):
        """Set a numerical directory parameter

        :param mixed path: Directory path or paths as a string or directory ID as int,
                           list/tuple of ints or a string to select directory IDs
        :param str pname: parameter name
        :param int pvalue: parameter value
        """
        result = getIDSelectString(path)
        if not result["OK"] and isinstance(path, str):
            result = self.__getDirID(path)
            if not result["OK"]:
                return result
            dirID = result["Value"]
            result = getIDSelectString(dirID)
            if not result["OK"]:
                return result

        dirIDString = result["Value"]
        req = "UPDATE FC_DirectoryInfo SET %s=%d, " "ModificationDate=UTC_TIMESTAMP() WHERE DirID IN ( %s )" % (
            pname,
            pvalue,
            dirIDString,
        )
        result = self.db._update(req)
        return result

    #####################################################################
    def _setDirectoryGroup(self, path, gname):
        """Set the directory group

        :param mixed path: directory path as a string or int or list of ints or select statement
        :param mixt group: new group as a string or int gid
        """

        result = self.db.ugManager.findGroup(gname)
        if not result["OK"]:
            return result

        gid = result["Value"]

        return self._setDirectoryParameter(path, "GID", gid)

    #####################################################################
    def _setDirectoryOwner(self, path, owner):
        """Set the directory owner

        :param mixed path: directory path as a string or int or list of ints or select statement
        :param mixt owner: new user as a string or int uid
        """

        result = self.db.ugManager.findUser(owner)
        if not result["OK"]:
            return result

        uid = result["Value"]

        return self._setDirectoryParameter(path, "UID", uid)

    #####################################################################
    def changeDirectoryOwner(self, paths, recursive=False):
        """Bulk setting of the directory owner

        :param dictionary paths: dictionary < lfn : owner >
        """
        return self._changeDirectoryParameter(
            paths, self._setDirectoryOwner, self.db.fileManager.setFileOwner, recursive=recursive
        )

    #####################################################################
    def changeDirectoryGroup(self, paths, recursive=False):
        """Bulk setting of the directory group

        :param dictionary paths: dictionary < lfn : group >
        """
        return self._changeDirectoryParameter(
            paths, self._setDirectoryGroup, self.db.fileManager.setFileGroup, recursive=recursive
        )

    #####################################################################
    def _setDirectoryMode(self, path, mode):
        """set the directory mode

        :param mixed path: directory path as a string or int or list of ints or select statement
        :param int mode: new mode
        """
        return self._setDirectoryParameter(path, "Mode", mode)

    #####################################################################
    def changeDirectoryMode(self, paths, recursive=False):
        """Bulk setting of the directory mode

        :param dictionary paths: dictionary < lfn : mode >
        """
        return self._changeDirectoryParameter(
            paths, self._setDirectoryMode, self.db.fileManager.setFileMode, recursive=recursive
        )

    #####################################################################
    def _changeDirectoryParameter(self, paths, directoryFunction, fileFunction, recursive=False):
        """Bulk setting of the directory parameter with recursion for all the subdirectories and files

        :param dictionary paths: dictionary < lfn : value >, where value is the value of parameter to be set
        :param function directoryFunction: function to change directory(ies) parameter
        :param function fileFunction: function to change file(s) parameter
        :param bool recursive: flag to apply the operation recursively
        """
        arguments = paths
        successful = {}
        failed = {}
        for path, attribute in arguments.items():
            result = directoryFunction(path, attribute)
            if not result["OK"]:
                failed[path] = result["Message"]
                continue
            if recursive:
                result = self.__getDirID(path)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue
                dirID = result["Value"]
                result = self.getSubdirectoriesByID(dirID, requestString=True, includeParent=True)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue

                subDirQuery = result["Value"]
                result = self.db.fileManager.getFileIDsInDirectory(subDirQuery, requestString=True)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue
                fileQuery = result["Value"]

                result = directoryFunction(subDirQuery, attribute)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue
                result = fileFunction(fileQuery, attribute)
                if not result["OK"]:
                    failed[path] = result["Message"]
                else:
                    successful[path] = True
            else:
                successful[path] = True

        return S_OK({"Successful": successful, "Failed": failed})

    #####################################################################
    def setDirectoryStatus(self, path, status):
        """set the directory status"""
        return self._setDirectoryParameter(path, "Status", status)

    def getPathPermissions(self, lfns, credDict):
        """Get permissions for the given user/group to manipulate the given lfns"""
        successful = {}
        failed = {}
        for path in lfns:
            result = self.getDirectoryPermissions(path, credDict)
            if not result["OK"]:
                failed[path] = result["Message"]
            else:
                successful[path] = result["Value"]

        return S_OK({"Successful": successful, "Failed": failed})

    #####################################################################
    def getDirectoryPermissions(self, path, credDict):
        """Get permissions for the given user/group to manipulate the given directory"""
        result = self.db.ugManager.getUserAndGroupID(credDict)
        if not result["OK"]:
            return result
        uid, gid = result["Value"]

        result = self.getDirectoryParameters(path)
        if not result["OK"]:
            if "not found" in result["Message"] or "not exist" in result["Message"]:
                # If the directory does not exist, check the nearest parent for the permissions
                if path == "/":
                    # Nothing yet exists, starting from the scratch
                    resultDict = {}
                    resultDict["Write"] = True
                    resultDict["Read"] = True
                    resultDict["Execute"] = True
                    return S_OK(resultDict)
                else:
                    pDir = os.path.dirname(path)
                    if not pDir:
                        return S_ERROR("Illegal Path")
                    if pDir == path:
                        # If pDir == path, then we're stuck in a loop
                        # There is probably a "//" in the path
                        return S_ERROR("Bad Path (double /?)")
                    result = self.getDirectoryPermissions(pDir, credDict)
                    return result
            else:
                return result

        dUid = result["Value"]["UID"]
        dGid = result["Value"]["GID"]
        mode = result["Value"]["Mode"]

        owner = uid == dUid
        group = gid == dGid

        resultDict = {}
        if self.db.globalReadAccess:
            resultDict["Read"] = True
        else:
            resultDict["Read"] = (
                (owner and mode & stat.S_IRUSR > 0) or (group and mode & stat.S_IRGRP > 0) or mode & stat.S_IROTH > 0
            )

        resultDict["Write"] = (
            (owner and mode & stat.S_IWUSR > 0) or (group and mode & stat.S_IWGRP > 0) or mode & stat.S_IWOTH > 0
        )

        resultDict["Execute"] = (
            (owner and mode & stat.S_IXUSR > 0) or (group and mode & stat.S_IXGRP > 0) or mode & stat.S_IXOTH > 0
        )

        return S_OK(resultDict)

    def getFileIDsInDirectoryWithLimits(self, dirID, credDict, startItem=1, maxItems=25):
        """Get file IDs for the given directory"""
        dirs = dirID
        if not isinstance(dirID, list):
            dirs = [dirID]

        if not dirs:
            dirs = [-1]

        dirListString = ",".join([str(dir) for dir in dirs])

        req = "SELECT COUNT( DirID ) FROM FC_Files USE INDEX (DirID) WHERE DirID IN ( %s )" % dirListString
        result = self.db._query(req)
        if not result["OK"]:
            return result

        totalRecords = result["Value"][0][0]

        if not totalRecords:
            result = S_OK([])
            result["TotalRecords"] = totalRecords
            return result

        req = f"SELECT FileID FROM FC_Files WHERE DirID IN ( {dirListString} ) LIMIT {startItem}, {maxItems} "
        result = self.db._query(req)
        if not result["OK"]:
            return result
        result = S_OK([fileId[0] for fileId in result["Value"]])
        result["TotalRecords"] = totalRecords
        return result

    def getFileLFNsInDirectory(self, dirID, credDict):
        """Get file lfns for the given directory or directory list"""
        dirs = dirID
        if not isinstance(dirID, list):
            dirs = [dirID]

        dirListString = ",".join([str(dir) for dir in dirs])
        treeTable = self.getTreeTable()
        req = "SELECT CONCAT(D.DirName,'/',F.FileName) FROM FC_Files as F, %s as D WHERE D.DirID IN ( %s ) and D.DirID=F.DirID"
        req = req % (treeTable, dirListString)
        result = self.db._query(req)
        if not result["OK"]:
            return result
        lfnList = [x[0] for x in result["Value"]]
        return S_OK(lfnList)

    def getFileLFNsInDirectoryByDirectory(self, dirIDList, credDict):
        """Get file LFNs and IDs for the given directory or directory list

        :param list dirIDList: List of directory IDs
        :param dict credDict: dictionary of user credentials

        :return: S_OK/S_ERROR with Value dictionary {"DirLFNDict": dirLfnDict, "IDLFNDict": idLfnDict}
                 where dirLfnDict has the structure <directory_name>:<list of contained file names>,
                 idLfnDict has structure <fileID>:<LFN>
        """
        dirs = dirIDList
        if not isinstance(dirIDList, list):
            dirs = [dirIDList]

        dirListString = ",".join([str(dir_) for dir_ in dirs])
        treeTable = self.getTreeTable()
        req = "SELECT D.DirName,F.FileName,F.FileID FROM FC_Files as F, %s as D WHERE D.DirID IN ( %s ) and D.DirID=F.DirID"
        req = req % (treeTable, dirListString)
        result = self.db._query(req)
        if not result["OK"]:
            return result

        dirLfnDict = {}
        idLfnDict = {}
        for dir_, fname, fileID in result["Value"]:
            dirLfnDict.setdefault(dir_, []).append(fname)
            idLfnDict[fileID] = dir_ + "/" + fname

        return S_OK({"DirLFNDict": dirLfnDict, "IDLFNDict": idLfnDict})

    def _getDirectoryContents(self, path, details=False):
        """Get contents of a given directory"""
        result = self.findDir(path)
        if not result["OK"]:
            return result
        directoryID = result["Value"]
        directories = {}
        files = {}
        links = {}
        result = self.getChildren(path)
        if not result["OK"]:
            return result

        # Get subdirectories
        dirIDList = result["Value"]
        for dirID in dirIDList:
            result = self.getDirectoryPath(dirID)
            if not result["OK"]:
                return result
            dirName = result["Value"]
            if details:
                result = self.getDirectoryParameters(dirID)
                if not result["OK"]:
                    directories[dirName] = False
                else:
                    directories[dirName] = result["Value"]
            else:
                directories[dirName] = True
        result = self.db.fileManager.getFilesInDirectory(directoryID, verbose=details)
        if not result["OK"]:
            return result
        files = result["Value"]
        result = self.db.datasetManager.getDatasetsInDirectory(directoryID, verbose=details)
        if not result["OK"]:
            return result
        datasets = result["Value"]
        pathDict = {"Files": files, "SubDirs": directories, "Links": links, "Datasets": datasets}

        return S_OK(pathDict)

    def listDirectory(self, lfns, verbose=False):
        """Get the directory listing"""
        successful = {}
        failed = {}
        for path in lfns:
            result = self._getDirectoryContents(path, details=verbose)
            if not result["OK"]:
                failed[path] = result["Message"]
            else:
                successful[path] = result["Value"]

        return S_OK({"Successful": successful, "Failed": failed})

    def getDirectoryDump(self, lfns):
        """Get the dump of the directories in lfns"""
        successful = {}
        failed = {}
        for path in lfns:
            result = self._getDirectoryDump(path)
            if not result["OK"]:
                failed[path] = result["Message"]
            else:
                successful[path] = result["Value"]

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryDump(self, path):
        """
        Recursively dump all the content of a directory

        :param str path: directory to dump

        :returns: dictionary with `Files` and `SubDirs` as keys
                  `Files` is a dict containing files metadata.
                  `SubDirs` is a list of directory
        """
        result = self.findDir(path)
        if not result["OK"]:
            return result
        directoryID = result["Value"]
        if not directoryID:
            return S_ERROR(errno.ENOENT, f"{path} does not exist")
        directories = []

        result = self.db.fileManager.getFilesInDirectory(directoryID)
        if not result["OK"]:
            return result

        filesInDir = result["Value"]
        files = {
            os.path.join(path, fileName): {
                "Size": fileMetadata["MetaData"]["Size"],
                "CreationDate": fileMetadata["MetaData"]["CreationDate"],
            }
            for fileName, fileMetadata in filesInDir.items()
        }

        dirIDList = [directoryID]

        while dirIDList:
            curDirID = dirIDList.pop()
            result = self.getChildren(curDirID)
            if not result["OK"]:
                return result
            newDirIDList = result["Value"]
            for dirID in newDirIDList:
                result = self.getDirectoryPath(dirID)
                if not result["OK"]:
                    return result
                dirName = result["Value"]

                directories.append(dirName)

                result = self.db.fileManager.getFilesInDirectory(dirID)
                if not result["OK"]:
                    return result

                filesInDir = result["Value"]

                files.update(
                    {
                        os.path.join(dirName, fileName): {
                            "Size": fileMetadata["MetaData"]["Size"],
                            "CreationDate": fileMetadata["MetaData"]["CreationDate"],
                        }
                        for fileName, fileMetadata in filesInDir.items()
                    }
                )

            # Add to this list to get subdirectories of these directories
            dirIDList.extend(newDirIDList)

        pathDict = {"Files": files, "SubDirs": directories}

        return S_OK(pathDict)

    def getDirectoryReplicas(self, lfns, allStatus=False):
        """Get replicas for files in the given directories"""
        successful = {}
        failed = {}
        for path in lfns:
            result = self.findDir(path)
            if not result["OK"]:
                failed[path] = result["Message"]
                continue
            directoryID = result["Value"]
            result = self.db.fileManager.getDirectoryReplicas(directoryID, path, allStatus)
            if not result["OK"]:
                failed[path] = result["Message"]
                continue
            fileDict = result["Value"]
            successful[path] = {}
            for fileName in fileDict:
                successful[path][fileName] = fileDict[fileName]

        return S_OK({"Successful": successful, "Failed": failed})

    def getDirectorySize(self, lfns, longOutput=False, rawFileTables=False, recursiveSum=True):
        """
        Get the total size of the requested directories. If longOutput flag
        is True, get also physical size per Storage Element

        :param bool longOutput: if True, also fetches the physical size per SE
        :param bool rawFileTables: if True, uses the File table instead of the pre-computed values
        :param bool recursiveSum: if True (default), takes into account subdirectories

        """
        start = time.time()

        result = self.db._getConnection()
        if not result["OK"]:
            return result
        connection = result["Value"]

        if rawFileTables:
            resultLogical = self._getDirectoryLogicalSize(lfns, recursiveSum=recursiveSum, connection=connection)
        else:
            resultLogical = self._getDirectoryLogicalSizeFromUsage(
                lfns, recursiveSum=recursiveSum, connection=connection
            )

        if not resultLogical["OK"]:
            connection.close()
            return resultLogical

        resultDict = resultLogical["Value"]
        if not resultDict["Successful"]:
            connection.close()
            return resultLogical

        if longOutput:
            # Continue with only successful directories
            if rawFileTables:
                resultPhysical = self._getDirectoryPhysicalSize(
                    resultDict["Successful"], recursiveSum=recursiveSum, connection=connection
                )
            else:
                resultPhysical = self._getDirectoryPhysicalSizeFromUsage(
                    resultDict["Successful"], recursiveSum=recursiveSum, connection=connection
                )
            if not resultPhysical["OK"]:
                resultDict["QueryTime"] = time.time() - start
                result = S_OK(resultDict)
                result["Message"] = "Failed to get the physical size on storage"
                connection.close()
                return result
            for lfn in resultPhysical["Value"]["Successful"]:
                resultDict["Successful"][lfn]["PhysicalSize"] = resultPhysical["Value"]["Successful"][lfn]
        connection.close()
        resultDict["QueryTime"] = time.time() - start

        return S_OK(resultDict)

    def _getDirectoryLogicalSizeFromUsage(self, lfns, recursiveSum=True, connection=None):
        """Get the total "logical" size of the requested directories

        :param recursiveSum: If false, don't take subdir into account
        """

        if not recursiveSum:
            return S_ERROR("Not implemented")

        successful = {}
        failed = {}
        for path in lfns:
            result = self.findDir(path)
            if not result["OK"]:
                failed[path] = "Directory not found"
                continue
            if not result["Value"]:
                failed[path] = "Directory not found"
                continue
            dirID = result["Value"]
            req = "SELECT SESize, SEFiles FROM FC_DirectoryUsage WHERE SEID=0 AND DirID=%d" % dirID

            result = self.db._query(req, conn=connection)
            if not result["OK"]:
                failed[path] = result["Message"]
            elif not result["Value"]:
                successful[path] = {"LogicalSize": 0, "LogicalFiles": 0, "LogicalDirectories": 0}
            elif result["Value"][0][0]:
                successful[path] = {
                    "LogicalSize": int(result["Value"][0][0]),
                    "LogicalFiles": int(result["Value"][0][1]),
                }
                result = self.countSubdirectories(dirID, includeParent=False)
                if result["OK"]:
                    successful[path]["LogicalDirectories"] = result["Value"]
                else:
                    successful[path]["LogicalDirectories"] = -1

            else:
                successful[path] = {"LogicalSize": 0, "LogicalFiles": 0, "LogicalDirectories": 0}

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryLogicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total "logical" size of the requested directories

        :param bool recursiveSum: If false, don't take subdir into account
        """

        if not recursiveSum:
            return S_ERROR("Not implemented")

        successful = {}
        failed = {}
        treeTable = self.getTreeTable()
        for path in lfns:

            if path == "/":
                req = "SELECT SUM(Size),COUNT(*) FROM FC_Files"
                reqDir = "SELECT count(*) FROM %s" % treeTable
            else:
                result = self.findDir(path)
                if not result["OK"]:
                    failed[path] = "Directory not found"
                    continue
                if not result["Value"]:
                    failed[path] = "Directory not found"
                    continue
                dirID = result["Value"]
                result = self.getSubdirectoriesByID(dirID, requestString=True, includeParent=True)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue
                else:
                    dirString = result["Value"]
                    req = (
                        "SELECT SUM(F.Size),COUNT(*) FROM FC_Files as F JOIN (%s) as T WHERE F.DirID=T.DirID"
                        % dirString
                    )
                    reqDir = dirString.replace("SELECT DirID FROM", "SELECT count(*) FROM")

            result = self.db._query(req, conn=connection)
            if not result["OK"]:
                failed[path] = result["Message"]
            elif not result["Value"]:
                successful[path] = {"LogicalSize": 0, "LogicalFiles": 0, "LogicalDirectories": 0}
            elif result["Value"][0][0]:
                successful[path] = {
                    "LogicalSize": int(result["Value"][0][0]),
                    "LogicalFiles": int(result["Value"][0][1]),
                }
                result = self.db._query(reqDir, conn=connection)
                if result["OK"] and result["Value"]:
                    successful[path]["LogicalDirectories"] = result["Value"][0][0] - 1
                else:
                    successful[path]["LogicalDirectories"] = -1

            else:
                successful[path] = {"LogicalSize": 0, "LogicalFiles": 0, "LogicalDirectories": 0}

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryPhysicalSizeFromUsage(self, lfns, recursiveSum=True, connection=None):
        """Get the total size of the requested directories

        :param recursiveSum: If false, don't take subdir into account
        """

        if not recursiveSum:
            return S_ERROR("Not implemented")

        successful = {}
        failed = {}
        for path in lfns:
            result = self.findDir(path)
            if not result["OK"]:
                failed[path] = "Directory not found"
                continue
            if not result["Value"]:
                failed[path] = "Directory not found"
                continue
            dirID = result["Value"]

            req = "SELECT S.SEID, S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
            req += "  WHERE S.SEID=D.SEID AND D.DirID=%d" % dirID
            result = self.db._query(req, conn=connection)
            if not result["OK"]:
                failed[path] = result["Message"]
            elif not result["Value"]:
                successful[path] = {}
            elif result["Value"][0][0]:
                seDict = {}
                totalSize = 0
                totalFiles = 0
                for seID, seName, seSize, seFiles in result["Value"]:
                    if seSize or seFiles:
                        seDict[seName] = {"Size": seSize, "Files": seFiles}
                        totalSize += seSize
                        totalFiles += seFiles
                    else:
                        req = "DELETE FROM FC_DirectoryUsage WHERE SEID=%d AND DirID=%d" % (seID, dirID)
                        result = self.db._update(req)
                        if not result["OK"]:
                            gLogger.error("Failed to delete entry from FC_DirectoryUsage", result["Message"])
                seDict["TotalSize"] = int(totalSize)
                seDict["TotalFiles"] = int(totalFiles)
                successful[path] = seDict
            else:
                successful[path] = {}

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryPhysicalSizeFromUsage_old(self, lfns, connection):
        """Get the total size of the requested directories"""
        successful = {}
        failed = {}
        for path in lfns:

            if path == "/":
                req = "SELECT S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
                req += "  WHERE S.SEID=D.SEID"
            else:
                result = self.findDir(path)
                if not result["OK"]:
                    failed[path] = "Directory not found"
                    continue
                if not result["Value"]:
                    failed[path] = "Directory not found"
                    continue
                dirID = result["Value"]
                result = self.getSubdirectoriesByID(dirID, requestString=True, includeParent=True)
                if not result["OK"]:
                    return result
                subDirString = result["Value"]
                req = "SELECT S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
                req += " JOIN (%s) AS F" % subDirString
                req += " WHERE S.SEID=D.SEID AND D.DirID=F.DirID"

            result = self.db._query(req, conn=connection)
            if not result["OK"]:
                failed[path] = result["Message"]
            elif not result["Value"]:
                successful[path] = {}
            elif result["Value"][0][0]:
                seDict = {}
                totalSize = 0
                totalFiles = 0
                for seName, seSize, seFiles in result["Value"]:
                    sfDict = seDict.get(seName, {"Size": 0, "Files": 0})
                    sfDict["Size"] += seSize
                    sfDict["Files"] += seFiles
                    seDict[seName] = sfDict
                    totalSize += seSize
                    totalFiles += seFiles
                seDict["TotalSize"] = int(totalSize)
                seDict["TotalFiles"] = int(totalFiles)
                successful[path] = seDict
            else:
                successful[path] = {}

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryPhysicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total size of the requested directories
        :param recursiveSum: If false, don't take subdir into account
        """
        if not recursiveSum:
            return S_ERROR("Not implemented")

        successful = {}
        failed = {}
        for path in lfns:
            if path == "/":
                req = "SELECT SUM(F.Size),COUNT(F.Size),S.SEName from FC_Files as F, FC_Replicas as R, FC_StorageElements as S "
                req += "WHERE R.SEID=S.SEID AND F.FileID=R.FileID "
                req += "GROUP BY S.SEID"
            else:
                result = self.findDir(path)
                if not result["OK"]:
                    failed[path] = "Directory not found"
                    continue
                if not result["Value"]:
                    failed[path] = "Directory not found"
                    continue
                dirID = result["Value"]
                result = self.getSubdirectoriesByID(dirID, requestString=True, includeParent=True)
                if not result["OK"]:
                    failed[path] = result["Message"]
                    continue
                else:
                    dirString = result["Value"]

                    req = (
                        "SELECT SUM(F.Size),COUNT(F.Size),S.SEName from FC_Files as F, FC_Replicas as R, FC_StorageElements as S JOIN (%s) as T "
                        % dirString
                    )
                    req += "WHERE R.SEID=S.SEID AND F.FileID=R.FileID AND F.DirID=T.DirID "
                    req += "GROUP BY S.SEID"

            result = self.db._query(req, conn=connection)
            if not result["OK"]:
                failed[path] = result["Message"]
            elif not result["Value"]:
                successful[path] = {}
            elif result["Value"][0][0]:
                seDict = {}
                totalSize = 0
                totalFiles = 0
                for size, files, seName in result["Value"]:
                    seDict[seName] = {"Size": int(size), "Files": int(files)}
                    totalSize += size
                    totalFiles += files
                seDict["TotalSize"] = int(totalSize)
                seDict["TotalFiles"] = int(totalFiles)
                successful[path] = seDict
            else:
                successful[path] = {}

        return S_OK({"Successful": successful, "Failed": failed})

    def _rebuildDirectoryUsage(self):
        """Recreate and replenish the Storage Usage tables"""

        req = "DROP TABLE IF EXISTS FC_DirectoryUsage_backup"
        result = self.db._update(req)
        req = "RENAME TABLE FC_DirectoryUsage TO FC_DirectoryUsage_backup"
        result = self.db._update(req)
        req = "CREATE TABLE `FC_DirectoryUsage` LIKE `FC_DirectoryUsage_backup`"
        result = self.db._update(req)
        if not result["OK"]:
            return result

        result = self.__rebuildDirectoryUsageLeaves()
        if not result["OK"]:
            return result

        result = self.db.dtree.findDir("/")
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory / not found")
        dirID = result["Value"]
        result = self.__rebuildDirectoryUsage(dirID)
        gLogger.verbose("Finished rebuilding Directory Usage")
        return result

    def __rebuildDirectoryUsageLeaves(self):
        """Rebuild DirectoryUsage entries for directories having files"""
        req = "SELECT DISTINCT(DirID) FROM FC_Files"
        result = self.db._query(req)
        if not result["OK"]:
            return result

        dirIDs = [x[0] for x in result["Value"]]
        gLogger.verbose("Starting rebuilding Directory Usage, number of visible directories %d" % len(dirIDs))

        insertFields = ["DirID", "SEID", "SESize", "SEFiles", "LastUpdate"]
        insertCount = 0
        insertValues = []

        count = 0
        empty = 0

        for dirID in dirIDs:

            count += 1

            # Get the physical size
            req = "SELECT SUM(F.Size),COUNT(F.Size),R.SEID from FC_Files as F, FC_Replicas as R "
            req += "WHERE F.FileID=R.FileID AND F.DirID=%d GROUP BY R.SEID" % int(dirID)
            result = self.db._query(req)
            if not result["OK"]:
                return result
            if not result["Value"]:
                empty += 1

            for seSize, seFiles, seID in result["Value"]:
                insertValues = [dirID, seID, seSize, seFiles, "UTC_TIMESTAMP()"]
                result = self.db.insertFields("FC_DirectoryUsage", insertFields, insertValues)
                if not result["OK"]:
                    if "Duplicate" in result["Message"]:
                        req = "UPDATE FC_DirectoryUsage SET SESize=%d, SEFiles=%d, LastUpdate=UTC_TIMESTAMP()" % (
                            seSize,
                            seFiles,
                        )
                        req += f" WHERE DirID={dirID} AND SEID={seID}"
                        result = self.db._update(req)
                        if not result["OK"]:
                            return result
                    return result

            # Get the logical size
            req = "SELECT SUM(Size),COUNT(Size) from FC_Files WHERE DirID=%d " % int(dirID)
            result = self.db._query(req)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR("Empty directory")
            seSize, seFiles = result["Value"][0]
            insertValues = [dirID, 0, seSize, seFiles, "UTC_TIMESTAMP()"]
            result = self.db.insertFields("FC_DirectoryUsage", insertFields, insertValues)
            if not result["OK"]:
                if "Duplicate" in result["Message"]:
                    req = "UPDATE FC_DirectoryUsage SET SESize=%d, SEFiles=%d, LastUpdate=UTC_TIMESTAMP()" % (
                        seSize,
                        seFiles,
                    )
                    req += " WHERE DirID=%s AND SEID=0" % dirID
                    result = self.db._update(req)
                    if not result["OK"]:
                        return result
                else:
                    return result

        gLogger.verbose("Processed %d directories, %d empty " % (count, empty))

        return S_OK()

    def __rebuildDirectoryUsage(self, directoryID):
        """Rebuild DirectoryUsage entries recursively for the given path"""
        result = self.getChildren(directoryID)
        if not result["OK"]:
            return result
        dirIDs = result["Value"]
        resultDict = {}
        for dirID in dirIDs:
            result = self.__rebuildDirectoryUsage(dirID)
            if not result["OK"]:
                return result
            dirDict = result["Value"]
            for seID in dirDict:
                resultDict.setdefault(seID, {"Size": 0, "Files": 0})
                resultDict[seID]["Size"] += dirDict[seID]["Size"]
                resultDict[seID]["Files"] += dirDict[seID]["Files"]

        insertFields = ["DirID", "SEID", "SESize", "SEFiles", "LastUpdate"]
        insertValues = []
        for seID in resultDict:
            size = resultDict[seID]["Size"]
            files = resultDict[seID]["Files"]
            req = "UPDATE FC_DirectoryUsage SET SESize=SESize+%d, SEFiles=SEFiles+%d WHERE DirID=%d AND SEID=%d"
            req = req % (size, files, directoryID, seID)
            result = self.db._update(req)
            if not result["OK"]:
                return result
            if not result["Value"]:
                insertValues = [directoryID, seID, size, files, "UTC_TIMESTAMP()"]
                result = self.db.insertFields("FC_DirectoryUsage", insertFields, insertValues)
                if not result["OK"]:
                    return result

        req = "SELECT SEID,SESize,SEFiles from FC_DirectoryUsage WHERE DirID=%d" % directoryID
        result = self.db._query(req)
        if not result["OK"]:
            return result

        resultDict = {}
        for seid, size, files in result["Value"]:
            resultDict[seid] = {"Size": size, "Files": files}

        return S_OK(resultDict)

    def getDirectoryCounters(self, connection=False):
        """Get the total number of directories"""
        conn = self._getConnection(connection)
        resultDict = {}
        req = "SELECT COUNT(*) from FC_DirectoryInfo"
        res = self.db._query(req, conn=connection)
        if not res["OK"]:
            return res
        resultDict["Directories"] = res["Value"][0][0]

        treeTable = self.getTreeTable()

        req = f"SELECT COUNT(DirID) FROM {treeTable} WHERE Parent NOT IN ( SELECT DirID from {treeTable} )"
        req += " AND DirID <> 1"
        res = self.db._query(req, conn=connection)
        if not res["OK"]:
            return res
        resultDict["Orphan Directories"] = res["Value"][0][0]

        req = f"SELECT COUNT(DirID) FROM {treeTable} WHERE DirID NOT IN ( SELECT Parent from {treeTable} )"
        req += " AND DirID NOT IN ( SELECT DirID from FC_Files ) "
        res = self.db._query(req, conn=connection)
        if not res["OK"]:
            return res
        resultDict["Empty Directories"] = res["Value"][0][0]

        req = "SELECT COUNT(DirID) FROM %s WHERE DirID NOT IN ( SELECT DirID FROM FC_DirectoryInfo )" % treeTable
        res = self.db._query(req, conn=connection)
        if not res["OK"]:
            return res
        resultDict["DirTree w/o DirInfo"] = res["Value"][0][0]

        req = "SELECT COUNT(DirID) FROM FC_DirectoryInfo WHERE DirID NOT IN ( SELECT DirID FROM %s )" % treeTable
        res = self.db._query(req, conn=connection)
        if not res["OK"]:
            return res
        resultDict["DirInfo w/o DirTree"] = res["Value"][0][0]

        return S_OK(resultDict)
