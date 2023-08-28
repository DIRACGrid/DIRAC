""" DIRAC FileCatalog component representing a directory tree with
    a closure table

    General warning: when we return the number of affected row, if the values did not change
                     then they are not taken into account, so we might return "Dir does not exist"
                     while it does.... the timestamp update should prevent this to happen, however if
                     you do it several times within 1 second, then there will be no changed, and affected = 0

"""
import errno
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString, stringListToString
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryTreeBase import DirectoryTreeBase


class DirectoryClosure(DirectoryTreeBase):
    """Class managing Directory Tree with a closure table
    http://technobytz.com/closure_table_store_hierarchical_data.html
    http://fungus.teststation.com/~jon/treehandling/TreeHandling.htm
    http://www.slideshare.net/billkarwin/sql-antipatterns-strike-back
    http://dirtsimple.org/2010/11/simplest-way-to-do-tree-based-queries.html
    """

    def __init__(self, database=None):
        DirectoryTreeBase.__init__(self, database)
        self.directoryTable = "FC_DirectoryList"
        self.closureTable = "FC_DirectoryClosure"

    def findDir(self, path, connection=False):
        """Find directory ID for the given path

        :param path: path of the directory

        :returns: S_OK(id) and res['Level'] as the depth
        """

        dpath = os.path.normpath(path)
        result = self.db.executeStoredProcedure("ps_find_dir", (dpath, "ret1", "ret2"), outputIds=[1, 2])
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK(0)

        res = S_OK(result["Value"][0])
        res["Level"] = result["Value"][1]
        return res

    def findDirs(self, paths, connection=False):
        """Find DirIDs for the given path list

        :param paths: list of path

        :returns: S_OK( { path : ID} )
        """

        dirDict = {}
        if not paths:
            return S_OK(dirDict)
        dpaths = stringListToString([os.path.normpath(path) for path in paths])
        result = self.db.executeStoredProcedureWithCursor("ps_find_dirs", (dpaths,))
        if not result["OK"]:
            return result
        for dirName, dirID in result["Value"]:
            dirDict[dirName] = dirID

        return S_OK(dirDict)

    def removeDir(self, path):
        """Remove directory

        Removing a non existing directory is successful. In that case, DirID is 0

        :param path: path of the dir

        :returns: S_OK() and res['DirID'] the id of the directory removed
        """

        # Find the directory ID
        result = self.findDir(path)
        if not result["OK"]:
            return result

        # If the directory does not exist, we exit successfully with DirID = 0
        if not result["Value"]:
            res = S_OK()
            res["DirID"] = 0
            return res

        dirId = result["Value"]
        result = self.db.executeStoredProcedure("ps_remove_dir", (dirId,), outputIds=[])
        if not result["OK"]:
            return result

        result["DirID"] = dirId
        return result

    def existsDir(self, path):
        """Check the existence of a directory at the specified path

        :param path: directory path

        :returns: S_OK( { 'Exists' : False } ) if the directory does not exist
                  S_OK( { 'Exists' : True, 'DirID' : directory id  } ) if the directory exists
        """

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({"Exists": False})
        else:
            return S_OK({"Exists": True, "DirID": result["Value"]})

    def getDirectoryPath(self, dirID):
        """Get directory name by directory ID

        :param dirID: directory ID

        :returns: S_OK(dir name), or S_ERROR if it does not exist

        """

        result = self.db.executeStoredProcedure("ps_get_dirName_from_id", (dirID, "out"), outputIds=[1])
        if not result["OK"]:
            return result

        dirName = result["Value"][0]

        if not dirName:
            return S_ERROR("Directory with id %d not found" % int(dirID))

        return S_OK(dirName)

    def getDirectoryPaths(self, dirIDList):
        """Get directory names by directory ID list

        :param dirIDList: list of dirIds
        :returns: S_OK( { dirID : dirName} )
        """

        dirs = dirIDList
        if not isinstance(dirIDList, list):
            dirs = [dirIDList]

        dirDict = {}

        # Format the list
        dIds = intListToString(dirs)
        result = self.db.executeStoredProcedureWithCursor("ps_get_dirNames_from_ids", (dIds,))
        if not result["OK"]:
            return result

        for dirId, dirName in result["Value"]:
            dirDict[dirId] = dirName

        return S_OK(dirDict)

    def getPathIDs(self, path):
        """Get IDs of all the directories in the parent hierarchy for a directory
        specified by its path, including itself

        :param path: path of the directory

        :returns: S_OK( list of ids ), S_ERROR if not found
        """

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Directory {path} not found")

        dirID = result["Value"]

        return self.getPathIDsByID(dirID)

    def getPathIDsByID(self, dirID):
        """Get IDs of all the directories in the parent hierarchy for a directory
        specified by its ID, including itself

        :param dirID: id of the dictionary

        :returns: S_OK( list of ids )

        """

        result = self.db.executeStoredProcedureWithCursor("ps_get_parentIds_from_id", (dirID,))

        if not result["OK"]:
            return result

        return S_OK([dId[0] for dId in result["Value"]])

    def getChildren(self, path, connection=False):
        """Get child directory IDs for the given directory"""
        if isinstance(path, str):
            result = self.findDir(path, connection=connection)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR(f"Directory does not exist: {path}")
            dirID = result["Value"]
        else:
            dirID = path

        result = self.db.executeStoredProcedureWithCursor("ps_get_direct_children", (dirID,))
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        return S_OK([x[0] for x in result["Value"]])

    def getSubdirectoriesByID(self, dirID, requestString=False, includeParent=False):
        """Get all the subdirectories of the given directory at a given level

        :param dirID: id of the directory
        :param requestString: if true, returns an sql query to get the information
        :param includeParent: if true, the parent (dirID) will be included

        :returns: S_OK ( { dirID, depth } ) if requestString is False
                 S_OK(request) if requestString is True

        """

        if requestString:
            reqStr = f"SELECT ChildID FROM FC_DirectoryClosure WHERE ParentID = {dirID}"
            if not includeParent:
                reqStr += " AND Depth != 0"
            return S_OK(reqStr)

        result = self.db.executeStoredProcedureWithCursor("ps_get_sub_directories", (dirID, includeParent))
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({})

        return S_OK({x[0]: x[1] for x in result["Value"]})

    def getAllSubdirectoriesByID(self, dirIdList):
        """Get IDs of all the subdirectories of directories in a given list

        :param dirList: list of dir Ids
        :returns: S_OK([ unordered dir ids ])
        """

        dirs = dirIdList
        if not isinstance(dirIdList, list):
            dirs = [dirIdList]

        dIds = intListToString(dirs)
        result = self.db.executeStoredProcedureWithCursor("ps_get_multiple_sub_directories", (dIds,))

        if not result["OK"]:
            return result

        resultList = [dirId[0] for dirId in result["Value"]]
        return S_OK(resultList)

    def getSubdirectories(self, path):
        """Get subdirectories of the given directory

        :param path: path of the directory

        :returns: S_OK ( { dirID, depth } )
        """

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({})

        dirID = result["Value"]
        result = self.getSubdirectoriesByID(dirID)
        return result

    def countSubdirectories(self, dirId, includeParent=True):
        """Count the number of subdirectories

        :param dirID: id of the directory
        :param includeParent: count itself

        :returns: S_OK(value)
        """

        result = self.db.executeStoredProcedure(
            "ps_count_sub_directories", (dirId, includeParent, "ret1"), outputIds=[2]
        )
        if not result["OK"]:
            return result

        res = S_OK(result["Value"][0])
        return res

    ########################################################################################################
    #
    #  We overwrite some methods from the base class because of the new DB constraints or perf reasons
    #
    #  Some methods could be inherited in the future if we have perf problems. For example
    #  * removeDirectory
    #  * changeDirectory[Group/Owner/Mode]
    #  * getDirectoryPermissions (when called by getPathPermissions, we could buffer the getUserAndGroupID call)
    #  * getFileIDsInDirectory (used only by DirectoryMetadata)
    #  * getFilesInDirectory (used only by DirectoryMetadata)
    #  * getFileLFNsInDirectory (used only by FileMetadata)
    #  * getFileLFNsInDirectoryByDirectory (used only by FileMetadata)
    #  * _getDirectoryContents (we could bring together some requests)
    #
    ########################################################################################################

    def makeDirectory(self, path, credDict, status=1):
        """Create a directory

        :param path: has to be an absolute path. The parent dir has to exist
        :param credDict: credential dict of the owner of the directory
        :param: status ????

        :returns: S_OK (dirID) with a flag res['NewDirectory'] to True or False
                S_ERROR if there is a problem, or if there is no parent
        """

        if path[0] != "/":
            return S_ERROR("Not an absolute path")

        # Strip off the trailing slash if necessary
        dpath = os.path.normpath(path)
        parentDir = os.path.dirname(dpath)

        # Try to see if the dir exists
        result = self.findDir(path)
        if not result["OK"]:
            return result

        # if it does, we return it's id, with a flag NewDirectory to false
        dirID = result["Value"]
        if dirID:
            result = S_OK(dirID)
            result["NewDirectory"] = False
            return result

        # If it is the root directory, we force the owner to 'root'/'root' (id 1 in the db)
        if path == "/":
            l_uid = 1
            l_gid = 1
        else:
            # get the uid/gid of the owner
            result = self.db.ugManager.getUserAndGroupID(credDict)
            if not result["OK"]:
                return result
            (l_uid, l_gid) = result["Value"]

        # Find the ID of the parent
        res = self.findDir(parentDir)
        if not res["OK"]:
            return res

        parentDirId = res["Value"]

        # We only insert if there is a parent or if it is the root '/'
        if parentDirId or path == "/":
            result = self.db.executeStoredProcedureWithCursor(
                packageName="ps_insert_dir", parameters=(parentDirId, dpath, l_uid, l_gid, self.db.umask, status)
            )

            if not result["OK"]:
                return result

            dirId = result["Value"][0][0]

            result = S_OK(dirId)
            result["NewDirectory"] = True
            return result
        else:
            return S_ERROR("Cannot create directory without parent")

    def isEmpty(self, path):
        """Find out if the given directory is empty

        Rem: the speed could be enhanced if we were joining the FC_Files and FC_Directory* in the query.
            For the time being, it can stay like this

        :param path: path of the directory

        :returns: S_OK(true) if there are no file nor directorie, S_OK(False) otherwise
        """

        result = self.findDir(path)
        if not result["OK"]:
            return result
        dirId = result["Value"]

        if not dirId:
            return S_ERROR(f"Directory does not exist {path}")

        # Check if there are subdirectories
        result = self.countSubdirectories(dirId, includeParent=False)
        if not result["OK"]:
            return result

        subDirCount = result["Value"]
        if subDirCount:
            return S_OK(False)

        # Check if there are files in it
        result = self.db.fileManager.countFilesInDir(dirId)
        if not result["OK"]:
            return result

        fileCount = result["Value"]

        if fileCount:
            return S_OK(False)

        # If no files or subdir, it's empty
        return S_OK(True)

    def getDirectoryParameters(self, pathOrDirId):
        """Get parameters of the given directory

        :param pathOrDirID: the path or the id of the directory

        :returns: S_OK(dict), where dict has the following keys:
                        "DirID", "UID", "Owner", "GID", "OwnerGroup", "Status", "Mode", "CreationDate", "ModificationDate"
        """
        # Which procedure to use
        psName = None
        # it is a path ...
        if isinstance(pathOrDirId, str):
            psName = "ps_get_all_directory_info"
        # it is the dirId
        elif isinstance(pathOrDirId, ((list,) + (int,))):
            psName = "ps_get_all_directory_info_from_id"
        else:
            return S_ERROR(f"Unknown type of pathOrDirId {type(pathOrDirId)}")

        result = self.db.executeStoredProcedureWithCursor(psName, (pathOrDirId,))
        if not result["OK"]:
            return result

        # All the fields returned
        fieldNames = [
            "DirID",
            "UID",
            "Owner",
            "GID",
            "OwnerGroup",
            "Status",
            "Mode",
            "CreationDate",
            "ModificationDate",
        ]

        if not result["Value"]:
            return S_ERROR(f"Directory does not exist {pathOrDirId}")

        row = result["Value"][0]

        # Create a dictionary from the fieldNames
        rowDict = dict(zip(fieldNames, row))

        return S_OK(rowDict)

    def _setDirectoryParameter(self, path, pname, pvalue, recursive=False):
        """Set a numerical directory parameter


        Rem: the parent class has a more generic method, which is called
             in case we are given an unknown parameter

        :param path: path of the directory
        :param pname: name of the parameter to set
        :param pvalue: value of the parameter (an id or a value)

        :returns: S_OK(nb of row changed). It should always be 1 !
                S_ERROR if the directory does not exist
        """

        # The PS associated with a given parameter
        psNames = {
            "UID": "ps_set_dir_uid",
            "GID": "ps_set_dir_gid",
            "Status": "ps_set_dir_status",
            "Mode": "ps_set_dir_mode",
        }

        psName = psNames.get(pname, None)

        # If we have a recursive procedure and it is wanted, call it
        if recursive and pname in ["UID", "GID", "Mode"] and psName:
            psName += "_recursive"

        # If there is an associated procedure, we go for it
        if psName:
            result = self.db.executeStoredProcedureWithCursor(psName, (path, pvalue))

            if not result["OK"]:
                return result

            errno, affected, errMsg = result["Value"][0]
            if errno:
                return S_ERROR(errMsg)

            if not affected:
                # Either there were no changes, or the directory does not exist
                exists = self.existsDir(path).get("Value", {}).get("Exists")
                if not exists:
                    return S_ERROR(errno.ENOENT, f"Directory does not exist: {path}")
                affected = 1

            return S_OK(affected)

        # In case this is a 'new' parameter, we have a fallback solution, but we should add a specific ps for it
        else:
            return DirectoryTreeBase._setDirectoryParameter(self, path, pname, pvalue)

    def _setDirectoryGroup(self, path, gname, recursive=False):
        """Set the directory owner"""

        result = self.db.ugManager.findGroup(gname)
        if not result["OK"]:
            return result

        gid = result["Value"]

        return self._setDirectoryParameter(path, "GID", gid, recursive=recursive)

    def _setDirectoryOwner(self, path, owner, recursive=False):
        """Set the directory owner"""

        result = self.db.ugManager.findUser(owner)
        if not result["OK"]:
            return result

        uid = result["Value"]

        return self._setDirectoryParameter(path, "UID", uid, recursive=recursive)

    def _setDirectoryMode(self, path, mode, recursive=False):
        """set the directory mode

        :param mixed path: directory path as a string or int or list of ints or select statement
        :param int mode: new mode
        """
        return self._setDirectoryParameter(path, "Mode", mode, recursive=recursive)

    def __getLogicalSize(self, lfns, ps_name, recursiveSum=True, connection=None):
        successful = {}
        failed = {}
        for path in lfns:
            result = self.findDir(path)
            if not result["OK"] or not result["Value"]:
                failed[path] = "Directory not found"
                continue

            dirID = result["Value"]
            result = self.db.executeStoredProcedureWithCursor(ps_name, (dirID, recursiveSum))

            if not result["OK"]:
                failed[path] = result["Message"]

            elif result["Value"]:
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

    def _getDirectoryLogicalSizeFromUsage(self, lfns, recursiveSum=True, connection=None):
        """Get the total "logical" size of the requested directories"""
        return self.__getLogicalSize(lfns, "ps_get_dir_logical_size", recursiveSum=recursiveSum, connection=connection)

    def _getDirectoryLogicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total "logical" size of the requested directories"""
        return self.__getLogicalSize(
            lfns, "ps_calculate_dir_logical_size", recursiveSum=recursiveSum, connection=connection
        )

    def __getPhysicalSize(self, lfns, ps_name, recursiveSum=True, connection=None):
        """Get the total size of the requested directories"""

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

            result = self.db.executeStoredProcedureWithCursor(ps_name, (dirID, recursiveSum))
            if not result["OK"]:
                failed[path] = result["Message"]
                continue

            if result["Value"]:
                seDict = {}
                totalSize = 0
                totalFiles = 0
                for seName, seSize, seFiles in result["Value"]:
                    seDict[seName] = {"Size": int(seSize), "Files": int(seFiles)}
                    totalSize += seSize
                    totalFiles += seFiles
                seDict["TotalSize"] = int(totalSize)
                seDict["TotalFiles"] = int(totalFiles)
                successful[path] = seDict

            else:
                successful[path] = {}

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryPhysicalSizeFromUsage(self, lfns, recursiveSum=True, connection=None):
        """Get the total size of the requested directories"""
        return self.__getPhysicalSize(
            lfns, "ps_get_dir_physical_size", recursiveSum=recursiveSum, connection=connection
        )

    def _getDirectoryPhysicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total size of the requested directories"""
        return self.__getPhysicalSize(
            lfns, "ps_calculate_dir_physical_size", recursiveSum=recursiveSum, connection=None
        )

    def _changeDirectoryParameter(self, paths, directoryFunction, _fileFunction, recursive=False):
        """Bulk setting of the directory parameter with recursion for all the subdirectories and files

        :param dict paths: dictionary < lfn : value >, where value is the value of parameter to be set
        :param function directoryFunction: function to change directory(ies) parameter
        :param function fileFunction: function to change file(s) parameter
        :param bool recursive: flag to apply the operation recursively
        """

        arguments = paths
        successful = {}
        failed = {}
        for path, attribute in arguments.items():
            result = directoryFunction(path, attribute, recursive=recursive)
            if not result["OK"]:
                failed[path] = result["Message"]
            else:
                successful[path] = True

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryDump(self, path):
        """Recursively dump all the content of a directory

        :param str path: directory to dump

        :returns: dictionary with `Files` and `SubDirs` as keys
                    `Files` is a dict containing files metadata.
                    `SubDirs` is a list of directory
        """

        result = self.findDir(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]
        if not dirID:
            return S_ERROR(errno.ENOENT, f"{path} does not exist")

        result = self.db.executeStoredProcedureWithCursor("ps_get_directory_dump", (dirID,))

        if not result["OK"]:
            return result

        rows = result["Value"]
        files = {}
        subDirs = []

        for lfn, size, creationDate in rows:
            if size is None:
                subDirs.append(lfn)
            else:
                files[lfn] = {"Size": int(size), "CreationDate": creationDate}

        return S_OK({"Files": files, "SubDirs": subDirs})
