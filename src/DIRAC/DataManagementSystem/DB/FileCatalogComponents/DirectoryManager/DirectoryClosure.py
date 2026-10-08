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
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryTreeBase import DirectoryTreeBase
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import executeInTransaction

# Queries used to compute the directory sizes, indexed on the recursiveSum flag
# Logical size, as stored in FC_DirectoryUsage
LOGICAL_SIZE_FROM_USAGE_QUERIES = {
    True: (
        "SELECT COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0) FROM FC_DirectoryUsage u "
        "JOIN FC_DirectoryClosure c ON c.ChildID = u.DirID "
        "JOIN FC_StorageElements s ON s.SEID = u.SEID "
        "WHERE s.SEName = 'FakeSE' AND c.ParentID = %s"
    ),
    False: (
        "SELECT COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0) FROM FC_DirectoryUsage u "
        "JOIN FC_StorageElements s ON s.SEID = u.SEID "
        "WHERE s.SEName = 'FakeSE' AND u.DirID = %s"
    ),
}

# Logical size, calculated from FC_Files
LOGICAL_SIZE_CALCULATED_QUERIES = {
    True: (
        "SELECT COALESCE(SUM(f.Size), 0), COUNT(*) FROM FC_Files f "
        "JOIN FC_DirectoryClosure d ON f.DirID = d.ChildID "
        "WHERE d.ParentID = %s"
    ),
    False: "SELECT COALESCE(SUM(f.Size), 0), COUNT(*) FROM FC_Files f WHERE f.DirID = %s",
}

# Physical size per SE, as stored in FC_DirectoryUsage
PHYSICAL_SIZE_FROM_USAGE_QUERIES = {
    True: (
        "SELECT se.SEName, COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0) FROM FC_DirectoryUsage u "
        "JOIN FC_DirectoryClosure c ON u.DirID = c.ChildID "
        "JOIN FC_StorageElements se ON se.SEID = u.SEID "
        "WHERE c.ParentID = %s AND se.SEName != 'FakeSE' AND (SESize != 0 OR SEFiles != 0) "
        "GROUP BY se.SEName ORDER BY NULL"
    ),
    False: (
        "SELECT se.SEName, COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0) FROM FC_DirectoryUsage u "
        "JOIN FC_StorageElements se ON se.SEID = u.SEID "
        "WHERE u.DirID = %s AND se.SEName != 'FakeSE' AND (SESize != 0 OR SEFiles != 0) "
        "GROUP BY se.SEName ORDER BY NULL"
    ),
}

# Physical size per SE, calculated from FC_Replicas
PHYSICAL_SIZE_CALCULATED_QUERIES = {
    True: (
        "SELECT se.SEName, COALESCE(SUM(f.Size), 0), COUNT(*) FROM FC_Replicas r "
        "JOIN FC_Files f ON f.FileID = r.FileID "
        "JOIN FC_StorageElements se ON se.SEID = r.SEID "
        "JOIN FC_DirectoryClosure dc ON dc.ChildID = f.DirID "
        "WHERE dc.ParentID = %s "
        "GROUP BY se.SEName ORDER BY NULL"
    ),
    False: (
        "SELECT se.SEName, COALESCE(SUM(f.Size), 0), COUNT(*) FROM FC_Replicas r "
        "JOIN FC_Files f ON f.FileID = r.FileID "
        "JOIN FC_StorageElements se ON se.SEID = r.SEID "
        "WHERE f.DirID = %s "
        "GROUP BY se.SEName ORDER BY NULL"
    ),
}

# Columns of FC_DirectoryList that can be set by _setDirectoryParameter
DIRECTORY_PARAMETER_COLUMNS = {"UID": "UID", "GID": "GID", "Status": "Status", "Mode": "Mode"}


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
        result = self.db._query("SELECT DirID FROM FC_DirectoryList WHERE Name = %s", args=(dpath,))
        if not result["OK"]:
            return result

        if not result["Value"]:
            res = S_OK(0)
            res["Level"] = None
            return res

        dirID = result["Value"][0][0]

        result = self.db._query("SELECT MAX(Depth) FROM FC_DirectoryClosure WHERE ChildID = %s", args=(dirID,))
        if not result["OK"]:
            return result

        res = S_OK(dirID)
        res["Level"] = result["Value"][0][0]
        return res

    def findDirs(self, paths, connection=False):
        """Find DirIDs for the given path list

        :param paths: list of path

        :returns: S_OK( { path : ID} )
        """

        dirDict = {}
        if not paths:
            return S_OK(dirDict)
        dpaths = [os.path.normpath(path) for path in paths]
        req = f"SELECT Name, DirID FROM FC_DirectoryList WHERE Name IN ({','.join(['%s'] * len(dpaths))})"  # nosec B608
        result = self.db._query(req, args=dpaths)
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
        # Because of the cascade, it also deletes the FC_DirectoryClosure and FC_DirectoryUsage entries
        result = self.db._update("DELETE FROM FC_DirectoryList WHERE DirID = %s", args=(dirId,))
        if not result["OK"]:
            return result

        res = S_OK()
        res["DirID"] = dirId
        return res

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

        result = self.db._query("SELECT Name FROM FC_DirectoryList WHERE DirID = %s", args=(dirID,))
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("Directory with id %d not found" % int(dirID))

        return S_OK(result["Value"][0][0])

    def getDirectoryPaths(self, dirIDList):
        """Get directory names by directory ID list

        :param dirIDList: list of dirIds
        :returns: S_OK( { dirID : dirName} )
        """

        dirs = dirIDList
        if not isinstance(dirIDList, list):
            dirs = [dirIDList]

        dirDict = {}
        if not dirs:
            return S_OK(dirDict)

        dIds = [int(dirId) for dirId in dirs]
        req = f"SELECT DirID, Name FROM FC_DirectoryList WHERE DirID IN ({','.join(['%s'] * len(dIds))})"  # nosec B608
        result = self.db._query(req, args=dIds)
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

        result = self.db._query(
            "SELECT ParentID FROM FC_DirectoryClosure WHERE ChildID = %s ORDER BY Depth DESC", args=(dirID,)
        )

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

        result = self.db._query(
            "SELECT ChildID FROM FC_DirectoryClosure WHERE ParentID = %s AND Depth = 1", args=(dirID,)
        )
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
            reqStr = "SELECT ChildID FROM FC_DirectoryClosure "
            reqStr += f"WHERE ParentID = {int(dirID)}"
            if not includeParent:
                reqStr += " AND Depth != 0"
            return S_OK(reqStr)

        req = (
            "SELECT c1.ChildID, MAX(c1.Depth) AS lvl FROM FC_DirectoryClosure c1 "
            "JOIN FC_DirectoryClosure c2 ON c1.ChildID = c2.ChildID "
            "WHERE c2.ParentID = %s"
        )
        if not includeParent:
            req += " AND c2.Depth != 0"
        req += " GROUP BY c1.ChildID ORDER BY NULL"

        result = self.db._query(req, args=(dirID,))
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

        if not dirs:
            return S_OK([])

        dIds = [int(dirId) for dirId in dirs]
        req = f"SELECT DISTINCT(ChildID) FROM FC_DirectoryClosure WHERE ParentID IN ({','.join(['%s'] * len(dIds))})"  # nosec B608
        result = self.db._query(req, args=dIds)

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

        result = self.db._query("SELECT COUNT(ChildID) FROM FC_DirectoryClosure WHERE ParentID = %s", args=(dirId,))
        if not result["OK"]:
            return result

        countDir = result["Value"][0][0]
        # The directory itself is in the closure table with Depth 0
        if not includeParent and countDir:
            countDir -= 1

        return S_OK(countDir)

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
            mode = self.db.umask

            def _insertDir(cursor):
                """Insert the directory and its closure entries"""
                cursor.execute(
                    "INSERT INTO FC_DirectoryList (UID, GID, CreationDate, ModificationDate, Mode, Status, Name) "
                    "VALUES (%s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s, %s, %s)",
                    (l_uid, l_gid, mode, status, dpath),
                )
                dirId = cursor.lastrowid

                cursor.execute(
                    "INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth) VALUES (%s, %s, 0)", (dirId, dirId)
                )

                if parentDirId:
                    cursor.execute(
                        "INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth) "
                        "SELECT p.ParentID, %s, p.Depth + 1 FROM FC_DirectoryClosure p WHERE p.ChildID = %s",
                        (dirId, parentDirId),
                    )
                return dirId

            result = executeInTransaction(self.db, _insertDir)
            if not result["OK"]:
                return result

            dirId = result["Value"]

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
        req = (
            "SELECT d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, "
            "d.ModificationDate FROM FC_DirectoryList d "
            "JOIN FC_Users u ON d.UID = u.UID "
            "JOIN FC_Groups g ON d.GID = g.GID "
        )
        # it is a path ...
        if isinstance(pathOrDirId, str):
            req += "WHERE d.Name = %s"
        # it is the dirId
        elif isinstance(pathOrDirId, ((list,) + (int,))):
            req += "WHERE d.DirID = %s"
        else:
            return S_ERROR(f"Unknown type of pathOrDirId {type(pathOrDirId)}")

        result = self.db._query(req, args=(pathOrDirId,))
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

        column = DIRECTORY_PARAMETER_COLUMNS.get(pname)

        # If it is a known parameter, we go for it
        if column:
            # Apply recursively on the subdirectories and the files they contain
            if recursive and pname in ["UID", "GID", "Mode"]:
                result = self.db._query("SELECT DirID FROM FC_DirectoryList WHERE Name = %s", args=(path,))
                if not result["OK"]:
                    return result
                startDirID = result["Value"][0][0] if result["Value"] else 0

                result = self.db._update(
                    f"UPDATE FC_DirectoryList d JOIN FC_DirectoryClosure c ON d.DirID = c.ChildID "  # nosec B608
                    f"SET d.{column} = %s, d.ModificationDate = UTC_TIMESTAMP() WHERE c.ParentID = %s",
                    args=(pvalue, startDirID),
                )
                if not result["OK"]:
                    return result
                dirUpdate = result["Value"]

                result = self.db._update(
                    f"UPDATE FC_Files f JOIN FC_DirectoryClosure c ON f.DirID = c.ChildID "  # nosec B608
                    f"SET f.{column} = %s, f.ModificationDate = UTC_TIMESTAMP() WHERE c.ParentID = %s",
                    args=(pvalue, startDirID),
                )
                if not result["OK"]:
                    return result
                fileUpdate = result["Value"]

                affected = dirUpdate + fileUpdate
            else:
                result = self.db._update(
                    f"UPDATE FC_DirectoryList SET {column} = %s, ModificationDate = UTC_TIMESTAMP() WHERE Name = %s",  # nosec B608
                    args=(pvalue, path),
                )
                if not result["OK"]:
                    return result
                affected = result["Value"]

            if not affected:
                # Either there were no changes, or the directory does not exist
                exists = self.existsDir(path).get("Value", {}).get("Exists")
                if not exists:
                    return S_ERROR(errno.ENOENT, f"Directory does not exist: {path}")
                affected = 1

            return S_OK(affected)

        # In case this is a 'new' parameter, we have a fallback solution
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

    def __getLogicalSize(self, lfns, queries, recursiveSum=True, connection=None):
        successful = {}
        failed = {}
        for path in lfns:
            result = self.findDir(path)
            if not result["OK"] or not result["Value"]:
                failed[path] = "Directory not found"
                continue

            dirID = result["Value"]
            result = self.db._query(queries[bool(recursiveSum)], args=(dirID,))

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
        return self.__getLogicalSize(
            lfns, LOGICAL_SIZE_FROM_USAGE_QUERIES, recursiveSum=recursiveSum, connection=connection
        )

    def _getDirectoryLogicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total "logical" size of the requested directories"""
        return self.__getLogicalSize(
            lfns, LOGICAL_SIZE_CALCULATED_QUERIES, recursiveSum=recursiveSum, connection=connection
        )

    def __getPhysicalSize(self, lfns, queries, recursiveSum=True, connection=None):
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

            result = self.db._query(queries[bool(recursiveSum)], args=(dirID,))
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
            lfns, PHYSICAL_SIZE_FROM_USAGE_QUERIES, recursiveSum=recursiveSum, connection=connection
        )

    def _getDirectoryPhysicalSize(self, lfns, recursiveSum=True, connection=None):
        """Get the total size of the requested directories"""
        return self.__getPhysicalSize(
            lfns, PHYSICAL_SIZE_CALCULATED_QUERIES, recursiveSum=recursiveSum, connection=None
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

        # Directories have a NULL size
        req = (
            "(SELECT d.Name, NULL, d.CreationDate FROM FC_DirectoryList d "
            "JOIN FC_DirectoryClosure c ON d.DirID = c.ChildID "
            "WHERE c.ParentID = %s AND c.Depth != 0) "
            "UNION ALL "
            "(SELECT CONCAT(d.Name, '/', f.FileName), f.Size, f.CreationDate FROM FC_Files f "
            "JOIN FC_DirectoryList d ON f.DirID = d.DirID "
            "JOIN FC_DirectoryClosure c ON c.ChildID = f.DirID "
            "WHERE c.ParentID = %s)"
        )
        result = self.db._query(req, args=(dirID, dirID))

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
