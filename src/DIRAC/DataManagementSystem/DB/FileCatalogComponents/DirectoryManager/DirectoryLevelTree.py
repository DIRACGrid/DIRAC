""" DIRAC FileCatalog component representing a directory tree with enumerated paths
"""
# pylint: disable=protected-access
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryTreeBase import DirectoryTreeBase

MAX_LEVELS = 15


class DirectoryLevelTree(DirectoryTreeBase):
    """Class managing Directory Tree as a simple self-linked structure
    with full directory path stored in each node
    """

    def __init__(self, database=None):
        DirectoryTreeBase.__init__(self, database)
        self.treeTable = "FC_DirectoryLevelTree"

    def getTreeType(self):

        return "Directory"

    def findDir(self, path, connection=False):
        """Find directory ID for the given path"""

        dpath = self.db._escapeString(os.path.normpath(path))
        if not dpath["OK"]:
            return dpath
        dpath = dpath["Value"]
        req = "SELECT DirID,Level from FC_DirectoryLevelTree WHERE DirName=%s" % dpath
        result = self.db._query(req, conn=connection)
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK("")

        res = S_OK(result["Value"][0][0])
        res["Level"] = result["Value"][0][1]
        return res

    def findDirs(self, paths, connection=False):
        """Find DirIDs for the given path list"""
        dpathList = []
        for path in paths:
            dpath = self.db._escapeString(os.path.normpath(path))
            if not dpath["OK"]:
                return dpath
            dpathList.append(dpath["Value"])
        dpaths = ",".join(dpathList)
        req = "SELECT DirName,DirID from FC_DirectoryLevelTree WHERE DirName in (%s)" % dpaths
        result = self.db._query(req, conn=connection)
        if not result["OK"]:
            return result
        dirDict = {}
        for dirName, dirID in result["Value"]:
            dirDict[dirName] = dirID

        return S_OK(dirDict)

    def removeDir(self, path):
        """Remove directory"""

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            res = S_OK()
            res["DirID"] = 0
            return res

        dirID = result["Value"]
        req = "DELETE FROM FC_DirectoryLevelTree WHERE DirID=%d" % dirID
        result = self.db._update(req)
        result["DirID"] = dirID
        return result

    def __getNumericPath(self, dirID, connection=False):
        """Get the enumerated path of the given directory"""
        epathString = ",".join(["LPATH%d" % (i + 1) for i in range(MAX_LEVELS)])
        req = "SELECT LEVEL,%s FROM FC_DirectoryLevelTree WHERE DirID=%d" % (epathString, dirID)
        result = self.db._query(req, conn=connection)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        row = result["Value"][0]
        level = row[0]
        epathList = []
        for i in range(level):
            epathList.append(row[i + 1])

        result = S_OK(epathList)
        result["Level"] = level
        return result

    def makeDir(self, path):
        """Create a new directory entry"""
        result = self.findDir(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]
        if dirID:
            result = S_OK(dirID)
            result["NewDirectory"] = False
            return result

        dpath = path
        if path == "/":
            level = 0
            elements = []
            parentDirID = 0
        else:
            if path[0] == "/":
                dpath = path[1:]
            elements = dpath.split("/")
            level = len(elements)
            if level > MAX_LEVELS:
                return S_ERROR("Too many directory levels: %d" % level)
            result = self.getParent(path)
            if not result["OK"]:
                return result
            parentDirID = result["Value"]

        epathList = []
        if parentDirID:
            result = self.__getNumericPath(parentDirID)
            if not result["OK"]:
                return result
            epathList = result["Value"]

        names = ["DirName", "Level", "Parent"]
        values = [path, level, parentDirID]
        if path != "/":
            for i in range(1, level, 1):
                names.append("LPATH%d" % i)
                values.append(epathList[i - 1])

        result = self.db._getConnection()
        conn = result["Value"]
        # result = self.db._query("LOCK TABLES FC_DirectoryLevelTree WRITE; ", conn=conn)
        result = self.db.insertFields("FC_DirectoryLevelTree", names, values, conn=conn)
        if not result["OK"]:
            # resUnlock = self.db._query("UNLOCK TABLES;", conn=conn)
            if result["Message"].find("Duplicate") != -1:
                # The directory is already added
                resFind = self.findDir(path)
                if not resFind["OK"]:
                    return resFind
                dirID = resFind["Value"]
                result = S_OK(dirID)
                result["NewDirectory"] = False
                return result
            else:
                return result
        dirID = result["lastRowId"]

        # Update the path number
        if parentDirID:
            #       lPath = "LPATH%d" % (level)
            #       req = " SELECT @tmpvar:=max(%s)+1 FROM FC_DirectoryLevelTree WHERE Parent=%d; " % (lPath,parentDirID)
            #       resultLock = self.db._query("LOCK TABLES FC_DirectoryLevelTree WRITE; ", conn=conn)
            #       result = self.db._query(req, conn=conn)
            #       req = "UPDATE FC_DirectoryLevelTree SET %s=@tmpvar WHERE DirID=%d; " % (lPath,dirID)
            #       result = self.db._update(req, conn=conn)
            #       result = self.db._query("UNLOCK TABLES;", conn=conn)
            lPath = "LPATH%d" % (level)
            req = " SELECT @tmpvar:=max(%s)+1 FROM FC_DirectoryLevelTree WHERE Parent=%d FOR UPDATE; " % (
                lPath,
                parentDirID,
            )
            resultLock = self.db._query("START TRANSACTION; ", conn=conn)
            result = self.db._query(req, conn=conn)
            req = "UPDATE FC_DirectoryLevelTree SET %s=@tmpvar WHERE DirID=%d; " % (lPath, dirID)
            result = self.db._update(req, conn=conn)
            result = self.db._query("COMMIT;", conn=conn)
            if not result["OK"]:
                return result
        else:
            result = self.db._query("ROLLBACK;", conn=conn)

        result = S_OK(dirID)
        result["NewDirectory"] = True
        return result

    def existsDir(self, path):
        """Check the existence of a directory at the specified path"""
        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({"Exists": False})
        return S_OK({"Exists": True, "DirID": result["Value"]})

    def getParent(self, path):
        """Get the parent ID of the given directory"""

        parent_dir = os.path.dirname(path)
        return self.findDir(parent_dir)

    def getParentID(self, dirPathOrID):
        """Get the ID of the parent of a directory specified by ID"""

        dirID = dirPathOrID
        if isinstance(dirPathOrID, str):
            result = self.findDir(dirPathOrID)
            if not result["OK"]:
                return result
            dirID = result["Value"]

        if dirID == 0:
            return S_ERROR("Root directory ID given")

        req = "SELECT Parent FROM FC_DirectoryLevelTree WHERE DirID=%d" % dirID
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("No parent found")

        return S_OK(result["Value"][0][0])

    def getDirectoryPath(self, dirID):
        """Get directory name by directory ID"""
        req = "SELECT DirName FROM FC_DirectoryLevelTree WHERE DirID=%d" % int(dirID)
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory with id %d not found" % int(dirID))

        return S_OK(result["Value"][0][0])

    def getDirectoryPaths(self, dirIDList):
        """Get directory name by directory ID list"""
        dirs = dirIDList
        if not isinstance(dirIDList, list):
            dirs = [dirIDList]

        if not dirs:
            return S_OK({})

        dirListString = ",".join([str(d) for d in dirs])
        req = "SELECT DirID,DirName FROM FC_DirectoryLevelTree WHERE DirID in ( %s )" % dirListString
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directories not found: %s" % dirListString)

        resultDict = {}
        for row in result["Value"]:
            resultDict[int(row[0])] = row[1]

        return S_OK(resultDict)

    def getDirectoryName(self, dirID):
        """Get directory name by directory ID"""

        result = self.getDirectoryPath(dirID)
        if not result["OK"]:
            return result

        return S_OK(os.path.basename(result["Value"]))

    def getPathIDs(self, path):
        """Get IDs of all the directories in the parent hierarchy for a directory
        specified by its path
        """

        elements = path.split("/")
        pelements = []
        dPath = ""
        for el in elements[1:]:
            dPath += "/" + el
            pelements.append(dPath)
        pelements.append("/")

        pathString = ["'" + p + "'" for p in pelements]
        req = "SELECT DirID FROM FC_DirectoryLevelTree WHERE DirName in (%s) ORDER BY DirID" % ",".join(pathString)
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory %s not found" % path)

        return S_OK([x[0] for x in result["Value"]])

    def getPathIDsByID_old(self, dirID):
        """Get IDs of all the directories in the parent hierarchy for a directory
        specified by its ID
        """

        # The method should be rather implemented using enumerated paths

        result = self.getDirectoryPath(dirID)
        if not result["OK"]:
            return result
        dPath = result["Value"]
        return self.getPathIDs(dPath)

    def getPathIDsByID(self, dirID):
        """Get IDs of all the directories in the parent hierarchy for a directory
        specified by its ID
        """
        result = self.__getNumericPath(dirID)
        if not result["OK"]:
            return result
        level = result["Level"]
        if level == 0:
            return S_OK([dirID])
        lpaths = result["Value"]

        lpathSelects = []
        for lev in range(level):
            sel = " AND ".join(["Level=%d" % lev] + ["LPATH%d=%d" % (ll + 1, lpaths[ll]) for ll in range(lev)])
            lpathSelects.append(sel)
        selection = "(" + ") OR (".join(lpathSelects) + ")"
        req = "SELECT Level,DirID from FC_DirectoryLevelTree WHERE %s ORDER BY Level" % selection
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("No result for the path of Directory with ID %d" % dirID)

        return S_OK([x[1] for x in result["Value"]] + [dirID])

    def getChildren(self, path, connection=False):
        """Get child directory IDs for the given directory"""
        if isinstance(path, str):
            result = self.findDir(path, connection=connection)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR("Directory does not exist: %s" % path)
            dirID = result["Value"]
        else:
            dirID = path
        req = "SELECT DirID FROM FC_DirectoryLevelTree WHERE Parent=%d" % dirID
        result = self.db._query(req, conn=connection)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        return S_OK([x[0] for x in result["Value"]])

    def getSubdirectoriesByID(self, dirID, requestString=False, includeParent=False):
        """Get all the subdirectories of the given directory at a given level"""
        req = "SELECT Level FROM FC_DirectoryLevelTree WHERE DirID=%d" % dirID
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory %d not found" % dirID)
        level = result["Value"][0][0]

        sPaths = []
        if requestString:
            req = "SELECT DirID FROM FC_DirectoryLevelTree"
        else:
            req = "SELECT Level,DirID FROM FC_DirectoryLevelTree"
        if level > 0:
            req += " AS F1"
            for i in range(1, level + 1):
                sPaths.append("LPATH%d" % i)
            pathString = ",".join(sPaths)
            req += " JOIN (SELECT %s FROM FC_DirectoryLevelTree WHERE DirID=%d) AS F2 ON " % (pathString, dirID)
            sPaths = []
            for i in range(1, level + 1):
                sPaths.append("F1.LPATH%d=F2.LPATH%d" % (i, i))
            pString = " AND ".join(sPaths)
            if includeParent:
                req += "%s AND F1.Level >= %d" % (pString, level)
            else:
                req += "%s AND F1.Level > %d" % (pString, level)

        if requestString:
            return S_OK(req)

        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({})

        resDict = {}
        for row in result["Value"]:
            resDict[row[1]] = row[0]

        return S_OK(resDict)

    def countSubdirectories(self, dirId, includeParent=True):
        result = self.getSubdirectoriesByID(dirId, requestString=True, includeParent=includeParent)
        if not result["OK"]:
            return result
        reqDir = result["Value"].replace("SELECT DirID FROM", "SELECT count(*) FROM")

        result = self.db._query(reqDir)
        if not result["OK"]:
            return result
        return S_OK(result["Value"][0][0])

    def getAllSubdirectoriesByID(self, dirList):
        """Get IDs of all the subdirectories of directories in a given list"""

        dirs = dirList
        if not isinstance(dirList, list):
            dirs = [dirList]
        resultList = []
        parentList = dirs
        while parentList:
            subResult = []
            dirListString = ",".join([str(d) for d in parentList])
            req = "SELECT DirID from FC_DirectoryLevelTree WHERE Parent in ( %s )" % dirListString
            result = self.db._query(req)
            if not result["OK"]:
                return result
            for row in result["Value"]:
                subResult.append(row[0])
            if subResult:
                resultList += subResult
            parentList = subResult

        return S_OK(resultList)

    def getSubdirectories(self, path):
        """Get subdirectories of the given directory"""

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({})

        dirID = result["Value"]
        result = self.getSubdirectoriesByID(dirID)
        return result

    def recoverOrphanDirectories(self, credDict):
        """Recover orphan directories"""
        # Find out orphan directories
        treeTable = "FC_DirectoryLevelTree"
        req = f"SELECT DirID,Parent,Level FROM {treeTable} WHERE Parent NOT IN ( SELECT DirID from {treeTable} )"
        result = self.db._query(req)
        if not result["OK"]:
            return result

        parentDict = {}
        for dirID, parentID, _level in result["Value"]:

            result = self.getDirectoryPath(dirID)
            if not result["OK"]:
                continue
            dirPath = result["Value"]
            parentPath = os.path.dirname(dirPath)
            if not dirPath == "/":
                parentDict.setdefault(parentPath, {})
                parentDict[parentPath].setdefault("DirList", [])
                parentDict[parentPath]["DirList"].append(dirID)
                parentDict[parentPath]["OldParentID"] = parentID

        for parentPath, dirDict in parentDict.items():
            dirIDList = dirDict["DirList"]
            oldParentID = dirDict["OldParentID"]
            result = self.findDir(parentPath)
            if not result["OK"]:
                continue
            if result["Value"]:
                # The parent directory was recreated already
                parentID = result["Value"]
            else:
                # The parent directory was lost
                result = self.makeDirectories(parentPath, credDict)
                if not result["OK"]:
                    continue
                parentID = result["Value"]
                # We have created a new directory but let's keep the old ID
                req = f"UPDATE FC_DirectoryLevelTree SET DirID={oldParentID} WHERE DirID={parentID}"
                result = self.db._update(req)
                if not result["OK"]:
                    continue
                req = f"UPDATE FC_DirectoryInfo SET DirID={oldParentID} WHERE DirID={parentID}"
                result = self.db._update(req)

                parentID = oldParentID
                # We have to change also the ownership of the new directory to the most likely one
                # which is the owner of the containing directory
                containerPath = os.path.dirname(parentPath)
                result = self.getDirectoryParameters(containerPath)
                if result["OK"]:
                    conDict = result["Value"]
                    uid = conDict["UID"]
                    gid = conDict["GID"]
                    result = self._setDirectoryParameter(parentID, "UID", uid)
                    result = self._setDirectoryParameter(parentID, "GID", gid)

            dirString = ",".join([str(dirID) for dirID in dirIDList])
            req = f"UPDATE FC_DirectoryLevelTree SET Parent={parentID} WHERE DirID IN ({dirString})"
            result = self.db._update(req)
            if not result["OK"]:
                continue

            connection = self._getConnection()
            result = self.db._query("LOCK TABLES FC_DirectoryLevelTree WRITE", conn=connection)
            if not result["OK"]:
                resUnlock = self.db._query("UNLOCK TABLES", conn=connection)
                return result
            result = self.__rebuildLevelIndexes(parentID, connection=connection)
            resUnlock = self.db._query("UNLOCK TABLES", conn=connection)

        return S_OK()

    def _getConnection(self, connection=False):
        if connection:
            return connection
        res = self.db._getConnection()
        if res["OK"]:
            return res["Value"]
        return connection

    def __rebuildLevelIndexes(self, parentID, connection=False):
        """Rebuild level indexes for all the subdirectories"""
        result = self.__getNumericPath(parentID, connection=connection)
        if not result["OK"]:
            return result

        parentIndexList = result["Value"]
        parentLevel = result["Level"]
        result = self.getChildren(parentID, connection=connection)
        if not result["OK"]:
            return result
        subIDList = result["Value"]

        indexList = list(parentIndexList)
        indexList.append(0)
        for dirID in subIDList:
            indexList[-1] += 1
            lpaths = ["LPATH%d=%d" % (i + 1, indexList[i]) for i in range(parentLevel + 1)]
            lpathString = "SET " + ",".join(lpaths)
            req = f"UPDATE FC_DirectoryLevelTree {lpathString} WHERE DirID={dirID}"
            result = self.db._update(req, conn=connection)
            if not result["OK"]:
                return result
            result = self.__rebuildLevelIndexes(dirID, connection=connection)

        return S_OK()
