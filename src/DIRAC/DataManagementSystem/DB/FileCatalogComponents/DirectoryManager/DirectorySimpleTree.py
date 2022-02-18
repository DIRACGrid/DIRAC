""" DIRAC FileCatalog component representing a simple directory tree
"""
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.DirectoryManager.DirectoryTreeBase import DirectoryTreeBase


class DirectorySimpleTree(DirectoryTreeBase):
    """Class managing Directory Tree as a simple self-linked structure with full
    directory path stored in each node
    """

    def __init__(self, database=None):
        DirectoryTreeBase.__init__(self, database)
        self.treeTable = "FC_DirectoryTree"

    def findDir(self, path):

        req = "SELECT DirID from FC_DirectoryTree WHERE DirName='%s'" % path
        result = self.db._query(req)
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK("")

        return S_OK(result["Value"][0][0])

    def removeDir(self, path):
        """Remove directory"""

        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK()

        dirID = result["Value"]
        req = "DELETE FROM FC_DirectoryTree WHERE DirID=%d" % dirID
        result = self.db._update(req)
        return result

    def makeDir(self, path):

        result = self.findDir(path)
        if not result["OK"]:
            return result
        dirID = result["Value"]
        if dirID:
            return S_OK(dirID)
        names = ["DirName"]
        values = [path]
        result = self.db.insertFields("FC_DirectoryTree", names, values)
        if not result["OK"]:
            return result
        return S_OK(result["lastRowId"])

    def existsDir(self, path):
        """Check the existence of a directory at the specified path"""
        result = self.findDir(path)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({"Exists": False})
        else:
            return S_OK({"Exists": True, "DirID": result["Value"]})

    def getParent(self, path):
        """Get the parent ID of the given directory"""
        parent_dir = os.path.dirname(path)
        if parent_dir == "/":
            return S_OK(0)
        return self.findDir(parent_dir)

    def getParentID(self, dirID):
        """Get the ID of the parent of a directory specified by ID"""
        if dirID == 0:
            return S_ERROR("Root directory ID given")

        req = "SELECT Parent FROM FC_DirectoryTree WHERE DirID=%d" % dirID
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("No parent found")

        return S_OK(result["Value"][0][0])

    def getDirectoryPath(self, dirID):
        """Get directory name by directory ID"""
        req = "SELECT DirName FROM FC_DirectoryTree WHERE DirID=%d" % int(dirID)
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory with id %d not found" % int(dirID))

        return S_OK(result["Value"][0][0])

    def getDirectoryName(self, dirID):
        """Get directory name by directory ID"""

        result = self.getDirectoryPath(dirID)
        if not result["OK"]:
            return result

        return S_OK(os.path.basename(result["Value"]))

    def getPathIDs(self, path):
        """Get IDs of all the directories in the parent hierarchy"""

        elements = path.split("/")
        pelements = []
        dPath = ""
        for el in elements[1:]:
            dPath += "/" + el
            pelements.append(dPath)

        pathString = ["'" + p + "'" for p in pelements]
        req = "SELECT DirID FROM FC_DirectoryTree WHERE DirName in (%s) ORDER BY DirID" % pathString
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Directory %s not found" % path)

        return S_OK([x[0] for x in result["Value"]])

    def getChildren(self, path):
        """Get child directory IDs for the given directory"""
        if isinstance(path, str):
            result = self.findDir(path)
            if not result["OK"]:
                return result
            dirID = result["Value"]
        else:
            dirID = path

        req = "SELECD DirID FROM FC_DirectoryTree WHERE Parent=%d" % dirID
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        return S_OK([x[0] for x in result["Value"]])
