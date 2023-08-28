""" DIRAC FileCatalog Security Manager mix-in class for access check only on the directory level
    with a special treatment of the Delete operation
"""
from DIRAC import S_OK
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.DirectorySecurityManager import (
    DirectorySecurityManager,
)
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import (
    _readMethods,
    _writeMethods,
)


class DirectorySecurityManagerWithDelete(DirectorySecurityManager):
    """This security manager implements a Delete operation.
    For Read, Write, Execute, it's behavior is the one of DirectorySecurityManager.
    For Delete, if the directory does not exist, we return True.
    If the directory exists, then we test the Write permission

    """

    def hasAccess(self, opType, paths, credDict):
        # The other SecurityManager do not support the Delete operation,
        # and it is transformed in Write
        # so we keep the original one

        if opType in ["removeFile", "removeReplica", "removeDirectory"]:
            self.opType = "Delete"
        elif opType in _readMethods:
            self.opType = "Read"
        elif opType in _writeMethods:
            self.opType = "Write"

        res = super().hasAccess(opType, paths, credDict)

        # We reinitialize self.opType in case someone would call getPathPermissions directly
        self.opType = ""

        return res

    def getPathPermissions(self, paths, credDict):
        """Get path permissions according to the policy"""

        # If we are testing in anything else than a Delete, just return the parent methods
        if hasattr(self, "opType") and self.opType.lower() != "delete":
            return super().getPathPermissions(paths, credDict)

        # If the object (file or dir) does not exist, we grant the permission
        res = self.db.dtree.exists(paths)
        if not res["OK"]:
            return res

        nonExistingDirectories = {path for path in res["Value"]["Successful"] if not res["Value"]["Successful"][path]}

        res = self.db.fileManager.exists(paths)
        if not res["OK"]:
            return res

        nonExistingFiles = {path for path in res["Value"]["Successful"] if not res["Value"]["Successful"][path]}

        nonExistingObjects = nonExistingDirectories & nonExistingFiles

        permissions = {}
        failed = {}

        for path in nonExistingObjects:
            permissions[path] = {"Read": True, "Write": True, "Execute": True}
            # The try catch is just to protect in case there are duplicate in the paths
            try:
                paths.remove(path)
            except Exception as _e:
                try:
                    paths.pop(path)
                except Exception as _ee:
                    pass

        # For all the paths that exist, check the write permission
        if paths:
            res = super().getPathPermissions(paths, credDict)
            if not res["OK"]:
                return res

            failed = res["Value"]["Failed"]
            permissions.update(res["Value"]["Successful"])

        return S_OK({"Successful": permissions, "Failed": failed})
