""" DIRAC FileCatalog Security Manager mix-in class with no access checks
"""
from DIRAC import S_OK
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import SecurityManagerBase


class NoSecurityManager(SecurityManagerBase):
    def getPathPermissions(self, paths, credDict):
        """Get path permissions according to the policy"""

        permissions = {}
        for path in paths:
            permissions[path] = {"Read": True, "Write": True, "Execute": True}

        return S_OK({"Successful": permissions, "Failed": {}})

    def hasAccess(self, opType, paths, credDict):
        successful = {}
        for path in paths:
            successful[path] = True
        resDict = {"Successful": successful, "Failed": {}}
        return S_OK(resDict)

    def hasAdminAccess(self, credDict):
        return S_OK(True)
