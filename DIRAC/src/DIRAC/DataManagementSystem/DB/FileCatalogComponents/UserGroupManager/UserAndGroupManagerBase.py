""" DIRAC FileCatalog base class for mix-in classes to manage users and groups
"""
import time
import threading

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Security import Properties


class UserAndGroupManagerBase:
    def _refreshGroups(self):
        """Refresh the group cache"""
        return S_ERROR("To be implemented on derived class")

    def _refreshUsers(self):
        """Refresh the user cache"""
        return S_ERROR("To be implemented on derived class")

    def __init__(self, database=None):
        self.db = database
        self.lock = threading.Lock()
        self._refreshUsers()
        self._refreshGroups()

    def setDatabase(self, database):
        self.db = database

    def getUserAndGroupRight(self, credDict):
        """Evaluate rights for user and group operations"""
        if Properties.FC_MANAGEMENT in credDict["properties"]:
            return S_OK(True)
        return S_OK(False)
