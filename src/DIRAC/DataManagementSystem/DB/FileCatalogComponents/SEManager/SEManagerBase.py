""" DIRAC FileCatalog Storage Element Manager base class for mix-in classes """
import threading

from DIRAC import S_ERROR


class SEManagerBase:
    def _refreshSEs(self, connection=False):
        """Refresh the SE cache"""

        return S_ERROR("To be implemented on derived class")

    def __init__(self, database=None):
        self.db = database
        if self.db:
            self.db.seNames = {}
            self.db.seids = {}
        self.lock = threading.Lock()
        self.seUpdatePeriod = 600

        # last time the cache was updated (epoch)
        self.lastUpdate = 0
        self._refreshSEs()

    def setUpdatePeriod(self, period):
        self.seUpdatePeriod = period

    def setDatabase(self, database):
        self.db = database
        self.db.seNames = {}
        self.db.seids = {}
        self._refreshSEs()
