"""
:mod: DataIntegrityHandler


.. module: DataIntegrityHandler

  :synopsis: DataIntegrityHandler is the implementation of the Data Integrity service in
  the DISET framework

"""
# from DIRAC
from DIRAC import S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB


class DataIntegrityHandlerMixin:
    """
    .. class:: DataIntegrityHandler

    Implementation of the Data Integrity service in the DISET framework.
    """

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB object"""

        cls.dataIntegrityDB = DataIntegrityDB(parentLogger=cls.log)
        return S_OK()

    types_removeProblematic = [[int, list]]

    def export_removeProblematic(self, fileID):
        """Remove the file with the supplied FileID from the database"""
        if isinstance(fileID, list):
            fileIDs = fileID
        else:
            fileIDs = [int(fileID)]
        self.log.info("DataIntegrityHandler.removeProblematic: Attempting to remove problematic.")
        res = self.dataIntegrityDB.removeProblematic(fileIDs)
        if not res["OK"]:
            self.log.error("DataIntegrityHandler.removeProblematic: Failed to remove problematic.", res["Message"])
        return res

    types_getProblematic = []

    def export_getProblematic(self):
        """Get the next problematic to resolve from the IntegrityDB"""
        self.log.info("DataIntegrityHandler.getProblematic: Getting file to resolve.")
        res = self.dataIntegrityDB.getProblematic()
        if not res["OK"]:
            self.log.error(
                "DataIntegrityHandler.getProblematic: Failed to get problematic file to resolve.", res["Message"]
            )
        return res

    types_getPrognosisProblematics = [str]

    def export_getPrognosisProblematics(self, prognosis):
        """Get problematic files from the problematics table of the IntegrityDB"""
        self.log.info("DataIntegrityHandler.getPrognosisProblematics: Getting files with %s prognosis." % prognosis)
        res = self.dataIntegrityDB.getPrognosisProblematics(prognosis)
        if not res["OK"]:
            self.log.error(
                "DataIntegrityHandler.getPrognosisProblematics: Failed to get prognosis files.", res["Message"]
            )
        return res

    types_setProblematicStatus = [int, str]

    def export_setProblematicStatus(self, fileID, status):
        """Update the status of the problematics with the provided fileID"""
        self.log.info(f"DataIntegrityHandler.setProblematicStatus: Setting file {fileID} status to {status}.")
        res = self.dataIntegrityDB.setProblematicStatus(fileID, status)
        if not res["OK"]:
            self.log.error("DataIntegrityHandler.setProblematicStatus: Failed to set status.", res["Message"])
        return res

    types_incrementProblematicRetry = [int]

    def export_incrementProblematicRetry(self, fileID):
        """Update the retry count for supplied file ID."""
        self.log.info("DataIntegrityHandler.incrementProblematicRetry: Incrementing retries for file %s." % (fileID))
        res = self.dataIntegrityDB.incrementProblematicRetry(fileID)
        if not res["OK"]:
            self.log.error(
                "DataIntegrityHandler.incrementProblematicRetry: Failed to increment retries.", res["Message"]
            )
        return res

    types_insertProblematic = [str, dict]

    def export_insertProblematic(self, source, fileMetadata):
        """Insert problematic files into the problematics table of the IntegrityDB"""
        self.log.info("DataIntegrityHandler.insertProblematic: Inserting problematic file to integrity DB.")
        res = self.dataIntegrityDB.insertProblematic(source, fileMetadata)
        if not res["OK"]:
            self.log.error("DataIntegrityHandler.insertProblematic: Failed to insert.", res["Message"])
        return res

    types_changeProblematicPrognosis = []

    def export_changeProblematicPrognosis(self, fileID, newPrognosis):
        """Change the prognosis for the supplied file"""
        self.log.info("DataIntegrityHandler.changeProblematicPrognosis: Changing problematic prognosis.")
        res = self.dataIntegrityDB.changeProblematicPrognosis(fileID, newPrognosis)
        if not res["OK"]:
            self.log.error("DataIntegrityHandler.changeProblematicPrognosis: Failed to update.", res["Message"])
        return res

    types_getTransformationProblematics = [int]

    def export_getTransformationProblematics(self, transID):
        """Get the problematics for a given transformation"""
        self.log.info("DataIntegrityHandler.getTransformationProblematics: Getting problematics for transformation.")
        res = self.dataIntegrityDB.getTransformationProblematics(transID)
        if not res["OK"]:
            self.log.error("DataIntegrityHandler.getTransformationProblematics: Failed.", res["Message"])
        return res

    types_getProblematicsSummary = []

    def export_getProblematicsSummary(self):
        """Get a summary from the Problematics table from the IntegrityDB"""
        self.log.info("DataIntegrityHandler.getProblematicsSummary: Getting problematics summary.")
        res = self.dataIntegrityDB.getProblematicsSummary()
        if res["OK"]:
            for prognosis, statusDict in res["Value"].items():
                self.log.info("DataIntegrityHandler.getProblematicsSummary: %s." % prognosis)
                for status, count in statusDict.items():
                    self.log.info("DataIntegrityHandler.getProblematicsSummary: \t%-10s %-10s." % (status, str(count)))
        else:
            self.log.error("DataIntegrityHandler.getProblematicsSummary: Failed to get summary.", res["Message"])
        return res

    types_getDistinctPrognosis = []

    def export_getDistinctPrognosis(self):
        """Get a list of the distinct prognosis from the IntegrityDB"""
        self.log.info("DataIntegrityHandler.getDistinctPrognosis: Getting distinct prognosis.")
        res = self.dataIntegrityDB.getDistinctPrognosis()
        if res["OK"]:
            for prognosis in res["Value"]:
                self.log.info("DataIntegrityHandler.getDistinctPrognosis: \t%s." % prognosis)
        else:
            self.log.error("DataIntegrityHandler.getDistinctPrognosis: Failed to get unique prognosis.", res["Message"])
        return res


class DataIntegrityHandler(DataIntegrityHandlerMixin, RequestHandler):
    pass
