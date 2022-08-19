"""
This is the Data Integrity Client which allows the simple reporting of
problematic file and replicas to the IntegrityDB and their status
correctly updated in the FileCatalog.
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Base.Client import Client, createClient


@createClient("DataManagement/DataIntegrity")
class DataIntegrityClient(Client):
    """Client exposing the DataIntegrity Service."""

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.setServer("DataManagement/DataIntegrity")
        self.dm = DataManager()
        self.fc = FileCatalog()

    def setFileProblematic(self, lfn, reason, sourceComponent=""):
        """This method updates the status of the file in the FileCatalog and the IntegrityDB

        lfn - the lfn of the file
        reason - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
        """
        if isinstance(lfn, list):
            lfns = lfn
        elif isinstance(lfn, str):
            lfns = [lfn]
        else:
            errStr = "DataIntegrityClient.setFileProblematic: Supplied file info must be list or a single LFN."
            gLogger.error(errStr)
            return S_ERROR(errStr)
        gLogger.info("DataIntegrityClient.setFileProblematic: Attempting to update %s files." % len(lfns))
        fileMetadata = {}
        for lfn in lfns:
            fileMetadata[lfn] = {"Prognosis": reason, "LFN": lfn, "PFN": "", "SE": ""}
        res = self.insertProblematic(sourceComponent, fileMetadata)
        if not res["OK"]:
            gLogger.error("DataIntegrityClient.setReplicaProblematic: Failed to insert problematics to integrity DB")
        return res

    def reportProblematicReplicas(self, replicaTuple, se, reason):
        """Simple wrapper function around setReplicaProblematic"""
        gLogger.info(f"The following {len(replicaTuple)} files had {reason} at {se}")
        for lfn, _pfn, se, reason in sorted(replicaTuple):
            if lfn:
                gLogger.info(lfn)
        res = self.setReplicaProblematic(replicaTuple, sourceComponent="DataIntegrityClient")
        if not res["OK"]:
            gLogger.info("Failed to update integrity DB with replicas", res["Message"])
        else:
            gLogger.info("Successfully updated integrity DB with replicas")

    def setReplicaProblematic(self, replicaTuple, sourceComponent=""):
        """This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaDict should be of the form {lfn :{'PFN':pfn,'SE':se,'Prognosis':prognosis}

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
        """
        if isinstance(replicaTuple, tuple):
            replicaTuple = [replicaTuple]
        elif isinstance(replicaTuple, list):
            pass
        else:
            errStr = (
                "DataIntegrityClient.setReplicaProblematic: Supplied replica info must be a tuple or list of tuples."
            )
            gLogger.error(errStr)
            return S_ERROR(errStr)
        gLogger.info("DataIntegrityClient.setReplicaProblematic: Attempting to update %s replicas." % len(replicaTuple))
        replicaDict = {}
        for lfn, pfn, se, reason in replicaTuple:
            replicaDict[lfn] = {"Prognosis": reason, "LFN": lfn, "PFN": pfn, "SE": se}
        res = self.insertProblematic(sourceComponent, replicaDict)
        if not res["OK"]:
            gLogger.error("DataIntegrityClient.setReplicaProblematic: Failed to insert problematic to integrity DB")
            return res
        for lfn in replicaDict.keys():
            replicaDict[lfn]["Status"] = "Problematic"

        res = self.fc.setReplicaStatus(replicaDict)
        if not res["OK"]:
            errStr = "DataIntegrityClient.setReplicaProblematic: Completely failed to update replicas."
            gLogger.error(errStr, res["Message"])
            return res
        failed = res["Value"]["Failed"]
        successful = res["Value"]["Successful"]
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    ##########################################################################
    #
    # This section contains the resolution methods for various prognoses
    #

    def __updateCompletedFiles(self, prognosis, fileID):
        gLogger.info("%s file (%d) is resolved" % (prognosis, fileID))
        return self.setProblematicStatus(fileID, "Resolved")

    def __returnProblematicError(self, fileID, res):
        self.incrementProblematicRetry(fileID)
        gLogger.error("DataIntegrityClient failure", res["Message"])
        return res

    def __updateReplicaToChecked(self, problematicDict):
        lfn = problematicDict["LFN"]
        fileID = problematicDict["FileID"]
        prognosis = problematicDict["Prognosis"]
        problematicDict["Status"] = "Checked"

        res = returnSingleResult(self.fc.setReplicaStatus({lfn: problematicDict}))

        if not res["OK"]:
            return self.__returnProblematicError(fileID, res)
        gLogger.info("%s replica (%d) is updated to Checked status" % (prognosis, fileID))
        return self.__updateCompletedFiles(prognosis, fileID)

    def resolveCatalogPFNSizeMismatch(self, problematicDict):
        """This takes the problematic dictionary returned by the integrity DB and resolved the CatalogPFNSizeMismatch prognosis"""
        lfn = problematicDict["LFN"]
        se = problematicDict["SE"]
        fileID = problematicDict["FileID"]

        res = returnSingleResult(self.fc.getFileSize(lfn))
        if not res["OK"]:
            return self.__returnProblematicError(fileID, res)
        catalogSize = res["Value"]
        res = returnSingleResult(StorageElement(se).getFileSize(lfn))
        if not res["OK"]:
            return self.__returnProblematicError(fileID, res)
        storageSize = res["Value"]
        bkKCatalog = FileCatalog(["BookkeepingDB"])
        res = returnSingleResult(bkKCatalog.getFileSize(lfn))
        if not res["OK"]:
            return self.__returnProblematicError(fileID, res)
        bookkeepingSize = res["Value"]
        if bookkeepingSize == catalogSize == storageSize:
            gLogger.info("CatalogPFNSizeMismatch replica (%d) matched all registered sizes." % fileID)
            return self.__updateReplicaToChecked(problematicDict)
        if catalogSize == bookkeepingSize:
            gLogger.info("CatalogPFNSizeMismatch replica (%d) found to mismatch the bookkeeping also" % fileID)
            res = returnSingleResult(self.fc.getReplicas(lfn))
            if not res["OK"]:
                return self.__returnProblematicError(fileID, res)
            if len(res["Value"]) <= 1:
                gLogger.info("CatalogPFNSizeMismatch replica (%d) has no other replicas." % fileID)
                return S_ERROR("Not removing catalog file mismatch since the only replica")
            else:
                gLogger.info("CatalogPFNSizeMismatch replica (%d) has other replicas. Removing..." % fileID)
                res = self.dm.removeReplica(se, lfn)
                if not res["OK"]:
                    return self.__returnProblematicError(fileID, res)
                return self.__updateCompletedFiles("CatalogPFNSizeMismatch", fileID)
        if (catalogSize != bookkeepingSize) and (bookkeepingSize == storageSize):
            gLogger.info("CatalogPFNSizeMismatch replica (%d) found to match the bookkeeping size" % fileID)
            res = self.__updateReplicaToChecked(problematicDict)
            if not res["OK"]:
                return self.__returnProblematicError(fileID, res)
            return self.changeProblematicPrognosis(fileID, "BKCatalogSizeMismatch")
        gLogger.info("CatalogPFNSizeMismatch replica (%d) all sizes found mismatch. Updating retry count" % fileID)
        return self.incrementProblematicRetry(fileID)

    ############################################################################################

    def _reportProblematicFiles(self, lfns, reason):
        """Simple wrapper function around setFileProblematic"""
        gLogger.info(f"The following {len(lfns)} files were found with {reason}")
        for lfn in sorted(lfns):
            gLogger.info(lfn)
        res = self.setFileProblematic(lfns, reason, sourceComponent="DataIntegrityClient")
        if not res["OK"]:
            gLogger.info("Failed to update integrity DB with files", res["Message"])
        else:
            gLogger.info("Successfully updated integrity DB with files")
