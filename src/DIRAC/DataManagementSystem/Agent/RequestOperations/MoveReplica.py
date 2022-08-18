""" MoveReplica operation handler

    This handler moves replicas from source SEs to target SEs. Replicas are first replicated to target SEs and then removed from the source SEs
"""
# # imports
import os

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase
from DIRAC.DataManagementSystem.Client.ConsistencyInspector import ConsistencyInspector

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

####


class MoveReplica(DMSRequestOperationsBase):
    """
    .. class:: MoveReplica

    MoveReplica operation handler
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param self: self reference
        :param ~Operation.Operation operation: Operation instance
        :param str csPath: CS path for this handler
        """
        super().__init__(operation, csPath)

        # Init ConsistencyInspector: used to check replicas
        self.ci = ConsistencyInspector()

    def __call__(self):
        """call me maybe"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # check replicas first
        res = self.__checkReplicas()
        if not res["OK"]:
            self.log.error("Failed to check replicas", res["Message"])

        sourceSE = self.operation.SourceSE if self.operation.SourceSE else None
        if sourceSE:
            # # check source se for read
            bannedSource = self.checkSEsRSS(sourceSE, "ReadAccess")
            if not bannedSource["OK"]:
                if self.rmsMonitoring:
                    for status in ["Attempted", "Failed"]:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(self.operation)))
                    self.rmsMonitoringReporter.commit()
                return bannedSource

            if bannedSource["Value"]:
                self.operation.Error = "SourceSE %s is banned for reading" % sourceSE
                self.log.info(self.operation.Error)
                return S_OK(self.operation.Error)

        # # check targetSEs for write
        bannedTargets = self.checkSEsRSS()
        if not bannedTargets["OK"]:
            if self.rmsMonitoring:
                for status in ["Attempted", "Failed"]:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(self.operation)))
                self.rmsMonitoringReporter.commit()
            return bannedTargets

        if bannedTargets["Value"]:
            self.operation.Error = "%s targets are banned for writing" % ",".join(bannedTargets["Value"])
            return S_OK(self.operation.Error)

        # Can continue now
        self.log.verbose("No targets banned for writing")

        # # check sourceSEs for removal
        # # for removal the targetSEs are the sourceSEs of the replication
        targetSEs = self.operation.sourceSEList
        bannedTargets = self.checkSEsRSS(targetSEs, access="RemoveAccess")
        if not bannedTargets["OK"]:
            if self.rmsMonitoring:
                for status in ["Attempted", "Failed"]:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(self.operation)))
                self.rmsMonitoringReporter.commit()
            return bannedTargets

        if bannedTargets["Value"]:
            return S_OK("%s targets are banned for removal" % ",".join(bannedTargets["Value"]))

        # Can continue now
        self.log.verbose("No targets banned for removal")

        # Do the transfer
        # # get waiting files. If none just return
        waitingFiles = self.getWaitingFilesList()
        if not waitingFiles:
            return S_OK()

        # # loop over files
        self.log.info("Transferring files using Data manager...")
        for opFile in waitingFiles:
            res = self.dmTransfer(opFile)
            if not res["OK"]:
                continue
            else:
                # Do the replica removal
                self.log.info("Removing files using Data manager...")
                toRemoveDict = {opFile.LFN: opFile for opFile in waitingFiles}
                self.log.info(f"todo: {len(toRemoveDict)} replicas to delete from {len(targetSEs)} sites")
                self.dmRemoval(toRemoveDict, targetSEs)

        return S_OK()

    def __checkReplicas(self):
        """check done replicas and update file states"""
        waitingFiles = {opFile.LFN: opFile for opFile in self.operation if opFile.Status in ("Waiting", "Scheduled")}
        targetSESet = set(self.operation.targetSEList)

        # Check replicas
        res = self.ci._getCatalogReplicas(list(waitingFiles))

        if not res["OK"]:
            self.log.error("Failed to get catalog replicas", res["Message"])
            return S_ERROR()

        allReplicas = res["Value"][0]

        replicas = self.ci.compareChecksum(list(waitingFiles))

        if not replicas["OK"]:
            self.log.error("Failed to check replicas", replicas["Message"])
            return S_ERROR()

        replicas = replicas["Value"]
        noReplicas = replicas["NoReplicas"]

        if noReplicas:
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", len(noReplicas)))
                self.rmsMonitoringReporter.commit()
            for lfn in noReplicas.keys():
                self.log.error("File %s doesn't exist" % lfn)
                waitingFiles[lfn].Status = "Failed"

        for lfn, reps in allReplicas.items():
            if targetSESet.issubset(set(reps)):
                self.log.info("file %s has been replicated to all targets" % lfn)
                waitingFiles[lfn].Status = "Done"

        return S_OK()

    def dmRemoval(self, toRemoveDict, targetSEs):

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(toRemoveDict)))
            self.rmsMonitoringReporter.commit()
        # # keep status for each targetSE
        removalStatus = dict.fromkeys(toRemoveDict.keys(), None)
        for lfn in removalStatus:
            removalStatus[lfn] = dict.fromkeys(targetSEs, None)

        # # loop over targetSEs
        for targetSE in targetSEs:
            self.log.info("removing replicas at %s" % targetSE)

            # # 1st step - bulk removal
            bulkRemoval = self.bulkRemoval(toRemoveDict, targetSE)
            if not bulkRemoval["OK"]:
                self.log.error("Bulk replica removal failed", bulkRemoval["Message"])
                return bulkRemoval
            bulkRemoval = bulkRemoval["Value"]

            # # update removal status for successful files
            removalOK = [opFile for opFile in bulkRemoval.values() if not opFile.Error]

            for opFile in removalOK:
                removalStatus[opFile.LFN][targetSE] = ""

            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", len(removalOK)))

            # # 2nd step - process the rest again
            toRetry = {lfn: opFile for lfn, opFile in bulkRemoval.items() if opFile.Error}
            for lfn, opFile in toRetry.items():
                self.singleRemoval(opFile, targetSE)
                if not opFile.Error:
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))
                    removalStatus[lfn][targetSE] = ""
                else:
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                    removalStatus[lfn][targetSE] = opFile.Error

        # # update file status for waiting files
        failed = 0
        for opFile in self.operation:
            if opFile.Status == "Waiting":
                errors = list({error for error in removalStatus[lfn].values() if error})
                if errors:
                    opFile.Error = ",".join(errors)
                    # This seems to be the only offending error
                    if "Write access not permitted for this credential" in opFile.Error:
                        failed += 1
                        continue
                opFile.Status = "Done"

        if failed:
            self.operation.Error = "failed to remove %s replicas" % failed

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        return S_OK(removalStatus)

    def dmTransfer(self, opFile):
        """replicate and register using dataManager"""
        # # get waiting files. If none just return
        # # source SE
        sourceSE = self.operation.SourceSE if self.operation.SourceSE else None

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", 1))
            self.rmsMonitoringReporter.commit()

        opFile.Error = ""
        lfn = opFile.LFN

        # Check replicas
        res = self.ci._getCatalogReplicas([lfn])

        if not res["OK"]:
            self.log.error("Failed to get catalog replicas", res["Message"])
            return S_ERROR()

        allReplicas = res["Value"][0]
        replicas = self.ci.compareChecksum([lfn])

        if not replicas["OK"]:
            self.log.error("Failed to check replicas", replicas["Message"])
            return S_ERROR()

        replicas = replicas["Value"]

        validReplicas = []
        noReplicas = replicas["NoReplicas"]
        missingAllReplicas = replicas["MissingAllReplicas"]
        missingReplica = replicas["MissingReplica"]
        someReplicasCorrupted = replicas["SomeReplicasCorrupted"]
        allReplicasCorrupted = replicas["AllReplicasCorrupted"]

        if noReplicas:
            self.log.error("Unable to replicate", "File %s doesn't exist" % (lfn))
            opFile.Error = "No replicas found"
            opFile.Status = "Failed"
        elif missingAllReplicas:
            self.log.error("Unable to replicate", "%s, all replicas are missing" % (lfn))
            opFile.Error = "Missing all replicas"
            opFile.Status = "Failed"
        elif allReplicasCorrupted:
            self.log.error("Unable to replicate", "%s, all replicas are corrupted" % (lfn))
            opFile.Error = "All replicas corrupted"
            opFile.Status = "Failed"
        elif someReplicasCorrupted:
            self.log.error("Unable to replicate", f"{lfn}, replicas corrupted at {someReplicasCorrupted[lfn]}")
            opFile.Error = "At least one replica corrupted"
            opFile.Status = "Failed"
        elif missingReplica:
            self.log.error("Unable to replicate", f"{lfn}, missing replicas at {missingReplica[lfn]}")
            opFile.Error = "At least one missing replica"
            opFile.Status = "Failed"

        if opFile.Error:
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                self.rmsMonitoringReporter.commit()
            return S_ERROR()

        # Check if replica is at the specified source
        for repSEName in allReplicas[lfn]:
            validReplicas.append(repSEName)

        # # get the first one in the list
        if sourceSE not in validReplicas:
            if sourceSE:
                self.log.warn(f"{lfn} is not at specified sourceSE {sourceSE}, changed to {validReplicas[0]}")
            sourceSE = validReplicas[0]

        # # loop over targetSE
        catalogs = self.operation.Catalog
        if catalogs:
            catalogs = [cat.strip() for cat in catalogs.split(",")]

        for targetSE in self.operation.targetSEList:
            # # call DataManager
            if targetSE in validReplicas:
                self.log.warn(f"Request to replicate {lfn} to an existing location: {targetSE}")
                continue

            res = self.dm.replicateAndRegister(lfn, targetSE, sourceSE=sourceSE, catalog=catalogs)

            if res["OK"]:
                if lfn in res["Value"]["Successful"]:
                    if "replicate" in res["Value"]["Successful"][lfn]:
                        repTime = res["Value"]["Successful"][lfn]["replicate"]
                        prString = f"file {lfn} replicated at {targetSE} in {repTime} s."

                        if "register" in res["Value"]["Successful"][lfn]:

                            regTime = res["Value"]["Successful"][lfn]["register"]
                            prString += " and registered in %s s." % regTime
                            self.log.info(prString)
                        else:

                            prString += " but failed to register"
                            self.log.warn(prString)

                            opFile.Error = "Failed to register"
                            # # add register replica operation
                            registerOperation = self.getRegisterOperation(opFile, targetSE, type="RegisterReplica")
                            self.request.insertAfter(registerOperation, self.operation)
                    else:
                        self.log.error("Failed to replicate", f"{lfn} to {targetSE}")

                        opFile.Error = "Failed to replicate"
                else:

                    reason = res["Value"]["Failed"][lfn]
                    self.log.error("Failed to replicate and register", f"File {lfn} at {targetSE}: {reason}")
                    opFile.Error = reason
            else:

                opFile.Error = "DataManager error: %s" % res["Message"]
                self.log.error("DataManager error", res["Message"])

        if not opFile.Error:
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))
            if len(self.operation.targetSEList) > 1:
                self.log.info("file %s has been replicated to all targetSEs" % lfn)
        else:
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                self.rmsMonitoringReporter.commit()
            return S_ERROR("dmTransfer failed")

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        return S_OK()

    def bulkRemoval(self, toRemoveDict, targetSE):
        """remove replicas :toRemoveDict: at :targetSE:

        :param dict toRemoveDict: { lfn: opFile, ... }
        :param str targetSE: target SE name
        :return: toRemoveDict with updated errors
        """
        removeReplicas = self.dm.removeReplica(targetSE, list(toRemoveDict))

        if not removeReplicas["OK"]:
            for opFile in toRemoveDict.values():
                opFile.Error = removeReplicas["Message"]
            return S_ERROR(removeReplicas["Message"])
        removeReplicas = removeReplicas["Value"]
        # # filter out failed
        for lfn, opFile in toRemoveDict.items():
            if lfn in removeReplicas["Failed"]:
                opFile.Error = str(removeReplicas["Failed"][lfn])
        return S_OK(toRemoveDict)

    def singleRemoval(self, opFile, targetSE):
        """remove opFile replica from targetSE

        :param ~DIRAC.RequestManagementSystem.Client.File.File opFile: File instance
        :param str targetSE: target SE name
        """
        proxyFile = None
        if "Write access not permitted for this credential" in opFile.Error:
            # # not a DataManger? set status to failed and return
            if "DataManager" in self.shifter:
                # #  you're a data manager - save current proxy and get a new one for LFN and retry
                saveProxy = os.environ["X509_USER_PROXY"]
                try:
                    fileProxy = self.getProxyForLFN(opFile.LFN)
                    if not fileProxy["OK"]:
                        opFile.Error = fileProxy["Message"]
                    else:
                        proxyFile = fileProxy["Value"]
                        removeReplica = self.dm.removeReplica(targetSE, opFile.LFN)
                        if not removeReplica["OK"]:
                            opFile.Error = removeReplica["Message"]
                        else:
                            removeReplica = removeReplica["Value"]
                            if opFile.LFN in removeReplica["Failed"]:
                                opFile.Error = removeReplica["Failed"][opFile.LFN]
                            else:
                                # # reset error - replica has been removed this time
                                opFile.Error = ""
                finally:
                    if proxyFile:
                        os.unlink(proxyFile)
                    # # put back request owner proxy to env
                    os.environ["X509_USER_PROXY"] = saveProxy
        return S_OK(opFile)
