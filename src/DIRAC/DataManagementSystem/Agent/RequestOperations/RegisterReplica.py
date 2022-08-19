""" :mod: RegisterReplica

    ==================

    .. module: RegisterReplica

    :synopsis: register replica handler

    RegisterReplica operation handler
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class RegisterReplica(DMSRequestOperationsBase):
    """
    .. class:: RegisterReplica

    RegisterReplica operation handler

    :param self: self reference
    :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor"""
        DMSRequestOperationsBase.__init__(self, operation, csPath)

    def __call__(self):
        """call me maybe"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # counter for failed replicas

        failedReplicas = 0
        # # catalog to use
        catalogs = self.operation.Catalog
        if catalogs:
            catalogs = [cat.strip() for cat in catalogs.split(",")]
        # # get waiting files
        waitingFiles = self.getWaitingFilesList()

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(waitingFiles)))

        # # loop over files
        registerOperations = {}
        successReplicas = 0
        for opFile in waitingFiles:

            # # get LFN
            lfn = opFile.LFN
            # # and others
            targetSE = self.operation.targetSEList[0]
            replicaTuple = (lfn, opFile.PFN, targetSE)
            # # call ReplicaManager
            registerReplica = self.dm.registerReplica(replicaTuple, catalogs)
            # # check results
            if not registerReplica["OK"] or lfn in registerReplica["Value"]["Failed"]:
                # There have been some errors

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                #        self.dataLoggingClient().addFileRecord( lfn, "RegisterReplicaFail", ','.join( catalogs ) if catalogs else "all catalogs", "", "RegisterReplica" )

                reason = registerReplica.get(
                    "Message", registerReplica.get("Value", {}).get("Failed", {}).get(lfn, "Unknown")
                )
                errorStr = f"failed to register LFN {lfn}: {str(reason)}"
                # FIXME: this is incompatible with the change made in the DM that we
                # ignore failures if successful in at least one catalog
                if lfn in registerReplica.get("Value", {}).get("Successful", {}) and isinstance(reason, dict):
                    # As we managed, let's create a new operation for just the remaining registration
                    errorStr += " - adding registerReplica operations to request"
                    for failedCatalog in reason:
                        key = f"{targetSE}/{failedCatalog}"
                        newOperation = self.getRegisterOperation(
                            opFile, targetSE, type="RegisterReplica", catalog=failedCatalog
                        )
                        if key not in registerOperations:
                            registerOperations[key] = newOperation
                        else:
                            registerOperations[key].addFile(newOperation[0])
                    opFile.Status = "Done"
                else:
                    opFile.Error = errorStr
                    catMaster = True
                    if isinstance(reason, dict):
                        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

                        for failedCatalog in reason:
                            catMaster = catMaster and FileCatalog()._getCatalogConfigDetails(failedCatalog).get(
                                "Value", {}
                            ).get("Master", False)
                    # If one targets explicitly a catalog and it fails or if it fails on the master catalog
                    if (catalogs or catMaster) and (
                        "file does not exist" in opFile.Error.lower() or "no such file" in opFile.Error.lower()
                    ):
                        # Check if the file really exists in SE, if not, consider this file registration as Done
                        res = self.dm.getReplicaMetadata(lfn, targetSE)
                        notExist = bool("No such file" in res.get("Value", {}).get("Failed", {}).get(lfn, ""))
                        if not notExist:
                            opFile.Status = "Failed"
                        else:
                            opFile.Status = "Done"
                    if opFile.Status != "Done":
                        failedReplicas += 1
                self.log.warn(errorStr)

            else:
                # All is OK
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))
                else:
                    successReplicas += 1
                    self.log.verbose(
                        "Replica %s has been registered at %s"
                        % (lfn, ",".join(catalogs) if catalogs else "all catalogs")
                    )

                opFile.Status = "Done"

        # # if we have new replications to take place, put them at the end
        if registerOperations:
            self.log.info("adding %d operations to the request" % len(registerOperations))
        for operation in registerOperations.values():
            self.operation._parent.addOperation(operation)

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        # # final check
        infoStr = ""
        if successReplicas:
            infoStr = "%d replicas successfully registered" % successReplicas
        if failedReplicas:
            infoStr += ", %d replicas failed to register" % failedReplicas
        self.log.info("All replicas processed", infoStr)
        if failedReplicas:
            self.operation.Error = "some replicas failed to register"
            return S_ERROR(self.operation.Error)

        return S_OK()
