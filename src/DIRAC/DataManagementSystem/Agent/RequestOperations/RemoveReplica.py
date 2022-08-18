########################################################################
# File: RemoveReplica.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:45:06
########################################################################

""" :mod: RemoveReplica

    ===================

    .. module: RemoveReplica

    :synopsis: removeReplica operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RemoveReplica operation handler
"""
# #
# @file RemoveReplica.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:45:17
# @brief Definition of RemoveReplica class.

# # imports
import os

# # from DIRAC
from DIRAC import S_OK
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class RemoveReplica(DMSRequestOperationsBase):
    """
    .. class:: RemoveReplica

    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param self: self reference
        :param Operation operation: operation to execute
        :param str csPath: CS path for this handler
        """
        # # base class ctor
        DMSRequestOperationsBase.__init__(self, operation, csPath)

    def __call__(self):
        """remove replicas"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # prepare list of targetSEs
        targetSEs = self.operation.targetSEList
        # # check targetSEs for removal
        bannedTargets = self.checkSEsRSS(targetSEs, access="RemoveAccess")
        if not bannedTargets["OK"]:
            if self.rmsMonitoring:
                for status in ["Attempted", "Failed"]:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(self.operation)))
                self.rmsMonitoringReporter.commit()
            return bannedTargets

        if bannedTargets["Value"]:
            return S_OK("%s targets are banned for removal" % ",".join(bannedTargets["Value"]))

        # # get waiting files
        waitingFiles = self.getWaitingFilesList()
        # # and prepare dict
        toRemoveDict = {opFile.LFN: opFile for opFile in waitingFiles}

        self.log.info(f"Todo: {len(toRemoveDict)} replicas to delete from {len(targetSEs)} SEs")

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(toRemoveDict)))

        # # keep status for each targetSE
        removalStatus = dict.fromkeys(toRemoveDict, None)
        for lfn in removalStatus:
            removalStatus[lfn] = dict.fromkeys(targetSEs, None)

        # # loop over targetSEs
        for targetSE in targetSEs:

            self.log.info("Removing replicas at %s" % targetSE)

            # # 1st step - bulk removal
            bulkRemoval = self._bulkRemoval(toRemoveDict, targetSE)
            if not bulkRemoval["OK"]:
                self.log.error("Bulk replica removal failed", bulkRemoval["Message"])

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.commit()

                return bulkRemoval

            # # report removal status for successful files
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(
                    self.createRMSRecord(
                        "Successful", len([opFile for opFile in toRemoveDict.values() if not opFile.Error])
                    )
                )
            # # 2nd step - process the rest again
            toRetry = {lfn: opFile for lfn, opFile in toRemoveDict.items() if opFile.Error}
            for lfn, opFile in toRetry.items():
                self._removeWithOwnerProxy(opFile, targetSE)
                if opFile.Error:
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                    removalStatus[lfn][targetSE] = opFile.Error
                else:
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))

        # # update file status for waiting files
        failed = 0
        for opFile in self.operation:
            if opFile.Status == "Waiting":
                errors = list({error for error in removalStatus[opFile.LFN].values() if error})
                if errors:
                    opFile.Error = "\n".join(errors)
                    # This seems to be the only unrecoverable error
                    if "Write access not permitted for this credential" in opFile.Error:
                        failed += 1
                        opFile.Status = "Failed"
                else:
                    opFile.Status = "Done"

        if failed:
            self.operation.Error = "failed to remove %s replicas" % failed

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        return S_OK()

    def _bulkRemoval(self, toRemoveDict, targetSE):
        """remove replicas :toRemoveDict: at :targetSE:

        :param dict toRemoveDict: { lfn: opFile, ... }
        :param str targetSE: target SE name
        """
        # Clear the error
        for opFile in toRemoveDict.values():
            opFile.Error = ""
        removeReplicas = self.dm.removeReplica(targetSE, list(toRemoveDict))
        if not removeReplicas["OK"]:
            for opFile in toRemoveDict.values():
                opFile.Error = removeReplicas["Message"]
            return removeReplicas
        removeReplicas = removeReplicas["Value"]
        # # filter out failed
        for lfn, opFile in toRemoveDict.items():
            if lfn in removeReplicas["Failed"]:
                errorReason = str(removeReplicas["Failed"][lfn])
                # If the reason is that the file does not exist,
                # we consider the removal successful
                # TODO: use cmpError once the FC returns the proper error msg corresponding to ENOENT
                if "No such file" not in errorReason:
                    opFile.Error = errorReason
                    self.log.error("Failed removing lfn", f"{lfn}:{opFile.Error}")

        return S_OK()

    def _removeWithOwnerProxy(self, opFile, targetSE):
        """remove opFile replica from targetSE using owner proxy

        :param File opFile: File instance
        :param str targetSE: target SE name
        """
        if "Write access not permitted for this credential" in opFile.Error:
            proxyFile = None
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
                            # Set or reset the error if all OK
                            opFile.Error = removeReplica["Value"]["Failed"].get(opFile.LFN, "")
                finally:
                    if proxyFile:
                        os.unlink(proxyFile)
                    # # put back request owner proxy to env
                    os.environ["X509_USER_PROXY"] = saveProxy
