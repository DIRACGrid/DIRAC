########################################################################
# File: ReTransfer.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 14:24:21
########################################################################
""" :mod: ReTransfer

    ================


    .. module: ReTransfer

    :synopsis: ReTransfer Operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReTransfer Operation handler
"""
# #
# @file ReTransfer.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 14:24:31
# @brief Definition of ReTransfer class.

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase
from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class ReTransfer(DMSRequestOperationsBase):
    """
    .. class:: ReTransfer


    online ReTransfer operation handler

    :param self: self reference
    :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: Operation instance
    :param str csPath: CS path for this handler

    """

    def __init__(self, operation=None, csPath=None):
        """c'tor"""
        # # base class ctor
        DMSRequestOperationsBase.__init__(self, operation, csPath)

    def __call__(self):
        """reTransfer operation execution"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # list of targetSEs
        targetSEs = self.operation.targetSEList
        # # check targetSEs for removal
        targetSE = targetSEs[0]
        bannedTargets = self.checkSEsRSS(targetSE)
        if not bannedTargets["OK"]:
            if self.rmsMonitoring:
                for status in ["Attempted", "Failed"]:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(self.operation)))
                self.rmsMonitoringReporter.commit()
            return bannedTargets

        if bannedTargets["Value"]:
            return S_OK("%s targets are banned for writing" % ",".join(bannedTargets["Value"]))

        # # get waiting files
        waitingFiles = self.getWaitingFilesList()
        # # prepare waiting files
        toRetransfer = {opFile.PFN: opFile for opFile in waitingFiles}

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(toRetransfer)))

        if len(targetSEs) != 1:
            error = "only one TargetSE allowed, got %d" % len(targetSEs)
            for opFile in toRetransfer.values():
                opFile.Error = error
                opFile.Status = "Failed"
            self.operation.Error = error

            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", len(toRetransfer)))
                self.rmsMonitoringReporter.commit()

            return S_ERROR(error)

        se = StorageElement(targetSE)
        for opFile in toRetransfer.values():

            reTransfer = se.retransferOnlineFile(opFile.LFN)
            if not reTransfer["OK"]:
                opFile.Error = reTransfer["Message"]
                self.log.error("Retransfer failed", opFile.Error)

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))

                continue
            reTransfer = reTransfer["Value"]
            if opFile.LFN in reTransfer["Failed"]:
                opFile.Error = reTransfer["Failed"][opFile.LFN]
                self.log.error("Retransfer failed", opFile.Error)

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))

                continue
            opFile.Status = "Done"
            self.log.info("%s retransfer done" % opFile.LFN)

            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        return S_OK()
