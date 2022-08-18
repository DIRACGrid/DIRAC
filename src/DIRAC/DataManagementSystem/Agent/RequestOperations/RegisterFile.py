########################################################################
# File: RegisterOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/19 13:55:14
########################################################################
""" :mod: RegisterFile

    ==================

    .. module: RegisterFile

    :synopsis: register operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RegisterFile operation handler
"""
# #
# @file RegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/19 13:55:24
# @brief Definition of RegisterOperation class.

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class RegisterFile(OperationHandlerBase):
    """
    .. class:: RegisterOperation

    RegisterFile operation handler

    :param self: self reference
    :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: Operation instance
    :param str csPath: CS path for this handler

    """

    def __init__(self, operation=None, csPath=None):
        """c'tor"""
        OperationHandlerBase.__init__(self, operation, csPath)

    def __call__(self):
        """call me maybe"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # counter for failed files
        failedFiles = 0
        # # catalog(s) to use
        catalogs = self.operation.Catalog
        if catalogs:
            catalogs = [cat.strip() for cat in catalogs.split(",")]
        dm = DataManager(catalogs=catalogs)
        # # get waiting files
        waitingFiles = self.getWaitingFilesList()

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(waitingFiles)))

        # # loop over files
        for opFile in waitingFiles:

            # # get LFN
            lfn = opFile.LFN
            # # and others

            # CHRIS: for whatever reason, we only take the first one
            targetSE = ""
            if self.operation.targetSEList:
                targetSE = self.operation.targetSEList[0]

            fileTuple = (lfn, opFile.PFN, opFile.Size, targetSE, opFile.GUID, opFile.Checksum)
            # # call DataManager
            registerFile = dm.registerFile(fileTuple)
            # # check results
            if not registerFile["OK"] or lfn in registerFile["Value"]["Failed"]:

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                # self.dataLoggingClient().addFileRecord(
                #     lfn, "RegisterFail", ','.join(catalogs) if catalogs else "all catalogs", "", "RegisterFile")

                reason = str(
                    registerFile.get("Message", registerFile.get("Value", {}).get("Failed", {}).get(lfn, "Unknown"))
                )
                errorStr = "failed to register LFN"
                opFile.Error = f"{errorStr}: {reason}"
                if "GUID already registered" in reason:
                    opFile.Status = "Failed"
                    self.log.error(errorStr, f"{lfn}: {reason}")
                elif "File already registered with no replicas" in reason:
                    self.log.warn(errorStr, f"{lfn}: {reason}, will remove it and retry")
                    dm.removeFile(lfn)
                else:
                    self.log.warn(errorStr, f"{lfn}: {reason}")
                failedFiles += 1

            else:

                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))
                # self.dataLoggingClient().addFileRecord(
                #     lfn, "Register", ','.join(catalogs) if catalogs else "all catalogs", "", "RegisterFile")

                self.log.verbose(
                    "file {} has been registered at {}".format(lfn, ",".join(catalogs) if catalogs else "all catalogs")
                )
                opFile.Status = "Done"

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        # # final check
        if failedFiles:
            self.log.warn("all files processed, %s files failed to register" % failedFiles)
            self.operation.Error = "some files failed to register"
            return S_ERROR(self.operation.Error)

        return S_OK()
