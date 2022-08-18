########################################################################
# File: RemoveFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:44:19
########################################################################

""" :mod: RemoveFile

    ================

    .. module: RemoveFile

    :synopsis: removeFile operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeFile operation handler
"""
# #
# @file RemoveFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:44:27
# @brief Definition of RemoveFile class.

# # imports
import os
import re

# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class RemoveFile(DMSRequestOperationsBase):
    """
    .. class:: RemoveFile

    remove file operation handler
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param self: self reference
        :param Operation operation: Operation to execute
        :param str csPath: CS path for this handler
        """
        # # call base class ctor
        DMSRequestOperationsBase.__init__(self, operation, csPath)

        # # re pattern for not existing files
        self.reNotExisting = re.compile(r"(no|not) such file.*", re.IGNORECASE)

    def __call__(self):
        """action for 'removeFile' operation"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # get waiting files
        waitingFiles = self.getWaitingFilesList()
        fc = FileCatalog(self.operation.catalogList)

        res = fc.getReplicas([wf.LFN for wf in waitingFiles])
        if not res["OK"]:
            if self.rmsMonitoring:
                for status in ["Attempted", "Failed"]:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(waitingFiles)))
                self.rmsMonitoringReporter.commit()
            return res

        # We check the status of the SE from the LFN that are successful
        # No idea what to do with the others...
        replicas = res["Value"]["Successful"]
        targetSEs = {se for lfn in replicas for se in replicas[lfn]}

        if targetSEs:
            # Check if SEs are allowed for remove but don't fail yet the operation if SEs are always banned
            bannedTargets = self.checkSEsRSS(targetSEs, access="RemoveAccess", failIfBanned=False)
            if not bannedTargets["OK"]:
                if self.rmsMonitoring:
                    for status in ["Attempted", "Failed"]:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord(status, len(replicas)))
                    self.rmsMonitoringReporter.commit()
                return bannedTargets
            bannedTargets = set(bannedTargets["Value"])
        else:
            bannedTargets = set()

        # # prepare waiting file dict
        # # We take only files that have no replica at the banned SEs... If no replica, don't
        toRemoveDict = {
            opFile.LFN: opFile
            for opFile in waitingFiles
            if not bannedTargets or not bannedTargets.intersection(replicas.get(opFile.LFN, []))
        }
        # If some SEs are always banned, set Failed the files that cannot be removed
        if bannedTargets and "always banned" in self.operation.Error:
            for opFile in waitingFiles:
                if opFile.LFN not in toRemoveDict:
                    # Set the files that cannot be removed Failed
                    opFile.Error = self.operation.Error
                    opFile.Status = "Failed"

            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(
                    self.createRMSRecord("Failed", len(waitingFiles) - len(toRemoveDict))
                )
                self.rmsMonitoringReporter.commit()

            if not toRemoveDict:
                # If there are no files that can be removed, exit, else try once to remove them anyway
                return S_OK("%s targets are always banned for removal" % ",".join(sorted(bannedTargets)))

        if toRemoveDict:
            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(toRemoveDict)))
            # # 1st step - bulk removal
            self.log.debug("bulk removal of %s files" % len(toRemoveDict))
            bulkRemoval = self.bulkRemoval(toRemoveDict)
            if not bulkRemoval["OK"]:
                self.log.error("Bulk file removal failed", bulkRemoval["Message"])
            else:
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(
                        self.createRMSRecord("Successful", len(toRemoveDict) - len(bulkRemoval["Value"]))
                    )

            # # 2nd step - single file removal
            for lfn, opFile in toRemoveDict.items():
                self.log.info("removing single file %s" % lfn)
                singleRemoval = self.singleRemoval(opFile)
                if not singleRemoval["OK"]:
                    self.log.error("Error removing single file", singleRemoval["Message"])
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                else:
                    self.log.info("file %s has been removed" % lfn)
                    if self.rmsMonitoring:
                        self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))

            # # set
            failedFiles = [
                (lfn, opFile) for (lfn, opFile) in toRemoveDict.items() if opFile.Status in ("Failed", "Waiting")
            ]
            if failedFiles:
                self.operation.Error = "failed to remove %d files" % len(failedFiles)

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        if bannedTargets:
            return S_OK("%s targets are banned for removal" % ",".join(sorted(bannedTargets)))
        return S_OK()

    def bulkRemoval(self, toRemoveDict):
        """bulk removal using request owner DN

        :param dict toRemoveDict: { lfn: opFile, ... }
        :return: S_ERROR or S_OK( { lfn: opFile, ... } ) -- dict with files still waiting to be removed
        """
        bulkRemoval = self.dm.removeFile(list(toRemoveDict), force=True)
        if not bulkRemoval["OK"]:
            error = bulkRemoval["Message"]
            self.log.error("Bulk file removal failed", error)
            self.operation.Error = error
            for opFile in self.operation:
                opFile.Error = error
            return bulkRemoval
        bulkRemoval = bulkRemoval["Value"]
        # # filter results
        for lfn, opFile in toRemoveDict.items():
            if lfn in bulkRemoval["Successful"]:
                opFile.Status = "Done"
            elif lfn in bulkRemoval["Failed"]:

                error = bulkRemoval["Failed"][lfn]
                if isinstance(error, dict):
                    error = ";".join([f"{k}-{v}" for k, v in error.items()])
                opFile.Error = error
                if self.reNotExisting.search(opFile.Error):
                    opFile.Status = "Done"

        # # return files still waiting
        toRemoveDict = {lfn: opFile for lfn, opFile in toRemoveDict.items() if opFile.Status == "Waiting"}
        return S_OK(toRemoveDict)

    def singleRemoval(self, opFile):
        """remove single file

        :param opFile: File instance
        """
        # # try to remove with owner proxy
        proxyFile = None
        if "Write access not permitted for this credential" in opFile.Error:
            if "DataManager" in self.shifter:
                # #  you're a data manager - get proxy for LFN and retry
                saveProxy = os.environ["X509_USER_PROXY"]
                try:
                    fileProxy = self.getProxyForLFN(opFile.LFN)
                    if not fileProxy["OK"]:
                        opFile.Error = "Error getting owner's proxy : %s" % fileProxy["Message"]
                    else:
                        proxyFile = fileProxy["Value"]
                        self.log.info("Trying to remove file with owner's proxy (file %s)" % proxyFile)

                        removeFile = self.dm.removeFile(opFile.LFN, force=True)
                        self.log.always(str(removeFile))

                        if not removeFile["OK"]:
                            opFile.Error = str(removeFile["Message"])
                            if self.reNotExisting.search(str(removeFile["Message"]).lower()):
                                opFile.Status = "Done"
                        else:
                            removeFile = removeFile["Value"]
                            if opFile.LFN in removeFile["Failed"]:
                                error = removeFile["Failed"][opFile.LFN]
                                if isinstance(error, dict):
                                    error = ";".join([f"{k}-{v}" for k, v in error.items()])
                                if self.reNotExisting.search(error):
                                    # This should never happen due to the "force" flag
                                    opFile.Status = "Done"
                                else:
                                    opFile.Error = error
                            else:
                                opFile.Status = "Done"
                finally:
                    if proxyFile:
                        os.unlink(proxyFile)
                    # # put back request owner proxy to env
                    os.environ["X509_USER_PROXY"] = saveProxy
        # # file removed? update its status to 'Done'
        if opFile.Status == "Done":
            return S_OK()
        return S_ERROR(opFile.Error)
