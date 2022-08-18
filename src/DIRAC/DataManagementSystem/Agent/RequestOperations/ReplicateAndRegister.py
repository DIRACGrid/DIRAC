########################################################################
# File: ReplicateAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegister

    ==========================

    .. module: ReplicateAndRegister

    :synopsis: ReplicateAndRegister operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegister operation handler
"""
# #
# @file ReplicateAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegister class.

# # imports
import re
from collections import defaultdict

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Adler import compareAdler, hexAdlerToInt, intAdlerToHex
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase

from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3TransferOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
from DIRAC.DataManagementSystem.private.FTS3Utilities import getFTS3Plugin

from DIRAC.ConfigurationSystem.Client.Helpers import Registry

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter


def filterReplicas(opFile, logger=None, dataManager=None, opSources=None):
    """filter out banned/invalid source SEs"""

    if logger is None:
        logger = gLogger
    if dataManager is None:
        dataManager = DataManager()

    log = logger.getSubLogger("filterReplicas")
    result = defaultdict(list)

    replicas = dataManager.getActiveReplicas(opFile.LFN, getUrl=False, preferDisk=True)
    if not replicas["OK"]:
        log.error("Failed to get active replicas", replicas["Message"])
        return replicas
    reNotExists = re.compile(r".*such file.*")
    replicas = replicas["Value"]
    failed = replicas["Failed"].get(opFile.LFN, "")
    if reNotExists.match(failed.lower()):
        opFile.Status = "Failed"
        opFile.Error = failed
        return S_ERROR(failed)

    replicas = replicas["Successful"].get(opFile.LFN, {})

    # If user set sourceSEs, only consider those replicas
    if opSources:
        replicas = {x: y for (x, y) in replicas.items() if x in opSources}

    noReplicas = False
    if not replicas:
        allReplicas = dataManager.getReplicas(opFile.LFN, getUrl=False)
        if allReplicas["OK"]:
            allReplicas = allReplicas["Value"]["Successful"].get(opFile.LFN, {})
            if not allReplicas:
                result["NoReplicas"].append(None)
                noReplicas = True
            else:
                # There are replicas but we cannot get metadata because the replica is not active
                result["NoActiveReplicas"] += list(allReplicas)
            log.verbose("File has no%s replica in File Catalog" % ("" if noReplicas else " active"), opFile.LFN)
        else:
            return allReplicas

    if not opFile.Checksum or hexAdlerToInt(opFile.Checksum) is False:
        # Set Checksum to FC checksum if not set in the request
        fcMetadata = FileCatalog().getFileMetadata(opFile.LFN)
        fcChecksum = fcMetadata.get("Value", {}).get("Successful", {}).get(opFile.LFN, {}).get("Checksum")
        # Replace opFile.Checksum if it doesn't match a valid FC checksum
        if fcChecksum:
            if hexAdlerToInt(fcChecksum) is not False:
                opFile.Checksum = fcChecksum
                opFile.ChecksumType = fcMetadata["Value"]["Successful"][opFile.LFN].get("ChecksumType", "Adler32")
            else:
                opFile.Checksum = None

    # If no replica was found, return what we collected as information
    if not replicas:
        return S_OK(result)

    for repSEName in replicas:
        repSEMetadata = StorageElement(repSEName).getFileMetadata(opFile.LFN)
        error = repSEMetadata.get("Message", repSEMetadata.get("Value", {}).get("Failed", {}).get(opFile.LFN))
        if error:
            log.warn(f"unable to get metadata at {repSEName} for {opFile.LFN}", error.replace("\n", ""))
            if "File does not exist" in error or "No such file" in error:
                result["NoReplicas"].append(repSEName)
            else:
                result["NoMetadata"].append(repSEName)
        elif not noReplicas:
            repSEMetadata = repSEMetadata["Value"]["Successful"][opFile.LFN]

            seChecksum = hexAdlerToInt(repSEMetadata.get("Checksum"))
            # As from here seChecksum is an integer or False, not a hex string!
            if seChecksum is False and opFile.Checksum:
                result["NoMetadata"].append(repSEName)
            elif not seChecksum and opFile.Checksum:
                opFile.Checksum = None
                opFile.ChecksumType = None
            elif seChecksum and (not opFile.Checksum or opFile.Checksum == "False"):
                # Use the SE checksum (convert to hex) and force type to be Adler32
                opFile.Checksum = intAdlerToHex(seChecksum)
                opFile.ChecksumType = "Adler32"
            if not opFile.Checksum or not seChecksum or compareAdler(intAdlerToHex(seChecksum), opFile.Checksum):
                # # All checksums are OK
                result["Valid"].append(repSEName)
            else:
                log.warn(
                    " %s checksum mismatch, FC: '%s' @%s: '%s'"
                    % (opFile.LFN, opFile.Checksum, repSEName, intAdlerToHex(seChecksum))
                )
                result["Bad"].append(repSEName)
        else:
            # If a replica was found somewhere, don't set the file as no replicas
            result["NoReplicas"] = []

    return S_OK(result)


########################################################################
class ReplicateAndRegister(DMSRequestOperationsBase):
    """
    .. class:: ReplicateAndRegister

    ReplicateAndRegister operation handler
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param self: self reference
        :param Operation operation: Operation instance
        :param str csPath: CS path for this handler
        """
        super().__init__(operation, csPath)

        # # SE cache

        # Clients
        self.fc = FileCatalog()

    def __call__(self):
        """call me maybe"""

        # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
        # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
        if self.rmsMonitoring:
            self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")

        # # check replicas first
        checkReplicas = self.__checkReplicas()
        if not checkReplicas["OK"]:
            self.log.error("Failed to check replicas", checkReplicas["Message"])
        if hasattr(self, "FTSMode") and getattr(self, "FTSMode"):
            bannedGroups = getattr(self, "FTSBannedGroups") if hasattr(self, "FTSBannedGroups") else ()
            if self.request.OwnerGroup in bannedGroups:
                self.log.verbose("usage of FTS system is banned for request's owner")
                return self.dmTransfer()

            return self.fts3Transfer()

        return self.dmTransfer()

    def __checkReplicas(self):
        """check done replicas and update file states"""
        waitingFiles = {opFile.LFN: opFile for opFile in self.operation if opFile.Status in ("Waiting", "Scheduled")}
        targetSESet = set(self.operation.targetSEList)

        replicas = self.fc.getReplicas(list(waitingFiles))
        if not replicas["OK"]:
            self.log.error("Failed to get replicas", replicas["Message"])
            return replicas

        reMissing = re.compile(r".*such file.*")
        for failedLFN, errStr in replicas["Value"]["Failed"].items():
            waitingFiles[failedLFN].Error = errStr
            if reMissing.search(errStr.lower()):
                self.log.error("File does not exists", failedLFN)
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                waitingFiles[failedLFN].Status = "Failed"

        for successfulLFN, reps in replicas["Value"]["Successful"].items():
            if targetSESet.issubset(set(reps)):
                self.log.info("file replicated to all targets", successfulLFN)
                waitingFiles[successfulLFN].Status = "Done"

        return S_OK()

    def _addMetadataToFiles(self, toSchedule):
        """Add metadata to those files that need to be scheduled through FTS

        toSchedule is a dictionary:
        {'lfn1': opFile, 'lfn2': opFile}
        """
        if toSchedule:
            self.log.info("found %s files to schedule, getting metadata from FC" % len(toSchedule))
        else:
            self.log.verbose("No files to schedule")
            return S_OK([])

        res = self.fc.getFileMetadata(list(toSchedule))
        if not res["OK"]:
            return res
        else:
            if res["Value"]["Failed"]:
                self.log.warn(
                    "Can't schedule %d files: problems getting the metadata: %s"
                    % (len(res["Value"]["Failed"]), ", ".join(res["Value"]["Failed"]))
                )
            metadata = res["Value"]["Successful"]

        filesToSchedule = {}

        for lfn, lfnMetadata in metadata.items():
            opFileToSchedule = toSchedule[lfn][0]
            opFileToSchedule.GUID = lfnMetadata["GUID"]
            # In principle this is defined already in filterReplicas()
            if not opFileToSchedule.Checksum:
                opFileToSchedule.Checksum = metadata[lfn]["Checksum"]
                opFileToSchedule.ChecksumType = metadata[lfn]["ChecksumType"]
            opFileToSchedule.Size = metadata[lfn]["Size"]

            filesToSchedule[opFileToSchedule.LFN] = opFileToSchedule

        return S_OK(filesToSchedule)

    def _filterReplicas(self, opFile):
        """filter out banned/invalid source SEs"""
        return filterReplicas(opFile, logger=self.log, dataManager=self.dm, opSources=self.operation.sourceSEList)

    def _checkExistingFTS3Operations(self):
        """
        Check if there are ongoing FTS3Operation for the current RMS Operation

        Under some conditions, we can be trying to schedule files while
        there is still an FTS transfer going on. This typically happens
        when the REA hangs. To prevent further race condition, we check
        if there are FTS3Operations in a non Final state matching the
        current operation ID. If so, we put the corresponding files in
        scheduled mode. We will then wait till the FTS3 Operation performs
        the callback

        :returns: S_OK with True if we can go on, False if we should stop the processing
        """

        res = FTS3Client().getOperationsFromRMSOpID(self.operation.OperationID)

        if not res["OK"]:
            self.log.debug("Could not get FTS3Operations matching OperationID", self.operation.OperationID)
            return res

        existingFTSOperations = res["Value"]
        # It is ok to have FTS Operations in a final state, so we
        # care only about the others
        unfinishedFTSOperations = [
            ops for ops in existingFTSOperations if ops.status not in FTS3TransferOperation.FINAL_STATES
        ]

        if not unfinishedFTSOperations:
            self.log.debug("No ongoing FTS3Operations, all good")
            return S_OK(True)

        self.log.warn(
            "Some FTS3Operations already exist for the RMS Operation:",
            [op.operationID for op in unfinishedFTSOperations],
        )

        # This would really be a screwed up situation !
        if len(unfinishedFTSOperations) > 1:
            self.log.warn("That's a serious problem !!")

        # We take the rmsFileID of the files in the Operations,
        # find the corresponding File object, and set them scheduled
        rmsFileIDsToSetScheduled = {
            ftsFile.rmsFileID for ftsOp in unfinishedFTSOperations for ftsFile in ftsOp.ftsFiles
        }

        for opFile in self.operation:
            # If it is in the DB, it has a FileID
            opFileID = opFile.FileID
            if opFileID in rmsFileIDsToSetScheduled:
                self.log.warn("Setting RMSFile as already scheduled", opFileID)
                opFile.Status = "Scheduled"

        # We return here such that the Request is set back to Scheduled in the DB
        # With no further modification
        return S_OK(False)

    def fts3Transfer(self):
        """replicate and register using FTS3"""

        self.log.info("scheduling files in FTS3...")

        # Check first if we do not have ongoing transfers

        res = self._checkExistingFTS3Operations()
        if not res["OK"]:
            return res

        # if res['Value'] is False
        # it means that there are ongoing transfers
        # and we should stop here
        if res["Value"] is False:
            # return S_OK such that the request is put back
            return S_OK()

        fts3Files = []
        toSchedule = {}

        # Dict which maps the FileID to the object
        rmsFilesIds = {}

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(self.getWaitingFilesList())))

        for opFile in self.getWaitingFilesList():
            rmsFilesIds[opFile.FileID] = opFile

            opFile.Error = ""

            # # check replicas
            replicas = self._filterReplicas(opFile)
            if not replicas["OK"]:
                continue
            replicas = replicas["Value"]

            validReplicas = replicas["Valid"]
            noMetaReplicas = replicas["NoMetadata"]
            noReplicas = replicas["NoReplicas"]
            badReplicas = replicas["Bad"]
            noPFN = replicas["NoPFN"]

            if validReplicas:
                validTargets = list(set(self.operation.targetSEList) - set(validReplicas))
                if not validTargets:
                    self.log.info("file %s is already present at all targets" % opFile.LFN)
                    opFile.Status = "Done"
                else:
                    toSchedule[opFile.LFN] = [opFile, validTargets]

            else:
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                if noMetaReplicas:
                    self.log.warn(
                        "unable to schedule file",
                        "'{}': couldn't get metadata at {}".format(opFile.LFN, ",".join(noMetaReplicas)),
                    )
                    opFile.Error = "Couldn't get metadata"
                elif noReplicas:
                    self.log.error(
                        "Unable to schedule transfer",
                        "File {} doesn't exist at {}".format(opFile.LFN, ",".join(noReplicas)),
                    )
                    opFile.Error = "No replicas found"
                    opFile.Status = "Failed"
                elif badReplicas:
                    self.log.error(
                        "Unable to schedule transfer",
                        "File {}, all replicas have a bad checksum at {}".format(opFile.LFN, ",".join(badReplicas)),
                    )
                    opFile.Error = "All replicas have a bad checksum"
                    opFile.Status = "Failed"
                elif noPFN:
                    self.log.warn(
                        "unable to schedule {}, could not get a PFN at {}".format(opFile.LFN, ",".join(noPFN))
                    )

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        res = self._addMetadataToFiles(toSchedule)
        if not res["OK"]:
            return res
        else:
            filesToSchedule = res["Value"]

            for lfn in filesToSchedule:
                opFile = filesToSchedule[lfn]
                validTargets = toSchedule[lfn][1]
                for targetSE in validTargets:
                    ftsFile = FTS3File.fromRMSFile(opFile, targetSE)
                    fts3Files.append(ftsFile)

        if fts3Files:
            res = Registry.getUsernameForDN(self.request.OwnerDN)
            if not res["OK"]:
                self.log.error("Cannot get username for DN", "{} {}".format(self.request.OwnerDN, res["Message"]))
                return res

            username = res["Value"]
            fts3Operation = FTS3TransferOperation.fromRMSObjects(self.request, self.operation, username)
            fts3Operation.ftsFiles = fts3Files

            try:
                if not fts3Operation.activity:
                    vo = getVOfromProxyGroup().get("Value")
                    fts3Plugin = getFTS3Plugin(vo=vo)
                    fts3Operation.activity = fts3Plugin.inferFTSActivity(fts3Operation, self.request, self.operation)
            except Exception:
                pass

            ftsSchedule = FTS3Client().persistOperation(fts3Operation)
            if not ftsSchedule["OK"]:
                self.log.error("Completely failed to schedule to FTS3:", ftsSchedule["Message"])
                return ftsSchedule

            # might have nothing to schedule
            ftsSchedule = ftsSchedule["Value"]
            self.log.info("Scheduled with FTS3Operation id %s" % ftsSchedule)

            self.log.info("%d files have been scheduled to FTS3" % len(fts3Files))

            if self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", len(fts3Files)))

            for ftsFile in fts3Files:
                opFile = rmsFilesIds[ftsFile.rmsFileID]
                opFile.Status = "Scheduled"
                self.log.debug("%s has been scheduled for FTS" % opFile.LFN)
        else:
            self.log.info("No files to schedule after metadata checks")

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        # Just in case some transfers could not be scheduled, try them with RM
        return self.dmTransfer(fromFTS=True)

    def dmTransfer(self, fromFTS=False):
        """replicate and register using dataManager"""
        # # get waiting files. If none just return
        # # source SE
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

        waitingFiles = self.getWaitingFilesList()
        if not waitingFiles:
            return S_OK()
        # # loop over files
        if fromFTS:
            self.log.info("Trying transfer using replica manager as FTS failed")
        else:
            self.log.info("Transferring files using Data manager...")
        errors = defaultdict(int)
        delayExecution = 0

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Attempted", len(waitingFiles)))

        for opFile in waitingFiles:
            if opFile.Error in (
                "Couldn't get metadata",
                "File doesn't exist",
                "No active replica found",
                "All replicas have a bad checksum",
            ):
                err = "File already in error status"
                errors[err] += 1

            opFile.Error = ""
            lfn = opFile.LFN

            # Check if replica is at the specified source
            replicas = self._filterReplicas(opFile)
            if not replicas["OK"]:
                self.log.error("Failed to check replicas", replicas["Message"])
                continue
            replicas = replicas["Value"]
            validReplicas = replicas.get("Valid")
            noMetaReplicas = replicas.get("NoMetadata")
            noReplicas = replicas.get("NoReplicas")
            badReplicas = replicas.get("Bad")
            noActiveReplicas = replicas.get("NoActiveReplicas")

            if not validReplicas:
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
                if noMetaReplicas:
                    err = "Couldn't get metadata"
                    errors[err] += 1
                    self.log.verbose(
                        "unable to replicate '{}', couldn't get metadata at {}".format(
                            opFile.LFN, ",".join(noMetaReplicas)
                        )
                    )
                    opFile.Error = err
                elif noReplicas:
                    err = "File doesn't exist"
                    errors[err] += 1
                    self.log.verbose(
                        "Unable to replicate", "File {} doesn't exist at {}".format(opFile.LFN, ",".join(noReplicas))
                    )
                    opFile.Error = err
                    opFile.Status = "Failed"
                elif badReplicas:
                    err = "All replicas have a bad checksum"
                    errors[err] += 1
                    self.log.error(
                        "Unable to replicate",
                        "{}, all replicas have a bad checksum at {}".format(opFile.LFN, ",".join(badReplicas)),
                    )
                    opFile.Error = err
                    opFile.Status = "Failed"
                elif noActiveReplicas:
                    err = "No active replica found"
                    errors[err] += 1
                    self.log.verbose(
                        "Unable to schedule transfer",
                        "{}, {} at {}".format(opFile.LFN, err, ",".join(noActiveReplicas)),
                    )
                    opFile.Error = err
                    # All source SEs are banned, delay execution by 1 hour
                    delayExecution = 60
                continue
            # # get the first one in the list
            if sourceSE not in validReplicas:
                if sourceSE:
                    err = "File not at specified source"
                    errors[err] += 1
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
                        self.log.error("Failed to replicate and register", f"File {lfn} at {targetSE}:", reason)
                        opFile.Error = reason

                else:

                    opFile.Error = "DataManager error: %s" % res["Message"]
                    self.log.error("DataManager error", res["Message"])

            if not opFile.Error:
                if self.rmsMonitoring:
                    self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Successful", 1))

                if len(self.operation.targetSEList) > 1:
                    self.log.info("file %s has been replicated to all targetSEs" % lfn)
                opFile.Status = "Done"
            elif self.rmsMonitoring:
                self.rmsMonitoringReporter.addRecord(self.createRMSRecord("Failed", 1))
        # Log error counts
        if delayExecution:
            self.log.info("Delay execution of the request by %d minutes" % delayExecution)
            self.request.delayNextExecution(delayExecution)
        for error, count in errors.items():
            self.log.error(error, "for %d files" % count)

        if self.rmsMonitoring:
            self.rmsMonitoringReporter.commit()

        return S_OK()
