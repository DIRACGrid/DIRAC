"""The Download Input Data module wraps around the Replica Management
components to provide access to datasets by downloading locally
"""

import os
import random
import tempfile

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.Core.Utilities.Os import getDiskSpace
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


def _isCached(lfn, seName):
    result = returnSingleResult(StorageElement(seName).getFileMetadata(lfn))
    if not result["OK"]:
        return False
    metadata = result["Value"]
    return metadata.get("Cached", metadata["Accessible"])


class DownloadInputData:
    """
    retrieve InputData LFN from localSEs (if available) or from elsewhere.
    """

    #############################################################################
    def __init__(self, argumentsDict):
        """Standard constructor"""
        self.inputData = argumentsDict["InputData"]
        self.configuration = argumentsDict["Configuration"]
        self.jobID = self.configuration.get("JobID")
        # Warning: this contains not only the SEs but also the file metadata
        self.fileCatalogResult = argumentsDict["FileCatalog"]
        # By default put each input data file into a separate directory
        self.inputDataDirectory = argumentsDict.get("InputDataDirectory", "PerFile")

        self.log = gLogger.getSubLogger(f"[{self.jobID}]{self.__class__.__name__}")
        self.log.showHeaders(True)

        self.counter = 1
        self.availableSEs = DMSHelpers().getStorageElements()

    #############################################################################
    def execute(self, dataToResolve=None):
        """This method is called to download the requested files in the case where
        enough local disk space is available.  A buffer is left in this calculation
        to leave room for any produced files.
        """

        # Define local configuration options present at every site
        localSESet = set(self.configuration["LocalSEList"])

        if dataToResolve:
            self.log.verbose("Data to resolve passed directly to DownloadInputData module")
            self.inputData = dataToResolve  # e.g. list supplied by another module

        self.inputData = sorted(lfn.replace("LFN:", "") for lfn in self.inputData)
        self.log.info("InputData to be downloaded is:\n%s" % "\n".join(self.inputData))

        replicas = self.fileCatalogResult["Value"]["Successful"]

        # Problematic files will be returned and can be handled by another module
        failedReplicas = set()
        # For the case that a file is found on two SEs at the same site
        # disk-based replicas are favoured.
        downloadReplicas = {}

        for lfn, reps in replicas.items():
            if lfn not in self.inputData:
                self.log.verbose("LFN %s is not in requested input data to download")
                failedReplicas.add(lfn)
                continue

            if not ("Size" in reps and "GUID" in reps):
                self.log.error("Missing LFN metadata", f"{lfn} {str(reps)}")
                failedReplicas.add(lfn)
                continue

            # Get and remove size and GUID
            size = reps.pop("Size")
            guid = reps.pop("GUID")
            # Remove all other items that are not SEs
            for item in list(reps):  # note the pop below
                if item not in self.availableSEs:
                    reps.pop(item)
            downloadReplicas[lfn] = {"SE": [], "Size": size, "GUID": guid}
            # First get Disk replicas
            localReps = set(reps) & localSESet
            for seName in localReps:
                seStatus = StorageElement(seName).status()
                if seStatus["DiskSE"] and seStatus["Read"]:
                    downloadReplicas[lfn]["SE"].append(seName)
            # If no disk replicas, take tape replicas
            if not downloadReplicas[lfn]["SE"]:
                for seName in localReps:
                    seStatus = StorageElement(seName).status()
                    if seStatus["TapeSE"] and seStatus["Read"] and _isCached(lfn, seName):
                        # Only consider replicas that are cached
                        downloadReplicas[lfn]["SE"].append(seName)

        totalSize = 0
        verbose = self.log.verbose("Replicas to download are:")
        for lfn, reps in downloadReplicas.items():
            self.log.verbose(lfn)
            if not reps["SE"]:
                self.log.info("Failed to find data at local SEs, will try to download from anywhere", lfn)
                reps["SE"] = ""
            else:
                if len(reps["SE"]) > 1:
                    # if more than one SE is available randomly select one
                    random.shuffle(reps["SE"])
                reps["SE"] = reps["SE"][0]
            totalSize += int(reps.get("Size", 0))
            if verbose:
                for item, value in sorted(reps.items()):
                    if value:
                        self.log.verbose(f"\t{item} {value}")

        self.log.info("Total size of files to be downloaded", f"is {totalSize} bytes")
        for lfn in failedReplicas:
            self.log.warn("Not all file metadata (SE,PFN,Size,GUID) was available for LFN", lfn)

        # Now need to check that the list of replicas to download fits into
        # the available disk space. Initially this is a simple check and if there is not
        # space for all input data, no downloads are attempted.
        result = self.__checkDiskSpace(totalSize)
        if not result["OK"]:
            self.log.warn(f"Problem checking available disk space:\n{result}")
            return result

        # FIXME: this can never happen at the moment
        if not result["Value"]:
            self.log.warn("Not enough disk space available for download", f"{result['Value']} / {totalSize} bytes")
            self.__setJobParam(
                self.__class__.__name__,
                f"Not enough disk space available for download: {result['Value']} / {totalSize} bytes",
            )
            return S_OK({"Failed": self.inputData, "Successful": {}})

        resolvedData = {}
        localSECount = 0
        for lfn, info in downloadReplicas.items():
            seName = info["SE"]
            guid = info["GUID"]
            reps = replicas.get(lfn, {})
            if seName:
                result = returnSingleResult(StorageElement(seName).getFileMetadata(lfn))
                if not result["OK"]:
                    self.log.error("Error getting metadata", result["Message"])
                    error = result["Message"]
                else:
                    metadata = result["Value"]
                    if metadata.get("Lost", False):
                        error = "PFN has been Lost by the StorageElement"
                    elif metadata.get("Unavailable", False):
                        error = "PFN is declared Unavailable by the StorageElement"
                    elif not metadata.get("Cached", metadata["Accessible"]):
                        error = "PFN is no longer in StorageElement Cache"
                    else:
                        error = ""
                if error:
                    self.log.error(error, lfn)
                    result = {"OK": False}
                else:
                    self.log.info("Preliminary checks OK", f": now downloading {lfn} from {seName}")
                    result = self._downloadFromSE(lfn, seName, reps, guid)
                    if not result["OK"]:
                        self.log.error("Download failed", f"Tried downloading from SE {seName}: {result['Message']}")
                    else:
                        self.log.info(f"Download of {lfn} from {seName} finalized")
            else:
                result = {"OK": False}

            if not result["OK"]:
                reps.pop(seName, None)
                # Check the other SEs
                if reps:
                    self.log.info("Trying to download from any SE")
                    result = self._downloadFromBestSE(lfn, reps, guid)
                    if not result["OK"]:
                        self.log.error("Download from best SE failed", f"Tried downloading {lfn}: {result['Message']}")
                        failedReplicas.add(lfn)
                else:
                    failedReplicas.add(lfn)
            else:
                localSECount += 1
            if result["OK"]:
                # Rename file if downloaded FileName does not match the LFN... How can this happen?
                lfnName = os.path.basename(lfn)
                oldPath = result["Value"]["path"]
                fileName = os.path.basename(oldPath)
                if lfnName != fileName:
                    newPath = os.path.join(os.path.dirname(oldPath), lfnName)
                    os.rename(oldPath, newPath)
                    result["Value"]["path"] = newPath
                resolvedData[lfn] = result["Value"]

        # Report datasets that could not be downloaded
        report = ""
        if resolvedData:
            report += f"Successfully downloaded {len(resolvedData)} LFN(s)"
            if localSECount != len(resolvedData):
                report += " (%d from local SEs):\n" % localSECount
            else:
                report += " from local SEs:\n"
            report += "\n".join(sorted(resolvedData))
        failedReplicas = sorted(failedReplicas.difference(resolvedData))
        if failedReplicas:
            self.log.warn(f"The following LFN(s) could not be downloaded to the WN:\n{'n'.join(failedReplicas)}")
            report += f"\nFailed to download {len(failedReplicas)} LFN(s):\n"
            report += "\n".join(failedReplicas)

        if report:
            self.__setJobParam(self.__class__.__name__, report)

        return S_OK({"Successful": resolvedData, "Failed": failedReplicas})

    #############################################################################
    def __checkDiskSpace(self, totalSize):
        """Compare available disk space to the file size reported from the catalog
        result.
        """
        diskSpace = getDiskSpace(self.__getDownloadDir(False))  # MB
        availableBytes = diskSpace * 1024 * 1024  # bytes
        bufferGBs = gConfig.getValue(
            os.path.join("/Systems/WorkloadManagement/JobWrapper", "MinOutputDataBufferGB"), 5.0
        )
        data = bufferGBs * 1024 * 1024 * 1024  # bufferGBs in bytes
        if (data + totalSize) < availableBytes:
            msg = f"Enough disk space available ({availableBytes} bytes)"
            self.log.verbose(msg)
            return S_OK(msg)
        else:
            msg = "Not enough disk space available for download %s (including %dGB buffer) > %s bytes" % (
                (data + totalSize),
                bufferGBs,
                availableBytes,
            )
            self.log.warn(msg)
            return S_ERROR(msg)

    def __getDownloadDir(self, incrementCounter=True):
        jobIDPath = str(self.configuration.get("JobIDPath", os.getcwd()))
        if self.inputDataDirectory == "PerFile":
            if incrementCounter:
                self.counter += 1
            return tempfile.mkdtemp(prefix=f"InputData_{self.counter}", dir=jobIDPath)
        elif self.inputDataDirectory == "CWD":
            return jobIDPath
        else:
            return self.inputDataDirectory

    #############################################################################
    def _downloadFromBestSE(self, lfn, reps, guid):
        """Download a local copy of a single LFN from a list of Storage Elements.
        This is used as a last resort to attempt to retrieve the file.
        """
        self.log.verbose("Attempting to download file from all SEs", f"({','.join(reps)}): {lfn}")
        diskSEs = set()
        tapeSEs = set()
        # Sort replicas, disk first
        for seName in reps:
            seStatus = StorageElement(seName).status()
            # FIXME: This is simply terrible - this notion of "DiskSE" vs "TapeSE" should NOT be used here!
            if seStatus["Read"] and seStatus["DiskSE"]:
                diskSEs.add(seName)
            elif seStatus["Read"] and seStatus["TapeSE"]:
                tapeSEs.add(seName)

        for seName in list(diskSEs) + list(tapeSEs):
            if seName in diskSEs or _isCached(lfn, seName):
                # On disk or cached from tape
                result = self._downloadFromSE(lfn, seName, reps, guid)
                if result["OK"]:
                    return result
                else:
                    self.log.error("Download failed", f"Tried downloading {lfn} from SE {seName}: {result['Message']}")

        return S_ERROR("Unable to download the file from any SE")

    #############################################################################
    def _downloadFromSE(self, lfn, seName, reps, guid):
        """Download a local copy from the specified Storage Element."""
        if not lfn:
            return S_ERROR("LFN not specified: assume file is not at this site")

        self.log.verbose("Attempting to download file", f"{lfn} from {seName}:")

        downloadDir = self.__getDownloadDir()
        fileName = os.path.basename(lfn)
        for localFile in (os.path.join(os.getcwd(), fileName), os.path.join(downloadDir, fileName)):
            if os.path.exists(localFile):
                self.log.info("File already exists locally", f"{fileName} as {localFile}")
                fileDict = {
                    "turl": "LocalData",
                    "protocol": "LocalData",
                    "se": seName,
                    "pfn": reps[seName],
                    "guid": guid,
                    "path": localFile,
                }
                return S_OK(fileDict)

        localFile = os.path.join(downloadDir, fileName)
        result = returnSingleResult(StorageElement(seName).getFile(lfn, localPath=downloadDir))
        if not result["OK"]:
            self.log.warn("Problem getting lfn", f"{lfn} from {seName}:\n{result['Message']}")
            self.__cleanFailedFile(lfn, downloadDir)
            return result

        if os.path.exists(localFile):
            self.log.verbose("File successfully downloaded locally", f"({lfn} to {localFile})")
            fileDict = {
                "turl": "Downloaded",
                "protocol": "Downloaded",
                "se": seName,
                "pfn": reps[seName],
                "guid": guid,
                "path": localFile,
            }
            return S_OK(fileDict)
        else:
            self.log.warn("File does not exist in local directory after download")
            return S_ERROR("OK download result but file missing in current directory")

    #############################################################################
    def __setJobParam(self, name, value):
        """Wraps around setJobParameter of state update client"""
        if not self.jobID:
            return S_ERROR("JobID not defined")

        self.log.verbose("setting job parameters", f"setJobParameter({self.jobID},{name},{value})")
        jobParam = JobStateUpdateClient().setJobParameter(int(self.jobID), str(name), str(value))
        if not jobParam["OK"]:
            self.log.warn("Failed to set job parameters", jobParam["Message"])

        return jobParam

    def __cleanFailedFile(self, lfn, downloadDir):
        """Try to remove a file after a failed download attempt"""
        filePath = os.path.join(downloadDir, os.path.basename(lfn))
        self.log.info("Trying to remove file after failed download", f"Local path: {filePath} ")
        if os.path.exists(filePath):
            try:
                os.remove(filePath)
                self.log.info("Removed file remnant after failed download", f"Local path: {filePath} ")
            except OSError as e:
                self.log.info("Failed to remove file after failed download", repr(e))


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
