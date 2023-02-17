"""
RequestOperation to create a tarball from a list of LFNs.

Download a list of files to local storage, then tars it and uploads it to a StorageElement

This operation requires the following arguments:

 * ArchiveLFN: The LFN of the tarball
 * SourceSE: Where the files to be archived are downloaded from
 * TarballSE: Where the tarball will be uploaded to
 * RegisterDescendent: If True the tarball will be registered as a descendent of the LFNs

"""
import os
import shutil

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase


class ArchiveFiles(OperationHandlerBase):
    """ArchiveFiles operation handler."""

    def __init__(self, operation=None, csPath=None):
        """Initialize the ArchifeFiles handler.

        :param self: self reference
        :param Operation operation: Operation instance
        :param string csPath: CS path for this handler
        """
        OperationHandlerBase.__init__(self, operation, csPath)
        self.cacheFolder = os.environ.get("AGENT_WORKDIRECTORY")
        self.parameterDict = {}
        self.waitingFiles = []
        self.lfns = []

    def __call__(self):
        """Process the ArchiveFiles operation."""
        try:
            self._run()
        except RuntimeError as e:
            self.log.info("Failed to execute ArchiveFiles", repr(e))
            return S_ERROR(str(e))
        except Exception as e:
            self.log.exception("Failed to execute ArchiveFiles", repr(e), lException=e)
            return S_ERROR(str(e))
        finally:
            self._cleanup()
        return S_OK()

    def _run(self):
        """Execute the download and tarring."""
        self.parameterDict = DEncode.decode(self.operation.Arguments)[0]  # tuple: dict, number of characters
        self.cacheFolder = os.path.join(self.cacheFolder, self.request.RequestName)
        self._checkArchiveLFN()
        for parameter, value in self.parameterDict.items():
            self.log.info(f"Parameters: {parameter} = {value}")
        self.log.info(f"Cache folder: {self.cacheFolder!r}")
        self.waitingFiles = self.getWaitingFilesList()
        self.lfns = [opFile.LFN for opFile in self.waitingFiles]
        self._checkReplicas()
        self._downloadFiles()
        self._tarFiles()
        self._uploadTarBall()
        self._registerDescendent()
        self._markFilesDone()

    def _checkArchiveLFN(self):
        """Make sure the archive LFN does not exist yet."""
        archiveLFN = self.parameterDict["ArchiveLFN"]
        exists = returnSingleResult(self.fc.isFile(archiveLFN))
        self.log.debug(f"Checking for Tarball existence {exists!r}")
        if exists["OK"] and exists["Value"]:
            raise RuntimeError(f"Tarball {archiveLFN!r} already exists")

    def _checkReplicas(self):
        """Make sure the source files are at the sourceSE."""
        resReplica = self.fc.getReplicas(self.lfns)
        if not resReplica["OK"]:
            self.log.error("Failed to get replica information:", resReplica["Message"])
            raise RuntimeError("Failed to get replica information")

        atSource = []
        notAt = []
        failed = []
        sourceSE = self.parameterDict["SourceSE"]
        for lfn, replInfo in resReplica["Value"]["Successful"].items():
            if sourceSE in replInfo:
                atSource.append(lfn)
            else:
                self.log.warn(f"LFN {lfn!r} not found at source, only at: {','.join(replInfo.keys())}")
                notAt.append(lfn)

        for lfn, errorMessage in resReplica["Value"]["Failed"].items():
            self.log.warn("Failed to get replica info", f"{lfn}: {errorMessage}")
            if "No such file or directory" in errorMessage:
                continue
            failed.append(lfn)

        if failed:
            self.log.error("LFNs failed to get replica info:", f"{' '.join(failed)!r}")
            raise RuntimeError("Failed to get some replica information")
        if notAt:
            self.log.error("LFNs not at sourceSE:", f"{' '.join(notAt)!r}")
            raise RuntimeError("Some replicas are not at the source")

    def _downloadFiles(self):
        """Download the files."""
        self._checkFilePermissions()

        for index, opFile in enumerate(self.waitingFiles):
            lfn = opFile.LFN
            self.log.info("Processing file (%d/%d) %r" % (index, len(self.waitingFiles), lfn))
            sourceSE = self.parameterDict["SourceSE"]

            attempts = 0
            destFolder = os.path.join(self.cacheFolder, os.path.dirname(lfn)[1:])
            self.log.debug(f"Local Cache Folder: {destFolder}")
            if not os.path.exists(destFolder):
                os.makedirs(destFolder)
            while True:
                attempts += 1
                download = returnSingleResult(self.dm.getFile(lfn, destinationDir=destFolder, sourceSE=sourceSE))
                if download["OK"]:
                    self.log.info(f"Downloaded file {lfn!r} to {destFolder!r}")
                    break
                errorString = download["Message"]
                self.log.error("Failed to download file:", errorString)
                opFile.Error = errorString
                opFile.Attempt += 1
                self.operation.Error = opFile.Error
                if "No such file or directory" in opFile.Error:
                    # The File does not exist, we just ignore this and continue, otherwise we never archive the other files
                    opFile.Status = "Done"
                    download = S_OK()
                    break
                if attempts > 10:
                    self.log.error("Completely failed to download file:", errorString)
                    raise RuntimeError(f"Completely failed to download file: {errorString}")
        return

    def _checkFilePermissions(self):
        """Check that the request owner has permission to read and remove the files.

        Otherwise the error might show up after considerable time was spent.
        """
        permissions = self.fc.hasAccess(self.lfns, "removeFile")
        if not permissions["OK"]:
            raise RuntimeError("Could not resolve permissions")
        if permissions["Value"]["Failed"]:
            for lfn in permissions["Value"]["Failed"]:
                self.log.error("Cannot archive file:", lfn)
                for opFile in self.waitingFiles:
                    if opFile.LFN == lfn:
                        opFile.Status = "Failed"
                        opFile.Error = "Permission denied"
                        break
            raise RuntimeError("Do not have sufficient permissions")
        return

    def _tarFiles(self):
        """Tar the files."""
        tarFileName = os.path.splitext(os.path.basename(self.parameterDict["ArchiveLFN"]))[0]
        baseDir = self.parameterDict["ArchiveLFN"].strip("/").split("/")[0]
        shutil.make_archive(
            tarFileName, format="tar", root_dir=self.cacheFolder, base_dir=baseDir, dry_run=False, logger=self.log
        )

    def _uploadTarBall(self):
        """Upload the tarball to specified LFN."""
        lfn = self.parameterDict["ArchiveLFN"]
        self.log.info(f"Uploading tarball to {lfn!r}")
        localFile = os.path.basename(lfn)
        tarballSE = self.parameterDict["TarballSE"]
        upload = returnSingleResult(self.dm.putAndRegister(lfn, localFile, tarballSE))
        if not upload["OK"]:
            raise RuntimeError(f"Failed to upload tarball: {upload['Message']}")
        self.log.verbose("Uploading finished")

    def _registerDescendent(self):
        """Register the tarball as a descendent of the archived LFNs.

        Actually registers all LFNs as an ancestor to the Tarball.
        """
        registerDescendents = self.parameterDict.get("RegisterDescendent", None)
        if not registerDescendents:
            self.log.verbose("Will not register tarball as descendent to the Archived LFNs.")
            return

        self.log.info("Will register tarball as descendent to the Archived LFNs.")
        tarballLFN = self.parameterDict["ArchiveLFN"]
        ancestorDict = {tarballLFN: {"Ancestors": self.lfns}}

        for _trial in range(3):
            resAncestors = returnSingleResult(self.fc.addFileAncestors(ancestorDict))
            if resAncestors["OK"]:
                break
        else:
            self.log.error("Failed to register ancestors", resAncestors["Message"])
            raise RuntimeError("Failed to register ancestors")
        self.log.info("Successfully registered ancestors")

    def _markFilesDone(self):
        """Mark all the files as done."""
        self.log.info("Marking files as done")
        for opFile in self.waitingFiles:
            opFile.Status = "Done"

    def _cleanup(self):
        """Remove the tarball and the downloaded files."""
        self.log.info("Cleaning files and tarball")
        try:
            if "ArchiveLFN" in self.parameterDict:
                os.remove(os.path.basename(self.parameterDict["ArchiveLFN"]))
        except OSError as e:
            self.log.debug(f"Error when removing tarball: {str(e)}")
        try:
            shutil.rmtree(self.cacheFolder, ignore_errors=True)
        except OSError as e:
            self.log.debug(f"Error when removing cacheFolder: {str(e)}")
