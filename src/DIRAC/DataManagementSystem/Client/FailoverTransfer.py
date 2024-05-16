""" Failover Transfer

    The failover transfer client exposes the following methods:
    - transferAndRegisterFile()
    - transferAndRegisterFileFailover()

    Initially these methods were developed inside workflow modules but
    have evolved to a generic 'transfer file with failover' client.

    The transferAndRegisterFile() method will correctly set registration
    requests in case of failure.

    The transferAndRegisterFileFailover() method will attempt to upload
    a file to a list of alternative SEs and set appropriate replication
    to the original target SE as well as the removal request for the
    temporary replica.

"""

import errno
import time

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Utilities.DErrno import cmpError, EFCERR
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class FailoverTransfer:
    """.. class:: FailoverTransfer"""

    #############################################################################
    def __init__(self, requestObject=None, log=None, defaultChecksumType="ADLER32"):
        """Constructor function, can specify request object to instantiate
        FailoverTransfer or a new request object is created.
        """
        self.log = log
        if not self.log:
            self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.request = requestObject
        if not self.request:
            self.request = Request()
            self.request.RequestName = "noname_request"
            self.request.SourceComponent = "FailoverTransfer"

        self.defaultChecksumType = defaultChecksumType
        self.registrationProtocols = DMSHelpers().getRegistrationProtocols()

    #############################################################################
    def transferAndRegisterFile(
        self,
        fileName,
        localPath,
        lfn,
        destinationSEList,
        fileMetaDict,
        fileCatalog=None,
        masterCatalogOnly=False,
        retryUpload=False,
    ):
        """Performs the transfer and register operation with failover.

        :param filename: of absolute no use except for printing logs.
        :param localPath: path to the file locally
        :param lfn: LFN
        :param destinationSEList: list of possible destination for the file.
          Loop over it until one succeeds or we reach the end of it.
        :param fileMetaDict: file metadata for registration
        :param fileCatalog: list of catalogs to use (see :py:class:`DIRAC.DataManagementSystem.Client.DataManager`)
        :param masterCatalogOnly: use only master catalog (see :py:class:`DIRAC.DataManagementSystem.Client.DataManager`)
        :param retryUpload: if set to True, and there is only one output SE in destinationSEList, retry several times.

        """
        errorList = []
        fileGUID = fileMetaDict.get("GUID", None)
        fileChecksum = fileMetaDict.get("Checksum", None)

        for se in destinationSEList:
            # We put here some retry in case the problem comes from the FileCatalog
            # being unavailable. If it is, then the `hasAccess` call would fail,
            # and we would not make any failover request. So the only way is to wait a bit
            # This keeps the WN busy for a while, but at least we do not lose all the processing
            # time we just spent
            # This same retry path is taken if we only have one possible stage out SE
            # and retryUpload is True
            for sleeptime in (10, 60, 300, 600):
                self.log.info(
                    "Attempting dm.putAndRegister",
                    "('%s','%s','%s',guid='%s',catalog='%s', checksum = '%s')"
                    % (lfn, localPath, se, fileGUID, fileCatalog, fileChecksum),
                )

                result = DataManager(catalogs=fileCatalog, masterCatalogOnly=masterCatalogOnly).putAndRegister(
                    lfn, localPath, se, guid=fileGUID, checksum=fileChecksum
                )
                # retry on any failure
                if result["OK"]:
                    self.log.verbose(result)
                    break
                elif cmpError(result, EFCERR):
                    self.log.debug("transferAndRegisterFile: FC unavailable, retry")
                elif cmpError(result, errno.ENAMETOOLONG):
                    self.log.debug(f"transferAndRegisterFile: this file won't be uploaded: {result}")
                    return result
                elif retryUpload and len(destinationSEList) == 1:
                    self.log.debug("transferAndRegisterFile: Failed uploading to the only SE, retry")
                else:
                    self.log.debug("dm.putAndRegister failed, but move to the next")
                    break
                time.sleep(sleeptime)

            if not result["OK"]:
                self.log.error("dm.putAndRegister failed with message", result["Message"])
                errorList.append(result["Message"])
                continue

            if not result["Value"]["Failed"]:
                self.log.info("dm.putAndRegister successfully uploaded and registered", f"{fileName} to {se}")
                return S_OK({"uploadedSE": se, "lfn": lfn})

            # Now we know something went wrong
            self.log.warn("Didn't manage to do everything, now adding requests for the missing operation")

            errorDict = result["Value"]["Failed"][lfn]
            if "register" not in errorDict:
                self.log.error("dm.putAndRegister failed with unknown error", str(errorDict))
                errorList.append(f"Unknown error while attempting upload to {se}")
                continue

            # fileDict = errorDict['register']
            # Therefore the registration failed but the upload was successful
            if not fileCatalog:
                fileCatalog = ""

            if masterCatalogOnly:
                fileCatalog = FileCatalog().getMasterCatalogNames()["Value"]

            result = self._setRegistrationRequest(lfn, se, fileMetaDict, fileCatalog)
            if not result["OK"]:
                self.log.error("Failed to set registration request", f"SE {se} and metadata: \n{fileMetaDict}")
                errorList.append(f"Failed to set registration request for: SE {se} and metadata: \n{fileMetaDict}")
                continue
            else:
                self.log.info("Successfully set registration request", f"for: SE {se} and metadata: \n{fileMetaDict}")
                metadata = {}
                metadata["filedict"] = fileMetaDict
                metadata["uploadedSE"] = se
                metadata["lfn"] = lfn
                metadata["registration"] = "request"
                return S_OK(metadata)

        self.log.error("Failed to upload output data file", f"Encountered {len(errorList)} errors")
        return S_ERROR("Failed to upload output data file")

    #############################################################################
    def transferAndRegisterFileFailover(
        self,
        fileName,
        localPath,
        lfn,
        targetSE,
        failoverSEList,
        fileMetaDict,
        fileCatalog=None,
        masterCatalogOnly=False,
    ):
        """Performs the transfer and register operation to failover storage and sets the
        necessary replication and removal requests to recover.
        """
        failover = self.transferAndRegisterFile(
            fileName, localPath, lfn, failoverSEList, fileMetaDict, fileCatalog, masterCatalogOnly=masterCatalogOnly
        )
        if not failover["OK"]:
            self.log.error("Could not upload file to failover SEs", failover["Message"])
            return failover

        # set removal requests and replication requests
        result = self._setFileReplicationRequest(lfn, targetSE, fileMetaDict, sourceSE=failover["Value"]["uploadedSE"])
        if not result["OK"]:
            self.log.error("Could not set file replication request", result["Message"])
            return result

        lfn = failover["Value"]["lfn"]
        failoverSE = failover["Value"]["uploadedSE"]
        self.log.info("Attempting to set replica removal request", f"for LFN {lfn} at failover SE {failoverSE}")
        result = self._setReplicaRemovalRequest(lfn, failoverSE)
        if not result["OK"]:
            self.log.error("Could not set removal request", result["Message"])
            return result

        return S_OK({"uploadedSE": failoverSE, "lfn": lfn})

    def getRequest(self):
        """get the accumulated request object"""
        return self.request

    def commitRequest(self):
        """Send request to the Request Management Service"""
        if self.request.isEmpty():
            return S_OK()

        isValid = RequestValidator().validate(self.request)
        if not isValid["OK"]:
            return S_ERROR(f"Failover request is not valid: {isValid['Message']}")
        else:
            requestClient = ReqClient()
            result = requestClient.putRequest(self.request)
            return result

    #############################################################################
    def _setFileReplicationRequest(self, lfn, targetSE, fileMetaDict, sourceSE=""):
        """Sets a registration request."""
        self.log.info("Setting ReplicateAndRegister request", f"for {lfn} to {targetSE}")

        transfer = Operation()
        transfer.Type = "ReplicateAndRegister"
        transfer.TargetSE = targetSE
        if sourceSE:
            transfer.SourceSE = sourceSE

        trFile = File()
        trFile.LFN = lfn

        cksm = fileMetaDict.get("Checksum", None)
        cksmType = fileMetaDict.get("ChecksumType", self.defaultChecksumType)
        if cksm and cksmType:
            trFile.Checksum = cksm
            trFile.ChecksumType = cksmType
        size = fileMetaDict.get("Size", 0)
        if size:
            trFile.Size = size
        guid = fileMetaDict.get("GUID", "")
        if guid:
            trFile.GUID = guid

        transfer.addFile(trFile)

        self.request.addOperation(transfer)

        return S_OK()

    #############################################################################
    def _setRegistrationRequest(self, lfn, targetSE, fileDict, catalog):
        """Sets a registration request

        :param str lfn: LFN
        :param list se: list of SE (or just string)
        :param list catalog: list (or string) of catalogs to use
        :param dict fileDict: file metadata
        """
        self.log.info("Setting registration request", f"for {lfn} at {targetSE}.")

        if not isinstance(catalog, list):
            catalog = [catalog]

        for cat in catalog:
            register = Operation()
            register.Type = "RegisterFile"
            register.Catalog = cat
            register.TargetSE = targetSE

            regFile = File()
            regFile.LFN = lfn
            regFile.Checksum = fileDict.get("Checksum", "")
            regFile.ChecksumType = fileDict.get("ChecksumType", self.defaultChecksumType)
            regFile.Size = fileDict.get("Size", 0)
            regFile.GUID = fileDict.get("GUID", "")

            se = StorageElement(targetSE)
            res = returnSingleResult(se.getURL(lfn, self.registrationProtocols))
            if not res["OK"]:
                self.log.error("Unable to get PFN for LFN", res["Message"])
                return res
            regFile.PFN = res["Value"]

            register.addFile(regFile)
            self.request.addOperation(register)

        return S_OK()

    #############################################################################
    def _setReplicaRemovalRequest(self, lfn, se):
        """Sets a removal request for a replica.

        :param str lfn: LFN
        :param se:
        """
        if isinstance(se, str):
            se = ",".join([se.strip() for se in se.split(",") if se.strip()])

        removeReplica = Operation()

        removeReplica.Type = "RemoveReplica"
        removeReplica.TargetSE = se

        replicaToRemove = File()
        replicaToRemove.LFN = lfn

        removeReplica.addFile(replicaToRemove)

        self.request.addOperation(removeReplica)
        return S_OK()

    #############################################################################
    def _setFileRemovalRequest(self, lfn, se="", pfn=""):
        """Sets a removal request for a file including all replicas."""
        remove = Operation()
        remove.Type = "RemoveFile"
        if se:
            remove.TargetSE = se
        rmFile = File()
        rmFile.LFN = lfn
        if pfn:
            rmFile.PFN = pfn
        remove.addFile(rmFile)
        self.request.addOperation(remove)
        return S_OK()
