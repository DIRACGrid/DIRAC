""" SandboxHandler is the implementation of the Sandbox service
    in the DISET framework

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SandboxStore
  :end-before: ##END
  :dedent: 2
  :caption: SandboxStore options
"""
import hashlib
import os
import requests
import tempfile
import threading
import time

from DIRAC import S_ERROR, S_OK, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Locations, Properties, X509Certificate
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Service.StorageElementHandler import getDiskSpace
from DIRAC.FrameworkSystem.Utilities.diracx import TheImpersonator
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.File import getGlobbedTotalSize

from diracx.client.models import SandboxInfo


class SandboxStoreHandlerMixin:
    __purgeCount = -1
    __purgeLock = threading.Lock()
    __purgeWorking = False

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB object"""
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.SandboxMetadataDB", "SandboxMetadataDB")
            if not result["OK"]:
                return result
            cls.sandboxDB = result["Value"]()

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {repr(excp)}")
        return S_OK()

    def initializeRequest(self):
        self.__backend = self.getCSOption("Backend", "local")
        self.__localSEName = self.getCSOption("LocalSE", "SandboxSE")
        self._useDiracXBackend = self.getCSOption("UseDiracXBackend", False)
        self._maxUploadBytes = self.getCSOption("MaxSandboxSizeMiB", 10) * 1048576
        if self.__backend.lower() == "local" or self.__backend == self.__localSEName:
            self.__useLocalStorage = True
            self.__seNameToUse = self.__localSEName
        else:
            self.__useLocalStorage = False
            self.__externalSEName = self.__backend
            self.__seNameToUse = self.__backend
        # Execute the purge once every 1000 calls
        SandboxStoreHandler.__purgeCount += 1
        if SandboxStoreHandler.__purgeCount > self.getCSOption("QueriesBeforePurge", 1000):
            SandboxStoreHandler.__purgeCount = 0
        if SandboxStoreHandler.__purgeCount == 0:
            threading.Thread(target=self.purgeUnusedSandboxes).start()

    def __getSandboxPath(self, md5):
        """Generate the sandbox path"""
        # prefix = self.getCSOption( "SandboxPrefix", "SandBox" )
        prefix = "SandBox"
        credDict = self.getRemoteCredentials()
        if Properties.JOB_SHARING in credDict["properties"]:
            idField = credDict["group"]
        else:
            idField = f"{credDict['username']}.{credDict['group']}"
        pathItems = ["/", prefix, idField[0], idField]
        pathItems.extend([md5[0:3], md5[3:6], md5])
        return os.path.join(*pathItems)

    def _getFromClient(self, fileId, token, fileSize, fileHelper=None, data=""):
        """
        Receive a file as a sandbox
        """

        if self._maxUploadBytes and fileSize > self._maxUploadBytes:
            if fileHelper:
                fileHelper.markAsTransferred()
            return S_ERROR("Sandbox is too big. Please upload it to a grid storage element")

        if isinstance(fileId, (list, tuple)):
            if len(fileId) > 1:
                assignTo = fileId[1]
                fileId = fileId[0]
            else:
                return S_ERROR("File identified tuple has to have length greater than 1")
        else:
            assignTo = {}

        extPos = fileId.find(".tar")
        if extPos > -1:
            extension = fileId[extPos + 1 :]
            aHash = fileId[:extPos]
        else:
            extension = ""
            aHash = fileId
        gLogger.info("Upload requested", f"for {aHash} [{extension}]")

        credDict = self.getRemoteCredentials()
        vo = Registry.getVOForGroup(credDict["group"])

        disabledVOs = gConfig.getValue("/DiracX/DisabledVOs", [])
        if self._useDiracXBackend and vo not in disabledVOs:
            gLogger.info("Forwarding to DiracX")
            with tempfile.TemporaryFile(mode="w+b") as tar_fh:
                result = fileHelper.networkToDataSink(tar_fh, maxFileSize=self._maxUploadBytes)
                if not result["OK"]:
                    return result
                tar_fh.seek(0)

                hasher = hashlib.sha256()
                while data := tar_fh.read(512 * 1024):
                    hasher.update(data)
                checksum = hasher.hexdigest()
                tar_fh.seek(0)
                gLogger.debug("Sandbox checksum is", checksum)

                sandbox_info = SandboxInfo(
                    checksum_algorithm="sha256",
                    checksum=checksum,
                    size=os.stat(tar_fh.fileno()).st_size,
                    format=extension,
                )

                with TheImpersonator(credDict) as client:
                    res = client.jobs.initiate_sandbox_upload(sandbox_info)

                if res.url:
                    gLogger.debug("Uploading sandbox for", res.pfn)
                    files = {"file": ("file", tar_fh)}

                    response = requests.post(res.url, data=res.fields, files=files, timeout=300)

                    gLogger.debug("Sandbox uploaded", f"for {res.pfn} with status code {response.status_code}")
                    # TODO: Handle this error better
                    try:
                        response.raise_for_status()
                    except Exception as e:
                        return S_ERROR("Error uploading sandbox", repr(e))
                else:
                    gLogger.debug("Sandbox already exists in storage backend", res.pfn)

                assignTo = {key: [(res.pfn, assignTo[key])] for key in assignTo}
                result = self.export_assignSandboxesToEntities(assignTo)
                if not result["OK"]:
                    return result
                return S_OK(res.pfn)

        sbPath = self.__getSandboxPath(f"{aHash}.{extension}")
        # Generate the location
        result = self.__generateLocation(sbPath)
        if not result["OK"]:
            return result
        seName, sePFN = result["Value"]

        result = self.sandboxDB.getSandboxId(seName, sePFN, credDict["username"], credDict["group"])
        if result["OK"]:
            gLogger.info("Sandbox already exists. Skipping upload")
            if fileHelper:
                fileHelper.markAsTransferred()
            sbURL = f"SB:{seName}|{sePFN}"
            assignTo = {key: [(sbURL, assignTo[key])] for key in assignTo}
            result = self.export_assignSandboxesToEntities(assignTo)
            if not result["OK"]:
                return result
            return S_OK(sbURL)

        if self.__useLocalStorage:
            hdPath = self.__sbToHDPath(sbPath)
        else:
            hdPath = False
        # Write to local file

        if fileHelper:
            result = self.__networkToFile(fileHelper, hdPath)
        elif data:
            hdPath = os.path.realpath(hdPath)
            mkDir(os.path.dirname(hdPath))
            with open(hdPath, "bw") as output:
                output.write(data)
            result = S_OK(hdPath)
        else:
            result = S_ERROR("No data provided")

        if not result["OK"]:
            gLogger.error("Error while receiving sandbox file", result["Message"])
            return result
        hdPath = result["Value"]
        gLogger.info("Wrote sandbox to file", hdPath)
        # Check hash!
        if fileHelper:
            hdHash = fileHelper.getHash()
        else:
            oMD5 = hashlib.md5()
            with open(hdPath, "rb") as fd:
                bData = fd.read(10240)
                while bData:
                    oMD5.update(bData)
                    bData = fd.read(10240)
            hdHash = oMD5.hexdigest()
        if hdHash != aHash:
            self.__secureUnlinkFile(hdPath)
            gLogger.error("Hashes don't match! Client defined hash is different with received data hash!")
            return S_ERROR("Hashes don't match!")
        # If using remote storage, copy there!
        if not self.__useLocalStorage:
            gLogger.info("Uploading sandbox to external storage")
            result = self.__copyToExternalSE(hdPath, sbPath)
            self.__secureUnlinkFile(hdPath)
            if not result["OK"]:
                return result
            sbPath = result["Value"][1]
        # Register!
        gLogger.info("Registering sandbox in the DB with", f"SB:{self.__seNameToUse}|{sbPath}")
        fSize = getGlobbedTotalSize(hdPath)
        result = self.sandboxDB.registerAndGetSandbox(
            credDict["username"],
            credDict["group"],
            self.__seNameToUse,
            sbPath,
            fSize,
        )
        if not result["OK"]:
            self.__secureUnlinkFile(hdPath)
            return result

        sbURL = f"SB:{self.__seNameToUse}|{sbPath}"
        assignTo = {key: [(sbURL, assignTo[key])] for key in assignTo}
        result = self.export_assignSandboxesToEntities(assignTo)
        if not result["OK"]:
            return result
        return S_OK(sbURL)

    def transfer_fromClient(self, fileId, token, fileSize, fileHelper):
        """
        Receive a file as a sandbox
        """

        return self._getFromClient(fileId, token, fileSize, fileHelper=fileHelper)

    def transfer_bulkFromClient(self, fileId, token, _fileSize, fileHelper):
        """Receive files packed into a tar archive by the fileHelper logic.
        token is used for access rights confirmation.
        """
        result = self.__networkToFile(fileHelper)
        if not result["OK"]:
            return result
        tmpFilePath = result["OK"]
        gLogger.info("Got Sandbox to local storage", tmpFilePath)

        extension = fileId[fileId.find(".tar") + 1 :]
        sbPath = f"{self.__getSandboxPath(fileHelper.getHash())}.{extension}"
        gLogger.info("Sandbox path will be", sbPath)
        # Generate the location
        result = self.__generateLocation(sbPath)
        if not result["OK"]:
            return result
        seName, sePFN = result["Value"]
        # Register in DB
        credDict = self.getRemoteCredentials()
        result = self.sandboxDB.getSandboxId(seName, sePFN, credDict["username"], credDict["group"])
        if result["OK"]:
            return S_OK(f"SB:{seName}|{sePFN}")

        result = self.sandboxDB.registerAndGetSandbox(
            credDict["username"], credDict["group"], seName, sePFN, fileHelper.getTransferedBytes()
        )
        if not result["OK"]:
            self.__secureUnlinkFile(tmpFilePath)
            return result
        sbid, _newSandbox = result["Value"]
        gLogger.info("Registered in DB", f"with SBId {sbid}")

        result = self.__moveToFinalLocation(tmpFilePath, sbPath)
        self.__secureUnlinkFile(tmpFilePath)
        if not result["OK"]:
            gLogger.error("Could not move sandbox to final destination", result["Message"])
            return result

        gLogger.info("Moved to final destination")

        # Unlink temporal file if it's there
        self.__secureUnlinkFile(tmpFilePath)
        return S_OK(f"SB:{seName}|{sePFN}")

    def __generateLocation(self, sbPath):
        """
        Generate the location string
        """
        if self.__useLocalStorage:
            return S_OK((self.__localSEName, sbPath))
        # It's external storage
        storageElement = StorageElement(self.__externalSEName)
        res = storageElement.isValid()
        if not res["OK"]:
            errStr = "Failed to instantiate destination StorageElement"
            gLogger.error(errStr, self.__externalSEName)
            return S_ERROR(errStr)
        result = storageElement.getURL(sbPath)
        if not result["OK"] or sbPath not in result["Value"]["Successful"]:
            errStr = "Failed to generate PFN"
            gLogger.error(errStr, self.__externalSEName)
            return S_ERROR(errStr)
        destPfn = result["Value"]["Successful"][sbPath]
        return S_OK((self.__externalSEName, destPfn))

    def __sbToHDPath(self, sbPath):
        while sbPath and sbPath[0] == "/":
            sbPath = sbPath[1:]
        basePath = self.getCSOption("BasePath", "/opt/dirac/storage/sandboxes")
        return os.path.join(basePath, sbPath)

    def __networkToFile(self, fileHelper, destFileName=False):
        """
        Dump incoming network data to temporal file
        """
        tfd = None
        if not destFileName:
            try:
                tfd, destFileName = tempfile.mkstemp(prefix="DSB.")
                tfd.close()
            except Exception as e:
                gLogger.error(f"{repr(e).replace(',)', ')')}")
                return S_ERROR("Cannot create temporary file")

        destFileName = os.path.realpath(destFileName)
        mkDir(os.path.dirname(destFileName))

        try:
            if tfd is not None:
                fd = tfd
            else:
                fd = open(destFileName, "wb")
            result = fileHelper.networkToDataSink(fd, maxFileSize=self._maxUploadBytes)
            fd.close()
        except Exception as e:
            gLogger.error("Cannot open to write destination file", f"{destFileName}: {repr(e).replace(',)', ')')}")
            return S_ERROR("Cannot open to write destination file")
        if not result["OK"]:
            return result
        return S_OK(destFileName)

    def __secureUnlinkFile(self, filePath):
        try:
            os.unlink(filePath)
        except Exception as e:
            gLogger.warn(f"Could not unlink file {filePath}: {repr(e).replace(',)', ')')}")
            return False
        return True

    def __moveToFinalLocation(self, localFilePath, sbPath):
        if self.__useLocalStorage:
            hdFilePath = self.__sbToHDPath(sbPath)
            result = S_OK((self.__localSEName, sbPath))
            if os.path.isfile(hdFilePath):
                gLogger.info("There was already a sandbox with that name, skipping copy", sbPath)
            else:
                hdDirPath = os.path.dirname(hdFilePath)
                mkDir(hdDirPath)
                try:
                    os.rename(localFilePath, hdFilePath)
                except OSError as e:
                    errMsg = "Cannot move temporal file to final path"
                    gLogger.error(errMsg, repr(e).replace(",)", ")"))
                    result = S_ERROR(errMsg)
        else:
            result = self.__copyToExternalSE(localFilePath, sbPath)

        return result

    def __copyToExternalSE(self, localFilePath, sbPath):
        """
        Copy uploaded file to external SE
        """
        try:
            dm = DataManager()
            result = dm.put(sbPath, localFilePath, self.__externalSEName)
            if not result["OK"]:
                return result
            if "Successful" not in result["Value"]:
                gLogger.verbose("Oops, no successful transfers there", str(result))
                return S_ERROR("RM returned OK to the action but no successful transfers were there")
            okTrans = result["Value"]["Successful"]
            if sbPath not in okTrans:
                gLogger.verbose("Ooops, SB transfer wasn't in the successful ones", str(result))
                return S_ERROR("RM returned OK to the action but SB transfer wasn't in the successful ones")
            return S_OK((self.__externalSEName, okTrans[sbPath]))
        except Exception as e:
            gLogger.error("Error while moving sandbox to SE", f"{repr(e).replace(',)', ')')}")
            return S_ERROR("Error while moving sandbox to SE")

    ##################
    # Assigning sbs to jobs

    types_assignSandboxesToEntities = [dict]

    def export_assignSandboxesToEntities(self, enDict, ownerName="", ownerGroup=""):
        """
        Assign sandboxes to jobs.
        Expects a dict of { entityId : [ ( SB, SBType ), ... ] }
        """
        credDict = self.getRemoteCredentials()
        return self.sandboxDB.assignSandboxesToEntities(
            enDict, credDict["username"], credDict["group"], ownerName, ownerGroup
        )

    ##################
    # Unassign sbs to jobs

    types_unassignEntities = [(list, tuple)]

    def export_unassignEntities(self, entitiesList):
        """
        Unassign a list of jobs
        """
        credDict = self.getRemoteCredentials()
        return self.sandboxDB.unassignEntities(entitiesList, credDict["username"], credDict["group"])

    ##################
    # Getting assigned sandboxes

    types_getSandboxesAssignedToEntity = [str]

    def export_getSandboxesAssignedToEntity(self, entityId):
        """
        Get the sandboxes associated to a job and the association type
        """
        credDict = self.getRemoteCredentials()
        result = self.sandboxDB.getSandboxesAssignedToEntity(entityId, credDict["username"], credDict["group"])
        if not result["OK"]:
            return result
        sbDict = {}
        for SEName, SEPFN, SBType in result["Value"]:  # pylint: disable=invalid-name
            if SBType not in sbDict:
                sbDict[SBType] = []
            sbDict[SBType].append(f"SB:{SEName}|{SEPFN}")
        return S_OK(sbDict)

    ##################
    # Disk space left management

    types_getFreeDiskSpace = []

    def export_getFreeDiskSpace(self):
        """Get the free disk space of the storage element
        If no size is specified, terabytes will be used by default.
        """

        return getDiskSpace(self.getCSOption("BasePath", "/opt/dirac/storage/sandboxes"))

    types_getTotalDiskSpace = []

    def export_getTotalDiskSpace(self):
        """Get the total disk space of the storage element
        If no size is specified, terabytes will be used by default.
        """
        return getDiskSpace(self.getCSOption("BasePath", "/opt/dirac/storage/sandboxes"), total=True)

    ##################
    # Download sandboxes

    def transfer_toClient(self, fileID, token, fileHelper):
        """Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
        """

        return self._sendToClient(fileID, token, fileHelper=fileHelper)

    def _sendToClient(self, fileID, token, fileHelper=None, raw=False):
        credDict = self.getRemoteCredentials()
        serviceURL = self.serviceInfoDict["URL"]
        filePath = fileID.replace(serviceURL, "")

        # If the PFN starts with S3, we know it has been uploaded to the
        # S3 sandbox store, so download it from there before sending it
        if filePath.startswith("/S3"):
            with TheImpersonator(credDict) as client:
                res = client.jobs.get_sandbox_file(pfn=filePath)
                r = requests.get(res.url)
                r.raise_for_status()
                sbData = r.content
                if fileHelper:
                    from io import BytesIO

                    result = fileHelper.DataSourceToNetwork(BytesIO(sbData))
                    # fileHelper.oFile.close()
                    return result
                if raw:
                    return sbData
                return S_OK(sbData)

        result = self.sandboxDB.getSandboxId(self.__localSEName, filePath, credDict["username"], credDict["group"])
        if not result["OK"]:
            return result
        sbId = result["Value"]
        self.sandboxDB.accessedSandboxById(sbId)
        # If it's a local file
        hdPath = self.__sbToHDPath(filePath)
        if not os.path.isfile(hdPath):
            return S_ERROR("Sandbox does not exist")

        if fileHelper:
            result = fileHelper.getFileDescriptor(hdPath, "rb")
            if not result["OK"]:
                return result
            fd = result["Value"]
            result = fileHelper.FDToNetwork(fd)
            fileHelper.oFile.close()
            return result

        with open(hdPath, "rb") as fd:
            if raw:
                return fd.read()
            return S_OK(fd.read())

    ##################
    # Purge sandboxes

    def purgeUnusedSandboxes(self):
        # If a purge is already working skip
        SandboxStoreHandler.__purgeLock.acquire()
        try:
            if SandboxStoreHandler.__purgeWorking:
                if time.time() - SandboxStoreHandler.__purgeWorking < 86400:
                    gLogger.info("Sandbox purge still working")
                    return S_OK()
                gLogger.error("Sandbox purge took over 24 hours, it either died or needs optimising")
            SandboxStoreHandler.__purgeWorking = time.time()
        finally:
            SandboxStoreHandler.__purgeLock.release()

        gLogger.info("Purging sandboxes")
        result = self.sandboxDB.getUnusedSandboxes()
        if not result["OK"]:
            gLogger.error("Error while retrieving sandboxes to purge", result["Message"])
            SandboxStoreHandler.__purgeWorking = False
            return result
        sbList = result["Value"]
        gLogger.info("Got sandboxes to purge", f"({len(sbList)})")
        for i, (sbId, SEName, SEPFN) in enumerate(sbList):
            if i % 10000 == 0:
                gLogger.info("Purging", "%d out of %d" % (i, len(sbList)))
            self.__purgeSandbox(sbId, SEName, SEPFN)

        SandboxStoreHandler.__purgeWorking = False
        return S_OK()

    def __purgeSandbox(self, sbId, SEName, SEPFN):
        result = self.__deleteSandboxFromBackend(SEName, SEPFN)
        if not result["OK"]:
            gLogger.error("Cannot delete sandbox from backend", result["Message"])
            return
        result = self.sandboxDB.deleteSandboxes([sbId])
        if not result["OK"]:
            gLogger.error("Cannot delete sandbox from DB", result["Message"])

    def __deleteSandboxFromBackend(self, SEName, SEPFN):
        gLogger.info("Purging sandbox", f"SB:{SEName}|{SEPFN}")
        if SEName != self.__localSEName:
            return self.__deleteSandboxFromExternalBackend(SEName, SEPFN)
        hdPath = self.__sbToHDPath(SEPFN)
        try:
            if not os.path.isfile(hdPath):
                return S_OK()
        except Exception as e:
            gLogger.error("Cannot perform isfile", f"{hdPath} : {repr(e).replace(',)', ')')}")
            return S_ERROR(f"Error checking {hdPath}")
        try:
            os.unlink(hdPath)
        except Exception as e:
            gLogger.error("Cannot delete local sandbox", f"{hdPath} : {repr(e).replace(',)', ')')}")
        while hdPath:
            hdPath = os.path.dirname(hdPath)
            gLogger.info("Checking if dir is empty", hdPath)
            try:
                if not os.path.isdir(hdPath):
                    break
                if os.listdir(hdPath):
                    break
                gLogger.info("Trying to clean dir", hdPath)
                # Empty dir!
                os.rmdir(hdPath)
            except Exception as e:
                gLogger.error("Cannot clean directory", f"{hdPath} : {repr(e).replace(',)', ')')}")
                break
        return S_OK()

    def __deleteSandboxFromExternalBackend(self, SEName, SEPFN):
        if self.getCSOption("DelayedExternalDeletion", True):
            gLogger.info("Setting deletion request")
            try:
                # We need the hostDN used in order to pass these credentials to the
                # SandboxStoreDB..
                hostCertLocation, _ = Locations.getHostCertificateAndKeyLocation()
                hostCert = X509Certificate.X509Certificate()
                hostCert.loadFromFile(hostCertLocation)
                hostDN = hostCert.getSubjectDN().get("Value")

                # use the host authentication to fetch the data
                result = self.sandboxDB.getSandboxOwner(SEName, SEPFN, hostDN, "hosts")
                if not result["OK"]:
                    return result
                owner, _ownerDN, ownerGroup = result["Value"]

                request = Request()
                request.RequestName = f"RemoteSBDeletion:{SEName}|{SEPFN}:{time.time()}"
                request.Owner = owner
                request.OwnerGroup = ownerGroup
                physicalRemoval = Operation()
                physicalRemoval.Type = "PhysicalRemoval"
                physicalRemoval.TargetSE = SEName
                fileToRemove = File()
                fileToRemove.PFN = SEPFN
                physicalRemoval.addFile(fileToRemove)
                request.addOperation(physicalRemoval)
                return ReqClient().putRequest(request)
            except Exception as e:
                gLogger.exception("Exception while setting deletion request")
                return S_ERROR(f"Cannot set deletion request: {e}")
        else:
            gLogger.info("Deleting external Sandbox")
            try:
                return StorageElement(SEName).removeFile(SEPFN)
            except Exception:
                gLogger.exception("RM raised an exception while trying to delete a remote sandbox")
                return S_ERROR("RM raised an exception while trying to delete a remote sandbox")


class SandboxStoreHandler(SandboxStoreHandlerMixin, RequestHandler):
    def initialize(self):
        return self.initializeRequest()
