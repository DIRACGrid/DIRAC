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
import tempfile
import threading
import time

import requests
from diracx.client.models import SandboxInfo

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.File import getGlobbedTotalSize, mkDir
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.FrameworkSystem.Utilities.diracx import TheImpersonator


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
        self.__localSEName = self.getCSOption("LocalSE", "SandboxSE")
        self._useDiracXBackend = self.getCSOption("UseDiracXBackend", False)
        self._maxUploadBytes = self.getCSOption("MaxSandboxSizeMiB", 10) * 1048576
        # Execute the purge once every 1000 calls
        SandboxStoreHandler.__purgeCount += 1
        if SandboxStoreHandler.__purgeCount > 1000:
            SandboxStoreHandler.__purgeCount = 0
        if SandboxStoreHandler.__purgeCount == 0:
            threading.Thread(target=self.purgeUnusedSandboxes).start()

    def __getSandboxPath(self, md5):
        """Generate the sandbox path"""
        credDict = self.getRemoteCredentials()
        if Properties.JOB_SHARING in credDict["properties"]:
            idField = credDict["group"]
        else:
            idField = f"{credDict['username']}.{credDict['group']}"
        pathItems = ["/", "SandBox", idField[0], idField]
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
        vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))

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

                with TheImpersonator(credDict, source="SandboxStore") as client:
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
                result = self.sandboxDB.assignSandboxesToEntities(assignTo, credDict["username"], credDict["group"])
                if not result["OK"]:
                    return result
                return S_OK(res.pfn)

        sbPath = self.__getSandboxPath(f"{aHash}.{extension}")

        result = self.sandboxDB.getSandboxId(self.__localSEName, sbPath, credDict["username"], credDict["group"])
        if result["OK"]:
            gLogger.info("Sandbox already exists. Skipping upload")
            if fileHelper:
                fileHelper.markAsTransferred()
            sbURL = f"SB:{self.__localSEName}|{sbPath}"
            assignTo = {key: [(sbURL, assignTo[key])] for key in assignTo}
            result = self.sandboxDB.assignSandboxesToEntities(assignTo, credDict["username"], credDict["group"])
            if not result["OK"]:
                return result
            return S_OK(sbURL)

        hdPath = self.__sbToHDPath(sbPath)

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
        # Register!
        gLogger.info("Registering sandbox in the DB with", f"SB:{self.__localSEName}|{sbPath}")
        fSize = getGlobbedTotalSize(hdPath)
        result = self.sandboxDB.registerAndGetSandbox(
            credDict["username"],
            credDict["group"],
            vo,
            self.__localSEName,
            sbPath,
            fSize,
        )
        if not result["OK"]:
            self.__secureUnlinkFile(hdPath)
            return result

        sbURL = f"SB:{self.__localSEName}|{sbPath}"
        assignTo = {key: [(sbURL, assignTo[key])] for key in assignTo}
        if not (result := self.sandboxDB.assignSandboxesToEntities(assignTo, credDict["username"], credDict["group"]))[
            "OK"
        ]:
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
        # Register in DB
        credDict = self.getRemoteCredentials()
        vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))
        result = self.sandboxDB.getSandboxId(self.__localSEName, sbPath, credDict["username"], credDict["group"])
        if result["OK"]:
            return S_OK(f"SB:{self.__localSEName}|{sbPath}")

        result = self.sandboxDB.registerAndGetSandbox(
            credDict["username"],
            credDict["group"],
            vo,
            self.__localSEName,
            sbPath,
            fileHelper.getTransferedBytes(),
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
        return S_OK(f"SB:{self.__localSEName}|{sbPath}")

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

        return result

    ##################
    # Getting assigned sandboxes

    types_getSandboxesAssignedToEntity = [str]

    def export_getSandboxesAssignedToEntity(self, entityId):
        """
        Get the sandboxes associated to a job and the association type
        """
        credDict = self.getRemoteCredentials()
        vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))
        result = self.sandboxDB.getSandboxesAssignedToEntity(entityId, credDict["username"], credDict["group"], vo)
        if not result["OK"]:
            return result
        sbDict = {}
        for SEName, SEPFN, SBType in result["Value"]:  # pylint: disable=invalid-name
            if SBType not in sbDict:
                sbDict[SBType] = []
            sbDict[SBType].append(f"SB:{SEName}|{SEPFN}")
        return S_OK(sbDict)

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
            with TheImpersonator(credDict, source="SandboxStore") as client:
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
        if not (result := self.__deleteSandboxFromBackend(SEName, SEPFN))["OK"]:
            gLogger.error("Cannot delete sandbox from backend", result["Message"])
            return
        if not (result := self.sandboxDB.deleteSandboxes([sbId]))["OK"]:
            gLogger.error("Cannot delete sandbox from DB", result["Message"])

    def __deleteSandboxFromBackend(self, SEName, SEPFN):
        gLogger.info("Purging sandbox", f"SB:{SEName}|{SEPFN}")
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


class SandboxStoreHandler(SandboxStoreHandlerMixin, RequestHandler):
    def initialize(self):
        return self.initializeRequest()
