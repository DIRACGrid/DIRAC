""" Handler for CAs + CRLs bundles
"""
import tarfile
import os
import io

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities import File, List
from DIRAC.Core.Security import Locations, Utilities


class BundleManager:
    def __init__(self, baseCSPath):
        self.__csPath = baseCSPath
        self.__bundles = {}
        self.updateBundles()

    def __getDirsToBundle(self):
        dirsToBundle = {}
        result = gConfig.getOptionsDict(f"{self.__csPath}/DirsToBundle")
        if result["OK"]:
            dB = result["Value"]
            for bId in dB:
                dirsToBundle[bId] = List.fromChar(dB[bId])
        if gConfig.getValue(f"{self.__csPath}/BundleCAs", True):
            dirsToBundle["CAs"] = [
                f"{Locations.getCAsLocation()}/*.0",
                f"{Locations.getCAsLocation()}/*.signing_policy",
                f"{Locations.getCAsLocation()}/*.pem",
            ]
        if gConfig.getValue(f"{self.__csPath}/BundleCRLs", True):
            dirsToBundle["CRLs"] = [f"{Locations.getCAsLocation()}/*.r0"]
        return dirsToBundle

    def getBundles(self):
        return {bId: self.__bundles[bId] for bId in self.__bundles}

    def bundleExists(self, bId):
        return bId in self.__bundles

    def getBundleVersion(self, bId):
        try:
            return self.__bundles[bId][0]
        except Exception:
            return ""

    def getBundleData(self, bId):
        try:
            return self.__bundles[bId][1]
        except Exception:
            return ""

    def updateBundles(self):
        dirsToBundle = self.__getDirsToBundle()
        # Delete bundles that don't have to be updated
        for bId in self.__bundles:
            if bId not in dirsToBundle:
                gLogger.info(f"Deleting old bundle {bId}")
                del self.__bundles[bId]
        for bId in dirsToBundle:
            bundlePaths = dirsToBundle[bId]
            gLogger.info(f"Updating {bId} bundle {bundlePaths}")
            buffer_ = io.BytesIO()
            filesToBundle = sorted(File.getGlobbedFiles(bundlePaths))
            if filesToBundle:
                commonPath = os.path.commonprefix(filesToBundle)
                commonEnd = len(commonPath)
                gLogger.info(f"Bundle will have {len(filesToBundle)} files with common path {commonPath}")
                with tarfile.open("dummy", "w:gz", buffer_) as tarBuffer:
                    for filePath in filesToBundle:
                        tarBuffer.add(filePath, filePath[commonEnd:])
                zippedData = buffer_.getvalue()
                buffer_.close()
                hash_ = File.getMD5ForFiles(filesToBundle)
                gLogger.info(f"Bundled {bId} : {len(zippedData)} bytes ({hash_})")
                self.__bundles[bId] = (hash_, zippedData)
            else:
                self.__bundles[bId] = (None, None)


class BundleDeliveryHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        csPath = serviceInfoDict["serviceSectionPath"]
        cls.bundleManager = BundleManager(csPath)
        updateBundleTime = gConfig.getValue(f"{csPath}/BundlesLifeTime", 3600 * 6)
        gLogger.info(f"Bundles will be updated each {updateBundleTime} secs")
        gThreadScheduler.addPeriodicTask(updateBundleTime, cls.bundleManager.updateBundles)
        return S_OK()

    types_getListOfBundles = []

    @classmethod
    def export_getListOfBundles(cls):
        return S_OK(cls.bundleManager.getBundles())

    def transfer_toClient(self, fileId, token, fileHelper):
        version = ""
        if isinstance(fileId, str):
            if fileId in ["CAs", "CRLs"]:
                return self.__transferFile(fileId, fileHelper)
            else:
                bId = fileId
        elif isinstance(fileId, (list, tuple)):
            if len(fileId) == 0:
                fileHelper.markAsTransferred()
                return S_ERROR("No bundle specified!")
            elif len(fileId) == 1:
                bId = fileId[0]
            else:
                bId = fileId[0]
                version = fileId[1]
        if not self.bundleManager.bundleExists(bId):
            fileHelper.markAsTransferred()
            return S_ERROR(f"Unknown bundle {bId}")

        bundleVersion = self.bundleManager.getBundleVersion(bId)
        if bundleVersion is None:
            fileHelper.markAsTransferred()
            return S_ERROR(f"Empty bundle {bId}")

        if version == bundleVersion:
            fileHelper.markAsTransferred()
            return S_OK(bundleVersion)

        buffer_ = io.BytesIO(self.bundleManager.getBundleData(bId))
        result = fileHelper.DataSourceToNetwork(buffer_)
        buffer_.close()
        if not result["OK"]:
            return result
        return S_OK(bundleVersion)

    def __transferFile(self, filetype, fileHelper):
        """
        This file is creates and transfers the CAs or CRLs file to the client.
        :param str filetype: we can define which file will be transfered to the client
        :param object fileHelper:
        :return: S_OK or S_ERROR
        """
        if filetype == "CAs":
            retVal = Utilities.generateCAFile()
        elif filetype == "CRLs":
            retVal = Utilities.generateRevokedCertsFile()
        else:
            return S_ERROR(f"Not supported file type {filetype}")

        if not retVal["OK"]:
            return retVal
        else:
            result = fileHelper.getFileDescriptor(retVal["Value"], "r")
            if not result["OK"]:
                result = fileHelper.sendEOF()
                # better to check again the existence of the file
                if not os.path.exists(retVal["Value"]):
                    return S_ERROR(f"File {os.path.basename(retVal['Value'])} does not exist")
                else:
                    return S_ERROR("Failed to get file descriptor")
            fileDescriptor = result["Value"]
            result = fileHelper.FDToNetwork(fileDescriptor)
            fileHelper.oFile.close()  # close the file and return
            return result


class BundleDeliveryHandler(BundleDeliveryHandlerMixin, RequestHandler):
    pass
