""" Handler for CAs + CRLs bundles
"""

import io
import os
import tarfile

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Locations, Utilities
from DIRAC.Core.Utilities import File, List


class BundleManager:
    """Utility class"""

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
                os.path.join(Locations.getCAsLocation()),
                "*.0",
                os.path.join(Locations.getCAsLocation()),
                "*.signing_policy",
                os.path.join(Locations.getCAsLocation()),
                "*.pem",
            ]
        if gConfig.getValue(f"{self.__csPath}/BundleCRLs", True):
            dirsToBundle["CRLs"] = [os.path.join(Locations.getCAsLocation(), "*.r0")]
        return dirsToBundle

    def bundleExists(self, bId):
        return bId in self.__bundles

    def getBundleVersion(self, bId):
        try:
            return self.__bundles[bId][0]
        except (KeyError, IndexError):
            return ""

    def getBundleData(self, bId):
        try:
            return self.__bundles[bId][1]
        except (KeyError, IndexError):
            return ""

    def updateBundles(self):
        dirsToBundle = self.__getDirsToBundle()
        # Delete bundles that don't have to be updated
        for bId in self.__bundles:
            if bId not in dirsToBundle:
                gLogger.info(f"Deleting old bundle {bId}")
                del self.__bundles[bId]
        for bId, bundlePaths in dirsToBundle.items():
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
        return S_OK()

    def transfer_toClient(self, fileId, _token, fileHelper):
        self.bundleManager.updateBundles()

        version = ""
        bId = None
        if isinstance(fileId, str):
            if fileId in ["CAs", "CRLs"]:
                return self.__transferFile(fileId, fileHelper)
            bId = fileId
        elif isinstance(fileId, (list, tuple)):
            if len(fileId) == 0:
                fileHelper.markAsTransferred()
                return S_ERROR("No bundle specified!")
            if len(fileId) == 1:
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
        result = fileHelper.getFileDescriptor(retVal["Value"], "r")
        if not result["OK"]:
            result = fileHelper.sendEOF()
            # better to check again the existence of the file
            if not os.path.exists(retVal["Value"]):
                return S_ERROR(f"File {os.path.basename(retVal['Value'])} does not exist")
            return S_ERROR("Failed to get file descriptor")
        fileDescriptor = result["Value"]
        result = fileHelper.FDToNetwork(fileDescriptor)
        fileHelper.oFile.close()  # close the file and return
        return result


class BundleDeliveryHandler(BundleDeliveryHandlerMixin, RequestHandler):
    """DISET version of the service"""
