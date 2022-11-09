""" This is the Proxy storage element client """
import os

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Tornado.Client.ClientSelector import TransferClientSelector as TransferClient
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.File import getSize


class ProxyStorage(StorageBase):

    _INPUT_PROTOCOLS = ["file", "dip", "dips"]
    _OUTPUT_PROTOCOLS = ["dip", "dips"]

    def __init__(self, storageName, parameters):

        StorageBase.__init__(self, storageName, parameters)
        self.pluginName = "Proxy"
        self.url = "DataManagement/StorageElementProxy"

    ######################################
    # File transfer functionalities
    ######################################

    def getFile(self, path, localPath=False):
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        failed = {}
        successful = {}
        client = Client(url=self.url)
        # Make sure transferClient uses the same ProxyStorage instance.
        # Only the this one holds the file we want to transfer.
        transferClient = TransferClient(client.serviceURL)
        for src_url in urls.keys():
            res = client.prepareFile(self.name, src_url)
            if not res["OK"]:
                gLogger.error("ProxyStorage.getFile: Failed to prepare file on remote server.", res["Message"])
                failed[src_url] = res["Message"]
            else:
                fileName = os.path.basename(src_url)
                if localPath:
                    dest_file = f"{localPath}/{fileName}"
                else:
                    dest_file = f"{os.getcwd()}/{fileName}"
                res = transferClient.receiveFile(dest_file, "getFile/%s" % fileName)
                if not res["OK"]:
                    gLogger.error("ProxyStorage.getFile: Failed to recieve file from proxy server.", res["Message"])
                    failed[src_url] = res["Message"]
                elif not os.path.exists(dest_file):
                    errStr = "ProxyStorage.getFile: The destination local file does not exist."
                    gLogger.error(errStr, dest_file)
                    failed[src_url] = errStr
                else:
                    destSize = getSize(dest_file)
                    if destSize == -1:
                        errStr = "ProxyStorage.getFile: Failed to get the local file size."
                        gLogger.error(errStr, dest_file)
                        failed[src_url] = errStr
                    else:
                        successful[src_url] = destSize
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def putFile(self, path, sourceSize=0):

        client = Client(url=self.url)

        if sourceSize:
            gLogger.debug(
                "ProxyStorage.putFile: The client has provided the source file size implying\
         a replication is requested."
            )
            return client.callProxyMethod(self.name, "putFile", [path], {"sourceSize": sourceSize})

        gLogger.debug("ProxyStorage.putFile: No source size was provided therefore a simple put will be performed.")
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]
        failed = {}
        successful = {}
        # make sure transferClient uses the same ProxyStorage instance we uploaded the file to
        transferClient = TransferClient(client.serviceURL)
        for dest_url, src_file in urls.items():
            fileName = os.path.basename(dest_url)
            res = transferClient.sendFile(src_file, "putFile/%s" % fileName)
            if not res["OK"]:
                gLogger.error("ProxyStorage.putFile: Failed to send file to proxy server.", res["Message"])
                failed[dest_url] = res["Message"]
            else:
                res = client.uploadFile(self.name, dest_url)
                if not res["OK"]:
                    gLogger.error(
                        "ProxyStorage.putFile: Failed to upload file to storage element from proxy server.",
                        res["Message"],
                    )
                    failed[dest_url] = res["Message"]
                else:
                    res = self.__executeOperation(dest_url, "getFileSize")
                    if not res["OK"]:
                        gLogger.error(
                            "ProxyStorage.putFile: Failed to determine destination file size.", res["Message"]
                        )
                        failed[dest_url] = res["Message"]
                    else:
                        successful[dest_url] = res["Value"]
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    ######################################
    # File manipulation functionalities
    ######################################

    def exists(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "exists", [path], {})

    def isFile(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "isFile", [path], {})

    def getFileSize(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "getFileSize", [path], {})

    def getFileMetadata(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "getFileMetadata", [path], {})

    def removeFile(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "removeFile", [path], {})

    def prestageFile(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "prestageFile", [path], {})

    def prestageFileStatus(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "prestageFileStatus", [path], {})

    def releaseFile(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "releaseFile", [path], {})

    ######################################
    # Directory manipulation functionalities
    ######################################

    def isDirectory(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "isDirectory", [path], {})

    def getDirectoryMetadata(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "getDirectoryMetadata", [path], {})

    def getDirectorySize(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "getDirectorySize", [path], {})

    def listDirectory(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "listDirectory", [path], {})

    def createDirectory(self, path):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "createDirectory", [path], {})

    def removeDirectory(self, path, recursive=False):
        client = Client(url=self.url)
        return client.callProxyMethod(self.name, "removeDirectory", [path], {"recursive": recursive})

    def getDirectory(self, path):
        return S_ERROR("Not supported")

    def putDirectory(self, path):
        return S_ERROR("Not supported")

    def __executeOperation(self, url, method):
        """Executes the requested functionality with the supplied url"""
        fcn = None
        if hasattr(self, method) and callable(getattr(self, method)):
            fcn = getattr(self, method)
        if not fcn:
            return S_ERROR("Unable to invoke %s, it isn't a member function of ProxyStorage" % method)
        res = fcn([url])
        if not res["OK"]:
            return res
        elif url not in res["Value"]["Successful"]:
            return S_ERROR(res["Value"]["Failed"][url])
        return S_OK(res["Value"]["Successful"][url])
