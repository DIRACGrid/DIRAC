""" This is for transfers what RPCClient is for RPC calls
"""
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import File
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper


class TransferClient(BaseClient):
    def _sendTransferHeader(self, actionName, fileInfo):
        """
        Send the header of the transfer

        :type actionName: string
        :param actionName: Action to execute
        :type fileInfo: tuple
        :param fileInfo: Information of the target file/bulk
        :return: S_OK/S_ERROR
        """
        retVal = self._connect()
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            # FFC -> File from Client
            retVal = self._proposeAction(transport, ("FileTransfer", actionName))
            if not retVal["OK"]:
                return retVal
            # We need to convert to list
            retVal = transport.sendData(S_OK(list(fileInfo)))
            if not retVal["OK"]:
                return retVal
            retVal = transport.receiveData()
            if not retVal["OK"]:
                return retVal
            return S_OK((trid, transport))
        except Exception as e:
            self._disconnect(trid)
            return S_ERROR("Cound not request transfer: %s" % str(e))

    def sendFile(self, filename, fileId, token=""):
        """
        Send a file to server

        :type filename: string / file descriptor / file object
        :param filename: File to send to server
        :type fileId: any
        :param fileId: Identification of the file being sent
        :type token: string
        :param token: Optional token for the file
        :return: S_OK/S_ERROR
        """
        fileHelper = FileHelper()
        if "NoCheckSum" in token:
            fileHelper.disableCheckSum()
        retVal = fileHelper.getFileDescriptor(filename, "r")
        if not retVal["OK"]:
            return retVal
        fd = retVal["Value"]
        retVal = self._sendTransferHeader("FromClient", (fileId, token, File.getSize(filename)))
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            fileHelper.setTransport(transport)
            retVal = fileHelper.FDToNetwork(fd)
            if not retVal["OK"]:
                return retVal
            retVal = transport.receiveData()
            return retVal
        finally:
            self._disconnect(trid)

    def receiveFile(self, filename, fileId, token=""):
        """
        Receive a file from the server

        :type filename: string / file descriptor / file object
        :param filename: File to receive from server
        :type fileId: any
        :param fileId: Identification of the file being received
        :type token: string
        :param token: Optional token for the file
        :return: S_OK/S_ERROR
        """
        fileHelper = FileHelper()
        if "NoCheckSum" in token:
            fileHelper.disableCheckSum()
        retVal = fileHelper.getDataSink(filename)
        if not retVal["OK"]:
            return retVal
        dS = retVal["Value"]
        closeAfterUse = retVal["closeAfterUse"]
        retVal = self._sendTransferHeader("ToClient", (fileId, token))
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            fileHelper.setTransport(transport)
            retVal = fileHelper.networkToDataSink(dS)
            if not retVal["OK"]:
                return retVal
            retVal = transport.receiveData()
            if closeAfterUse:
                dS.close()
            return retVal
        finally:
            self._disconnect(trid)

    def __checkFileList(self, fileList):
        bogusEntries = []
        for entry in fileList:
            if not os.path.exists(entry):
                bogusEntries.append(entry)
        return bogusEntries

    def sendBulk(self, fileList, bulkId, token="", compress=True, bulkSize=-1, onthefly=True):
        """
        Send a bulk of files to server

        :type fileList: list of ( string / file descriptor / file object )
        :param fileList: Files to send to server
        :type bulkId: any
        :param bulkId: Identification of the files being sent
        :type token: string
        :param token: Token for the bulk
        :type compress: boolean
        :param compress: Enable compression for the bulk. By default its True
        :type bulkSize: integer
        :param bulkSize: Optional size of the bulk
        :return: S_OK/S_ERROR
        """
        bogusEntries = self.__checkFileList(fileList)
        if bogusEntries:
            return S_ERROR("Some files or directories don't exist :\n\t%s" % "\n\t".join(bogusEntries))
        if compress:
            bulkId = "%s.tar.bz2" % bulkId
        else:
            bulkId = "%s.tar" % bulkId
        retVal = self._sendTransferHeader("BulkFromClient", (bulkId, token, bulkSize))
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            fileHelper = FileHelper(transport)
            retVal = fileHelper.bulkToNetwork(fileList, compress, onthefly)
            if not retVal["OK"]:
                return retVal
            retVal = transport.receiveData()
            return retVal
        finally:
            self._disconnect(trid)

    def receiveBulk(self, destDir, bulkId, token="", compress=True):
        """
        Receive a bulk of files from server

        :type destDir: list of ( string / file descriptor / file object )
        :param destDir: Files to receive from server
        :type bulkId: any
        :param bulkId: Identification of the files being received
        :type token: string
        :param token: Token for the bulk
        :type compress: boolean
        :param compress: Enable compression for the bulk. By default its True
        :return: S_OK/S_ERROR
        """
        if not os.path.isdir(destDir):
            return S_ERROR("%s is not a directory for bulk receival" % destDir)
        if compress:
            bulkId = "%s.tar.bz2" % bulkId
        else:
            bulkId = "%s.tar" % bulkId
        retVal = self._sendTransferHeader("BulkToClient", (bulkId, token))
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            fileHelper = FileHelper(transport)
            retVal = fileHelper.networkToBulk(destDir, compress)
            if not retVal["OK"]:
                return retVal
            retVal = transport.receiveData()
            return retVal
        finally:
            self._disconnect(trid)

    def listBulk(self, bulkId, token="", compress=True):
        """
        List the contents of a bulk

        :type bulkId: any
        :param bulkId: Identification of the bulk to list
        :type token: string
        :param token: Token for the bulk
        :type compress: boolean
        :param compress: Enable compression for the bulk. By default its True
        :return: S_OK/S_ERROR
        """
        if compress:
            bulkId = "%s.tar.bz2" % bulkId
        else:
            bulkId = "%s.tar" % bulkId
        trid = None
        retVal = self._sendTransferHeader("ListBulk", (bulkId, token))
        if not retVal["OK"]:
            return retVal
        trid, transport = retVal["Value"]
        try:
            response = transport.receiveData(1048576)
            return response
        finally:
            self._disconnect(trid)
