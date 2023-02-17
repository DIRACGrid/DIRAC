import hashlib
import io
import os
import tarfile
import tempfile
import threading

from io import StringIO, BytesIO

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger

file_types = (io.IOBase,)


class FileHelper:
    __validDirections = ("toClient", "fromClient", "receive", "send")
    __directionsMapping = {"toClient": "send", "fromClient": "receive"}

    def __init__(self, oTransport=None, checkSum=True):
        self.oTransport = oTransport
        self.__checkMD5 = checkSum
        self.__oMD5 = hashlib.md5()
        self.bFinishedTransmission = False
        self.bReceivedEOF = False
        self.direction = False
        self.packetSize = 1048576
        self.__fileBytes = 0
        self.__log = gLogger.getSubLogger(self.__class__.__name__)

    def disableCheckSum(self):
        self.__checkMD5 = False

    def enableCheckSum(self):
        self.__checkMD5 = True

    def setTransport(self, oTransport):
        self.oTransport = oTransport

    def setDirection(self, direction):
        if direction in FileHelper.__validDirections:
            if direction in FileHelper.__directionsMapping:
                self.direction = FileHelper.__directionsMapping[direction]
            else:
                self.direction = direction

    def getHash(self):
        return self.__oMD5.hexdigest()

    def getTransferedBytes(self):
        return self.__fileBytes

    def sendData(self, sBuffer):
        if isinstance(sBuffer, str):
            sBuffer = sBuffer.encode(errors="surrogateescape")
        if self.__checkMD5:
            self.__oMD5.update(sBuffer)
        retVal = self.oTransport.sendData(S_OK([True, sBuffer]))
        if not retVal["OK"]:
            return retVal
        retVal = self.oTransport.receiveData()
        return retVal

    def sendEOF(self):
        retVal = self.oTransport.sendData(S_OK([False, self.__oMD5.hexdigest()]))
        if not retVal["OK"]:
            return retVal
        self.__finishedTransmission()
        return S_OK()

    def sendError(self, errorMsg):
        retVal = self.oTransport.sendData(S_ERROR(errorMsg))
        if not retVal["OK"]:
            return retVal
        self.__finishedTransmission()
        return S_OK()

    def receiveData(self, maxBufferSize=0):
        retVal = self.oTransport.receiveData(maxBufferSize=maxBufferSize)
        if "AbortTransfer" in retVal and retVal["AbortTransfer"]:
            self.oTransport.sendData(S_OK())
            self.__finishedTransmission()
            self.bReceivedEOF = True
            return S_OK("")
        if not retVal["OK"]:
            return retVal
        stBuffer = retVal["Value"]
        if stBuffer[0]:
            if isinstance(stBuffer[1], str):
                stBuffer[1] = stBuffer[1].encode(errors="surrogateescape")
            if self.__checkMD5:
                self.__oMD5.update(stBuffer[1])
            self.oTransport.sendData(S_OK())
        else:
            self.bReceivedEOF = True
            if self.__checkMD5 and not self.__oMD5.hexdigest() == stBuffer[1]:
                self.bErrorInMD5 = True
            self.__finishedTransmission()
            return S_OK("")
        return S_OK(stBuffer[1])

    def receivedEOF(self):
        return self.bReceivedEOF

    def markAsTransferred(self):
        if not self.bFinishedTransmission:
            if self.direction == "receive":
                self.oTransport.receiveData()
                abortTrans = S_OK()
                abortTrans["AbortTransfer"] = True
                self.oTransport.sendData(abortTrans)
            else:
                abortTrans = S_OK([False, ""])
                abortTrans["AbortTransfer"] = True
                retVal = self.oTransport.sendData(abortTrans)
                if not retVal["OK"]:
                    return retVal
                self.oTransport.receiveData()
        self.__finishedTransmission()

    def __finishedTransmission(self):
        self.bFinishedTransmission = True

    def finishedTransmission(self):
        return self.bFinishedTransmission

    def errorInTransmission(self):
        return self.bErrorInMD5

    def networkToString(self, maxFileSize=0):
        """Receive the input from a DISET client and return it as a string"""

        bytesIO = BytesIO()
        result = self.networkToDataSink(bytesIO, maxFileSize=maxFileSize)
        if not result["OK"]:
            return result
        return S_OK(bytesIO.getvalue())

    def networkToFD(self, iFD, maxFileSize=0):
        dataSink = os.fdopen(iFD, "wb")
        try:
            return self.networkToDataSink(dataSink, maxFileSize=maxFileSize)
        finally:
            try:
                dataSink.close()
            except Exception:
                pass

    def networkToDataSink(self, dataSink, maxFileSize=0):
        if "write" not in dir(dataSink):
            return S_ERROR(f"{str(dataSink)} data sink object does not have a write method")
        self.__oMD5 = hashlib.md5()
        self.bReceivedEOF = False
        self.bErrorInMD5 = False
        receivedBytes = 0
        # try:
        result = self.receiveData(maxBufferSize=maxFileSize)
        if not result["OK"]:
            return result
        strBuffer = result["Value"]
        if isinstance(strBuffer, str):
            strBuffer = strBuffer.encode(errors="surrogateescape")
        receivedBytes += len(strBuffer)
        while not self.receivedEOF():
            if maxFileSize > 0 and receivedBytes > maxFileSize:
                self.sendError("Exceeded maximum file size")
                return S_ERROR(f"Received file exceeded maximum size of {maxFileSize} bytes")
            dataSink.write(strBuffer)
            result = self.receiveData(maxBufferSize=(maxFileSize - len(strBuffer)))
            if not result["OK"]:
                return result
            strBuffer = result["Value"]
            if isinstance(strBuffer, str):
                strBuffer = strBuffer.encode(errors="surrogateescape")
            receivedBytes += len(strBuffer)
        if strBuffer:
            dataSink.write(strBuffer)
        # except Exception as e:
        #   return S_ERROR("Error while receiving file, %s" % str(e))
        if self.errorInTransmission():
            return S_ERROR("Error in the file CRC")
        self.__fileBytes = receivedBytes
        return S_OK()

    def stringToNetwork(self, stringVal):
        """Send a given string to the DISET client over the network"""

        stringIO = StringIO(stringVal)

        iPacketSize = self.packetSize
        ioffset = 0
        strlen = len(stringVal)
        try:
            while (ioffset) < strlen:
                if (ioffset + iPacketSize) < strlen:
                    result = self.sendData(stringVal[ioffset : ioffset + iPacketSize])
                else:
                    result = self.sendData(stringVal[ioffset:strlen])
                if not result["OK"]:
                    return result
                if "AbortTransfer" in result and result["AbortTransfer"]:
                    self.__log.verbose("Transfer aborted")
                    return S_OK()
                ioffset += iPacketSize
            self.sendEOF()
        except Exception as e:
            return S_ERROR(f"Error while sending string: {str(e)}")
        try:
            stringIO.close()
        except Exception:
            pass
        return S_OK()

    def FDToNetwork(self, iFD):
        self.__oMD5 = hashlib.md5()
        iPacketSize = self.packetSize
        self.__fileBytes = 0
        sentBytes = 0
        try:
            sBuffer = os.read(iFD, iPacketSize)
            while len(sBuffer) > 0:
                dRetVal = self.sendData(sBuffer)
                if not dRetVal["OK"]:
                    return dRetVal
                if "AbortTransfer" in dRetVal and dRetVal["AbortTransfer"]:
                    self.__log.verbose("Transfer aborted")
                    return S_OK()
                sentBytes += len(sBuffer)
                sBuffer = os.read(iFD, iPacketSize)
            self.sendEOF()
        except Exception as e:
            gLogger.exception("Error while sending file")
            return S_ERROR(f"Error while sending file: {str(e)}")
        self.__fileBytes = sentBytes
        return S_OK()

    def BufferToNetwork(self, stringToSend):
        sIO = StringIO(stringToSend)
        try:
            return self.DataSourceToNetwork(sIO)
        finally:
            sIO.close()

    def DataSourceToNetwork(self, dataSource):
        if "read" not in dir(dataSource):
            return S_ERROR(f"{str(dataSource)} data source object does not have a read method")
        self.__oMD5 = hashlib.md5()
        iPacketSize = self.packetSize
        self.__fileBytes = 0
        sentBytes = 0
        try:
            sBuffer = dataSource.read(iPacketSize)
            while len(sBuffer) > 0:
                dRetVal = self.sendData(sBuffer)
                if not dRetVal["OK"]:
                    return dRetVal
                if "AbortTransfer" in dRetVal and dRetVal["AbortTransfer"]:
                    self.__log.verbose("Transfer aborted")
                    return S_OK()
                sentBytes += len(sBuffer)
                sBuffer = dataSource.read(iPacketSize)
            self.sendEOF()
        except Exception as e:
            gLogger.exception("Error while sending file")
            return S_ERROR(f"Error while sending file: {str(e)}")
        self.__fileBytes = sentBytes
        return S_OK()

    def getFileDescriptor(self, uFile, sFileMode):
        closeAfter = True
        if isinstance(uFile, str):
            try:
                self.oFile = open(uFile, sFileMode)
            except OSError:
                return S_ERROR(f"{uFile} can't be opened")
            iFD = self.oFile.fileno()
        elif isinstance(uFile, file_types):
            iFD = uFile.fileno()
        elif isinstance(uFile, int):
            iFD = uFile
            closeAfter = False
        else:
            return S_ERROR(f"{uFile} is not a valid file.")
        result = S_OK(iFD)
        result["closeAfterUse"] = closeAfter
        return result

    def getDataSink(self, uFile):
        closeAfter = True
        if isinstance(uFile, str):
            try:
                oFile = open(uFile, "wb")
            except OSError:
                return S_ERROR(f"{uFile} can't be opened")
        elif isinstance(uFile, file_types):
            oFile = uFile
            closeAfter = False
        elif isinstance(uFile, int):
            oFile = os.fdopen(uFile, "wb")
            closeAfter = True
        elif "write" in dir(uFile):
            oFile = uFile
            closeAfter = False
        else:
            return S_ERROR(f"{uFile} is not a valid file.")
        result = S_OK(oFile)
        result["closeAfterUse"] = closeAfter
        return result

    def __createTar(self, fileList, wPipe, compress, autoClose=True):
        if "write" in dir(wPipe):
            filePipe = wPipe
        else:
            filePipe = os.fdopen(wPipe, "wb")
        tarMode = "w|"
        if compress:
            tarMode = "w|bz2"

        with tarfile.open(name="Pipe", mode=tarMode, fileobj=filePipe) as tar:
            for entry in fileList:
                tar.add(os.path.realpath(entry), os.path.basename(entry), recursive=True)
        if autoClose:
            try:
                filePipe.close()
            except Exception:
                pass

    def bulkToNetwork(self, fileList, compress=True, onthefly=True):
        if not onthefly:
            try:
                filePipe, filePath = tempfile.mkstemp()
            except Exception as e:
                return S_ERROR(f"Can't create temporary file to pregenerate the bulk: {str(e)}")
            self.__createTar(fileList, filePipe, compress)
            try:
                fo = open(filePath, "rb")
            except Exception as e:
                return S_ERROR(f"Can't read pregenerated bulk: {str(e)}")
            result = self.DataSourceToNetwork(fo)
            try:
                fo.close()
                os.unlink(filePath)
            except Exception:
                pass
            return result
        else:
            rPipe, wPipe = os.pipe()
            thrd = threading.Thread(target=self.__createTar, args=(fileList, wPipe, compress))
            thrd.start()
            response = self.FDToNetwork(rPipe)
            try:
                os.close(rPipe)
            except Exception:
                pass
            return response

    def __extractTar(self, destDir, rPipe, compress):
        filePipe = os.fdopen(rPipe, "rb")
        tarMode = "r|*"
        if compress:
            tarMode = "r|bz2"
        with tarfile.open(mode=tarMode, fileobj=filePipe) as tar:
            for tarInfo in tar:
                tar.extract(tarInfo, destDir)
        try:
            filePipe.close()
        except Exception:
            pass

    def __receiveToPipe(self, wPipe, retList, maxFileSize):
        retList.append(self.networkToFD(wPipe, maxFileSize=maxFileSize))
        try:
            os.close(wPipe)
        except Exception:
            pass

    def networkToBulk(self, destDir, compress=True, maxFileSize=0):
        retList = []
        rPipe, wPipe = os.pipe()
        thrd = threading.Thread(target=self.__receiveToPipe, args=(wPipe, retList, maxFileSize))
        thrd.start()
        try:
            self.__extractTar(destDir, rPipe, compress)
        except Exception as e:
            return S_ERROR(f"Error while extracting bulk: {e}")
        thrd.join()
        return retList[0]

    def bulkListToNetwork(self, iFD, compress=True):
        filePipe = os.fdopen(iFD, "rb")
        try:
            tarMode = "r|"
            if compress:
                tarMode = "r|bz2"
            entries = []
            with tarfile.open(mode=tarMode, fileobj=filePipe) as tar:
                for tarInfo in tar:
                    entries.append(tarInfo.name)
            filePipe.close()
            return S_OK(entries)
        except tarfile.ReadError as v:
            return S_ERROR(f"Error reading bulk: {str(v)}")
        except tarfile.CompressionError as v:
            return S_ERROR(f"Error in bulk compression setting: {str(v)}")
        except Exception as v:
            return S_ERROR(f"Error in listing bulk: {str(v)}")
