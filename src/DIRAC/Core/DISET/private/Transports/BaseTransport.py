""" Hosts BaseTransport class, which is a base for PlainTransport and SSLTransport

BaseTransport is used by the client and the service, it describes an opened connection.
Here a diagram of basic Client/Service exchange

Client -> ServiceReactor : Connect

Client<-->Service        : Handshake (only in SSLTransport)

Client -> Service        : Propose action

Client <- Service        : S_OK

Client -> RequestHandler : Arguments

Client <- RequestHandler : Response

Client <- Service        : Close
"""

import time
from io import BytesIO
from hashlib import md5

import selectors

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import MixedEncode

# https://datatracker.ietf.org/doc/html/rfc8446#section-5.1
TLS_PAYLOAD_SIZE = 16384


class BaseTransport:
    """Invokes MixedEncode for marshaling/unmarshaling of data calls in transit"""

    bAllowReuseAddress = True

    # This option corresponds to the `backlog` param of the `listen` syscall.
    # you may want to read the man page before tuning it...
    iListenQueueSize = 128
    iReadTimeout = 600
    keepAliveMagic = b"dka"

    def __init__(self, stServerAddress, bServerMode=False, **kwargs):
        self.bServerMode = bServerMode
        self.extraArgsDict = kwargs
        self.byteStream = bytearray()
        self.packetSize = 1048576  # 1MiB
        self.stServerAddress = stServerAddress
        self.peerCredentials = {}
        self.remoteAddress = False
        self.appData = ""
        self.startedKeepAlives = set()
        self.keepAliveId = md5((str(stServerAddress) + str(bServerMode)).encode()).hexdigest()
        self.receivedMessages = []
        self.sentKeepAlives = 0
        self.waitingForKeepAlivePong = False
        self.__keepAliveLapse = 0
        self.oSocket = None
        if "keepAliveLapse" in kwargs:
            try:
                self.__keepAliveLapse = max(150, int(kwargs["keepAliveLapse"]))
            except Exception:
                pass
        self.iListenQueueSize = max(self.iListenQueueSize, int(kwargs.get("SocketBacklog", 0)))
        self.__lastActionTimestamp = time.time()
        self.__lastServerRenewTimestamp = self.__lastActionTimestamp

    def __updateLastActionTimestamp(self):
        self.__lastActionTimestamp = time.time()

    def getLastActionTimestamp(self):
        return self.__lastActionTimestamp

    def getKeepAliveLapse(self):
        return self.__keepAliveLapse

    def handshake(self):
        """This method is overwritten by SSLTransport if we use a secured transport."""
        return S_OK()

    def close(self):
        self.oSocket.close()

    def setAppData(self, appData):
        self.appData = appData

    def getAppData(self):
        return self.appData

    def renewServerContext(self):
        self.__lastServerRenewTimestamp = time.time()
        return S_OK()

    def latestServerRenewTime(self):
        return self.__lastServerRenewTimestamp

    def getConnectingCredentials(self):
        """

        :return: dictionary with credentials

          Return empty dictionary for plainTransport.

          In SSLTransport it contains (after the handshake):

           - 'DN' : All identity name, e.g. ```/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser```
           - 'CN' : Only the user name e.g. ciuser
           - 'x509Chain' : List of all certificates in the chain
           - 'isProxy' : True if the client use proxy certificate
           - 'isLimitedProxy' : True if the client use limited proxy certificate
           - 'group' (optional): Dirac group attached to the client
           - 'extraCredentials' (optional): Extra credentials if exists

          Before the handshake, dictionary is empty
        """
        return self.peerCredentials

    def setExtraCredentials(self, extraCredentials):
        """Add extra credentials to peerCredentials

        :param extraCredentials: group or tuple with DN and group
        :type extraCredentials: str or tuple
        """
        self.peerCredentials["extraCredentials"] = extraCredentials

    def serverMode(self):
        return self.bServerMode

    def getRemoteAddress(self):
        return self.remoteAddress

    def getLocalAddress(self):
        return self.oSocket.getsockname()

    def getSocket(self):
        return self.oSocket

    def _readReady(self):
        if not self.iReadTimeout:
            return True
        sel = selectors.DefaultSelector()
        sel.register(self.oSocket, selectors.EVENT_READ)
        if sel.select(timeout=self.iReadTimeout):
            return True
        return False

    def _read(self, bufSize=4096, skipReadyCheck=False):
        try:
            if skipReadyCheck or self._readReady():
                data = self.oSocket.recv(bufSize)
                if not data:
                    return S_ERROR("Connection closed by peer")
                else:
                    return S_OK(data)
            else:
                return S_ERROR("Connection seems stalled. Closing...")
        except Exception as e:
            return S_ERROR(f"Exception while reading from peer: {str(e)}")

    def _write(self, buf):
        return S_OK(self.oSocket.send(buf))

    def sendData(self, uData, prefix=b""):
        self.__updateLastActionTimestamp()
        sCodedData = MixedEncode.encode(uData)
        if isinstance(sCodedData, str):
            sCodedData = sCodedData.encode()
        dataToSend = b"".join([prefix, str(len(sCodedData)).encode(), b":", sCodedData])
        for index in range(0, len(dataToSend), self.packetSize):
            bytesToSend = min(self.packetSize, len(dataToSend) - index)
            packSentBytes = 0
            while packSentBytes < bytesToSend:
                try:
                    result = self._write(dataToSend[index + packSentBytes : index + bytesToSend])
                    if not result["OK"]:
                        return result
                    sentBytes = result["Value"]
                except Exception as e:
                    return S_ERROR(f"Exception while sending data: {e}")
                if sentBytes < 0:
                    return S_ERROR("Unknown unrecoverable error from socket while sending data")
                if sentBytes == 0:
                    return S_ERROR("Connection closed by peer")
                packSentBytes += sentBytes
        del sCodedData
        sCodedData = None
        return S_OK()

    def receiveData(self, maxBufferSize=0, blockAfterKeepAlive=True, idleReceive=False):
        self.__updateLastActionTimestamp()
        if self.receivedMessages:
            return self.receivedMessages.pop(0)
        # Buffer size can't be less than 0
        maxBufferSize = max(maxBufferSize, 0)
        try:
            # Look either for message length of keep alive magic string
            iSeparatorPosition = self.byteStream.find(b":")
            keepAliveMagicLen = len(BaseTransport.keepAliveMagic)
            isKeepAlive = self.byteStream.find(BaseTransport.keepAliveMagic, 0, keepAliveMagicLen) == 0
            # While not found the message length or the ka, keep receiving
            while iSeparatorPosition == -1 and not isKeepAlive:
                retVal = self._read(TLS_PAYLOAD_SIZE)
                # If error return
                if not retVal["OK"]:
                    return retVal
                # If closed return error
                if not retVal["Value"]:
                    return S_ERROR("Peer closed connection")
                # New data!
                self.byteStream.extend(retVal["Value"])

                # Look again for either message length or keep alive magic string
                iSeparatorPosition = self.byteStream.find(b":")
                isKeepAlive = self.byteStream.find(BaseTransport.keepAliveMagic, 0, keepAliveMagicLen) == 0
                # Over the limit?
                if maxBufferSize and len(self.byteStream) > maxBufferSize and iSeparatorPosition == -1:
                    return S_ERROR(f"Read limit exceeded ({maxBufferSize} chars)")
            # Keep alive magic!
            if isKeepAlive:
                gLogger.debug("Received keep alive header")
                # Remove the keep-alive magic from the buffer and process the keep-alive
                self.byteStream = self.byteStream[keepAliveMagicLen:]
                return self.__processKeepAlive(maxBufferSize, blockAfterKeepAlive)
            # From here it must be a real message!
            # Process the size and remove the msg length from the bytestream
            pkgSize = int(self.byteStream[:iSeparatorPosition])
            pkgData = self.byteStream[iSeparatorPosition + 1 :]
            readSize = len(pkgData)

            if readSize >= pkgSize:
                # If we already have all the data we need
                data = pkgData[:pkgSize]
                self.byteStream = self.byteStream[pkgSize + iSeparatorPosition + 1 :]
            else:
                # If we still need to read stuff
                pkgMem = BytesIO()
                pkgMem.write(pkgData)
                # Receive while there's still data to be received
                while readSize < pkgSize:
                    retVal = self._read(min(TLS_PAYLOAD_SIZE, pkgSize - readSize), skipReadyCheck=True)
                    if not retVal["OK"]:
                        return retVal
                    if not retVal["Value"]:
                        return S_ERROR("Peer closed connection")
                    rcvData = retVal["Value"]
                    readSize += len(rcvData)
                    pkgMem.write(rcvData)
                    if maxBufferSize and readSize > maxBufferSize:
                        return S_ERROR(f"Read limit exceeded ({maxBufferSize} chars)")
                # Data is here! take it out from the bytestream, dencode and return
                if readSize == pkgSize:
                    data = pkgMem.getvalue()
                    self.byteStream = bytearray()  # Reset the byteStream
                else:
                    pkgMem.seek(0, 0)
                    data = pkgMem.read(pkgSize)
                    self.byteStream = bytearray(pkgMem.read())  # store the rest in bytearray

            try:
                data = MixedEncode.decode(data)[0]
            except Exception as e:
                return S_ERROR(f"Could not decode received data: {str(e)}")
            if idleReceive:
                self.receivedMessages.append(data)
                return S_OK()
            return data
        except Exception as e:
            gLogger.exception("Network error while receiving data")
            return S_ERROR(f"Network error while receiving data: {str(e)}")

    def __processKeepAlive(self, maxBufferSize, blockAfterKeepAlive=True):
        gLogger.debug("Received Keep Alive")
        # Next message down the stream will be the ka data
        result = self.receiveData(maxBufferSize, blockAfterKeepAlive=False)
        if not result["OK"]:
            gLogger.debug(f"Error while receiving keep alive: {result['Message']}")
            return result
        # Is it a valid ka?
        kaData = result["Value"]
        for reqField in ("id", "kaping"):
            if reqField not in kaData:
                errMsg = f"Invalid keep alive, missing {reqField}"
                gLogger.debug(errMsg)
                return S_ERROR(errMsg)
        gLogger.debug(f"Received keep alive id {kaData}")
        # Need to check if it's one of the keep alives we sent or one started from the other side
        if kaData["kaping"]:
            # This is a keep alive PING. Let's send the PONG
            self.sendKeepAlive(responseId=kaData["id"])
        else:
            # If it's a pong then we flag that we don't need to wait for a pong
            self.waitingForKeepAlivePong = False
        # No blockAfterKeepAlive means return without further read
        if not blockAfterKeepAlive:
            result = S_OK()
            result["keepAlive"] = True
            return result
        # Let's listen for the next message downstream
        return self.receiveData(maxBufferSize, blockAfterKeepAlive)

    def sendKeepAlive(self, responseId=None, now=False):
        # If not responseId or not keepAliveLapse or not enough time has passed don't send keep alive
        if not responseId:
            if not self.__keepAliveLapse:
                return S_OK()
            if not now:
                now = time.time()
            if now - self.__lastActionTimestamp < self.__keepAliveLapse:
                return S_OK()

        self.__updateLastActionTimestamp()
        if responseId:
            self.waitingForKeepAlivePong = False
            kaData = S_OK({"id": responseId, "kaping": False})
        else:
            if self.waitingForKeepAlivePong:
                return S_OK()
            idK = self.keepAliveId + str(self.sentKeepAlives)
            self.sentKeepAlives += 1
            kaData = S_OK({"id": idK, "kaping": True})
            self.waitingForKeepAlivePong = True
        return self.sendData(kaData, prefix=BaseTransport.keepAliveMagic)

    def getFormattedCredentials(self):
        peerCreds = self.getConnectingCredentials()
        address = self.getRemoteAddress()
        if "username" in peerCreds:
            peerId = f"[{peerCreds['group']}:{peerCreds['username']}]"
        else:
            peerId = ""
        if address[0].find(":") > -1:
            return f"([{address[0]}]:{address[1]}){peerId}"
        return f"({address[0]}:{address[1]}){peerId}"

    def setSocketTimeout(self, timeout):
        """
        This method has to be overwritten, if we want to increase the socket timeout.
        """
        pass
