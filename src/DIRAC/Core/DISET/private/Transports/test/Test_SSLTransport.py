""" Test the SSLTransport mechanism """
import os
import socket
import threading

try:
    import selectors
except ImportError:
    import selectors2 as selectors

from pytest import fixture

from diraccfg import CFG
from DIRAC.Core.Security.test.x509TestUtilities import CERTDIR, USERCERT, getCertOption
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.DISET.private.Transports import PlainTransport, M2SSLTransport

# TODO: Expired hostcert
# TODO: Expired usercert
# TODO: Expired proxy
# TODO: Invalid/missing CA
# TODO: Connect Timeouts
# TODO: SSL Algorithms & Ciphers
# TODO: Missing hostcert
# TODO: Missing usercert
# TODO: Missing proxy
# TODO: Session test?
# TODO: Reload of CAs?

# Define all the locations

caLocation = os.path.join(CERTDIR, "ca")
hostCertLocation = os.path.join(CERTDIR, "host/hostcert.pem")
hostKeyLocation = os.path.join(CERTDIR, "host/hostkey.pem")
proxyFile = os.path.join(os.path.dirname(__file__), "proxy.pem")


MAGIC_QUESTION = "Who let the dog out"
MAGIC_ANSWER = "Who, Who, who ?"

PORT_NUMBER = 50000

# Transports are now tested in pairs:
# "Server-Client"
# Each pair is defined as a string.
TRANSPORTTESTS = ("Plain-Plain", "M2-M2")


# https://www.ibm.com/developerworks/linux/library/l-openssl/index.html
# http://www.herongyang.com/Cryptography/


class DummyServiceReactor:
    """This class behaves like a ServiceReactor, except that it exists after treating a single request"""

    def __init__(self, transportObject, port):
        """c'tor

        :param transportObject: type of TransportObject we will use
        :param port: port to listen to
        """
        self.__prepared = False
        self.port = port
        self.transportObject = transportObject

        # Server transport object
        self.transport = None
        # Client connection
        self.clientTransport = None
        # Message received from the client
        self.receivedMessage = None

    def handleConnection(self, clientTransport):
        """This is normally done is Service.py in different thread
        It more or less does Service._processInThread
        """

        self.clientTransport = clientTransport
        res = clientTransport.handshake()
        assert res["OK"], res

        self.receivedMessage = clientTransport.receiveData(1024)
        clientTransport.sendData(MAGIC_ANSWER)
        clientTransport.close()

    def prepare(self):
        """Start listening"""
        if not self.__prepared:
            self.__createListeners()
        self.__prepared = True

    def serve(self):
        """Wait for connections and handle the first one."""
        self.prepare()
        self.__acceptIncomingConnection()

    def __createListeners(self):
        """Create the listener transport"""
        self.transport = self.transportObject(("", self.port), bServerMode=True)
        res = self.transport.initAsServer()
        assert res["OK"]

    def __acceptIncomingConnection(self):
        """
        This method just gets the incoming connection, and handle it, once.
        """
        try:
            sel = selectors.DefaultSelector()
            sel.register(self.transport.getSocket(), selectors.EVENT_READ)
            assert sel.select(timeout=2)
            result = self.transport.acceptConnection()
            assert result["OK"], result
            clientTransport = result["Value"]

            self.handleConnection(clientTransport)

        except OSError:
            return

    def closeListeningConnections(self):
        """Close the connection"""
        self.transport.close()


def transportByName(transport):
    """helper function to get a transport class by 'friendly' name."""
    if transport.lower() == "plain":
        return PlainTransport.PlainTransport
    elif transport.lower() == "m2":
        return M2SSLTransport.SSLTransport
    raise RuntimeError("Unknown Transport Name: %s" % transport)


@fixture(scope="function", params=TRANSPORTTESTS)
def create_serverAndClient(request):
    """This function starts a server, and closes it after
    The server will use the parametrized transport type
    """

    # Reinitialize the configuration.
    # We do it here rather than at the start of the module
    # to accommodate for pytest when going through all the DIRAC tests

    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()
    gConfigurationData.setOptionInCFG("/DIRAC/Security/CALocation", caLocation)
    gConfigurationData.setOptionInCFG("/DIRAC/Security/CertFile", hostCertLocation)
    gConfigurationData.setOptionInCFG("/DIRAC/Security/KeyFile", hostKeyLocation)

    testStr = request.param
    serverName, clientName = testStr.split("-")
    serverClass = transportByName(serverName)
    clientClass = transportByName(clientName)

    sr = DummyServiceReactor(serverClass, PORT_NUMBER)
    server_thread = threading.Thread(target=sr.serve)
    sr.prepare()
    server_thread.start()

    # Create the client
    clientOptions = {
        "clientMode": True,
        "proxyLocation": proxyFile,
    }
    clientTransport = clientClass(("localhost", PORT_NUMBER), bServerMode=False, **clientOptions)
    res = clientTransport.initAsClient()
    assert res["OK"], res

    yield sr, clientTransport

    clientTransport.close()
    sr.closeListeningConnections()
    server_thread.join()

    # Clean the config
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


def ping_server(clientTransport):
    """This sends a message to the server and expects an answer
    This basically does the same as BaseClient.py

    :param clientTransport: the Transport object to be used as client
    """

    clientTransport.setSocketTimeout(5)
    result = clientTransport.sendData(MAGIC_QUESTION)
    assert result["OK"]
    serverReturn = clientTransport.receiveData()
    return serverReturn


def test_simpleMessage(create_serverAndClient):
    """Send a message, wait for an answer"""
    serv, client = create_serverAndClient
    serverAnswer = ping_server(client)
    assert serv.receivedMessage == MAGIC_QUESTION
    assert serverAnswer == MAGIC_ANSWER


def test_getRemoteInfo(create_serverAndClient):
    """Check the information from remote peer"""
    serv, client = create_serverAndClient
    ping_server(client)

    addr_info = client.getRemoteAddress()
    assert addr_info[0] in ("127.0.0.1", "::ffff:127.0.0.1", "::1")
    assert addr_info[1] == PORT_NUMBER
    # The peer credentials are not filled on the client side
    assert client.peerCredentials == {}

    # We do not know about the port, so check only the address, taking into account bloody IPv6
    assert serv.clientTransport.getRemoteAddress()[0] in ("127.0.0.1", "::ffff:127.0.0.1", "::1")
    peerCreds = serv.clientTransport.peerCredentials

    # There are no credentials for PlainTransport
    if client.__class__.__name__ == "PlainTransport":
        assert peerCreds == {}
    else:
        assert peerCreds["DN"] == getCertOption(USERCERT, "subjectDN")
        assert peerCreds["x509Chain"].getNumCertsInChain()["Value"] == 2
        assert peerCreds["isProxy"] is True
        assert peerCreds["isLimitedProxy"] is False
