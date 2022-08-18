"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""
import json
import random
import os
import socket
import ssl
import time
import stomp

from DIRAC.Resources.MessageQueue.MQConnector import MQConnector
from DIRAC.Core.Security import Locations
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQCONN

LOG = gLogger.getSubLogger(__name__)


class StompMQConnector(MQConnector):
    """
    Class for management of message queue connections
    Allows to both send and receive messages from a queue

    When several IPs are behind an alias, we shuffle the ips, and connect to one.
    The others are used as failover by stomp's internals
    """

    # Setting for the reconnection handling by stomp interface.
    # See e.g. the description of Transport class in
    # https://github.com/jasonrbriggs/stomp.py/blob/master/stomp/transport.py

    RECONNECT_SLEEP_INITIAL = 1  # [s]  Initial delay before reattempting to establish a connection.
    RECONNECT_SLEEP_INCREASE = 0.5  # Factor by which sleep delay is increased 0.5 means increase by 50%.
    RECONNECT_SLEEP_MAX = 120  # [s] The maximum delay that can be reached independent of increasing procedure.
    RECONNECT_SLEEP_JITTER = 0.1  # Random factor to add. 0.1 means a random number from 0 to 10% of the current time.
    RECONNECT_ATTEMPTS_MAX = 1e4  # Maximum attempts to reconnect.

    PORT = 61613

    def __init__(self, parameters=None):
        """Standard constructor"""
        super().__init__(parameters=parameters)
        self.connection = None

        if "DIRAC_DEBUG_STOMP" in os.environ:
            gLogger.enableLogsFromExternalLibs()

    def setupConnection(self, parameters=None):
        """
         Establishes a new connection to a Stomp server, e.g. RabbitMQ

        Args:
          parameters(dict): dictionary with additional MQ parameters if any.
        Returns:
          S_OK/S_ERROR
        """
        log = LOG.getSubLogger("setupConnection")

        if parameters is not None:
            self.parameters.update(parameters)

        # Check that the minimum set of parameters is present
        if not all(p in parameters for p in ("Host", "VHost")):
            return S_ERROR("Input parameters are missing!")

        reconnectSleepInitial = self.parameters.get("ReconnectSleepInitial", StompMQConnector.RECONNECT_SLEEP_INITIAL)
        reconnectSleepIncrease = self.parameters.get(
            "ReconnectSleepIncrease", StompMQConnector.RECONNECT_SLEEP_INCREASE
        )
        reconnectSleepMax = self.parameters.get("ReconnectSleepMax", StompMQConnector.RECONNECT_SLEEP_MAX)
        reconnectSleepJitter = self.parameters.get("ReconnectSleepJitter", StompMQConnector.RECONNECT_SLEEP_JITTER)
        reconnectAttemptsMax = self.parameters.get("ReconnectAttemptsMax", StompMQConnector.RECONNECT_ATTEMPTS_MAX)

        host = self.parameters.get("Host")
        port = self.parameters.get("Port", StompMQConnector.PORT)
        vhost = self.parameters.get("VHost")

        sslVersion = self.parameters.get("SSLVersion")
        hostcert = self.parameters.get("HostCertificate")
        hostkey = self.parameters.get("HostKey")

        connectionArgs = {
            "vhost": vhost,
            "keepalive": True,
            "reconnect_sleep_initial": reconnectSleepInitial,
            "reconnect_sleep_increase": reconnectSleepIncrease,
            "reconnect_sleep_max": reconnectSleepMax,
            "reconnect_sleep_jitter": reconnectSleepJitter,
            "reconnect_attempts_max": reconnectAttemptsMax,
        }
        sslArgs = None

        # We use ssl credentials and not user-password.
        if sslVersion is not None:
            if sslVersion == "TLSv1":
                sslVersion = ssl.PROTOCOL_TLSv1
                # get local key and certificate if not available via configuration
                if not (hostcert or hostkey):
                    paths = Locations.getHostCertificateAndKeyLocation()
                    if not paths:
                        return S_ERROR("Could not find a certificate!")
                    hostcert = paths[0]
                    hostkey = paths[1]
                sslArgs = {
                    "use_ssl": True,
                    "ssl_version": sslVersion,
                    "ssl_key_file": hostkey,
                    "ssl_cert_file": hostcert,
                }

            else:
                return S_ERROR(EMQCONN, "Invalid SSL version provided: %s" % sslVersion)

        try:

            # Get IP addresses of brokers
            # Start with the IPv6, and randomize it
            ipv6_addrInfo = socket.getaddrinfo(host, port, socket.AF_INET6, socket.SOCK_STREAM)
            random.shuffle(ipv6_addrInfo)
            # Same with IPv4
            ipv4_addrInfo = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
            random.shuffle(ipv4_addrInfo)

            # Create the host_port tuples, keeping the ipv6 in front
            host_and_ports = []
            for _family, _socktype, _proto, _canonname, sockaddr in ipv6_addrInfo + ipv4_addrInfo:
                host_and_ports.append((sockaddr[0], sockaddr[1]))

            connectionArgs.update({"host_and_ports": host_and_ports})
            log.debug("Connection args: %s" % str(connectionArgs))
            self.connection = stomp.Connection(**connectionArgs)
            if sslArgs:
                self.connection.set_ssl(**sslArgs)

        except Exception as e:
            log.debug("Failed setting up connection", repr(e))
            return S_ERROR(EMQCONN, "Failed to setup connection: %s" % e)

        return S_OK("Setup successful")

    def reconnect(self):
        """
        Callback method when a disconnection happens

        :param serverIP: IP of the server disconnected
        """
        log = LOG.getSubLogger("reconnect")
        log.info("Trigger reconnection for broker")
        res = self.connect(self.parameters)
        return res

    def put(self, message, parameters=None):
        """
        Sends a message to the queue
        message contains the body of the message

        Args:
          message(str): string or any json encodable structure.
          parameters(dict): parameters with 'destination' key defined.
        """
        log = LOG.getSubLogger("put")
        destination = parameters.get("destination", "")

        try:
            try:
                self.connection.send(body=json.dumps(message), destination=destination)
            except stomp.exception.StompException:
                self.connect()
                self.connection.send(body=json.dumps(message), destination=destination)
        except Exception as e:
            log.debug("Failed to send message", repr(e))
            return S_ERROR(EMQUKN, "Failed to send message: %s" % repr(e))

        return S_OK("Message sent successfully")

    def connect(self, parameters=None):
        """Call the ~stomp.Connection.connect method for each endpoint

        :param parameters: connection parameter
        """

        log = LOG.getSubLogger("connect")

        # Since I use a dirty trick to know to what IP I am connected,
        # I'd rather not rely too much on it
        remoteIP = "unknown"
        user = self.parameters.get("User")
        password = self.parameters.get("Password")

        for _ in range(10):
            try:
                self.connection.connect(username=user, passcode=password, wait=True)

                if self.connection.is_connected():
                    # Go to the socket of the Stomp to find the remote host
                    try:
                        remoteIP = self.connection.transport.socket.getpeername()[0]
                    except Exception:
                        pass
                    log.info("MQ Connected to %s" % remoteIP)
                    return S_OK("Connected to %s" % remoteIP)
                else:
                    log.warn("Not connected")
            except Exception as e:
                log.error("Failed to connect: %s" % repr(e))

            # Wait a bit before retrying
            time.sleep(5)

        return S_ERROR(EMQCONN, "Failed to connect")

    def disconnect(self, parameters=None):
        """
        Disconnects from the message queue server
        """
        log = LOG.getSubLogger("disconnect")

        try:
            # Indicate to the Listener that we want a disconnection
            listener = self.connection.get_listener("StompListener")
            if listener:
                listener.wantsDisconnect = True

            self.connection.disconnect()
            log.info("Disconnected from broker")
        except Exception as e:
            log.error("Failed to disconnect from broker", repr(e))
            return S_ERROR(EMQUKN, "Failed to disconnect from broker %s" % repr(e))

        return S_OK("Successfully disconnected from broker")

    def subscribe(self, parameters=None):
        log = LOG.getSubLogger("subscribe")

        mId = parameters.get("messengerId", "")
        callback = parameters.get("callback", None)
        dest = parameters.get("destination", "")
        headers = {}
        if self.parameters.get("Persistent", "").lower() in ["true", "yes", "1"]:
            headers = {"persistent": "true"}
        ack = "auto"
        acknowledgement = False
        if self.parameters.get("Acknowledgement", "").lower() in ["true", "yes", "1"]:
            acknowledgement = True
            ack = "client-individual"
        if not callback:
            # Chris 26.02.20
            # If it is an error, why not returning ?!
            log.error("No callback specified!")

        try:
            listener = StompListener(callback, acknowledgement, self.connection, mId, self.connect)
            self.connection.set_listener("StompListener", listener)
            self.connection.subscribe(destination=dest, id=mId, ack=ack, headers=headers)
        except Exception as e:
            log.error("Failed to subscribe: %s" % e)
            return S_ERROR(EMQUKN, "Failed to subscribe to broker: %s" % repr(e))

        return S_OK("Subscription successful")

    def unsubscribe(self, parameters):
        log = LOG.getSubLogger("unsubscribe")

        dest = parameters.get("destination", "")
        mId = parameters.get("messengerId", "")

        try:
            self.connection.unsubscribe(destination=dest, id=mId)
        except Exception as e:
            log.error("Failed to unsubscribe", repr(e))
            return S_ERROR(EMQUKN, "Failed to unsubscribe: %s" % repr(e))

        return S_OK("Successfully unsubscribed from all destinations")


class StompListener(stomp.ConnectionListener):
    """
    Internal listener class responsible for handling new messages and errors.
    """

    def __init__(self, callback, ack, connection, messengerId, connectCallback):
        """
        Initializes the internal listener object

        Args:
          callback: a defaultCallback compatible function.
          ack(bool): if set to true an acknowledgement will be send back to the sender.
          messengerId(str): messenger identifier sent with acknowledgement messages.
          connectCallback: the connect method to call in case of disconnection
        """

        self.log = LOG.getSubLogger("StompListener")
        if not callback:
            self.log.error("Error initializing StompMQConnector!callback is None")
        self.callback = callback
        self.ack = ack
        self.mId = messengerId
        self.connection = connection
        self.connectCallback = connectCallback

        # This boolean is to know whether we effectively
        # want to disconnect or if it is because of a failure
        self.wantsDisconnect = False

    def on_message(self, headers, body):
        """
        Function called upon receiving a message

        :param dict headers: message headers
        :param json body: message body
        """
        result = self.callback(headers, json.loads(body))
        if self.ack:
            if result["OK"]:
                self.connection.ack(headers["message-id"], self.mId)
            else:
                self.connection.nack(headers["message-id"], self.mId)

    def on_error(self, headers, message):
        """Function called when an error happens

        Args:
          headers(dict): message headers.
          body(json): message body.
        """
        self.log.error(message)

    def on_disconnected(self):
        """Callback function called after disconnecting from broker."""
        if not self.wantsDisconnect:
            self.log.warn("Disconnected from broker")
            try:
                res = self.connectCallback()
                if res["OK"]:
                    self.log.info("Reconnection successful to broker")
                else:
                    self.log.error("Error reconnectiong broker", "%s" % res)

            except Exception as e:
                self.log.error("Unexpected error while calling reconnect callback: %s" % e)
