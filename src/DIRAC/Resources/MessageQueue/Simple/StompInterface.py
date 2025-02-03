import json
import os
import socket
import hashlib
import random
import stomp

from typing import Optional, Type

from DIRAC import gConfig
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise

# Setting for the reconnection handling by stomp interface.
# See e.g. the description of Transport class in
# https://github.com/jasonrbriggs/stomp.py/blob/master/stomp/transport.py

RECONNECT_SLEEP_INITIAL = 1  # [s]  Initial delay before reattempting to establish a connection.
RECONNECT_SLEEP_INCREASE = 0.5  # Factor by which sleep delay is increased 0.5 means increase by 50%.
RECONNECT_SLEEP_MAX = 120  # [s] The maximum delay that can be reached independent of increasing procedure.
RECONNECT_SLEEP_JITTER = 0.1  # Random factor to add. 0.1 means a random number from 0 to 10% of the current time.
RECONNECT_ATTEMPTS_MAX = 1e4  # Maximum attempts to reconnect.

OUTGOING_HEARTBEAT_MS = 15_000
INCOMING_HEARTBEAT_MS = 15_000
STOMP_TIMEOUT = 60


DEFAULT_CONNECTION_KWARGS = {
    "keepalive": True,
    "timeout": STOMP_TIMEOUT,
    "heartbeats": (OUTGOING_HEARTBEAT_MS, INCOMING_HEARTBEAT_MS),
    "reconnect_sleep_initial": RECONNECT_SLEEP_INITIAL,
    "reconnect_sleep_increase": RECONNECT_SLEEP_INCREASE,
    "reconnect_sleep_max": RECONNECT_SLEEP_MAX,
    "reconnect_sleep_jitter": RECONNECT_SLEEP_JITTER,
    "reconnect_attempts_max": RECONNECT_ATTEMPTS_MAX,
}


def _resolve_brokers(alias: str, port: int, ipv4Only: bool = False, ipv6Only: bool = False) -> list[tuple[str, int]]:
    """
    To consume all the messages, we need to subscribe to all the hosts behind
    the DNS alias. In this case though, we should use either IPv4 or IPv6 but not both
    to avoid double processing in case of topics.

    To have Producers a bit balanced accross machines, we need to resolve the alias and
    randomize it.


    :param alias: The DNS alias
    :param port: The TCP port
    :param ipv4Only: Only return IPv4
    :param ipv6Only: Only return IPv6
    :return: A list of tuples (resolved ip, port)
    """
    assert not (ipv4Only and ipv6Only)

    brokers = list()

    for family, _, _, _, addr in socket.getaddrinfo(alias, port, 0, socket.SOCK_STREAM):
        ip, port = addr[:2]
        # Disable IPv6 until worker nodes can use it
        if (family == socket.AF_INET and not ipv6Only) or (family == socket.AF_INET6 and not ipv4Only):
            brokers.append((ip, port))

    random.shuffle(brokers)
    return brokers


class ReconnectListener(stomp.ConnectionListener):
    """Listener that takes care of reconnection"""

    def __init__(self, connectCallback, *args):
        """
        :param connectCallback: callback to call for reconnection
        :param args: all the arguments to pass to the connectCallback

        """
        # This boolean is to know whether we effectively
        # want to disconnect or if it is because of a failure
        self.wantsDisconnect = False
        self.connectCallback = connectCallback
        self.args = args

    def on_disconnected(self):
        """Callback function called after disconnecting from broker."""

        if not self.wantsDisconnect:
            try:
                self.connectCallback(*self.args)
            except Exception as e:
                print(f"Unexpected error while calling reconnect callback: {e}")


def getSubscriptionID(broker: tuple[str, int], dest: str) -> str:
    """Generate a unique subscribtionID based on the broker host, port and destination

    :param broker: tuple (host,port) to which we connect
    :param dest: name of the destination (topic or queue)

    """
    host, port = broker
    return hashlib.md5((f"{host}_{port}_{dest}").encode()).hexdigest()


class StompConsumer:
    """Class to listen to a stomp broker.
    It supports the use of aliases, so will create one connection per host behind the alias
    It will also ensure reconnection.

    You can also attach multiple listener to it, but be careful to the following points:

    * It creates one instance of listener per connection (so per host behind the broker alias)
    * The ack/nack logic is left to the listener, so be careful not to compete there.
    * The reconnection logic is already handled by a separate listener, so do not do it yourself


    Having multiple listener sharing the same connection can seem nice, but it is tricky, and you have
    to make sure the different Listener do not interfere with each other.
    That is why it is often wiser to just have separate Connections (so StompConsumer instances) for each Listener
    at the cost of a few extra sockets and threads.

    Example on how to use:

    .. code-block :: python

        class MyCovidListener(stomp.ConnectionListener):
            def __init__(self):
                super().__init__()
                self.covidDB = CovidDB()

            def on_message(self, frame):
                headers = frame.headers
                body = frame.body
                msgId = headers["message-id"]
                subsId = headers["subscription"]
                try:
                    if headers["destination"] == "/queue/positive":
                        self.covidDb.IncreaseCases()
                    elif headers["destination"] == "/queue/dead":
                        self.covidDb.DecreaseCases()
                    self.conn.ack(msgId, subsId)
                except Exception:
                    self.conn.nack(msgId, subsId)


        host = "myBrokerAlias.cern.ch"
        port = 61113
        username = "myUsername"
        password = "IWouldLikeToBuyAHamburger"


        destinations = ["/queue/postive", "/queue/dead"]
        connectionParams = {"heartbeats": (2000, 2000)}

        cons = StompConsumer(
            host,
            port,
            username,
            password,
            destinations=destinations,
            connectionParams=connectionParams,
        )

        cons.addListener(MyCovidListener)

        while PandemyLasts:
            sleep(5)

        conn.disconnect()

    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        destinations: list[str],
        connectionParams: Optional[dict] = None,
        ack: str = "auto",
    ):
        """
        Be careful with the ``ack`` parameter. This will just set the ``ack`` parameter of the
        ~stomp.Connection.subscribe method, but it is up to the listener to effectively ack/nack
        if needed.


        :param host: alias of the broker
        :param port: port to connect to
        :param username: username to connect to the broker
        :param password: password to connect to the broker
        :param destinations: list of topic or queues to listen to
        :param connectionParams: any parameters that should be passed to ~stomp.Connection
        :param ack: see ~stomp.Connection.subscribe

        """

        if not connectionParams:
            connectionParams = {}

        # Keep the list of connections
        self.connections = {}

        # Resolve the various brokers behind the alias
        # We have to make sure to use only either ipv4 or ipv6
        # to avoid doubling the messages
        brokers = _resolve_brokers(host, port, ipv4Only=True)

        # We create independant connections for each host behind the broker alias
        for broker in brokers:
            conn = stomp.Connection([broker], **connectionParams)

            connAndSubArgs = [conn, broker, username, password, destinations, ack]
            self._connectAndSubscribe(*connAndSubArgs)

            conn.set_listener("ReconnectListener", ReconnectListener(self._connectAndSubscribe, *connAndSubArgs))

            connectionID = f"{broker[0]}-{broker[1]}"
            self.connections[connectionID] = conn

    def _connectAndSubscribe(
        self,
        conn: stomp.Connection,
        broker: tuple[str, int],
        username: str,
        password: str,
        destinations: list[str],
        ack: str,
    ) -> None:
        """Just factorize the connection and subscription such that it can be given
        as a callback to the reconnect listener

        """
        # We need to explicitely call disconnect to avoid leaving
        # threads behind
        conn.disconnect()
        conn.connect(username=username, passcode=password, wait=True)
        for dest in destinations:
            subscribtionID = getSubscriptionID(broker, dest)
            conn.subscribe(dest, subscribtionID, ack=ack)

    def addListener(self, listenerCls: type[stomp.ConnectionListener]) -> None:
        """
        Add a listener to each of the connection.
        Also sets the connection asa attribute to the Listener, such that the ack

        :param listenerCls: class of listener. We will instanciate one class per
            connection.
        """

        for connId, conn in self.connections.items():
            lstn = listenerCls()
            lstn.conn = conn
            conn.set_listener(f"{connId}-{id(lstn)}", lstn)

    def disconnect(self):
        """
        Disconnects cleanly from the message queue server
        """
        try:
            for _connId, conn in self.connections.items():
                # Indicate to the ReconnectListener that we want a disconnection
                listener = conn.get_listener("ReconnectListener")
                if listener:
                    listener.wantsDisconnect = True

                conn.disconnect()

        except Exception as e:
            print("Failed to disconnect from broker", repr(e))


class StompProducer(stomp.Connection):
    """Class to send messages to a stomp broker.

    It supports the use of aliases, by randomizing the host behind the aliases and use the others as
    failover.

    The ``send`` method overwrites the one from ~stomp.Connection. It uses a fixed destination given in the constructor,
    and ensures that there are retries

    Usage example:

    .. code-block :: python


        host = "myBrokerAlias.cern.ch"
        port = 61113
        username = "myUsername"
        password = "IWouldLikeToBuyAHamburger"

        logRecord = {"componentname":"DataManagement/DataIntegrity", "levelname":"WARNING", "message":"Chris message"}

        prod = StompProducer(host, port, username, password, "/queue/lhcb.dirac.logging")
        prod.send(json.dumps(logRecord))


    """

    def __init__(self, host: str, port: int, username: str, password: str, destination: str, *args, **kwargs):
        """

        :param host: alias of the broker
        :param port: port to connect to
        :param username: username to connect to the broker
        :param password: password to connect to the broker
        :param destination: topic or queues to which to send the message
        :param args: given to ~stomp.Connection constructor
        :param kwargs: given to ~stomp.Connection constructor
        """
        brokers = _resolve_brokers(host, port)

        super().__init__(brokers, *args, **kwargs)

        self.connect(username, password, True)
        self._destination = destination
        self._retryAttempts = len(brokers)
        self._username = username
        self._password = password

    def send(self, body, **kwargs):
        """Overwrite the send method of ~stomp.Connection

        It catches stomp exception and attempts a reconnection before
        giving up.

        All the parameters are those from ~stomp.Protocol.send, except
        that we force the destination

        :returns: True if everything went fine, False otherwise
        """
        for _ in range(self._retryAttempts):
            try:
                super().send(self._destination, body, **kwargs)
            except stomp.exception.StompException:
                self.disconnect()
                self.connect(self._username, self._password, True)
            else:
                return True
        return False


def _getBrokerParamsFromCS(mqService: str) -> dict:
    """Return the configuration of the broker for a given MQService
    The Sections ``Topics`` and ``Queues`` are returned as the "destinations" key.

    """

    # Compatibility layer in case the full ``qualified`` name is given
    if "::" in mqService:
        if os.environ.get("DIRAC_DEPRECATED_FAIL", None):
            raise NotImplementedError(
                f"ERROR: deprecated do not give the full mqURI, just the service name: {mqService}"
            )

        print(f"WARNING: deprecated do not give the full mqURI, just the service name: {mqService}")
        mqService = mqService.split("::")[0]

    brokerParams = returnValueOrRaise(gConfig.getOptionsDict(f"/Resources/MQServices/{mqService}"))
    # This is for compatibility reasons with the existing configuration definition
    # Although there are no reasons to separate queues and topics for stomp
    topics = [
        f"/topic/{dest}" for dest in gConfig.getSections(f"/Resources/MQServices/{mqService}/Topics").get("Value", [])
    ]
    queues = [
        f"/queue/{dest}" for dest in gConfig.getSections(f"/Resources/MQServices/{mqService}/Queues").get("Value", [])
    ]

    brokerParams["destinations"] = topics + queues

    return brokerParams


@convertToReturnValue
def createConsumer(
    mqService: str,
    destinations: Optional[list[str]] = None,
    listenerCls: Optional[type[stomp.ConnectionListener]] = None,
) -> StompConsumer:
    """Create a consumer for the given mqService

    :param mqService: name of the MQService as defined under /Resources/MQServices/
    :param destinations: list of destinations to listen to. If not defined, take what is
            defined in the CS
    :param listenerCls: if defined, given to StompConsumer.addListener
    """
    brokerParams = _getBrokerParamsFromCS(mqService)

    host = brokerParams["Host"]
    port = int(brokerParams["Port"])
    username = brokerParams["User"]
    password = brokerParams["Password"]

    csDestinations = brokerParams.pop("destinations", None)
    destinations = destinations or csDestinations
    if not destinations:
        raise ValueError("Destinations should either be given as parameter or defined in the CS")
    consumer = StompConsumer(host, port, username, password, destinations)

    if listenerCls:
        consumer.addListener(listenerCls)

    return consumer


@convertToReturnValue
def createProducer(
    mqService: str,
    destination: Optional[str] = None,
) -> StompProducer:
    """Create a Producer for the given mqService

    :param mqService: name of the MQService as defined under /Resources/MQServices/
    :param destination: destination to send to. If not defined, take what is
            defined in the CS
    """
    brokerParams = _getBrokerParamsFromCS(mqService)

    host = brokerParams["Host"]
    port = int(brokerParams["Port"])
    username = brokerParams["User"]
    password = brokerParams["Password"]

    csDestinations = brokerParams.pop("destinations", [])

    if not destination:
        if len(csDestinations) != 1:
            raise ValueError("There should be exactly one destination given in parameter or in the CS")
        destination = csDestinations[0]

    producer = StompProducer(host, port, username, password, destination, **DEFAULT_CONNECTION_KWARGS)

    return producer
