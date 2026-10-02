"""Unit tests for the STOMP message queue connector."""

import socket
from unittest import mock

from DIRAC.Resources.MessageQueue.StompMQConnector import StompMQConnector


@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.random.shuffle")
@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.stomp.Connection")
@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.socket.getaddrinfo")
def test_setup_connection_with_ipv4_only(getaddrinfo, connection, _shuffle):
    getaddrinfo.return_value = [
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.1", 61613)),
    ]

    result = StompMQConnector().setupConnection({"Host": "mq.example", "VHost": "/"})

    assert result["OK"]
    getaddrinfo.assert_called_once_with("mq.example", 61613, socket.AF_UNSPEC, socket.SOCK_STREAM)
    connection.assert_called_once_with(
        vhost="/",
        keepalive=True,
        timeout=60,
        heartbeats=(15_000, 15_000),
        reconnect_sleep_initial=1,
        reconnect_sleep_increase=0.5,
        reconnect_sleep_max=120,
        reconnect_sleep_jitter=0.1,
        reconnect_attempts_max=1e4,
        host_and_ports=[("192.0.2.1", 61613)],
    )


@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.random.shuffle")
@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.stomp.Connection")
@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.socket.getaddrinfo")
def test_setup_connection_prefers_ipv6(getaddrinfo, connection, _shuffle):
    getaddrinfo.return_value = [
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.1", 61613)),
        (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("2001:db8::1", 61613, 0, 0)),
    ]

    result = StompMQConnector().setupConnection({"Host": "mq.example", "VHost": "/"})

    assert result["OK"]
    assert connection.call_args.kwargs["host_and_ports"] == [
        ("2001:db8::1", 61613),
        ("192.0.2.1", 61613),
    ]


@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.socket.getaddrinfo")
def test_setup_connection_reports_resolution_failure(getaddrinfo):
    getaddrinfo.side_effect = socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    result = StompMQConnector().setupConnection({"Host": "missing.example", "VHost": "/"})

    assert not result["OK"]
    assert "Name or service not known" in result["Message"]


def _brokenConnector():
    """A connector whose connection always fails to send and is not connected"""
    connector = StompMQConnector()
    connector.connection = mock.MagicMock()
    connector.connection.send.side_effect = BrokenPipeError(32, "Broken pipe")
    connector.connection.is_connected.return_value = False
    connector.connection.transport.connection_error = True
    return connector


@mock.patch("DIRAC.Resources.MessageQueue.StompMQConnector.time.sleep")
def test_put_makes_a_single_quick_reconnection_attempt(sleep):
    connector = _brokenConnector()

    result = connector.put("msg", {"destination": "/queue/test"})

    assert not result["OK"]
    connector.connection.connect.assert_called_once()
    connector.connection.transport.wait_for_connection.assert_called_once_with(StompMQConnector.PUT_CONNECT_TIMEOUT)
    sleep.assert_not_called()


def test_put_backs_off_after_a_failed_reconnection():
    connector = _brokenConnector()

    assert not connector.put("msg", {"destination": "/queue/test"})["OK"]
    assert not connector.put("msg", {"destination": "/queue/test"})["OK"]

    # The second put is within the backoff window: no new connection attempt
    connector.connection.connect.assert_called_once()

    # Once the backoff has expired, a new attempt is made and the backoff doubles
    connector._nextPutReconnect = 0
    assert not connector.put("msg", {"destination": "/queue/test"})["OK"]
    assert connector.connection.connect.call_count == 2
    assert connector._putReconnectBackoff == 2 * StompMQConnector.PUT_RECONNECT_BACKOFF_INITIAL


def test_put_does_not_wait_for_another_thread_reconnecting():
    connector = _brokenConnector()

    with connector._connectLock:
        result = connector.put("msg", {"destination": "/queue/test"})

    assert not result["OK"]
    assert "in progress" in result["Message"]
    connector.connection.connect.assert_not_called()


def test_put_resends_after_successful_reconnection():
    connector = _brokenConnector()
    connector.connection.send.side_effect = [BrokenPipeError(32, "Broken pipe"), None]
    connector.connection.transport.connection_error = False
    connector.connection.is_connected.side_effect = [False, True]

    result = connector.put({"a": 1}, {"destination": "/queue/test"})

    assert result["OK"]
    assert connector.connection.send.call_count == 2
    assert connector._putReconnectBackoff == 0


def test_connect_is_a_noop_when_already_connected():
    connector = StompMQConnector()
    connector.connection = mock.MagicMock()
    connector.connection.is_connected.return_value = True

    assert connector.connect()["OK"]
    connector.connection.disconnect.assert_not_called()
    connector.connection.connect.assert_not_called()
