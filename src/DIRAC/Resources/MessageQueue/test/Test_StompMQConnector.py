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
