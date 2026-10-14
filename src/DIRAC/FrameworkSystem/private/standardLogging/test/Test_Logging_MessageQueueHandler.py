"""Tests for the MessageQueueHandler: logging must never block on the message queue"""

import json
import logging
import threading
import time
from unittest import mock

import pytest

from DIRAC.FrameworkSystem.private.standardLogging.Handler import MessageQueueHandler as mqhModule
from DIRAC.FrameworkSystem.private.standardLogging.Handler.MessageQueueHandler import MessageQueueHandler

S_OK = {"OK": True, "Value": None}


class FakeProducer:
    """Records what is put, optionally blocking or failing"""

    def __init__(self, block=None, fail=False):
        self.block = block
        self.fail = fail
        self.messages = []
        self.calls = 0

    def put(self, msg):
        self.calls += 1
        if self.block is not None:
            self.block.wait(10)
        if self.fail:
            return {"OK": False, "Message": "broker unreachable"}
        self.messages.append(msg)
        return S_OK


def makeHandler(monkeypatch, producer, **kwargs):
    # The producer is created by the sender thread, so the patch must outlive the constructor
    monkeypatch.setattr(mqhModule, "createProducer", mock.Mock(return_value={"OK": True, "Value": producer}))
    handler = MessageQueueHandler("mq.example::Queues::Test", **kwargs)
    handler.setFormatter(logging.Formatter('{"message": "%(message)s"}'))
    return handler


def makeRecord(msg="hello"):
    return logging.LogRecord("test", logging.INFO, __file__, 1, msg, None, None)


def waitFor(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


@pytest.fixture(autouse=True)
def fastBackoff(monkeypatch):
    monkeypatch.setattr(MessageQueueHandler, "RETRY_BACKOFF_INITIAL", 0.01)
    monkeypatch.setattr(MessageQueueHandler, "RETRY_BACKOFF_MAX", 0.02)
    monkeypatch.setattr(MessageQueueHandler, "CLOSE_TIMEOUT", 1)


def test_records_are_sent_in_the_background(monkeypatch):
    producer = FakeProducer()
    handler = makeHandler(monkeypatch, producer)

    handler.handle(makeRecord("one"))
    handler.handle(makeRecord("two"))

    assert waitFor(lambda: len(producer.messages) == 2)
    assert [m["message"] for m in producer.messages] == ["one", "two"]
    handler.close()


def test_emit_does_not_block_when_the_broker_hangs(monkeypatch):
    release = threading.Event()
    producer = FakeProducer(block=release)
    handler = makeHandler(monkeypatch, producer)

    start = time.monotonic()
    for i in range(100):
        handler.handle(makeRecord(f"msg {i}"))
    assert time.monotonic() - start < 1

    release.set()
    assert waitFor(lambda: len(producer.messages) == 100)
    handler.close()


def test_records_are_dropped_when_the_queue_is_full(monkeypatch, capsys):
    release = threading.Event()
    producer = FakeProducer(block=release)
    handler = makeHandler(monkeypatch, producer, maxQueueSize=5)

    for i in range(20):
        handler.handle(makeRecord(f"msg {i}"))

    # One record is being sent (blocked), 5 are queued, the rest is dropped
    assert waitFor(lambda: handler.dropped >= 14)
    assert "log records dropped" in capsys.readouterr().err
    release.set()
    handler.close()


def test_failed_records_are_dropped_and_sending_backs_off(monkeypatch):
    producer = FakeProducer(fail=True)
    handler = makeHandler(monkeypatch, producer)

    handler.handle(makeRecord("lost"))

    assert waitFor(lambda: handler.dropped == 1)
    assert handler._backoff > 0

    # Sending resumes once the broker is back
    producer.fail = False
    handler.handle(makeRecord("kept"))
    assert waitFor(lambda: [m["message"] for m in producer.messages] == ["kept"])
    assert handler._backoff == 0
    handler.close()


def test_records_from_the_sender_thread_are_not_queued(monkeypatch):
    producer = FakeProducer()
    handler = makeHandler(monkeypatch, producer)

    def logFromSender(msg):
        # e.g. the MQ connector logging an error while sending
        handler.handle(makeRecord("from the sender"))
        return FakeProducer.put(producer, msg)

    producer.put = logFromSender
    handler.handle(makeRecord("original"))

    assert waitFor(lambda: len(producer.messages) == 1)
    time.sleep(0.1)
    assert [m["message"] for m in producer.messages] == ["original"]
    handler.close()


def test_producer_creation_happens_in_the_background_and_is_retried(monkeypatch):
    producer = FakeProducer()
    results = [{"OK": False, "Message": "no broker"}, {"OK": True, "Value": producer}]
    createProducer = mock.Mock(side_effect=results)
    monkeypatch.setattr(mqhModule, "createProducer", createProducer)
    handler = MessageQueueHandler("mq.example::Queues::Test")
    handler.setFormatter(logging.Formatter('{"message": "%(message)s"}'))
    handler.handle(makeRecord("first"))
    handler.handle(makeRecord("second"))

    assert waitFor(lambda: [m["message"] for m in producer.messages] == ["second"])
    assert createProducer.call_count == 2
    assert handler.dropped == 1
    handler.close()


def test_close_flushes_pending_records(monkeypatch):
    producer = FakeProducer()
    handler = makeHandler(monkeypatch, producer)

    for i in range(10):
        handler.handle(makeRecord(f"msg {i}"))
    handler.close()

    assert len(producer.messages) == 10
    assert not handler._sender.is_alive()
    assert json.dumps(producer.messages[0])
