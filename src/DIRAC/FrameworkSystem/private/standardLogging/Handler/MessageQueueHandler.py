"""
Message Queue Handler
"""
import json
import logging
import os
import queue as _queue
import socket
import sys
import threading
import time

from DIRAC.Resources.MessageQueue.MQCommunication import createProducer


class MessageQueueHandler(logging.Handler):
    """
    MessageQueueHandler is a custom handler from logging.
    It has no equivalent in the standard logging library because it is linked to DIRAC.

    It is useful to send log messages to a destination, like the StreamHandler to a stream, the FileHandler to a file.
    Here, this handler send log messages to a message queue server.

    Sending is done by a background thread: emit() only formats the record and puts it in a
    bounded in-memory queue, so that logging never blocks the caller, even when the broker is
    slow or unreachable. When the queue is full, or a record cannot be sent, the record is
    dropped and counted, and the number of dropped records is reported on stderr.

    There is an assumption made that the formatter used is JsonFormatter
    """

    #: Maximum number of records waiting to be sent
    MAX_QUEUE_SIZE = 10_000
    #: Seconds to pause sending after a failure, doubled up to the maximum while failures persist
    RETRY_BACKOFF_INITIAL = 1
    RETRY_BACKOFF_MAX = 60
    #: Minimum seconds between two reports of dropped records
    DROP_REPORT_INTERVAL = 60
    #: Seconds given to the sender to flush the queue when the handler is closed
    CLOSE_TIMEOUT = 5

    def __init__(self, queue, maxQueueSize=None):
        """
        Initialization of the MessageQueueHandler.

        :param queue: queue identifier in the configuration.
                      example: "mardirac3.in2p3.fr::Queues::TestQueue"
        :param int maxQueueSize: maximum number of records waiting to be sent
        """
        super().__init__()
        self.hostname = socket.gethostname()
        self._mqURI = queue
        self._maxQueueSize = maxQueueSize or self.MAX_QUEUE_SIZE
        self._droppedLock = threading.Lock()
        self._forkLock = threading.Lock()
        self._dropped = 0
        self._lastDropReport = 0.0
        self._startSender()

    def _startSender(self):
        """Create the queue and start the sender thread (again, in a forked child)"""
        # The producer is created by the sender thread, as connecting may take a while
        self.producer = None
        self._records = _queue.Queue(maxsize=self._maxQueueSize)
        self._stopping = threading.Event()
        self._backoff = 0
        self._sender = threading.Thread(target=self._run, name="MessageQueueHandler", daemon=True)
        self._sender.start()
        # Set last, so that other threads of a forked child only see the new pid once all is ready
        self._pid = os.getpid()

    def emit(self, record):
        """
        Add the record to the queue of records to be sent. Never blocks.

        :param record: log record object
        """
        # Records emitted while sending (e.g. errors from the MQ connector itself) are not
        # sent to the broker: during an outage they would feed failures back into the queue.
        if threading.current_thread() is self._sender:
            return
        # Threads do not survive a fork: the child needs its own sender
        if os.getpid() != self._pid:
            with self._forkLock:
                if os.getpid() != self._pid:
                    self._startSender()
        try:
            # add the hostname to the record
            record.hostname = self.hostname
            strRecord = self.format(record)
        except Exception:
            self.handleError(record)
            return
        try:
            self._records.put_nowait(strRecord)
        except _queue.Full:
            self._recordDropped()

    def handle(self, record):
        """
        Conditionally emit the specified logging record.

        Override the handle method from logging.Handler as there is no need to
        acquire the lock to emit the record.
        """
        rv = self.filter(record)
        if rv:
            self.emit(record)
        return rv

    def close(self):
        """Give the sender a short time to flush the queue, then stop it"""
        self._stopping.set()
        # Wake the sender up once it has sent everything queued before (if there is room)
        try:
            self._records.put_nowait(None)
        except _queue.Full:
            pass
        if self._sender.is_alive() and self._sender is not threading.current_thread():
            self._sender.join(self.CLOSE_TIMEOUT)
        super().close()

    @property
    def dropped(self):
        """Number of records dropped so far"""
        return self._dropped

    def _recordDropped(self, count=1):
        with self._droppedLock:
            self._dropped += count
            now = time.monotonic()
            if now - self._lastDropReport < self.DROP_REPORT_INTERVAL:
                return
            self._lastDropReport = now
            dropped = self._dropped
        # Not logged: that would add records to the queue being dropped
        print(f"WARNING MessageQueueHandler: {dropped} log records dropped so far for {self._mqURI}", file=sys.stderr)

    def _run(self):
        """Body of the sender thread"""
        while True:
            try:
                strRecord = self._records.get(timeout=1)
            except _queue.Empty:
                if self._stopping.is_set():
                    return
                continue
            if strRecord is None:
                return
            if not self._send(strRecord):
                self._recordDropped()
                # Pause before the next attempt; when stopping, give up on what is left
                if self._stopping.wait(self._backoff):
                    self._drain()
                    return

    def _send(self, strRecord):
        """Send one record, creating the producer if needed

        :returns: True if the record was sent
        """
        try:
            if self.producer is None:
                result = createProducer(self._mqURI)
                if result["OK"]:
                    self.producer = result["Value"]
            if self.producer is not None:
                result = self.producer.put(json.loads(strRecord))
        except Exception as e:
            result = {"OK": False, "Message": repr(e)}
        if result["OK"]:
            self._backoff = 0
            return True
        self._backoff = min(max(2 * self._backoff, self.RETRY_BACKOFF_INITIAL), self.RETRY_BACKOFF_MAX)
        return False

    def _drain(self):
        """Count the records left in the queue as dropped"""
        count = 0
        while True:
            try:
                strRecord = self._records.get_nowait()
            except _queue.Empty:
                break
            if strRecord is not None:
                count += 1
        if count:
            self._recordDropped(count)
