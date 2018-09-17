"""
Message Queue Handler
"""

__RCSID__ = "$Id$"

import json
import logging
import socket

from DIRAC.Resources.MessageQueue.MQCommunication import createProducer


class MessageQueueHandler(logging.Handler):
  """
  MessageQueueHandler is a custom handler from logging.
  It has no equivalent in the standard logging library because it is linked to DIRAC.

  It is useful to send log messages to a destination, like the StreamHandler to a stream, the FileHandler to a file.
  Here, this handler send log messages to a message queue server.

  There is an assumption made that the formatter used is JsonFormatter
  """

  def __init__(self, queue):
    """
    Initialization of the MessageQueueHandler.

    :params queue: string representing the queue identifier in the configuration.
                   example: "mardirac3.in2p3.fr::Queue::TestQueue"
    """
    super(MessageQueueHandler, self).__init__()
    self.producer = None
    result = createProducer(queue)
    if result['OK']:
      self.producer = result['Value']
    self.hostname = socket.gethostname()

  def emit(self, record):
    """
    Add the record to the message queue.

    :params record: log record object
    """
    # add the hostname to the record
    record.hostname = self.hostname
    strRecord = self.format(record)
    if self.producer is not None:
      self.producer.put(json.loads(strRecord))
