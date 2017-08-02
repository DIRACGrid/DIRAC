"""
Message Queue Handler
"""

__RCSID__ = "$Id$"

import logging

from DIRAC.Resources.MessageQueue.MQCommunication import createProducer


class MessageQueueHandler(logging.Handler):
  """
  MessageQueueHandler is a custom handler from logging.
  It has no equivalent in the standard logging library because it is linked to DIRAC.

  It is useful to send log messages to a destination, like the StreamHandler to a stream, the FileHandler to a file.
  Here, this handler send log messages to a message queue server.
  """

  def __init__(self, queue):
    """
    Initialization of the MessageQueueHandler.

    :params queue: string representing the queue identifier in the configuration.
                   example: "mardirac3.in2p3.fr::Queue::TestQueue"
    """
    logging.Handler.__init__(self)
    self.producer = None
    result = createProducer(queue)
    if result['OK']:
      self.producer = result['Value']

  def emit(self, record):
    """
    Add the record to the message queue.

    :params record: log record object
    """
    strRecord = self.format(record)
    if self.producer is not None:
      self.producer.put(strRecord)
