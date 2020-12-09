"""
System Logging Handler
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging
from six.moves import queue as Queue
import threading

from DIRAC.Core.Utilities import Network


class ServerHandler(logging.Handler, threading.Thread):
  """
  ServerHandler is a custom handler from logging.
  It has no equivalent in the standard logging library because it is highly linked to DIRAC.

  It is useful to send log messages to a destination, like the StreamHandler to a stream, the FileHandler to a file.
  Here, this handler send log messages to a DIRAC service: SystemLogging which store log messages in a database.

  This handler send only log messages superior to WARN. It works in a thread, and send messages every 'sleepTime'.
  When a message must be emit, it is added to queue before sending.
  """

  def __init__(self, sleepTime, interactive, site):
    """
    Initialization of the ServerHandler.
    The queue is initialized with the hostname and the start of the thread.

    :params sleepTime: integer, representing time in seconds where the handler can send messages.
    :params interactive: not used at the moment.
    :params site: the site where the log messages come from.
    """
    super(ServerHandler, self).__init__()
    threading.Thread.__init__(self)
    self.__logQueue = Queue.Queue()

    self.__sleepTime = sleepTime
    self.__interactive = interactive
    self.__site = site
    self.__transactions = []
    self.__hostname = Network.getFQDN()
    self.__alive = True
    self.__maxBundledLogs = 20

    self.setDaemon(True)
    self.start()

  def emit(self, record):
    """
    Add the record to the queue.

    :params record: log record object
    """
    self.__logQueue.put(record)

  def run(self):
    import time
    while self.__alive:
      self.__bundleLogs()
      time.sleep(float(self.__sleepTime))

  def __bundleLogs(self):
    """
    Prepare the log to the sending.
    This method create a tuple based on the record and add it to the bundle for the sending.

    A tuple is necessary for because the service manage messages under this form.
    """
    while not self.__logQueue.empty():
      bundle = []
      while (len(bundle) < self.__maxBundledLogs) and (not self.__logQueue.empty()):
        record = self.__logQueue.get()
        self.format(record)
        logTuple = (record.componentname, record.levelname, record.created, record.getMessage(), record.varmessage,
                    record.pathname + ":" + str(record.lineno), record.name)
        bundle.append(logTuple)

      if bundle:
        self.__sendLogToServer(bundle)

    if self.__transactions:
      self.__sendLogToServer()

  def __sendLogToServer(self, logBundle=None):
    """
    Send log to the SystemLogging service.

    :params logBundle: list of logs ready to be send to the service
    """
    from DIRAC.Core.DISET.RPCClient import RPCClient
    if logBundle:
      self.__transactions.append(logBundle)
    transactionsLength = len(self.__transactions)
    if transactionsLength > 100:
      del self.__transactions[:transactionsLength - 100]
      transactionsLength = 100

    try:
      oSock = RPCClient("Framework/SystemLogging")
    except Exception:
      return False

    while transactionsLength:
      result = oSock.addMessages(self.__transactions[0], self.__site, self.__hostname)
      if result['OK']:
        transactionsLength = transactionsLength - 1
        self.__transactions.pop(0)
      else:
        return False
    return True
