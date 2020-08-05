"""
Message Queue wrapper
"""

__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.private.standardLogging.Handler.MessageQueueHandler import MessageQueueHandler
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.JsonFormatter import JsonFormatter


class MessageQueueBackend(AbstractBackend):
  """
  MessageQueueBackend is used to create an abstraction of the handler and the formatter concepts from logging.
  Here, we have:

    - MessageQueueHandler: which is a custom handler created in DIRAC to send
      log records to a Message Queue server. You can find it in: FrameworkSys./private/standardlogging/Handler
    - BaseFormatter: is a custom Formatter object, created for DIRAC in order to get the appropriate display.

    You can find it in FrameworkSystem/private/standardLogging/Formatter
  """

  def __init__(self):
    """
    Initialization of the MessageQueueBackend
    """
    super(MessageQueueBackend, self).__init__(None, JsonFormatter)
    self.__queue = ''

  def createHandler(self, parameters=None):
    """
    Each backend can initialize its attributes and create its handler with them.

    :params parameters: dictionary of parameters. ex: {'FileName': file.log}
    """
    if parameters is not None:
      self.__queue = parameters.get("MsgQueue", self.__queue)
    self._handler = MessageQueueHandler(self.__queue)
    self._handler.setLevel(LogLevels.VERBOSE)

  def setLevel(self, level):
    """
    No possibility to set the level of the MessageQueue handler.
    It is not set by default so it can send all Log Records of all levels to the MessageQueue.
    """
    pass
