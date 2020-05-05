"""
ServerBackend wrapper
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.BaseFormatter import BaseFormatter
from DIRAC.FrameworkSystem.private.standardLogging.Handler.ServerHandler import ServerHandler
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels


class ServerBackend(AbstractBackend):
  """
  ServerBackend is used to create an abstraction of the handler and the formatter concepts from logging.
  Here, we gather a ServerHandler object and a BaseFormatter.

  - ServerHandler is a custom handler object, created for DIRAC because it has no equivalent:
    it is used to write log messages in a server DIRAC service: SystemLogging from FrameworkSystem.
    You can find it in FrameworkSystem/private/standardLogging/Handler

  - BaseFormatter is a custom Formatter object, created for DIRAC in order to get the appropriate display.
    You can find it in FrameworkSystem/private/standardLogging/Formatter
  """

  def __init__(self):
    """
    :params __site: string representing the site where the log messages are from.
    :params __interactive: not used at the moment.
    :params __sleepTime: the time separating the log messages sending, in seconds.
    """
    super(ServerBackend, self).__init__(None, BaseFormatter)
    self.__site = None
    self.__interactive = True
    self.__sleepTime = 150

  def createHandler(self, parameters=None):
    """
    Each backend can initialize its attributes and create its handler with them.

    :params parameters: dictionary of parameters. ex: {'FileName': file.log}
    """
    if parameters is not None:
      self.__interactive = parameters.get('Interactive', self.__interactive)
      self.__sleepTime = parameters.get('SleepTime', self.__sleepTime)
      self.__site = DIRAC.siteName()

    self._handler = ServerHandler(self.__sleepTime, self.__interactive, self.__site)
    self._handler.setLevel(LogLevels.ERROR)

  def setLevel(self, level):
    """
    No possibility to set the level of the server backend because it is hardcoded to ERROR
    and must not be changed
    """
    pass
