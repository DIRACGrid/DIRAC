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

DEFAULT_SERVER_LEVEL = 'error'


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

  def __init__(self, backendParams=None):
    super(ServerBackend, self).__init__(ServerHandler, BaseFormatter, backendParams, level=DEFAULT_SERVER_LEVEL)

  def _setHandlerParameters(self, backendParams=None):
    """
    Get the handler parameters from the backendParams.
    The keys of handlerParams should correspond to the parameter names of the associated handler.
    The method should be overridden in every backend that needs handler parameters.
    The method should be called before creating the handler object.

    :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
    """
    # default values
    self._handlerParams['sleepTime'] = 150
    self._handlerParams['interactive'] = True
    self._handlerParams['site'] = None

    if backendParams is not None:
      self._handlerParams['sleepTime'] = backendParams.get('SleepTime', self._handlerParams['sleepTime'])
      self._handlerParams['interactive'] = backendParams.get('Interactive', self._handlerParams['interactive'])
      self._handlerParams['site'] = DIRAC.siteName()
