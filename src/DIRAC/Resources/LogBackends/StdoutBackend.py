"""
StdoutBackend wrapper
"""
import logging
import sys

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.ColoredBaseFormatter import ColoredBaseFormatter


class StdoutBackend(AbstractBackend):
    """
    StdoutBackend is used to create an abstraction of the handler and the formatter concepts from logging.
    Here, we gather a StreamHandler object and a BaseFormatter.

    - StreamHandler is from the standard logging library: it is used to write log messages in a desired stream
      so it needs a name: here it is stdout.

    - ColorBaseFormatter is a custom Formatter object, created for DIRAC in order to get the appropriate display
      with color.
      You can find it in FrameworkSystem/private/standardLogging/Formatter
    """

    def __init__(self, backendParams=None, backendFilters=None):
        super().__init__(logging.StreamHandler, ColoredBaseFormatter, backendParams, backendFilters)

    def _setHandlerParameters(self, backendParams=None):
        """
        Get the handler parameters from the backendParams.
        The keys of handlerParams should correspond to the parameter names of the associated handler.
        The method should be overridden in every backend that needs handler parameters.
        The method should be called before creating the handler object.

        :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
        """
        self._handlerParams["stream"] = sys.stdout
