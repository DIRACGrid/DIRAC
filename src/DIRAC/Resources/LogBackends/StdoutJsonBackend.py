"""
JSON output on stdout
"""
import logging
import sys

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.MicrosecondJsonFormatter import MicrosecondJsonFormatter


class StdoutJsonBackend(AbstractBackend):
    """
    This just spits out the log on stdout in a json format.
    """

    def __init__(self, backendParams=None):
        super().__init__(logging.StreamHandler, MicrosecondJsonFormatter, backendParams)

    def _setHandlerParameters(self, backendParams=None):
        self._handlerParams["stream"] = sys.stdout
