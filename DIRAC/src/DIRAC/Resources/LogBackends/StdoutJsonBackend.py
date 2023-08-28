"""
JSON output on stdout
"""
import logging
import sys

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.MicrosecondJsonFormatter import MicrosecondJsonFormatter


# These are the standard logging fields that we want to see
# in the json. All the non default are printed anyway
DEFAULT_FMT = "%(levelname)s %(message)s %(asctime)s"


class StdoutJsonBackend(AbstractBackend):
    """
    This just spits out the log on stdout in a json format.
    """

    def __init__(self, backendParams=None, backendFilters=None):
        # The `Format` parameter is passed as `fmt` to MicrosecondJsonFormatter
        # which uses it to know which "standard" fields to keep in the
        # json output. So we need these
        if not backendParams:
            backendParams = {}
        backendParams.setdefault("Format", DEFAULT_FMT)

        super().__init__(logging.StreamHandler, MicrosecondJsonFormatter, backendParams, backendFilters)

    def _setHandlerParameters(self, backendParams=None):
        self._handlerParams["stream"] = sys.stdout
