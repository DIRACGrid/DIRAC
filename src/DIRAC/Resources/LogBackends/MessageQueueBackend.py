"""
Message Queue wrapper
"""
from DIRAC.FrameworkSystem.private.standardLogging.Handler.MessageQueueHandler import MessageQueueHandler
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.MicrosecondJsonFormatter import MicrosecondJsonFormatter
from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend


DEFAULT_MQ_LEVEL = "verbose"
# These are the standard logging fields that we want to see
# in the json. All the non default are printed anyway
DEFAULT_FMT = "%(levelname)s %(message)s %(asctime)s"


class MessageQueueBackend(AbstractBackend):
    """
    MessageQueueBackend is used to create an abstraction of the handler and the formatter concepts from logging.
    Here, we have:

      - MessageQueueHandler: which is a custom handler created in DIRAC to send
        log records to a Message Queue server. You can find it in: FrameworkSys./private/standardlogging/Handler
      - BaseFormatter: is a custom Formatter object, created for DIRAC in order to get the appropriate display.

      You can find it in FrameworkSystem/private/standardLogging/Formatter
    """

    def __init__(self, backendParams=None):
        """
        Initialization of the MessageQueueBackend
        """
        # The `Format` parameter is passed as `fmt` to MicrosecondJsonFormatter
        # which uses it to know which "standard" fields to keep in the
        # json output. So we need these
        if not backendParams:
            backendParams = {}
        backendParams.setdefault("Format", DEFAULT_FMT)

        super().__init__(MessageQueueHandler, MicrosecondJsonFormatter, backendParams, level=DEFAULT_MQ_LEVEL)

    def _setHandlerParameters(self, backendParams=None):
        """
        Get the handler parameters from the backendParams.
        The keys of handlerParams should correspond to the parameter names of the associated handler.
        The method should be overridden in every backend that needs handler parameters.
        The method should be called before creating the handler object.

        :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
        """
        # default values
        self._handlerParams["queue"] = ""

        if backendParams is not None:
            self._handlerParams["queue"] = backendParams.get("MsgQueue", self._handlerParams["queue"])
