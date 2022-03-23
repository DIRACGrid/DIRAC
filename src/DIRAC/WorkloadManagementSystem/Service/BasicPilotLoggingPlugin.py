"""
Basic Pilot logging plugin. Just log messages.
"""
from DIRAC import S_OK, S_ERROR, gLogger

sLog = gLogger.getSubLogger(__name__)


class BasicPilotLoggingPlugin(object):
    """
    This is a no-op fallback solution class, to be used when no plugin is defined for remote logging.
    Any pilot logger plugin could inherit from this class to receive a set of no-op methods required by
    :class:`TornadoPilotLoggingHandler` and only overwrite needed methods.
    """

    def __init__(self):

        sLog.warning("BasicPilotLoggingPlugin is being used. It only logs locally at a debug level.")

    def sendMessage(self, message):
        """
        Dummy sendMessage method.

        :param message: text to log
        :type message: str
        :return: None
        :rtype: None
        """
        sLog.debug(message)
        return S_OK("Message sent")

    def finaliseLogs(self, payload):
        """
        Dummy finaliseLogs method.

        :param payload:
        :type payload:
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        return S_OK("Finaliser!")

    def getMeta(self):
        """
        Get metadata dummy method.

        :return: S_OK with an empty dict
        :rtype: dict
        """
        return S_OK({})
