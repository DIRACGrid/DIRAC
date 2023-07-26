"""
Pilot logging plugin abstract class.
"""
from abc import ABC, abstractmethod
from DIRAC import S_OK, S_ERROR, gLogger

sLog = gLogger.getSubLogger(__name__)


class PilotLoggingPlugin(ABC):
    """
    Remote pilot logging bas abstract class. It defines abstract methods used to sent messages to the server.
    Any pilot logger plugin should inherit from this class and implement a (sub)set of methods required by
    :class:`TornadoPilotLoggingHandler`.
    """

    @abstractmethod
    def sendMessage(self, message, pilotID, vo):
        """
        sendMessage method, carrying the unique pilot identity and a VO name.

        :param str message: text to log in json format
        :param str pilotID: pilot id. Optimally it should be a pilot stamp if available, otherwise a generated UUID.
        :param str vo: VO name of a pilot which sent the message.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        pass

    @abstractmethod
    def finaliseLogs(self, payload, pilotID, vo):
        """
        Log finaliser method. To indicate that the log is now complete.

        :param dict payload: additional info, a plugin might want to use (i.e. the system return code of a pilot script)
        :param str pilotID: unique pilot ID.
        :param str vo: VO name of a pilot which sent the message.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        pass
