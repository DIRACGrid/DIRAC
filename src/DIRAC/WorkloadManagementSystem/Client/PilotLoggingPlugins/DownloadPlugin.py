"""
Pilot logging plugin abstract class.
"""
from abc import ABC, abstractmethod
from DIRAC import S_OK, S_ERROR, gLogger

sLog = gLogger.getSubLogger(__name__)


class DownloadPlugin(ABC):
    """
    Remote pilot log retriever base abstract class. It defines abstract methods used to download log files from a remote
    storage to the server.
    Any pilot logger download plugin should inherit from this class and implement a (sub)set of methods required by
    :class:`PilotManagerHandler`.
    """

    @abstractmethod
    def getRemotePilotLogs(self, pilotStamp, vo):
        """
        Pilot log getter method, carrying the unique pilot identity and a VO name.

        :param str pilotStamp: pilot stamp.
        :param str vo: VO name of a pilot which generated the logs.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        pass
