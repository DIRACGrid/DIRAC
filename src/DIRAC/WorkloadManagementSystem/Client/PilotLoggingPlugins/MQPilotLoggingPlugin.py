"""
MeessageQueue Pilot logging plugin. Just log messages.
"""
import re
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.PilotLoggingPlugin import PilotLoggingPlugin

sLog = gLogger.getSubLogger(__name__)


class MQPilotLoggingPlugin(PilotLoggingPlugin):
    """
    A template of a MQ logging plugin.
    It gets the message and converts it to a list of Dictionaries to be shipped to a remote MQ service
    """

    def __init__(self):
        sLog.warning("MQPilotLoggingPlugin skeleton is being used. NO-op")
        self.rcompiled = re.compile(
            r"(?P<date>[0-9-]+)T(?P<time>[0-9:,]+)Z (?P<loglevel>DEBUG|INFO|ERROR|NOTICE) (?:\[(?P<source>[a-zA-Z]+)\] )?(?P<message>.*)"
        )

    def sendMessage(self, message, pilotID, vo):
        """
        A message could of a form:
        2022-06-10T11:02:02,823512Z DEBUG    [pilotLogger] X509_USER_PROXY=/scratch/dir_2313/user.proxy


        :param str message: text to log
        :param str pilotID: pilot id. Optimally it should be a pilot stamp if available, otherwise a generated UUID.
        :param str vo: VO name of a pilot which sent the message.
        :return: None
        :rtype: None
        """

        res = self.rcompiled.match(message)
        if res:
            resDict = res.groupdict()
            # {'date': '2022-06-10', 'loglevel': 'DEBUG',
            # 'message': '   [pilotLogger] X509_USER_PROXY=/scratch/dir_2313/user.proxy',
            # 'time': '11:02:02,823512', 'source': None}
            return S_OK()
        else:
            return S_ERROR("No match - message could not be parsed")

    def finaliseLogs(self, payload, pilotID, vo):
        """
        Log finaliser method. To indicate that the log is now complete.

        :param dict payload: additional info, a plugin might want to use (i.e. the system return code of a pilot script)
        :param str pilotID: unique pilot ID.
        :param str vo: VO name of a pilot which sent the message.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        return S_OK()
