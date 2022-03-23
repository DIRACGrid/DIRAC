"""
Basic Pilot logging plugin. Just log messages.
"""
import os, json
from DIRAC import S_OK, S_ERROR, gLogger

sLog = gLogger.getSubLogger(__name__)


class FileCacheLoggingPlugin(object):
    """
    File cache logging. Log records are appended to a file, one for each pilot.
    It is assumed that an agent will be installed together with this plugin, which will copy
    the files to a safe place and clear the cache.
    """

    def __init__(self):
        """
        Sets the pilot log files location for a WebServer.

        """
        self.meta = {}
        logPath = os.path.join(os.getcwd(), "pilotlogs")
        self.meta["LogPath"] = logPath
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        sLog.info("Pilot logging directory:", logPath)

    def sendMessage(self, message):
        """
        File cache sendMessage method.

        :param message: text to log
        :type message: str
        :return: None
        :rtype: None
        """
        sLog.info(message)
        messageDict = json.loads(message)
        pilotUUID = messageDict.get("pilotUUID", "Unspecified_ID")
        with open(os.path.join(self.meta["LogPath"], pilotUUID), "a") as pilotLog:
            try:
                pilotLog.write(message + "\n")
            except IOError as ioerr:
                self.log.error("Error writing to log file:", str(ioerr))
                return S_ERROR(str(ioerr))
        return S_OK("Message logged successfully for pilot: %s" % (pilotUUID,))

    def finaliseLogs(self, payload):
        """
        Finalise a log file. Finalised logfile can be copied to a secure location.

        :param logfile: log filename
        :type logfile: str
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        try:
            logfile = json.loads(payload)
            filepath = self.meta["LogPath"]
            os.rename(os.path.join(filepath, logfile), os.path.join(filepath, logfile + ".log"))
            return S_OK("Log file finalised for pilot: %s " % (logfile,))
        except Exception as err:
            return S_ERROR(str(err))

    def getMeta(self):
        """
        Return any metadata related to this plugin. The "LogPath" is the minimum requirement for the dict to contain.

        :return: Dirac S_OK containing the metadata or S_ERROR if the LogPath is not defined.
        :rtype: dict
        """
        if "LogPath" in self.meta:
            return S_OK(self.meta)
        return S_ERROR("No Pilot logging directory defined")
