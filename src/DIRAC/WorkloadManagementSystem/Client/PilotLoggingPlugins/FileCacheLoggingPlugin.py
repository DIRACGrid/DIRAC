"""
File cache logging plugin.
"""
import os
import json
import re
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.PilotLoggingPlugins.PilotLoggingPlugin import PilotLoggingPlugin

sLog = gLogger.getSubLogger(__name__)


class FileCacheLoggingPlugin(PilotLoggingPlugin):
    """
    File cache logging. Log records are appended to a file, one for each pilot.
    It is assumed that an agent will be installed together with this plugin, which will copy
    the files to a safe place and clear the cache.
    """

    def __init__(self):
        """
        Sets the pilot log files location for a WebServer.

        """
        # UUID pattern
        self.pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        # pilot stamp pattern (relax to a-z to allow test specific names)
        self.stamppattern = re.compile(r"^[0-9a-z]{32}$")
        self.meta = {}
        logPath = os.path.join(os.getcwd(), "pilotlogs")
        self.meta["LogPath"] = logPath
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        sLog.verbose("Pilot logging directory:", logPath)

    def sendMessage(self, message, pilotUUID, vo):
        """
        File cache sendMessage method. Write the log message to a file line by line.

        :param str message: text to log in json format
        :param str pilotUUID: pilot id. Optimally it should be a pilot stamp if available, otherwise a generated UUID.
        :param str vo: VO name of a pilot which sent the message.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        if not self._verifyUUIDPattern(pilotUUID):
            return S_ERROR("Pilot UUID is invalid")

        dirname = os.path.join(self.meta["LogPath"], vo)
        try:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
            with open(os.path.join(dirname, pilotUUID), "a") as pilotLog:
                try:
                    messageContent = json.loads(message)
                    if isinstance(messageContent, list):
                        for elem in messageContent:
                            pilotLog.write(elem + "\n")
                    else:
                        # it could be a string, if emitted by pilot logger StringIO handler
                        pilotLog.write(messageContent)
                except OSError as oserr:
                    sLog.error("Error writing to log file:", repr(oserr))
                    return S_ERROR(repr(oserr))
        except OSError as err:
            sLog.exception("Error opening a pilot log file", lException=err)
            return S_ERROR(repr(err))
        return S_OK(f"Message logged successfully for pilot: {pilotUUID} and {vo}")

    def finaliseLogs(self, payload, logfile, vo):
        """
        Finalise a log file. Finalised logfile can be copied to a secure location.

        :param dict payload: additional info, a plugin might want to use (i.e. the system return code of a pilot script)
        :param str logfile: log filename (pilotUUID).
        :param str vo: VO name of a pilot which sent the message.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        returnCode = json.loads(payload).get("retCode", 0)

        if not self._verifyUUIDPattern(logfile):
            return S_ERROR("Pilot UUID is invalid")

        try:
            filepath = self.meta["LogPath"]
            os.rename(os.path.join(filepath, vo, logfile), os.path.join(filepath, vo, logfile + ".log"))
            sLog.info(f"Log file {logfile} finalised for pilot: (return code: {returnCode})")
            return S_OK()
        except Exception as err:
            sLog.exception("Exception when finalising log")
            return S_ERROR(repr(err))

    def getMeta(self):
        """
        Return any metadata related to this plugin. The "LogPath" is the minimum requirement for the dict to contain.

        :return: Dirac S_OK containing the metadata or S_ERROR if the LogPath is not defined.
        :rtype: dict
        """
        if "LogPath" in self.meta:
            return S_OK(self.meta)
        return S_ERROR("No Pilot logging directory defined")

    def getLogs(self, logfile, vo):
        """
        Get the "instant" logs from Tornado log storage area. There are not finalised (incomplete) logs.

        :return:  Dirac S_OK containing the logs
        :rtype: dict
        """

        filename = os.path.join(self.meta["LogPath"], vo, logfile)
        resultDict = {}
        try:
            with open(filename) as f:
                stdout = f.read()
                resultDict["StdOut"] = stdout
        except FileNotFoundError as err:
            sLog.error(f"Error opening a log file:{filename}", err)
            return S_ERROR(repr(err))

        return S_OK(resultDict)

    def _verifyUUIDPattern(self, logfile):
        """
        Verify if the name of the log file matches the required pattern.

        :param str name: file name
        :return: re.match result
        :rtype: re.Match object or None.
        """

        res = self.stamppattern.match(logfile)
        if not res:
            res = self.pattern.match(logfile)
        if not res:
            sLog.error(
                "Pilot UUID does not match the UUID nor the stamp pattern. ",
                f"UUID: {logfile}, pilot stamp pattern {self.stamppattern}, UUID pattern {self.pattern}",
            )
        return res
