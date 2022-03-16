""" Tornado-based HTTPs JobMonitoring service.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import json
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService

sLog = gLogger.getSubLogger(__name__)


class TornadoPilotLoggingHandler(TornadoService):
    log = sLog

    @classmethod
    def initializeHandler(cls, infosDict):
        """
        Called once, at the first request. Create a directory where pilot logs will be stored.

        :param infosDict:
        :return: None
        """

        TornadoPilotLoggingHandler.log.info("Handler initialised ...")
        cls.meta = {}
        logPath = os.path.join(os.getcwd(), "pilotlogs")
        cls.meta["LogPath"] = logPath
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        TornadoPilotLoggingHandler.log.info("Pilot logging directory:", logPath)

    def initializeRequest(self):
        """
        Called for each request.

        :return: None
        """

        self.log.info("Request initialised.. ")

    auth_sayHello = ["all"]

    def export_sayHello(self):
        ## Insert your method here, don't forget the return should be serializable
        ## Returned value may be an S_OK/S_ERROR
        ## You don't need to serialize in JSON, Tornado will do it
        self.log.info("Hello...")
        return S_OK("Hello!")

    auth_sendMessage = ["all"]

    def export_sendMessage(self, message):
        # def export_sendMessage(self, message, pilotUUID):
        """
        The method logs messages to Tornado and writes pilot log files, one per pilot.

        :param message: message sent by a client, in JSON format
        :param pilotUUID: pilot UUID - used to create a log file
        :return: S_OK or S_ERROR if a file cannot be created or written to.
        """
        ## Insert your method here, don't forget the return should be serializable
        ## Returned value may be an S_OK/S_ERROR
        ## You don't need to serialize in JSON, Tornado will do it
        self.log.info("Message: ", message)
        messageDict = json.loads(message)
        pilotUUID = messageDict.get("pilotUUID", "Unspecified_ID")
        with open(os.path.join(TornadoPilotLoggingHandler.meta["LogPath"], pilotUUID), "a") as pilotLog:
            try:
                pilotLog.write(message + "\n")
            except IOError as ioerr:
                self.log.error("Error writing to log file:", str(ioerr))
                return S_ERROR(str(ioerr))
        return S_OK("Message logged successfully for pilot: %s" % (pilotUUID,))

    auth_getMetadata = ["all"]

    def export_getMetadata(self):
        """
        Get PilotLoggingHandler metadata.

        :return: S_OK containing a metadata dictionary
        """
        if "LogPath" in TornadoPilotLoggingHandler.meta:
            return S_OK(TornadoPilotLoggingHandler.meta)
        return S_ERROR("No Pilot logging directory defined")

    auth_finaliseLogs = ["all"]

    def export_finaliseLogs(self, payload):
        """
        Finalise a log file. Finalised logfile can be copied to a secure location.

        :param logfile: log filename
        :type logfile: str
        :return: S_OK or S_ERROR
        :rtype: dict
        """
        try:
            logfile = json.loads(payload)
            filepath = TornadoPilotLoggingHandler.meta["LogPath"]
            os.rename(os.path.join(filepath, logfile), os.path.join(filepath, logfile + ".log"))
            return S_OK("Log file finalised for pilot: %s " % (logfile,))
        except Exception as err:
            return S_ERROR(str(err))
