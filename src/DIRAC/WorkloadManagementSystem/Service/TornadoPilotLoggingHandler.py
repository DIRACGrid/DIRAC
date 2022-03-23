""" Tornado-based HTTPs JobMonitoring service.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os, json
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

sLog = gLogger.getSubLogger(__name__)


class TornadoPilotLoggingHandler(TornadoService):
    log = sLog

    @classmethod
    def initializeHandler(cls, infoDict):
        """
        Called once, at the first request. Create a directory where pilot logs will be stored.

        :param infoDict:
        :return: None
        """

        cls.log.info("Handler initialised ...")
        cls.log.debug("with a dict: ", str(infoDict))
        defaultOption, defaultClass = "LoggingPlugin", "BasicPilotLoggingPlugin"
        configValue = getServiceOption(infoDict, defaultOption, defaultClass)

        result = ObjectLoader().loadObject("WorkloadManagementSystem.Service.%s" % (configValue,), configValue)
        if not result["OK"]:
            cls.log.error("Failed to load LoggingPlugin", "%s: %s" % (configValue, result["Message"]))
            return result

        componentClass = result["Value"]
        cls.loggingPlugin = componentClass()
        cls.log.info("Loaded: PilotLoggingPlugin class", configValue)

        cls.meta = {}
        logPath = os.path.join(os.getcwd(), "pilotlogs")
        cls.meta["LogPath"] = logPath
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        cls.log.info("Pilot logging directory:", logPath)

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
        # the plugin returns S_OK or S_ERROR
        result = self.loggingPlugin.sendMessage(message)
        return result

    auth_getMetadata = ["all"]

    def export_getMetadata(self):
        """
        Get PilotLoggingHandler metadata. Intended to be used by a client or an agent.

        :return: S_OK containing a metadata dictionary
        """
        return self.loggingPlugin.getMeta()

    auth_finaliseLogs = ["all"]

    def export_finaliseLogs(self, payload):
        """
        Finalise a log file. Finalised logfile can be copied to a secure location. if a file cache is used.

        :param payload: data passed to the plugin finaliser, a string in the file cache plugin.
        :type payload: str or dict
        :return: S_OK or S_ERROR (via the plugin involved)
        :rtype: dict
        """

        # The plugin returns the Dirac S_OK or S_ERROR object

        return self.loggingPlugin.finaliseLogs(payload)
