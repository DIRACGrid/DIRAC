""" Tornado-based HTTPs PilotLogging service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoPilotLogging
  :end-before: ##END
  :dedent: 2
  :caption: PilotLogging options
"""

import os
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.Core.DISET.RequestHandler import getServiceOption
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

        defaultOption, defaultClass = "LoggingPlugin", "PilotLoggingPlugin"
        configValue = getServiceOption(infoDict, defaultOption, defaultClass)

        result = ObjectLoader().loadObject(
            f"WorkloadManagementSystem.Client.PilotLoggingPlugins.{configValue}", configValue
        )
        if not result["OK"]:
            cls.log.error("Failed to load LoggingPlugin", f"{configValue}: {result['Message']}")
            return result

        componentClass = result["Value"]
        cls.loggingPlugin = componentClass()
        cls.log.debug("Loaded: PilotLoggingPlugin class", configValue)

        cls.meta = {}
        logPath = os.path.join(os.getcwd(), "pilotlogs")
        cls.meta["LogPath"] = logPath
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        cls.log.verbose("Pilot logging directory:", logPath)

    def export_sendMessage(self, message, pilotUUID, wnVO):
        # def export_sendMessage(self, message, pilotUUID):
        """
        The method logs messages to Tornado and forwards pilot log files, one per pilot, to a relevant plugin.
        The pilot is identified by its UUID.

        :param message: message sent by a client, a list of strings in JSON format
        :param pilotUUID: pilot UUID
        :param pilotUUID: VO manually enforced by a client, used if no VO in a proxy/token
        :return: S_OK or S_ERROR if a plugin cannot process the message.
        :rtype: dict
        """
        ## Insert your method here, don't forget the return should be serializable
        ## Returned value may be an S_OK/S_ERROR
        ## You don't need to serialize in JSON, Tornado will do it

        # determine client VO
        vo = self.__getClientVO()
        if vo == "":
            vo = wnVO

        # the plugin returns S_OK or S_ERROR
        # leave JSON decoding to the selected plugin:
        result = self.loggingPlugin.sendMessage(message, pilotUUID, vo)
        return result

    def export_getMetadata(self):
        """
        Get PilotLoggingHandler metadata. Intended to be used by a client or an agent.

        :return: S_OK containing a metadata dictionary
        """
        return self.loggingPlugin.getMeta()

    def export_getLogs(self, logfile, vo):
        """
        Get (not yet finalised) logs from the server.

        :return:  S_OK containing a metadata dictionary
        :rtype: dict
        """

        return self.loggingPlugin.getLogs(logfile, vo)

    def export_finaliseLogs(self, payload, pilotUUID, wnVO):
        """
        Finalise a log file. Finalised logfile can be copied to a secure location, if a file cache is used.

        :param payload: data passed to the plugin finaliser.
        :type payload: dict
        :param pilotUUID: pilot UUID
        :param pilotUUID: VO manually enforced by a client, used if no VO in a proxy/token
        :return: S_OK or S_ERROR (via the plugin involved)
        :rtype: dict
        """

        vo = self.__getClientVO()
        if vo == "":
            vo = wnVO

        # The plugin returns the Dirac S_OK or S_ERROR object
        return self.loggingPlugin.finaliseLogs(payload, pilotUUID, vo)

    def __getClientVO(self):
        # get client credentials to determine the VO
        return Registry.getVOForGroup(self.getRemoteCredentials()["group"])
