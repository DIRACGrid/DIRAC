""" The CS! (Configuration Service)

Modified to work with Tornado
Encode data in base64 because of JSON limitations
In client side you must use a specific client

"""

from base64 import b64encode, b64decode

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.private.ServiceInterfaceTornado import ServiceInterfaceTornado as ServiceInterface
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService

sLog = gLogger.getSubLogger(__name__)


class TornadoConfigurationHandler(TornadoService):
    """
    The CS handler
    """

    ServiceInterface = None
    PilotSynchronizer = None

    @classmethod
    def initializeHandler(cls, serviceInfo):
        """
        Initialize the configuration server
        Behind it starts thread which refresh configuration
        """
        cls.ServiceInterface = ServiceInterface(serviceInfo["URL"])
        return S_OK()

    def export_getVersion(self):
        """
        Returns the version of the configuration
        """
        return S_OK(self.ServiceInterface.getVersion())

    def export_getCompressedData(self):
        """
        Returns the configuration
        """
        sData = self.ServiceInterface.getCompressedConfigurationData()
        return S_OK(b64encode(sData).decode())

    def export_getCompressedDataIfNewer(self, sClientVersion):
        """
        Returns the configuration if a newer configuration exists, if not just returns the version

        :param sClientVersion: Version used by client
        """
        sVersion = self.ServiceInterface.getVersion()
        retDict = {"newestVersion": sVersion}
        if sClientVersion < sVersion:
            retDict["data"] = b64encode(self.ServiceInterface.getCompressedConfigurationData()).decode()
        return S_OK(retDict)

    def export_publishSlaveServer(self, sURL):
        """
        Used by worker server to register as a worker server.

        :param sURL: The url of the worker server.
        """
        self.ServiceInterface.publishSlaveServer(sURL)
        return S_OK()

    def export_commitNewData(self, sData):
        """
        Write the new configuration
        """
        credDict = self.getRemoteCredentials()
        if "DN" not in credDict or "username" not in credDict:
            return S_ERROR("You must be authenticated!")
        sData = b64decode(sData)
        return self.ServiceInterface.updateConfiguration(sData, credDict["username"])

    def export_writeEnabled(self):
        """
        Used to know if we can change the configuration on this server
        """
        return S_OK(self.ServiceInterface.isMaster())

    def export_getCommitHistory(self, limit=100):
        """
        Get the history of modifications in the configuration
        """
        if limit > 100:
            limit = 100
        history = self.ServiceInterface.getCommitHistory()
        if limit:
            history = history[:limit]
        return S_OK(history)

    def export_getVersionContents(self, versionList):
        """
        Get an old version of the configuration, can also be used to get more than one version.

        :param versionList: List of the version we are trying to get
        """
        contentsList = []
        for version in versionList:
            retVal = self.ServiceInterface.getVersionContents(version)
            if retVal["OK"]:
                contentsList.append(retVal["Value"])
            else:
                return S_ERROR(f"Can't get contents for version {version}: {retVal['Message']}")
        return S_OK(contentsList)

    def export_rollbackToVersion(self, version):
        """
        Rollback to an older version of the configuration

        :param version: The version we want to apply
        """
        retVal = self.ServiceInterface.getVersionContents(version)
        if not retVal["OK"]:
            return S_ERROR(f"Can't get contents for version {version}: {retVal['Message']}")
        credDict = self.getRemoteCredentials()
        if "DN" not in credDict or "username" not in credDict:
            return S_ERROR("You must be authenticated!")
        return self.ServiceInterface.updateConfiguration(
            retVal["Value"], credDict["username"], updateVersionOption=True
        )
