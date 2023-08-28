""" The CS! (Configuration Service)


The following options can be set for the Configuration Service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN Server
  :end-before: ##END
  :dedent: 2
  :caption: Service options

"""
from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security.Properties import CS_ADMINISTRATOR
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

gServiceInterface = None
gPilotSynchronizer = None


def initializeConfigurationHandler(serviceInfo):
    global gServiceInterface
    gServiceInterface = ServiceInterface(serviceInfo["URL"])
    return S_OK()


class ConfigurationHandler(RequestHandler):
    """The CS handler"""

    types_getVersion = []

    @classmethod
    def export_getVersion(cls):
        return S_OK(gServiceInterface.getVersion())

    types_getCompressedData = []

    @classmethod
    def export_getCompressedData(cls):
        sData = gServiceInterface.getCompressedConfigurationData()
        return S_OK(sData)

    types_getCompressedDataIfNewer = [str]

    @classmethod
    def export_getCompressedDataIfNewer(cls, sClientVersion):
        sVersion = gServiceInterface.getVersion()
        retDict = {"newestVersion": sVersion}
        if sClientVersion < sVersion:
            retDict["data"] = gServiceInterface.getCompressedConfigurationData()
        return S_OK(retDict)

    types_publishSlaveServer = [str]

    @classmethod
    def export_publishSlaveServer(cls, sURL):
        gServiceInterface.publishSlaveServer(sURL)
        return S_OK()

    # Normally we try to avoid passing bytes data however this case it needed for
    # DISET+JEncode to work easily. There is no point in fixing it in a nicer way
    # as the TornadoConfigurationHandler already re-implements this.
    types_commitNewData = [(bytes, str)]

    def export_commitNewData(self, sData):
        global gPilotSynchronizer
        credDict = self.getRemoteCredentials()
        if "DN" not in credDict or "username" not in credDict:
            return S_ERROR("You must be authenticated!")
        return gServiceInterface.updateConfiguration(sData, credDict["username"])

    types_forceGlobalConfigurationUpdate = []
    auth_forceGlobalConfigurationUpdate = [CS_ADMINISTRATOR]

    def export_forceGlobalConfigurationUpdate(self):
        """
        Attempt to request all the configured services to update their configuration

        :return: S_OK
        """
        return gServiceInterface.forceGlobalUpdate()

    types_writeEnabled = []

    @classmethod
    def export_writeEnabled(cls):
        return S_OK(gServiceInterface.isMaster())

    types_getCommitHistory = []

    @classmethod
    def export_getCommitHistory(cls, limit=100):
        if limit > 100:
            limit = 100
        history = gServiceInterface.getCommitHistory()
        if limit:
            history = history[:limit]
        return S_OK(history)

    types_getVersionContents = [list]

    @classmethod
    def export_getVersionContents(cls, versionList):
        contentsList = []
        for version in versionList:
            retVal = gServiceInterface.getVersionContents(version)
            if retVal["OK"]:
                contentsList.append(retVal["Value"])
            else:
                return S_ERROR(f"Can't get contents for version {version}: {retVal['Message']}")
        return S_OK(contentsList)

    types_rollbackToVersion = [str]

    def export_rollbackToVersion(self, version):
        retVal = gServiceInterface.getVersionContents(version)
        if not retVal["OK"]:
            return S_ERROR(f"Can't get contents for version {version}: {retVal['Message']}")
        credDict = self.getRemoteCredentials()
        if "DN" not in credDict or "username" not in credDict:
            return S_ERROR("You must be authenticated!")
        return gServiceInterface.updateConfiguration(retVal["Value"], credDict["username"], updateVersionOption=True)
