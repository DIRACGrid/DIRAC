import time
import random


from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.PathFinder import getGatewayURLs
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.EventDispatcher import gEventDispatcher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


def _updateFromRemoteLocation(serviceClient):
    """
    Refresh the configuration
    """
    gLogger.debug("", "Trying to refresh from %s" % serviceClient.serverURL)
    localVersion = gConfigurationData.getVersion()
    retVal = serviceClient.getCompressedDataIfNewer(localVersion)
    if retVal["OK"]:
        dataDict = retVal["Value"]
        newestVersion = dataDict["newestVersion"]
        if localVersion < newestVersion:
            gLogger.debug("New version available", "Updating to version %s..." % newestVersion)
            gConfigurationData.loadRemoteCFGFromCompressedMem(dataDict["data"])
            gLogger.debug("Updated to version %s" % gConfigurationData.getVersion())
            gEventDispatcher.triggerEvent("CSNewVersion", newestVersion, threaded=True)
        return S_OK()
    return retVal


class RefresherBase:
    """
    Code factorisation for the refresher
    """

    def __init__(self):
        self._automaticUpdate = False
        self._lastUpdateTime = 0
        self._refreshTime = gConfigurationData.getRefreshTime()
        self._url = False
        self._refreshEnabled = True
        self._timeout = 60
        self._callbacks = {"newVersion": []}
        gEventDispatcher.registerEvent("CSNewVersion")

    def disable(self):
        """
        Disable the refresher and prevent any request to another server
        """
        self._refreshEnabled = False

    def enable(self):
        """
        Enable the refresher and authorize request to another server
        WARNING: It will not activate automatic updates, use autoRefreshAndPublish() for that
        """
        self._refreshEnabled = True
        if self._lastRefreshExpired():
            return self.forceRefresh()
        return S_OK()

    def isEnabled(self):
        """
        Returns if you can use refresher or not, use automaticUpdateEnabled() to know
        if refresh is automatic.
        """
        return self._refreshEnabled

    def addListenerToNewVersionEvent(self, functor):
        gEventDispatcher.addListener("CSNewVersion", functor)

    def _lastRefreshExpired(self):
        """
        Just returns if last refresh must be considered as expired or not
        """
        return time.time() - self._lastUpdateTime >= self._refreshTime

    def forceRefresh(self, fromMaster=False):
        """
        Force refresh
        WARNING: If refresher is disabled, force a refresh will do nothing
        """
        if self._refreshEnabled:
            return self._refresh(fromMaster=fromMaster)
        return S_OK()

    def _refreshAndPublish(self):
        """
        Refresh configuration and publish local updates
        """
        self._lastUpdateTime = time.time()
        gLogger.info("Refreshing from master server")
        sMasterServer = gConfigurationData.getMasterServer()
        if sMasterServer:
            from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient

            oClient = ConfigurationClient(
                url=sMasterServer,
                timeout=self._timeout,
                useCertificates=gConfigurationData.useServerCertificate(),
                skipCACheck=gConfigurationData.skipCACheck(),
            )
            dRetVal = _updateFromRemoteLocation(oClient)
            if not dRetVal["OK"]:
                gLogger.error("Can't update from master server", dRetVal["Message"])
                return False
            if gConfigurationData.getAutoPublish():
                gLogger.info("Publishing to master server...")
                dRetVal = oClient.publishSlaveServer(self._url)
                if not dRetVal["OK"]:
                    gLogger.error("Can't publish to master server", dRetVal["Message"])
            return True
        else:
            gLogger.warn("No master server is specified in the configuration, trying to get data from other slaves")
            return self._refresh()["OK"]

    def _refresh(self, fromMaster=False):
        """
        Refresh configuration
        """
        self._lastUpdateTime = time.time()
        gLogger.debug("Refreshing configuration...")
        gatewayList = getGatewayURLs("Configuration/Server")
        updatingErrorsList = []
        if gatewayList:
            initialServerList = gatewayList
            gLogger.debug("Using configuration gateway", str(initialServerList[0]))
        elif fromMaster:
            masterServer = gConfigurationData.getMasterServer()
            initialServerList = [masterServer]
            gLogger.debug("Refreshing from master %s" % masterServer)
        else:
            initialServerList = gConfigurationData.getServers()
            gLogger.debug("Refreshing from list %s" % str(initialServerList))

        # If no servers in the initial list, we are supposed to use the local configuration only
        if not initialServerList:
            return S_OK()

        randomServerList = List.randomize(initialServerList)
        gLogger.debug("Randomized server list is %s" % ", ".join(randomServerList))

        for sServer in randomServerList:
            from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient

            oClient = ConfigurationClient(
                url=sServer,
                useCertificates=gConfigurationData.useServerCertificate(),
                skipCACheck=gConfigurationData.skipCACheck(),
            )
            dRetVal = _updateFromRemoteLocation(oClient)
            if dRetVal["OK"]:
                self._refreshTime = gConfigurationData.getRefreshTime()
                return dRetVal
            else:
                updatingErrorsList.append(dRetVal["Message"])
                gLogger.warn(
                    "Can't update from server", "Error while updating from {}: {}".format(sServer, dRetVal["Message"])
                )
                if dRetVal["Message"].find("Insane environment") > -1:
                    break
        return S_ERROR("Reason(s):\n\t%s" % "\n\t".join(List.uniqueElements(updatingErrorsList)))
