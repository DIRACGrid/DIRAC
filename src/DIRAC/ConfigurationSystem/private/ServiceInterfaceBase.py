"""Service interface is the service which provide config for client and synchronize Master/Slave servers"""

import os
import time
import re
import zipfile
import zlib

import DIRAC
from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData, ConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger


class ServiceInterfaceBase:
    """Service interface is the service which provide config for client and synchronize Master/Slave servers"""

    def __init__(self, sURL):
        self.sURL = sURL
        gLogger.info("Initializing Configuration Service", "URL is %s" % sURL)
        self.__modificationsIgnoreMask = ["/DIRAC/Configuration/Servers", "/DIRAC/Configuration/Version"]
        gConfigurationData.setAsService()
        if not gConfigurationData.isMaster():
            gLogger.info("Starting configuration service as slave")
            gRefresher.autoRefreshAndPublish(self.sURL)
        else:
            gLogger.info("Starting configuration service as master")
            gRefresher.disable()
            self.__loadConfigurationData()
            self.dAliveSlaveServers = {}
            self._launchCheckSlaves()

    def isMaster(self):
        return gConfigurationData.isMaster()

    def _launchCheckSlaves(self):
        raise NotImplementedError("Should be implemented by the children class")

    def __loadConfigurationData(self):
        """
        As the name said, it just load the configuration
        """

        mkDir(os.path.join(DIRAC.rootPath, "etc", "csbackup"))
        gConfigurationData.loadConfigurationData()
        if gConfigurationData.isMaster():
            bBuiltNewConfiguration = False
            if not gConfigurationData.getName():
                DIRAC.abort(10, "Missing name for the configuration to be exported!")
            gConfigurationData.exportName()
            sVersion = gConfigurationData.getVersion()
            if sVersion == "0":
                gLogger.info("There's no version. Generating a new one")
                gConfigurationData.generateNewVersion()
                bBuiltNewConfiguration = True

            if self.sURL not in gConfigurationData.getServers():
                gConfigurationData.setServers(self.sURL)
                bBuiltNewConfiguration = True

            gConfigurationData.setMasterServer(self.sURL)

            if bBuiltNewConfiguration:
                gConfigurationData.writeRemoteConfigurationToDisk()

    def __generateNewVersion(self):
        """
        After changing configuration, we use this method to save them
        """
        if gConfigurationData.isMaster():
            gConfigurationData.generateNewVersion()
            gConfigurationData.writeRemoteConfigurationToDisk()

    def publishSlaveServer(self, sSlaveURL):
        """
        Called by the slave server via service, it register a new slave server

        :param sSlaveURL: url of slave server
        """

        if not gConfigurationData.isMaster():
            return S_ERROR("Configuration modification is not allowed in this server")
        gLogger.info("Pinging slave %s" % sSlaveURL)
        rpcClient = ConfigurationClient(url=sSlaveURL, timeout=10, useCertificates=True)
        retVal = rpcClient.ping()
        if not retVal["OK"]:
            gLogger.info("Slave %s didn't reply" % sSlaveURL)
            return
        if retVal["Value"]["name"] != "Configuration/Server":
            gLogger.info("Slave %s is not a CS serveR" % sSlaveURL)
            return
        bNewSlave = False
        if sSlaveURL not in self.dAliveSlaveServers:
            bNewSlave = True
            gLogger.info("New slave registered", sSlaveURL)
        self.dAliveSlaveServers[sSlaveURL] = time.time()
        if bNewSlave:
            gConfigurationData.setServers(", ".join(self.dAliveSlaveServers))
            self.__generateNewVersion()

    def _checkSlavesStatus(self, forceWriteConfiguration=False):
        """
        Check if Slaves server are still availlable

        :param forceWriteConfiguration: (default False) Force rewriting configuration after checking slaves
        """

        gLogger.info("Checking status of slave servers")
        iGraceTime = gConfigurationData.getSlavesGraceTime()
        bModifiedSlaveServers = False
        for sSlaveURL in list(self.dAliveSlaveServers):
            if time.time() - self.dAliveSlaveServers[sSlaveURL] > iGraceTime:
                gLogger.warn("Found dead slave", sSlaveURL)
                del self.dAliveSlaveServers[sSlaveURL]
                bModifiedSlaveServers = True
        if bModifiedSlaveServers or forceWriteConfiguration:
            gConfigurationData.setServers(", ".join(self.dAliveSlaveServers))
            self.__generateNewVersion()

    @staticmethod
    def _forceServiceUpdate(url, fromMaster):
        """
        Force updating configuration on a given service
        This should be called by _updateServiceConfiguration

        :param str url: service URL
        :param bool fromMaster: flag to force updating from the master CS
        :return: S_OK/S_ERROR
        """
        gLogger.info("Updating service configuration on", url)

        result = Client(url=url).refreshConfiguration(fromMaster)
        result["URL"] = url
        return result

    def _updateServiceConfiguration(self, urlSet, fromMaster=False):
        """
        Update configuration in a set of service in parallel

        :param set urlSet: a set of service URLs
        :param fromMaster: flag to force updating from the master CS
        :return: Nothing
        """
        raise NotImplementedError("Should be implemented by the children class")

    def forceSlavesUpdate(self):
        """
        Force updating configuration on all the slave configuration servers

        :return: Nothing
        """
        gLogger.info("Updating configuration on slave servers")
        iGraceTime = gConfigurationData.getSlavesGraceTime()
        urlSet = set()
        for slaveURL in self.dAliveSlaveServers:
            if time.time() - self.dAliveSlaveServers[slaveURL] <= iGraceTime:
                urlSet.add(slaveURL)
        self._updateServiceConfiguration(urlSet, fromMaster=True)

    def forceGlobalUpdate(self):
        """
        Force updating configuration of all the registered services

        :returns: S_OK (needed for DISET return call)
        """
        gLogger.info("Updating services configuration")
        # Get URLs of all the services except for Configuration services
        cfg = gConfigurationData.remoteCFG.getAsDict()["Systems"]
        urlSet = set()
        for system_ in cfg:
            for instance in cfg[system_]:
                for url in cfg[system_][instance]["URLs"]:
                    urlSet = urlSet.union(
                        {
                            u.strip()
                            for u in cfg[system_][instance]["URLs"][url].split(",")
                            if "Configuration/Server" not in u
                        }
                    )
        self._updateServiceConfiguration(urlSet)
        return S_OK()

    def updateConfiguration(self, sBuffer, committer="", updateVersionOption=False):
        """
        Update the master configuration with the newly received changes

        :param str sBuffer: newly received configuration data
        :param str committer: the user name of the committer
        :param bool updateVersionOption: flag to update the current configuration version
        :return: S_OK/S_ERROR of the write-to-disk of the new configuration
        """
        if not gConfigurationData.isMaster():
            return S_ERROR("Configuration modification is not allowed in this server")
        # Load the data in a ConfigurationData object
        oRemoteConfData = ConfigurationData(False)
        oRemoteConfData.loadRemoteCFGFromCompressedMem(sBuffer)
        if updateVersionOption:
            oRemoteConfData.setVersion(gConfigurationData.getVersion())
        # Test that remote and new versions are the same
        sRemoteVersion = oRemoteConfData.getVersion()
        sLocalVersion = gConfigurationData.getVersion()
        gLogger.info("Checking versions\n", f"remote: {sRemoteVersion}\nlocal:  {sLocalVersion}")
        if sRemoteVersion != sLocalVersion:
            if not gConfigurationData.mergingEnabled():
                return S_ERROR(
                    f"Local and remote versions differ ({sLocalVersion} vs {sRemoteVersion}). Cannot commit."
                )
            else:
                gLogger.info("AutoMerging new data!")
                if updateVersionOption:
                    return S_ERROR("Cannot AutoMerge! version was overwritten")
                result = self.__mergeIndependentUpdates(oRemoteConfData)
                if not result["OK"]:
                    gLogger.warn("Could not AutoMerge!", result["Message"])
                    return S_ERROR("AutoMerge failed: %s" % result["Message"])
                requestedRemoteCFG = result["Value"]
                gLogger.info("AutoMerge successful!")
                oRemoteConfData.setRemoteCFG(requestedRemoteCFG)
        # Test that configuration names are the same
        sRemoteName = oRemoteConfData.getName()
        sLocalName = gConfigurationData.getName()
        if sRemoteName != sLocalName:
            return S_ERROR(f"Names differ: Server is {sLocalName} and remote is {sRemoteName}")
        # Update and generate a new version
        gLogger.info("Committing new data...")
        gConfigurationData.lock()
        gLogger.info("Setting the new CFG")
        gConfigurationData.setRemoteCFG(oRemoteConfData.getRemoteCFG())
        gConfigurationData.unlock()
        gLogger.info("Generating new version")
        gConfigurationData.generateNewVersion()
        # self.__checkSlavesStatus( forceWriteConfiguration = True )
        gLogger.info("Writing new version to disk")
        retVal = gConfigurationData.writeRemoteConfigurationToDisk(f"{committer}@{gConfigurationData.getVersion()}")
        gLogger.info("New version", gConfigurationData.getVersion())

        # Attempt to update the configuration on currently registered slave services
        if gConfigurationData.getAutoSlaveSync():
            self.forceSlavesUpdate()

        return retVal

    def getCompressedConfigurationData(self):
        return gConfigurationData.getCompressedData()

    def getVersion(self):
        return gConfigurationData.getVersion()

    def getCommitHistory(self):
        files = self.__getCfgBackups(gConfigurationData.getBackupDir())
        backups = [".".join(fileName.split(".")[1:-1]).split("@") for fileName in files]
        return backups

    def getVersionContents(self, date):
        backupDir = gConfigurationData.getBackupDir()
        files = self.__getCfgBackups(backupDir, date)
        for fileName in files:
            with zipfile.ZipFile(f"{backupDir}/{fileName}", "rb") as zFile:
                cfgName = zFile.namelist()[0]
                retVal = S_OK(zlib.compress(zFile.read(cfgName), 9))
            return retVal
        return S_ERROR("Version %s does not exist" % date)

    def __getCfgBackups(self, basePath, date="", subPath=""):
        rs = re.compile(rf"^{gConfigurationData.getName()}\..*{date}.*\.zip$")
        fsEntries = os.listdir(f"{basePath}/{subPath}")
        fsEntries.sort(reverse=True)
        backupsList = []
        for entry in fsEntries:
            entryPath = f"{basePath}/{subPath}/{entry}"
            if os.path.isdir(entryPath):
                backupsList.extend(self.__getCfgBackups(basePath, date, f"{subPath}/{entry}"))
            elif os.path.isfile(entryPath):
                if rs.search(entry):
                    backupsList.append(f"{subPath}/{entry}")
        return backupsList

    def __getPreviousCFG(self, oRemoteConfData):
        backupsList = self.__getCfgBackups(gConfigurationData.getBackupDir(), date=oRemoteConfData.getVersion())
        if not backupsList:
            return S_ERROR("Could not AutoMerge. Could not retrieve original committer's version")
        prevRemoteConfData = ConfigurationData()
        backFile = backupsList[0]
        if backFile[0] == "/":
            backFile = os.path.join(gConfigurationData.getBackupDir(), backFile[1:])
        try:
            prevRemoteConfData.loadConfigurationData(backFile)
        except Exception as e:
            return S_ERROR("Could not load original committer's version: %s" % str(e))
        gLogger.info("Loaded client original version %s" % prevRemoteConfData.getVersion())
        return S_OK(prevRemoteConfData.getRemoteCFG())

    def _checkConflictsInModifications(self, realModList, reqModList, parentSection=""):
        realModifiedSections = {
            modAc[1]: modAc[3] for modAc in realModList if modAc[0].find("Sec") == len(modAc[0]) - 3
        }
        reqOptionsModificationList = {
            modAc[1]: modAc[3] for modAc in reqModList if modAc[0].find("Opt") == len(modAc[0]) - 3
        }
        for modAc in reqModList:
            action = modAc[0]
            objectName = modAc[1]
            if action == "addSec":
                if objectName in realModifiedSections:
                    return S_ERROR(f"Section {parentSection}/{objectName} already exists")
            elif action == "delSec":
                if objectName in realModifiedSections:
                    return S_ERROR(f"Section {parentSection}/{objectName} cannot be deleted. It has been modified.")
            elif action == "modSec":
                if objectName in realModifiedSections:
                    result = self._checkConflictsInModifications(
                        realModifiedSections[objectName], modAc[3], f"{parentSection}/{objectName}"
                    )
                    if not result["OK"]:
                        return result
        for modAc in realModList:
            action = modAc[0]
            objectName = modAc[1]
            if action.find("Opt") == len(action) - 3:
                return S_ERROR(
                    "Section %s cannot be merged. Option %s/%s has been modified"
                    % (parentSection, parentSection, objectName)
                )
        return S_OK()

    def __mergeIndependentUpdates(self, oRemoteConfData):
        # return S_ERROR( "AutoMerge is still not finished.
        # Meanwhile... why don't you get the newest conf and update from there?" )
        # Get all the CFGs
        curSrvCFG = gConfigurationData.getRemoteCFG().clone()
        curCliCFG = oRemoteConfData.getRemoteCFG().clone()
        result = self.__getPreviousCFG(oRemoteConfData)
        if not result["OK"]:
            return result
        prevCliCFG = result["Value"]
        # Try to merge curCli with curSrv. To do so we check the updates from
        # prevCli -> curSrv VS prevCli -> curCli
        prevCliToCurCliModList = prevCliCFG.getModifications(curCliCFG)
        prevCliToCurSrvModList = prevCliCFG.getModifications(curSrvCFG)
        result = self._checkConflictsInModifications(prevCliToCurSrvModList, prevCliToCurCliModList)
        if not result["OK"]:
            return S_ERROR("Cannot AutoMerge: %s" % result["Message"])
        # Merge!
        result = curSrvCFG.applyModifications(prevCliToCurCliModList)
        if not result["OK"]:
            return result
        return S_OK(curSrvCFG)
