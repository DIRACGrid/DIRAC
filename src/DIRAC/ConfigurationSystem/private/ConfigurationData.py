""" ConfigurationData module is the base for cfg files management
"""

import os.path
import zlib
import zipfile
import _thread
import time
import datetime
import secrets

from diraccfg import CFG

import DIRAC
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.FrameworkSystem.Client.Logger import gLogger


class ConfigurationData:
    def __init__(self, loadDefaultCFG=True):
        envVar = os.environ.get("DIRAC_FEWER_CFG_LOCKS", "no").lower()
        self.__locksEnabled = envVar not in ("y", "yes", "t", "true", "on", "1")
        if self.__locksEnabled:
            lr = LockRing()
            self.threadingEvent = lr.getEvent()
            self.threadingEvent.set()
            self.threadingLock = lr.getLock()
            self.runningThreadsNumber = 0

        self.__compressedConfigurationData = None
        self.configurationPath = "/DIRAC/Configuration"
        self.backupsDir = os.path.join(DIRAC.rootPath, "etc", "csbackup")
        self._isService = False
        self.localCFG = CFG()
        self.remoteCFG = CFG()
        self.mergedCFG = CFG()
        self.remoteServerList = []
        if loadDefaultCFG:
            defaultCFGFile = os.path.join(DIRAC.rootPath, "etc", "dirac.cfg")
            gLogger.debug("dirac.cfg should be at", f"{defaultCFGFile}")
            retVal = self.loadFile(defaultCFGFile)
            if not retVal["OK"]:
                gLogger.debug(f"Can't load {defaultCFGFile} file")
        self.sync()

    def getBackupDir(self):
        return self.backupsDir

    def sync(self):
        gLogger.debug("Updating configuration internals")
        self.mergedCFG = self.remoteCFG.mergeWith(self.localCFG)
        self.remoteServerList = []
        localServers = self.extractOptionFromCFG(
            f"{self.configurationPath}/Servers", self.localCFG, disableDangerZones=True
        )
        if localServers:
            self.remoteServerList.extend(List.fromChar(localServers, ","))
        remoteServers = self.extractOptionFromCFG(
            f"{self.configurationPath}/Servers", self.remoteCFG, disableDangerZones=True
        )
        if remoteServers:
            self.remoteServerList.extend(List.fromChar(remoteServers, ","))
        self.remoteServerList = List.uniqueElements(self.remoteServerList)
        self.__compressedConfigurationData = None

    def loadFile(self, fileName):
        try:
            fileCFG = CFG()
            fileCFG.loadFromFile(fileName)
        except OSError:
            self.localCFG = self.localCFG.mergeWith(fileCFG)
            return S_ERROR(f"Can't load a cfg file '{fileName}'")
        return self.mergeWithLocal(fileCFG)

    def mergeWithLocal(self, extraCFG):
        self.lock()
        try:
            self.localCFG = self.localCFG.mergeWith(extraCFG)
            self.unlock()
            gLogger.debug("CFG merged")
        except Exception as e:
            self.unlock()
            return S_ERROR(f"Cannot merge with new cfg: {str(e)}")
        self.sync()
        return S_OK()

    def loadRemoteCFGFromCompressedMem(self, data):
        if isinstance(data, str):
            data = data.encode(errors="surrogateescape")
        sUncompressedData = zlib.decompress(data).decode()
        self.loadRemoteCFGFromMem(sUncompressedData)

    def loadRemoteCFGFromMem(self, data):
        self.lock()
        self.remoteCFG.loadFromBuffer(data)
        self.unlock()
        self.sync()

    def loadConfigurationData(self, fileName=False):
        name = self.getName()
        self.lock()
        try:
            if not fileName:
                fileName = f"{name}.cfg"
            if fileName[0] != "/":
                fileName = os.path.join(DIRAC.rootPath, "etc", fileName)
            self.remoteCFG.loadFromFile(fileName)
        except Exception as e:
            print(e)
        self.unlock()
        self.sync()

    def getCommentFromCFG(self, path, cfg=False):
        if not cfg:
            cfg = self.mergedCFG
        self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList[:-1]:
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.getComment(levelList[-1]))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def getSectionsFromCFG(self, path, cfg=False, ordered=False):
        if not cfg:
            cfg = self.mergedCFG
        self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList:
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.listSections(ordered))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def getOptionsFromCFG(self, path, cfg=False, ordered=False):
        if not cfg:
            cfg = self.mergedCFG
        self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList:
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.listOptions(ordered))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def extractOptionFromCFG(self, path, cfg=False, disableDangerZones=False):
        if not cfg:
            cfg = self.mergedCFG
        if not disableDangerZones:
            self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList[:-1]:
                cfg = cfg[section]
            if levelList[-1] in cfg.listOptions():
                return self.dangerZoneEnd(cfg[levelList[-1]])
        except Exception:
            pass
        if not disableDangerZones:
            self.dangerZoneEnd()

    def setOptionInCFG(self, path, value, cfg=False, disableDangerZones=False):
        if not cfg:
            cfg = self.localCFG
        if not disableDangerZones:
            self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList[:-1]:
                if section not in cfg.listSections():
                    cfg.createNewSection(section)
                cfg = cfg[section]
            cfg.setOption(levelList[-1], value)
        finally:
            if not disableDangerZones:
                self.dangerZoneEnd()
        self.sync()

    def deleteOptionInCFG(self, path, cfg=False):
        if not cfg:
            cfg = self.localCFG
        self.dangerZoneStart()
        try:
            levelList = [level.strip() for level in path.split("/") if level.strip() != ""]
            for section in levelList[:-1]:
                if section not in cfg.listSections():
                    return
                cfg = cfg[section]
            cfg.deleteKey(levelList[-1])
        finally:
            self.dangerZoneEnd()
        self.sync()

    def generateNewVersion(self):
        self.setVersion(str(datetime.datetime.utcnow()))
        self.sync()
        gLogger.info(f"Generated new version {self.getVersion()}")

    def setVersion(self, version, cfg=False):
        if not cfg:
            cfg = self.remoteCFG
        self.setOptionInCFG(f"{self.configurationPath}/Version", version, cfg)

    def getVersion(self, cfg=False):
        if not cfg:
            cfg = self.remoteCFG
        value = self.extractOptionFromCFG(f"{self.configurationPath}/Version", cfg)
        if value:
            return value
        return "0"

    def getName(self):
        return self.extractOptionFromCFG(f"{self.configurationPath}/Name", self.mergedCFG)

    def exportName(self):
        return self.setOptionInCFG(f"{self.configurationPath}/Name", self.getName(), self.remoteCFG)

    def getRefreshTime(self):
        try:
            return int(self.extractOptionFromCFG(f"{self.configurationPath}/RefreshTime", self.mergedCFG))
        except Exception:
            return 300

    def getPropagationTime(self):
        try:
            return int(self.extractOptionFromCFG(f"{self.configurationPath}/PropagationTime", self.mergedCFG))
        except Exception:
            return 300

    def getSlavesGraceTime(self):
        try:
            return int(self.extractOptionFromCFG(f"{self.configurationPath}/SlavesGraceTime", self.mergedCFG))
        except Exception:
            return 600

    def mergingEnabled(self):
        try:
            val = self.extractOptionFromCFG(f"{self.configurationPath}/EnableAutoMerge", self.mergedCFG)
            return val.lower() in ("yes", "true", "y")
        except Exception:
            return False

    def getAutoPublish(self):
        value = self.extractOptionFromCFG(f"{self.configurationPath}/AutoPublish", self.localCFG)
        return not bool(value and value.lower() in ("no", "false", "n"))

    def getAutoSlaveSync(self):
        value = self.extractOptionFromCFG(f"{self.configurationPath}/AutoSlaveSync", self.localCFG)
        return not bool(value and value.lower() in ("no", "false", "n"))

    def getServers(self):
        return list(self.remoteServerList)

    def getConfigurationGateway(self):
        return self.extractOptionFromCFG("/DIRAC/Gateway", self.localCFG)

    def setServers(self, sServers):
        self.setOptionInCFG(f"{self.configurationPath}/Servers", sServers, self.remoteCFG)
        self.sync()

    def deleteLocalOption(self, optionPath):
        self.deleteOptionInCFG(optionPath, self.localCFG)

    def getMasterServer(self):
        return self.extractOptionFromCFG(f"{self.configurationPath}/MasterServer", self.remoteCFG)

    def setMasterServer(self, sURL):
        self.setOptionInCFG(f"{self.configurationPath}/MasterServer", sURL, self.remoteCFG)
        self.sync()

    def getCompressedData(self):
        if self.__compressedConfigurationData is None:
            self.__compressedConfigurationData = zlib.compress(str(self.remoteCFG).encode(), 9)
        return self.__compressedConfigurationData

    def isMaster(self):
        value = self.extractOptionFromCFG(f"{self.configurationPath}/Master", self.localCFG)
        return bool(value and value.lower() in ("yes", "true", "y"))

    def getServicesPath(self):
        return "/Services"

    def setAsService(self):
        self._isService = True

    def isService(self):
        return self._isService

    def useServerCertificate(self):
        value = self.extractOptionFromCFG("/DIRAC/Security/UseServerCertificate")
        return bool(value and value.lower() in ("yes", "true", "y"))

    def skipCACheck(self):
        value = self.extractOptionFromCFG("/DIRAC/Security/SkipCAChecks")
        return bool(value and value.lower() in ("yes", "true", "y"))

    def dumpLocalCFGToFile(self, fileName):
        try:
            with open(fileName, "w") as fd:
                fd.write(str(self.localCFG))
            gLogger.verbose("Configuration file dumped", f"'{fileName}'")
        except OSError:
            gLogger.error("Can't dump cfg file", f"'{fileName}'")
            return S_ERROR(f"Can't dump cfg file '{fileName}'")
        return S_OK()

    def getRemoteCFG(self):
        return self.remoteCFG

    def getMergedCFGAsString(self):
        return str(self.mergedCFG)

    def dumpRemoteCFGToFile(self, fileName):
        with open(fileName, "w") as fd:
            fd.write(str(self.remoteCFG))

    def __backupCurrentConfiguration(self, backupName):
        configurationFilename = f"{self.getName()}.cfg"
        configurationFile = os.path.join(DIRAC.rootPath, "etc", configurationFilename)
        today = datetime.datetime.utcnow().date()
        backupPath = os.path.join(self.getBackupDir(), str(today.year), "%02d" % today.month)
        mkDir(backupPath)
        backupFile = os.path.join(backupPath, configurationFilename.replace(".cfg", f".{backupName}.zip"))
        if os.path.isfile(configurationFile):
            gLogger.info(f"Making a backup of configuration in {backupFile}")
            try:
                with zipfile.ZipFile(backupFile, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(configurationFile, f"{os.path.split(configurationFile)[1]}.backup.{backupName}")
            except Exception:
                gLogger.exception()
                gLogger.error("Cannot backup configuration data file", f"file {backupFile}")
        else:
            gLogger.warn("CS data file does not exist", configurationFile)

    def writeRemoteConfigurationToDisk(self, backupName=False):
        configurationFile = os.path.join(DIRAC.rootPath, "etc", f"{self.getName()}.cfg")
        configurationFileTmp = f"{configurationFile}.{secrets.token_hex(8)}"
        try:
            with open(configurationFileTmp, "w") as fd:
                fd.write(str(self.remoteCFG))
            os.rename(configurationFileTmp, configurationFile)
        except Exception as e:
            gLogger.fatal("Cannot write new configuration to disk!", f"file {configurationFile} exception {repr(e)}")
            if os.path.isfile(configurationFileTmp):
                os.remove(configurationFileTmp)
            return S_ERROR(f"Can't write cs file {configurationFile}!: {repr(e).replace(',)', ')')}")
        if backupName:
            self.__backupCurrentConfiguration(backupName)
        return S_OK()

    def setRemoteCFG(self, cfg, disableSync=False):
        self.remoteCFG = cfg.clone()
        if not disableSync:
            self.sync()

    def lock(self):
        """
        Locks Event to prevent further threads from reading.
        Stops current thread until no other thread is accessing.
        PRIVATE USE
        """
        if not self.__locksEnabled:
            return
        self.threadingEvent.clear()
        while self.runningThreadsNumber > 0:
            time.sleep(0.1)

    def unlock(self):
        """
        Unlocks Event.
        PRIVATE USE
        """
        if not self.__locksEnabled:
            return
        self.threadingEvent.set()

    def dangerZoneStart(self):
        """
        Start of danger zone. This danger zone may be or may not be a mutual exclusion zone.
        Counter is maintained to know how many threads are inside and be able to enable and disable mutual exclusion.
        PRIVATE USE
        """
        if not self.__locksEnabled:
            return
        self.threadingEvent.wait()
        self.threadingLock.acquire()
        self.runningThreadsNumber += 1
        try:
            self.threadingLock.release()
        except _thread.error:
            pass

    def dangerZoneEnd(self, returnValue=None):
        """
        End of danger zone.
        PRIVATE USE
        """
        if not self.__locksEnabled:
            return returnValue
        self.threadingLock.acquire()
        self.runningThreadsNumber -= 1
        try:
            self.threadingLock.release()
        except _thread.error:
            pass
        return returnValue
