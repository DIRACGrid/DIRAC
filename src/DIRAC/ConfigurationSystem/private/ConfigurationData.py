""" ConfigurationData module is the base for cfg files management.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os.path
import zlib
import zipfile
import six
from six.moves import _thread as thread
import time
import DIRAC

from diraccfg import CFG
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities import List, Time
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.FrameworkSystem.Client.Logger import gLogger

__RCSID__ = "$Id$"


class ConfigurationData(object):
    """In this class there is a functionality that allows you to merge
    the configuration given the sensitive to VO and setup settings in the SenSettings section.

    This special section in the root configuration has the following structure and principle of operation::

      DIRAC/..
      Registry/..
      Systems/..
      WebApp/..
      Resources/..

      Operations/
      ../DataManagement
      ../../myOption = myValue

      SenSettings/
      ../lhcb
      ../../Operations
      ../../../DataManagement
      ../../../../myOption = mylhcbvalue

      ../lhcb
      ../../mySetup
      ../../../Operations

      ../mySetup
      ../../Operations

    """

    # Root configuration section name that collect all VO/Setup -sensitive settings
    sensitiveSection = "SenSettings"

    # Dictionary that store VO/Setup -sensitive CFG
    _sensitiveCFG = {}

    def __init__(self, loadDefaultCFG=True):
        """C'r

        :param bool loadDefaultCFG: to load default CFG
        """
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
            gLogger.debug("dirac.cfg should be at", "%s" % defaultCFGFile)
            retVal = self.loadFile(defaultCFGFile)
            if not retVal["OK"]:
                gLogger.warn("Can't load %s file" % defaultCFGFile)
        self.sync()

    def getBackupDir(self):
        """Return backup dir

        :return: str
        """
        return self.backupsDir

    def sync(self):
        """Provide synchronization with remote CFG and create mergedCFG."""
        gLogger.debug("Updating configuration internals")
        self.mergedCFG = self.remoteCFG.mergeWith(self.localCFG)
        self.remoteServerList = []
        localServers = self.extractOptionFromCFG(
            "%s/Servers" % self.configurationPath, self.localCFG, disableDangerZones=True
        )
        if localServers:
            self.remoteServerList.extend(List.fromChar(localServers, ","))
        remoteServers = self.extractOptionFromCFG(
            "%s/Servers" % self.configurationPath, self.remoteCFG, disableDangerZones=True
        )
        if remoteServers:
            self.remoteServerList.extend(List.fromChar(remoteServers, ","))
        self.remoteServerList = List.uniqueElements(self.remoteServerList)
        self.__compressedConfigurationData = None

    def loadFile(self, fileName):
        """Load configuration file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
        """
        try:
            fileCFG = CFG()
            fileCFG.loadFromFile(fileName)
        except IOError:
            self.localCFG = self.localCFG.mergeWith(fileCFG)
            return S_ERROR("Can't load a cfg file '%s'" % fileName)
        return self.mergeWithLocal(fileCFG)

    def mergeWithLocal(self, extraCFG):
        """Merge CFG with local CFG.

        :param extraCFG: CFG to merge

        :return: S_OK()/S_ERROR()
        """
        self.lock()
        try:
            self.localCFG = self.localCFG.mergeWith(extraCFG)
            self.unlock()
            gLogger.debug("CFG merged")
        except Exception as e:
            self.unlock()
            return S_ERROR("Cannot merge with new cfg: %s" % str(e))
        self.sync()
        return S_OK()

    def loadRemoteCFGFromCompressedMem(self, data):
        """Load remote CFG from compressed mem

        :param str data: compressed configuration data
        """
        if isinstance(data, str):
            data = data.encode(errors="surrogateescape")
        sUncompressedData = zlib.decompress(data).decode()
        self.loadRemoteCFGFromMem(sUncompressedData)

    def loadRemoteCFGFromMem(self, data):
        """Load remote CFG from mem

        :param str data: configuration data
        """
        self.lock()
        self.remoteCFG.loadFromBuffer(data)
        self.unlock()
        self.sync()

    def loadConfigurationData(self, fileName=False):
        """Load configuration data from file

        :param str fileName: file name or full path
        """
        name = self.getName()
        self.lock()
        try:
            fileName = fileName or "%s.cfg" % name
            if not fileName.startswith("/"):
                fileName = os.path.join(DIRAC.rootPath, "etc", fileName)
            self.remoteCFG.loadFromFile(fileName)
        except Exception as e:
            print(e)
        self.unlock()
        self.sync()

    def getCommentFromCFG(self, path, cfg=False, vo=None, setup=None):
        """Get comment form CFG

        :param str path: path
        :param cfg: CFG
        :param str vo: VO name to get result in VO context
        :param str setup: Setup name to get result in Setup context

        :return: str
        """
        self.dangerZoneStart()
        try:
            cfg = cfg or (self.getSenSettings(vo, setup) if vo or setup else self.mergedCFG)
            levelList = self.splitPath(path)
            for section in levelList[:-1]:
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.getComment(levelList[-1]))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def getSectionsFromCFG(self, path, cfg=False, ordered=False, vo=None, setup=None):
        """Get sections form CFG

        :param str path: path
        :param cfg: CFG
        :param bool ordered: to sort result
        :param str vo: VO name to get result in VO context
        :param str setup: Setup name to get result in Setup context

        :return: list
        """
        self.dangerZoneStart()
        try:
            cfg = cfg or (self.getSenSettings(vo, setup) if vo or setup else self.mergedCFG)
            for section in self.splitPath(path):
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.listSections(ordered))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def getOptionsFromCFG(self, path, cfg=False, ordered=False, vo=None, setup=None):
        """Get options form CFG

        :param str path: path
        :param cfg: CFG
        :param bool ordered: to sort result
        :param str vo: VO name to get result in VO context
        :param str setup: Setup name to get result in Setup context

        :return: list
        """
        self.dangerZoneStart()
        try:
            cfg = cfg or (self.getSenSettings(vo, setup) if vo or setup else self.mergedCFG)
            for section in self.splitPath(path):
                cfg = cfg[section]
            return self.dangerZoneEnd(cfg.listOptions(ordered))
        except Exception:
            pass
        return self.dangerZoneEnd(None)

    def getSenSettings(self, vo, setup):
        """Get VO/Setup sensitive CFG.


        :param str vo: VO name
        :param str setup: Setup name

        :return: CFG()
        """

        # Use original CFG without merging if first argument is False
        if vo is False:
            return self.mergedCFG

        vo = vo or self.mergedCFG["/DIRAC/VirtualOrganization"]
        setup = setup or self.mergedCFG["/DIRAC/Setup"]

        # Return configuration if not expired
        if (vo, setup) in self._sensitiveCFG:
            if self.getVersion(self._sensitiveCFG[(vo, setup)]) == self.getVersion():
                return self._sensitiveCFG[(vo, setup)]

        # Find root sections, but not section with VO/Setup -sensitive settings
        rootPaths = [s for s in self.mergedCFG.listSections() if s != self.sensitiveSection]

        voSetupCFG = CFG()

        for rootPath in rootPaths:

            # Define the configuration sections that will be merged in the following order:
            # -> /<rootPath>/
            # -> /<sensitiveSection>/<setup>/<rootPath>/
            # -> /<sensitiveSection>/<vo>/<rootPath>/
            # -> /<sensitiveSection>/<vo>/<setup>/<rootPath>/

            paths = [os.path.join("/", rootPath)]
            if setup:
                paths.append(os.path.join("/", self.sensitiveSection, setup, rootPath))
            if vo:
                paths.append(os.path.join("/", self.sensitiveSection, vo, rootPath))
                if setup:
                    paths.append(os.path.join("/", self.sensitiveSection, vo, setup, rootPath))

            rootPathCFG = CFG()

            for path in paths:
                pathCFG = self.mergedCFG[path]
                if pathCFG:
                    rootPathCFG = rootPathCFG.mergeWith(pathCFG)

            voSetupCFG.createNewSection(rootPath, contents=rootPathCFG)

        self._sensitiveCFG[(vo, setup)] = voSetupCFG

        return self._sensitiveCFG[(vo, setup)]

    def splitPath(self, path):
        """Split path

        :param str path: path

        :return: list
        """
        return [level.strip() for level in path.split("/") if level.strip() != ""]

    def extractOptionFromCFG(self, path, cfg=False, disableDangerZones=False, vo=None, setup=None):
        """Extract option from CFG

        :param str path: path
        :param cfg: CFG
        :param bool disableDangerZones: to use disableDangerZones
        :param str vo: VO name to get result in VO context
        :param str setup: Setup name to get result in Setup context

        :return: option value or None
        """
        if not disableDangerZones:
            self.dangerZoneStart()
        try:
            cfg = cfg or (self.getSenSettings(vo, setup) if vo or setup else self.mergedCFG)
            levelList = self.splitPath(path)
            for section in levelList[:-1]:
                cfg = cfg[section]
            if levelList[-1] in cfg.listOptions():
                return self.dangerZoneEnd(cfg[levelList[-1]])
        except Exception:
            pass
        if not disableDangerZones:
            self.dangerZoneEnd()

    def setOptionInCFG(self, path, value, cfg=False, disableDangerZones=False):
        """Set option in CFG

        :param str path: path
        :param value: option value
        :param cfg: CFG
        :param bool disableDangerZones: to use disableDangerZones
        """
        cfg = cfg or self.localCFG
        if not disableDangerZones:
            self.dangerZoneStart()
        try:
            levelList = self.splitPath(path)
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
        """Delete option in CFG

        :param str path: path
        :param cfg: CFG
        """
        cfg = cfg or self.localCFG
        self.dangerZoneStart()
        try:
            levelList = self.splitPath(path)
            for section in levelList[:-1]:
                if section not in cfg.listSections():
                    return
                cfg = cfg[section]
            cfg.deleteKey(levelList[-1])
        finally:
            self.dangerZoneEnd()
        self.sync()

    def generateNewVersion(self):
        """Generate new CFG version"""
        self.setVersion(Time.toString())
        self.sync()
        gLogger.info("Generated new version %s" % self.getVersion())

    def setVersion(self, version, cfg=False):
        """Set new CFG version

        :param str version: version
        :param cfg: CFG
        """
        self.setOptionInCFG("%s/Version" % self.configurationPath, version, cfg or self.remoteCFG)

    def getVersion(self, cfg=False):
        """Get CFG version

        :param cfg: CFG

        :return: str
        """
        return self.extractOptionFromCFG("%s/Version" % self.configurationPath, cfg or self.remoteCFG) or "0"

    def getName(self):
        """Get configuration name

        :return: str or None
        """
        return self.extractOptionFromCFG("%s/Name" % self.configurationPath, self.mergedCFG)

    def exportName(self):
        """Set configuration name"""
        return self.setOptionInCFG("%s/Name" % self.configurationPath, self.getName(), self.remoteCFG)

    def getRefreshTime(self):
        """Get refresh time

        :return: int
        """
        try:
            return int(self.extractOptionFromCFG("%s/RefreshTime" % self.configurationPath, self.mergedCFG))
        except Exception:
            return 300

    def getPropagationTime(self):
        """Get propagation time

        :return: int
        """
        try:
            return int(self.extractOptionFromCFG("%s/PropagationTime" % self.configurationPath, self.mergedCFG))
        except Exception:
            return 300

    def getSlavesGraceTime(self):
        """Get SlavesGrace time

        :return: int
        """
        try:
            return int(self.extractOptionFromCFG("%s/SlavesGraceTime" % self.configurationPath, self.mergedCFG))
        except Exception:
            return 600

    def mergingEnabled(self):
        """Return automerge flag

        :return: bool
        """
        try:
            val = self.extractOptionFromCFG("%s/EnableAutoMerge" % self.configurationPath, self.mergedCFG)
            return val.lower() in ("yes", "true", "y")
        except Exception:
            return False

    def getAutoPublish(self):
        """Return autopublish flag

        :return: bool
        """
        value = self.extractOptionFromCFG("%s/AutoPublish" % self.configurationPath, self.localCFG)
        return False if value and value.lower() in ("no", "false", "n") else True

    def getAutoSlaveSync(self):
        """Return autoslave sync flag

        :return: bool
        """
        value = self.extractOptionFromCFG("%s/AutoSlaveSync" % self.configurationPath, self.localCFG)
        return False if value and value.lower() in ("no", "false", "n") else True

    def getServers(self):
        """Return remote servers

        :return: list
        """
        return list(self.remoteServerList)

    def getConfigurationGateway(self):
        """Get configuration getway

        :return: str
        """
        return self.extractOptionFromCFG("/DIRAC/Gateway", self.localCFG)

    def setServers(self, sServers):
        """Set servers

        :param str sServers: servers URLs
        """
        self.setOptionInCFG("%s/Servers" % self.configurationPath, sServers, self.remoteCFG)
        self.sync()

    def deleteLocalOption(self, optionPath):
        """Delete local option

        :param str optionPath: option path
        """
        self.deleteOptionInCFG(optionPath, self.localCFG)

    def getMasterServer(self):
        """Get master server URL

        :return: str
        """
        return self.extractOptionFromCFG("%s/MasterServer" % self.configurationPath, self.remoteCFG)

    def setMasterServer(self, sURL):
        """Set master server

        :param str sURL: server URL
        """
        self.setOptionInCFG("%s/MasterServer" % self.configurationPath, sURL, self.remoteCFG)
        self.sync()

    def getCompressedData(self):
        """Return compressed congiguration

        :return: str
        """
        if self.__compressedConfigurationData is None:
            self.__compressedConfigurationData = zlib.compress(str(self.remoteCFG).encode(), 9)
        return self.__compressedConfigurationData

    def isMaster(self):
        """Return Master flag

        :return: bool
        """
        value = self.extractOptionFromCFG("%s/Master" % self.configurationPath, self.localCFG)
        return True if value and value.lower() in ("yes", "true", "y") else False

    def getServicesPath(self):
        """Return services path

        :return: str
        """
        return "/Services"

    def setAsService(self):
        """Set _isService flag to True"""
        self._isService = True

    def isService(self):
        """Return _isService flag

        :return: bool
        """
        return self._isService

    def useServerCertificate(self):
        """Skip CA check

        :return: bool
        """
        value = self.extractOptionFromCFG("/DIRAC/Security/UseServerCertificate")
        if value and value.lower() in ("y", "yes", "true"):
            return True
        return False

    def skipCACheck(self):
        """Get skip CA check flag

        :return: bool
        """
        value = self.extractOptionFromCFG("/DIRAC/Security/SkipCAChecks")
        if value and value.lower() in ("y", "yes", "true"):
            return True
        return False

    def dumpLocalCFGToFile(self, fileName):
        """Dump local CFG to file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
        """
        try:
            with open(fileName, "w") as fd:
                fd.write(str(self.localCFG))
            gLogger.verbose("Configuration file dumped", "'%s'" % fileName)
        except IOError:
            gLogger.error("Can't dump cfg file", "'%s'" % fileName)
            return S_ERROR("Can't dump cfg file '%s'" % fileName)
        return S_OK()

    def getRemoteCFG(self):
        """Get remote CFG

        :return: CFG
        """
        return self.remoteCFG

    def getMergedCFGAsString(self):
        """Get merged CFG as string

        :return: str
        """
        return str(self.mergedCFG)

    def dumpRemoteCFGToFile(self, fileName):
        """Dump remote CFG to file

        :param str fileName: file name
        """
        with open(fileName, "w") as fd:
            fd.write(str(self.remoteCFG))

    def __backupCurrentConfiguration(self, backupName):
        """Create configuration backup

        :param str backupName: backup name
        """
        configurationFilename = "%s.cfg" % self.getName()
        configurationFile = os.path.join(DIRAC.rootPath, "etc", configurationFilename)
        today = Time.date()
        backupPath = os.path.join(self.getBackupDir(), str(today.year), "%02d" % today.month)
        mkDir(backupPath)
        backupFile = os.path.join(backupPath, configurationFilename.replace(".cfg", ".%s.zip" % backupName))
        if os.path.isfile(configurationFile):
            gLogger.info("Making a backup of configuration in %s" % backupFile)
            try:
                with zipfile.ZipFile(backupFile, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(configurationFile, "%s.backup.%s" % (os.path.split(configurationFile)[1], backupName))
            except Exception:
                gLogger.exception()
                gLogger.error("Cannot backup configuration data file", "file %s" % backupFile)
        else:
            gLogger.warn("CS data file does not exist", configurationFile)

    def writeRemoteConfigurationToDisk(self, backupName=False):
        """Write remote configuration to disk

        :param str backupName: set backup name to create configuration backup

        :return: S_OK()/S_ERROR()
        """
        configurationFile = os.path.join(DIRAC.rootPath, "etc", "%s.cfg" % self.getName())
        try:
            with open(configurationFile, "w") as fd:
                fd.write(str(self.remoteCFG))
        except Exception as e:
            gLogger.fatal(
                "Cannot write new configuration to disk!", "file %s exception %s" % (configurationFile, repr(e))
            )
            return S_ERROR("Can't write cs file %s!: %s" % (configurationFile, repr(e).replace(",)", ")")))
        if backupName:
            self.__backupCurrentConfiguration(backupName)
        return S_OK()

    def setRemoteCFG(self, cfg, disableSync=False):
        """Set remote CFG

        :param cfg: CFG to set
        :param bool disableSync: to disable synchronization
        """
        self.remoteCFG = cfg.clone()
        if not disableSync:
            self.sync()

    def lock(self):
        """Locks Event to prevent further threads from reading.
        Stops current thread until no other thread is accessing.
        PRIVATE USE
        """
        if self.__locksEnabled:
            self.threadingEvent.clear()
            while self.runningThreadsNumber > 0:
                time.sleep(0.1)

    def unlock(self):
        """Unlocks Event.
        PRIVATE USE
        """
        if self.__locksEnabled:
            self.threadingEvent.set()

    def dangerZoneStart(self):
        """Start of danger zone. This danger zone may be or may not be a mutual exclusion zone.
        Counter is maintained to know how many threads are inside and be able to enable and disable mutual exclusion.
        PRIVATE USE
        """
        if self.__locksEnabled:
            self.threadingEvent.wait()
            self.threadingLock.acquire()
            self.runningThreadsNumber += 1
            try:
                self.threadingLock.release()
            except thread.error:
                pass

    def dangerZoneEnd(self, returnValue=None):
        """End of danger zone.
        PRIVATE USE

        :param returnValue: value to return

        :return: returnValue
        """
        if self.__locksEnabled:
            self.threadingLock.acquire()
            self.runningThreadsNumber -= 1
            try:
                self.threadingLock.release()
            except thread.error:
                pass
        return returnValue
