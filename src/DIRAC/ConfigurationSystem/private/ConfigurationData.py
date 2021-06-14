""" ConfigurationData module is the base for cfg files management
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
      gLogger.debug("dirac.cfg should be at", "%s" % defaultCFGFile)
      retVal = self.loadFile(defaultCFGFile)
      if not retVal['OK']:
        gLogger.warn("Can't load %s file" % defaultCFGFile)
    self.sync()

  def getBackupDir(self):
    return self.backupsDir

  def sync(self):
    gLogger.debug("Updating configuration internals")
    self.mergedCFG = self.remoteCFG.mergeWith(self.localCFG)
    self.remoteServerList = []
    localServers = self.extractOptionFromCFG("%s/Servers" % self.configurationPath,
                                             self.localCFG,
                                             disableDangerZones=True)
    if localServers:
      self.remoteServerList.extend(List.fromChar(localServers, ","))
    remoteServers = self.extractOptionFromCFG("%s/Servers" % self.configurationPath,
                                              self.remoteCFG,
                                              disableDangerZones=True)
    if remoteServers:
      self.remoteServerList.extend(List.fromChar(remoteServers, ","))
    self.remoteServerList = List.uniqueElements(self.remoteServerList)
    self.__compressedConfigurationData = None

  def loadFile(self, fileName):
    try:
      fileCFG = CFG()
      fileCFG.loadFromFile(fileName)
    except IOError:
      self.localCFG = self.localCFG.mergeWith(fileCFG)
      return S_ERROR("Can't load a cfg file '%s'" % fileName)
    return self.mergeWithLocal(fileCFG)

  def mergeWithLocal(self, extraCFG):
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
    if six.PY3 and isinstance(data, str):
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
        fileName = "%s.cfg" % name
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
    self.setVersion(Time.toString())
    self.sync()
    gLogger.info("Generated new version %s" % self.getVersion())

  def setVersion(self, version, cfg=False):
    if not cfg:
      cfg = self.remoteCFG
    self.setOptionInCFG("%s/Version" % self.configurationPath, version, cfg)

  def getVersion(self, cfg=False):
    if not cfg:
      cfg = self.remoteCFG
    value = self.extractOptionFromCFG("%s/Version" % self.configurationPath, cfg)
    if value:
      return value
    return "0"

  def getName(self):
    return self.extractOptionFromCFG("%s/Name" % self.configurationPath, self.mergedCFG)

  def exportName(self):
    return self.setOptionInCFG("%s/Name" % self.configurationPath, self.getName(), self.remoteCFG)

  def getRefreshTime(self):
    try:
      return int(self.extractOptionFromCFG("%s/RefreshTime" % self.configurationPath, self.mergedCFG))
    except Exception:
      return 300

  def getPropagationTime(self):
    try:
      return int(self.extractOptionFromCFG("%s/PropagationTime" % self.configurationPath, self.mergedCFG))
    except Exception:
      return 300

  def getSlavesGraceTime(self):
    try:
      return int(self.extractOptionFromCFG("%s/SlavesGraceTime" % self.configurationPath, self.mergedCFG))
    except Exception:
      return 600

  def mergingEnabled(self):
    try:
      val = self.extractOptionFromCFG("%s/EnableAutoMerge" % self.configurationPath, self.mergedCFG)
      return val.lower() in ("yes", "true", "y")
    except Exception:
      return False

  def getAutoPublish(self):
    value = self.extractOptionFromCFG("%s/AutoPublish" % self.configurationPath, self.localCFG)
    if value and value.lower() in ("no", "false", "n"):
      return False
    else:
      return True

  def getAutoSlaveSync(self):
    value = self.extractOptionFromCFG("%s/AutoSlaveSync" % self.configurationPath, self.localCFG)
    if value and value.lower() in ("no", "false", "n"):
      return False
    else:
      return True

  def getServers(self):
    return list(self.remoteServerList)

  def getConfigurationGateway(self):
    return self.extractOptionFromCFG("/DIRAC/Gateway", self.localCFG)

  def setServers(self, sServers):
    self.setOptionInCFG("%s/Servers" % self.configurationPath, sServers, self.remoteCFG)
    self.sync()

  def deleteLocalOption(self, optionPath):
    self.deleteOptionInCFG(optionPath, self.localCFG)

  def getMasterServer(self):
    return self.extractOptionFromCFG("%s/MasterServer" % self.configurationPath, self.remoteCFG)

  def setMasterServer(self, sURL):
    self.setOptionInCFG("%s/MasterServer" % self.configurationPath, sURL, self.remoteCFG)
    self.sync()

  def getCompressedData(self):
    if self.__compressedConfigurationData is None:
      self.__compressedConfigurationData = zlib.compress(str(self.remoteCFG).encode(), 9)
    return self.__compressedConfigurationData

  def isMaster(self):
    value = self.extractOptionFromCFG("%s/Master" % self.configurationPath, self.localCFG)
    if value and value.lower() in ("yes", "true", "y"):
      return True
    else:
      return False

  def getServicesPath(self):
    return "/Services"

  def setAsService(self):
    self._isService = True

  def isService(self):
    return self._isService

  def useServerCertificate(self):
    value = self.extractOptionFromCFG("/DIRAC/Security/UseServerCertificate")
    if value and value.lower() in ("y", "yes", "true"):
      return True
    return False

  def skipCACheck(self):
    value = self.extractOptionFromCFG("/DIRAC/Security/SkipCAChecks")
    if value and value.lower() in ("y", "yes", "true"):
      return True
    return False

  def dumpLocalCFGToFile(self, fileName):
    try:
      with open(fileName, "w") as fd:
        fd.write(str(self.localCFG))
      gLogger.verbose("Configuration file dumped", "'%s'" % fileName)
    except IOError:
      gLogger.error("Can't dump cfg file", "'%s'" % fileName)
      return S_ERROR("Can't dump cfg file '%s'" % fileName)
    return S_OK()

  def getRemoteCFG(self):
    return self.remoteCFG

  def getMergedCFGAsString(self):
    return str(self.mergedCFG)

  def dumpRemoteCFGToFile(self, fileName):
    with open(fileName, "w") as fd:
      fd.write(str(self.remoteCFG))

  def __backupCurrentConfiguration(self, backupName):
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
    configurationFile = os.path.join(DIRAC.rootPath, "etc", "%s.cfg" % self.getName())
    try:
      with open(configurationFile, "w") as fd:
        fd.write(str(self.remoteCFG))
    except Exception as e:
      gLogger.fatal("Cannot write new configuration to disk!",
                    "file %s exception %s" % (configurationFile, repr(e)))
      return S_ERROR("Can't write cs file %s!: %s" % (configurationFile, repr(e).replace(',)', ')')))
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
    except thread.error:
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
    except thread.error:
      pass
    return returnValue
