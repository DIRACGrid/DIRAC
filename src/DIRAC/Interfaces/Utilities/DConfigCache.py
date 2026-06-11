#!/usr/bin/env python
import os
import re
import time
import pickle  # nosec: B403
import tempfile

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.File import cleanDirectory, secureOpenForWrite
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import reset_all_caches


class ConfigCache:
    @classmethod
    def cacheFilePrefix(cls):
        return "DSession.configCache"

    cacheDir = tempfile.gettempdir()

    def __init__(self, forceRefresh=False):
        self.newConfig = True
        self.configCacheLifetime = 600.0  # ten minutes
        self.pid = os.getppid()
        self.configCacheName = os.path.join(self.cacheDir, self.cacheFilePrefix() + ".%d.%d" % (os.getuid(), self.pid))

        if not forceRefresh:
            self.loadConfig()

    def __cleanCacheDirectory(self):
        def pid_exists(pid):
            try:
                os.kill(pid, 0)
            except OSError:
                return False
            return True

        cachePat = "^" + self.cacheFilePrefix() + r"\.%s\.(?P<pid>[0-9]+)$" % os.getuid()
        cacheRe = re.compile(cachePat)

        def _deleteCacheFile(filePath):
            """Callback for cleanDirectory: only delete cache files for dead PIDs.

            Returns True on success, False to skip.
            """
            match = cacheRe.match(filePath.name)
            if not match:
                return False
            if not os.access(filePath, os.W_OK):
                return False
            pid = int(match.group("pid"))
            if pid_exists(pid):
                return False
            filePath.unlink()
            return True

        errFiles = cleanDirectory(
            self.cacheDir,
            maxSecs=0,  # age is irrelevant; all matching files are candidates
            filePatterns=[self.cacheFilePrefix() + ".*"],
            maxDepth=1,
            callbackFn=_deleteCacheFile,
        )
        if errFiles:
            gLogger.warn("Failed to clean cache files:", ",".join(errFiles))

    def loadConfig(self):
        self.newConfig = True

        if os.path.isfile(self.configCacheName):
            cacheStamp = os.stat(self.configCacheName).st_mtime
            # print(time.time() - cacheStamp, self.configCacheLifetime, time.time() - cacheStamp <= self.configCacheLifetime)
            if time.time() - cacheStamp <= self.configCacheLifetime:
                Script.disableCS()
                self.newConfig = False
                # print('use cached config')

    def cacheConfig(self):
        if self.newConfig:
            self.__cleanCacheDirectory()

            with secureOpenForWrite(self.configCacheName, text=False) as (fcache, self.configCacheName):
                pickle.dump(gConfigurationData.mergedCFG, fcache)
        else:
            try:
                with open(self.configCacheName, "rb") as fh:
                    # Pickle files are cached locally, so should be safe
                    gConfigurationData.mergedCFG = pickle.load(fh)  # nosec: B301
                    reset_all_caches()
            except:
                gLogger.error("Cache corrupt or unreadable")
