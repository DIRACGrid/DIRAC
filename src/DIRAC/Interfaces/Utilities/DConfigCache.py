#!/usr/bin/env python
import os
import re
import stat
import time
import pickle
import tempfile

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.File import secureOpenForWrite
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
            except OSError as _err:
                return False
            return True

        cachePat = "^" + self.cacheFilePrefix() + r"\.%s\.(?P<pid>[0-9]+)$" % os.getuid()
        cacheRe = re.compile(cachePat)
        for fname in os.listdir(self.cacheDir):
            match = cacheRe.match(fname)
            if match is not None:
                pid = int(match.group("pid"))

                path = os.path.join(self.cacheDir, fname)
                # delete session files for non running processes
                if not pid_exists(pid) and os.access(path, os.W_OK):
                    # print("remove old session file", path)
                    os.unlink(path)

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
                    # Check file permissions of cache file post-open to avoid check race conditions
                    info = os.fstat(fh.fileno())
                    if info.st_uid != os.getuid():
                        gLogger.error(f"Cache file {self.configCacheName} not owned by current user! Ignoring...")
                        return
                    if stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
                        gLogger.error(f"Cache file {self.configCacheName} is writeable by group/others! Ignoring...")
                        return
                    # Pickle files are cached locally, so should be safe
                    gConfigurationData.mergedCFG = pickle.load(fh)
                    reset_all_caches()
            except:
                gLogger.error("Cache corrupt or unreadable")
