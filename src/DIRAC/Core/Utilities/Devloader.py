""" Here, we need some documentation...
"""
import sys
import os
import types
import threading
import time

from DIRAC import gLogger
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton


class Devloader(metaclass=DIRACSingleton):
    def __init__(self):
        self.__log = gLogger.getSubLogger(self.__class__.__name__)
        self.__reloaded = False
        self.__enabled = True
        self.__reloadTask = False
        self.__stuffToClose = []
        self.__watchedFiles = []
        self.__modifyTimes = {}

    def addStuffToClose(self, stuff):
        self.__stuffToClose.append(stuff)

    @property
    def enabled(self):
        return self.__enabled

    def watchFile(self, fp):
        if os.path.isfile(fp):
            self.__watchedFiles.append(fp)
            return True
        return False

    def __restart(self):
        self.__reloaded = True

        for stuff in self.__stuffToClose:
            try:
                self.__log.always("Closing %s" % stuff)
                sys.stdout.flush()
                stuff.close()
            except Exception:
                gLogger.exception("Could not close %s" % stuff)

        python = sys.executable
        os.execl(python, python, *sys.argv)

    def bootstrap(self):
        if not self.__enabled:
            return False
        if self.__reloadTask:
            return True

        self.__reloadTask = threading.Thread(target=self.__reloadOnUpdate)
        self.__reloadTask.daemon = True
        self.__reloadTask.start()

    def __reloadOnUpdate(self):
        while True:
            time.sleep(1)
            if self.__reloaded:
                return
            for modName in sys.modules:
                modObj = sys.modules[modName]
                if not isinstance(modObj, types.ModuleType):
                    continue
                path = getattr(modObj, "__file__", None)
                if not path:
                    continue
                if path.endswith(".pyc") or path.endswith(".pyo"):
                    path = path[:-1]
                self.__checkFile(path)
            for path in self.__watchedFiles:
                self.__checkFile(path)

    def __checkFile(self, path):
        try:
            modified = os.stat(path).st_mtime
        except Exception:
            return
        if path not in self.__modifyTimes:
            self.__modifyTimes[path] = modified
            return
        if self.__modifyTimes[path] != modified:
            self.__log.always("File system changed (%s). Restarting..." % (path))
            self.__restart()
