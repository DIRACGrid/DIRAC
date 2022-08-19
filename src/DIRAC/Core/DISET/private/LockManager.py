import threading


class LockManager:
    def __init__(self, iMaxThreads=None):
        self.iMaxThreads = iMaxThreads
        if iMaxThreads:
            self.oGlobalLock = threading.Semaphore(iMaxThreads)
        else:
            self.oGlobalLock = False
        self.dLocks = {}
        self.dSubManagers = {}

    def createLock(self, sLockName, iMaxThreads):
        if sLockName in self.dLocks:
            raise RuntimeError("%s lock already exists" % sLockName)
        if iMaxThreads < 1:
            return
        self.dLocks[sLockName] = threading.Semaphore(iMaxThreads)
        self.dLocks[sLockName].release()

    def lockGlobal(self):
        if self.oGlobalLock:
            self.oGlobalLock.acquire()

    def unlockGlobal(self):
        if self.oGlobalLock:
            self.oGlobalLock.release()

    def lock(self, sLockName):
        if sLockName in self.dLocks:
            self.dLocks[sLockName].acquire()

    def unlock(self, sLockName):
        if sLockName in self.dLocks:
            self.dLocks[sLockName].release()
