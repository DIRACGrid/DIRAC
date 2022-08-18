""" This is the Job Repository which stores and manipulates DIRAC job metadata in CFG format """
from diraccfg import CFG

from DIRAC import gLogger, S_OK, S_ERROR

import os
import time
import tempfile
import shutil


class JobRepository:
    def __init__(self, repository=None):
        self.location = repository
        if not self.location:
            if "HOME" in os.environ:
                self.location = "%s/.dirac.repo.rep" % os.environ["HOME"]
            else:
                self.location = "%s/.dirac.repo.rep" % os.getcwd()
        self.repo = CFG()
        if os.path.exists(self.location):
            self.repo.loadFromFile(self.location)
            if not self.repo.existsKey("Jobs"):
                self.repo.createNewSection("Jobs")
        else:
            self.repo.createNewSection("Jobs")
        self.OK = True
        written = self._writeRepository(self.location)
        if not written:
            self.OK = False

    def isOK(self):
        return self.OK

    def readRepository(self):
        return S_OK(self.repo.getAsDict("Jobs"))

    def writeRepository(self, alternativePath=None):
        destination = self.location
        if alternativePath:
            destination = alternativePath
        written = self._writeRepository(destination)
        if not written:
            return S_ERROR("Failed to write repository")
        return S_OK(destination)

    def resetRepository(self, jobIDs=[]):
        if not jobIDs:
            jobs = self.readRepository()["Value"]
            jobIDs = list(jobs)
        paramDict = {"State": "Submitted", "Retrieved": 0, "OutputData": 0}
        for jobID in jobIDs:
            self._writeJob(jobID, paramDict, True)
        self._writeRepository(self.location)
        return S_OK()

    def _writeRepository(self, path):
        handle, tmpName = tempfile.mkstemp()
        written = self.repo.writeToFile(tmpName)
        os.close(handle)
        if not written:
            if os.path.exists(tmpName):
                os.remove(tmpName)
            return written
        if os.path.exists(path):
            gLogger.debug("Replacing %s" % path)
        try:
            shutil.move(tmpName, path)
            return True
        except Exception as x:
            gLogger.error("Failed to overwrite repository.", x)
            gLogger.info("If your repository is corrupted a backup can be found %s" % tmpName)
            return False

    def appendToRepository(self, repoLocation):
        if not os.path.exists(repoLocation):
            gLogger.error("Secondary repository does not exist", repoLocation)
            return S_ERROR("Secondary repository does not exist")
        self.repo = CFG().loadFromFile(repoLocation).mergeWith(self.repo)
        self._writeRepository(self.location)
        return S_OK()

    def addJob(self, jobID, state="Submitted", retrieved=0, outputData=0, update=False):
        paramDict = {"State": state, "Time": self._getTime(), "Retrieved": int(retrieved), "OutputData": outputData}
        self._writeJob(jobID, paramDict, update)
        self._writeRepository(self.location)
        return S_OK(jobID)

    def updateJob(self, jobID, paramDict):
        if self._existsJob(jobID):
            paramDict["Time"] = self._getTime()
            self._writeJob(jobID, paramDict, True)
            self._writeRepository(self.location)
        return S_OK()

    def updateJobs(self, jobDict):
        for jobID, paramDict in jobDict.items():
            if self._existsJob(jobID):
                paramDict["Time"] = self._getTime()
                self._writeJob(jobID, paramDict, True)
        self._writeRepository(self.location)
        return S_OK()

    def _getTime(self):
        runtime = time.ctime()
        return runtime.replace(" ", "_")

    def _writeJob(self, jobID, paramDict, update):
        jobID = str(jobID)
        jobExists = self._existsJob(jobID)
        if jobExists and (not update):
            gLogger.warn("Job exists and not overwriting")
            return S_ERROR("Job exists and not overwriting")
        if not jobExists:
            self.repo.createNewSection("Jobs/%s" % jobID)
        for key, value in paramDict.items():
            self.repo.setOption(f"Jobs/{jobID}/{key}", value)
        return S_OK()

    def removeJob(self, jobID):
        res = self.repo["Jobs"].deleteKey(str(jobID))  # pylint: disable=no-member
        if res:
            self._writeRepository(self.location)
        return S_OK()

    def existsJob(self, jobID):
        return S_OK(self._existsJob(jobID))

    def _existsJob(self, jobID):
        return self.repo.isSection("Jobs/%s" % jobID)

    def getLocation(self):
        return S_OK(self.location)

    def getSize(self):
        return S_OK(len(self.repo.getAsDict("Jobs")))
