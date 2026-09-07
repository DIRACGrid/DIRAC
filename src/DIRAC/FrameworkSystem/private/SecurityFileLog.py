import os
import time
import datetime
import gzip
import queue
import shutil
import threading
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.File import cleanDirectory, mkDir
from DIRAC.Core.Utilities.TimeUtilities import DiracTime


class SecurityFileLog(threading.Thread):
    def __init__(self, basePath, daysToLog=100):
        self.__basePath = basePath
        self.__messagesQueue = queue.Queue()
        self.__requiredFields = (
            "timestamp",
            "success",
            "sourceIP",
            "sourcePort",
            "sourceIdentity",
            "destinationIP",
            "destinationPort",
            "destinationService",
            "action",
        )
        threading.Thread.__init__(self)
        self.__secsToLog = daysToLog * 86400
        gThreadScheduler.addPeriodicTask(
            86400, self.__launchCleaningOldLogFiles, elapsedTime=(time.time() % 86400) + 3600
        )
        self.daemon = True
        self.start()

    def run(self):
        while True:
            secMsg = self.__messagesQueue.get()
            msgTime = secMsg[0]
            path = "%s/%s/%02d" % (self.__basePath, msgTime.year, msgTime.month)
            mkDir(path)
            logFile = "%s/%s%02d%02d.security.log.csv" % (path, msgTime.year, msgTime.month, msgTime.day)
            if not os.path.isfile(logFile):
                fd = open(logFile, "w")
                fd.write(
                    "Time, Success, Source IP, Source Port, source Identity, destinationIP,\
           destinationPort, destinationService, action\n"
                )
            else:
                fd = open(logFile, "a")
            fd.write(", ".join(str(item).replace(",", "").replace("\n", "") for item in secMsg) + "\n")
            fd.close()

    def __launchCleaningOldLogFiles(self):
        self._cleanupLogs(self.__basePath, 86400, self.__zipOldLog, "*.security.log.csv")
        self._cleanupLogs(self.__basePath, self.__secsToLog, self.__unlinkOldLog, "*.security.log.csv.gz")

    def _cleanupLogs(self, basePath, maxSecs, functor, pattern):
        """Clean old logs matching a pattern, optionally zipping first."""

        errFiles = cleanDirectory(
            basePath, maxSecs=maxSecs, filePatterns=[pattern], maxDepth=0, callbackFn=functor, delEmptyDirs=True
        )
        if errFiles:
            for fp in errFiles:
                gLogger.error("Failed to clean security log", fp)

    def __unlinkOldLog(self, filePath):
        try:
            gLogger.info(f"Unlinking file {filePath}")
            os.unlink(filePath)
        except Exception as e:
            gLogger.error("Can't unlink old log file", f"{filePath}: {str(e)}")
            return False
        return True

    def __zipOldLog(self, filePath):
        try:
            gLogger.info(f"Compressing file {filePath}")
            with open(filePath, "rb") as f_in:
                with gzip.open(f"{filePath}.gz", "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception:
            gLogger.exception("Can't compress old log file", filePath)
            return False
        return self.__unlinkOldLog(filePath)

    def logAction(self, msg):
        if len(msg) != len(self.__requiredFields):
            return S_ERROR(f"Mismatch in the msg size, it should be {len(self.__requiredFields)} and it's {len(msg)}")
        if not isinstance(msg[0], datetime.datetime):
            gLogger.warn("Received security log message with corrupt timestamp of type " + str(type(msg[0])))
            msg[0] = DiracTime.utcnow()
        elif abs((msg[0] - DiracTime.utcnow()).total_seconds()) > 600:
            gLogger.warn(f"Received security log message with timestamp {msg[0]} more than 10 minutes from local time")
            msg[0] = DiracTime.utcnow()
        self.__messagesQueue.put(msg)
        return S_OK()
