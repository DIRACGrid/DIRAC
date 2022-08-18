import os
import re
import time
import gzip
import queue
import shutil
import threading
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.File import mkDir


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
            fd.write("%s\n" % ", ".join([str(item) for item in secMsg]))
            fd.close()

    def __launchCleaningOldLogFiles(self):
        nowEpoch = time.time()
        self.__walkOldLogs(self.__basePath, nowEpoch, re.compile(r"^\d*\.security\.log\.csv$"), 86400, self.__zipOldLog)
        self.__walkOldLogs(
            self.__basePath,
            nowEpoch,
            re.compile(r"^\d*\.security\.log\.csv\.gz$"),
            self.__secsToLog,
            self.__unlinkOldLog,
        )

    def __unlinkOldLog(self, filePath):
        try:
            gLogger.info("Unlinking file %s" % filePath)
            os.unlink(filePath)
        except Exception as e:
            gLogger.error("Can't unlink old log file", f"{filePath}: {str(e)}")
            return 1
        return 0

    def __zipOldLog(self, filePath):
        try:
            gLogger.info("Compressing file %s" % filePath)
            with open(filePath, "rb") as f_in:
                with gzip.open("%s.gz" % filePath, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception:
            gLogger.exception("Can't compress old log file", filePath)
            return 1
        return self.__unlinkOldLog(filePath) + 1

    def __walkOldLogs(self, path, nowEpoch, reLog, executionInSecs, functor):
        initialEntries = os.listdir(path)
        numEntries = 0
        for entry in initialEntries:
            entryPath = os.path.join(path, entry)
            if os.path.isdir(entryPath):
                numEntries += 1
                numEntriesSubDir = self.__walkOldLogs(entryPath, nowEpoch, reLog, executionInSecs, functor)
                if numEntriesSubDir == 0:
                    gLogger.info("Removing dir %s" % entryPath)
                    try:
                        os.rmdir(entryPath)
                        numEntries -= 1
                    except Exception as e:
                        gLogger.error("Can't delete directory", f"{entryPath}: {str(e)}")
            elif os.path.isfile(entryPath):
                numEntries += 1
                if reLog.match(entry):
                    if nowEpoch - os.stat(entryPath)[8] > executionInSecs:
                        numEntries += functor(entryPath) - 1
        return numEntries

    def logAction(self, msg):
        if len(msg) != len(self.__requiredFields):
            return S_ERROR(f"Mismatch in the msg size, it should be {len(self.__requiredFields)} and it's {len(msg)}")
        self.__messagesQueue.put(msg)
        return S_OK()
