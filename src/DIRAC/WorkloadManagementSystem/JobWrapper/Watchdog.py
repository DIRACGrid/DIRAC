########################################################################
# File  : Watchdog.py
# Author: Stuart Paterson
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system resource consumption.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.
     Furthermore, the Watchdog will identify when the Job CPU limit has been
     exceeded and fail jobs meaningfully.

     Information is returned to the WMS via the heart-beat mechanism.  This
     also interprets control signals from the WMS e.g. to kill a running
     job.

     - Still to implement:
          - CPU normalization for correct comparison with job limit
"""
import datetime
import errno
import getpass
import os
import resource
import signal
import socket
import time
from pathlib import Path

import psutil

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getSystemInstance
from DIRAC.Core.Utilities import MJF
from DIRAC.Core.Utilities.Os import getDiskSpace
from DIRAC.Core.Utilities.Profiler import Profiler
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


def kill_proc_tree(pid, sig=signal.SIGTERM, includeParent=True):
    """Kill a process tree (including grandchildren) with signal
    "sig" and return a (gone, still_alive) tuple.
    called as soon as a child terminates.

    Taken from https://psutil.readthedocs.io/en/latest/index.html#kill-process-tree
    """
    assert pid != os.getpid(), "won't kill myself"
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if includeParent:
        children.append(parent)
    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    _gone, alive = psutil.wait_procs(children, timeout=10)
    for p in alive:
        p.kill()


class Watchdog:

    #############################################################################
    def __init__(self, pid, exeThread, spObject, jobCPUTime, memoryLimit=0, processors=1, jobArgs={}):
        """Constructor, takes system flag as argument."""
        self.stopSigStartSeconds = int(jobArgs.get("StopSigStartSeconds", 1800))  # 30 minutes
        self.stopSigFinishSeconds = int(jobArgs.get("StopSigFinishSeconds", 1800))  # 30 minutes
        self.stopSigNumber = int(jobArgs.get("StopSigNumber", 2))  # SIGINT
        self.stopSigRegex = jobArgs.get("StopSigRegex", None)
        self.stopSigSent = False

        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.exeThread = exeThread
        self.wrapperPID = pid
        self.appPID = self.exeThread.getCurrentPID()
        self.spObject = spObject
        self.jobCPUTime = jobCPUTime
        self.memoryLimit = memoryLimit
        self.calibration = 0
        self.initialValues = {}
        self.parameters = {}
        self.peekFailCount = 0
        self.peekRetry = 5
        self.profiler = Profiler(pid)
        self.checkError = ""
        self.currentStats = {}
        self.initialized = False
        self.count = 0

        # defaults
        self.testWallClock = 1
        self.testDiskSpace = 1
        self.testLoadAvg = 1
        self.maxWallClockTime = 3 * 24 * 60 * 60
        self.testCPUConsumed = 1
        self.testCPULimit = 0
        self.testMemoryLimit = 0
        self.testTimeLeft = 1
        self.pollingTime = 10  # 10 seconds
        self.checkingTime = 30 * 60  # 30 minute period
        self.minCheckingTime = 20 * 60  # 20 mins
        self.wallClockCheckSeconds = 5 * 60  # 5 minutes
        self.maxWallClockTime = 3 * 24 * 60 * 60  # e.g. 4 days
        self.jobPeekFlag = 1  # on / off
        self.minDiskSpace = 10  # MB
        self.loadAvgLimit = 1000  # > 1000 and jobs killed
        self.sampleCPUTime = 30 * 60  # e.g. up to 20mins sample
        self.jobCPUMargin = 20  # age buffer before killing job
        self.minCPUWallClockRatio = 5  # ratio age
        self.nullCPULimit = 5  # After 5 sample times return null CPU consumption kill job
        self.checkCount = 0
        self.wallClockCheckCount = 0
        self.nullCPUCount = 0

        self.grossTimeLeftLimit = 10 * self.checkingTime
        self.timeLeftUtil = TimeLeft()
        self.timeLeft = 0
        self.littleTimeLeft = False
        self.cpuPower = 1.0
        self.processors = processors

    #############################################################################
    def initialize(self):
        """Watchdog initialization."""
        if self.initialized:
            self.log.info("Watchdog already initialized")
            return S_OK()
        else:
            self.initialized = True

        wms_instance = getSystemInstance("WorkloadManagement")
        if not wms_instance:
            return S_ERROR("Can not get the WorkloadManagement system instance")
        self.section = "/Systems/WorkloadManagement/%s/JobWrapper" % wms_instance

        self.log.verbose("Watchdog initialization")
        # Test control flags
        self.testWallClock = gConfig.getValue(self.section + "/CheckWallClockFlag", 1)
        self.testDiskSpace = gConfig.getValue(self.section + "/CheckDiskSpaceFlag", 1)
        self.testLoadAvg = gConfig.getValue(self.section + "/CheckLoadAvgFlag", 1)
        self.testCPUConsumed = gConfig.getValue(self.section + "/CheckCPUConsumedFlag", 1)
        self.testCPULimit = gConfig.getValue(self.section + "/CheckCPULimitFlag", 0)
        self.testMemoryLimit = gConfig.getValue(self.section + "/CheckMemoryLimitFlag", 0)
        self.testTimeLeft = gConfig.getValue(self.section + "/CheckTimeLeftFlag", 1)
        # Other parameters
        self.pollingTime = gConfig.getValue(self.section + "/PollingTime", 10)  # 10 seconds
        self.checkingTime = gConfig.getValue(self.section + "/CheckingTime", 30 * 60)  # 30 minute period
        self.minCheckingTime = gConfig.getValue(self.section + "/MinCheckingTime", 20 * 60)  # 20 mins
        self.maxWallClockTime = gConfig.getValue(self.section + "/MaxWallClockTime", 3 * 24 * 60 * 60)  # e.g. 4 days
        self.jobPeekFlag = gConfig.getValue(self.section + "/JobPeekFlag", 1)  # on / off
        self.minDiskSpace = gConfig.getValue(self.section + "/MinDiskSpace", 10)  # MB
        self.loadAvgLimit = gConfig.getValue(self.section + "/LoadAverageLimit", 1000)  # > 1000 and jobs killed
        self.sampleCPUTime = gConfig.getValue(self.section + "/CPUSampleTime", 30 * 60)  # e.g. up to 20mins sample
        self.jobCPUMargin = gConfig.getValue(self.section + "/JobCPULimitMargin", 20)  # age buffer before killing job
        self.minCPUWallClockRatio = gConfig.getValue(self.section + "/MinCPUWallClockRatio", 5)  # ratio age
        # After 5 sample times return null CPU consumption kill job
        self.nullCPULimit = gConfig.getValue(self.section + "/NullCPUCountLimit", 5)
        if self.checkingTime < self.minCheckingTime:
            self.log.info(
                "Requested CheckingTime of %s setting to %s seconds (minimum)"
                % (self.checkingTime, self.minCheckingTime)
            )
            self.checkingTime = self.minCheckingTime

        # The time left is returned in seconds @ 250 SI00 = 1 HS06,
        # the self.checkingTime and self.pollingTime are in seconds,
        # thus they need to be multiplied by a large enough factor
        self.fineTimeLeftLimit = gConfig.getValue(self.section + "/TimeLeftLimit", 150 * self.pollingTime)
        self.cpuPower = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 1.0)

        return S_OK()

    def run(self):
        """The main watchdog execution method"""

        result = self.initialize()
        if not result["OK"]:
            self.log.always("Can not start watchdog for the following reason")
            self.log.always(result["Message"])
            return result

        try:
            while True:
                self.log.debug("Starting watchdog loop # %d" % self.count)
                start_cycle_time = time.time()
                result = self.execute()
                exec_cycle_time = time.time() - start_cycle_time
                if not result["OK"]:
                    self.log.error("Watchdog error during execution", result["Message"])
                    break
                elif result["Value"] == "Ended":
                    break
                self.count += 1
                if exec_cycle_time < self.pollingTime:
                    time.sleep(self.pollingTime - exec_cycle_time)
            return S_OK()
        except Exception:
            self.log.exception()
            return S_ERROR("Exception")

    #############################################################################
    def execute(self):
        """The main agent execution method of the Watchdog."""

        if not self.exeThread.is_alive():
            self.__getUsageSummary()
            self.log.info("Process to monitor has completed, Watchdog will exit.")
            return S_OK("Ended")

        # WallClock checks every self.wallClockCheckSeconds, but only if StopSigRegex is defined in JDL
        if (
            not self.stopSigSent
            and self.stopSigRegex is not None
            and (time.time() - self.initialValues["StartTime"]) > self.wallClockCheckSeconds * self.wallClockCheckCount
        ):
            self.wallClockCheckCount += 1
            self._performWallClockChecks()

        if self.littleTimeLeft:
            # if we have gone over enough iterations query again
            if self.littleTimeLeftCount == 0 and self.__timeLeft() == -1:
                self.checkError = JobMinorStatus.JOB_EXCEEDED_CPU
                self.log.error(self.checkError, self.timeLeft)
                self.__killRunningThread()
                return S_OK()

            self.littleTimeLeftCount -= 1

        # Note: need to poll regularly to see if the thread is alive
        #      but only perform checks with a certain frequency
        if (time.time() - self.initialValues["StartTime"]) > self.checkingTime * self.checkCount:
            self.checkCount += 1
            result = self._performChecks()
            if not result["OK"]:
                self.log.warn("Problem during recent checks")
                self.log.warn(result["Message"])
            return S_OK()
        else:
            # self.log.debug('Application thread is alive: checking count is %s' %(self.checkCount))
            return S_OK()

    #############################################################################
    def _performWallClockChecks(self):
        """Watchdog performs the wall clock checks based on MJF. Signals are sent
        to processes if we need to stop, but function always returns S_OK()
        """
        mjf = MJF.MJF()

        try:
            wallClockSecondsLeft = mjf.getWallClockSecondsLeft()
        except Exception:
            # Just stop if we can't get the wall clock seconds left
            return S_OK()

        jobstartSeconds = mjf.getIntJobFeature("jobstart_secs")
        if jobstartSeconds is None:
            # Just stop if we don't know when the job started
            return S_OK()

        if (int(time.time()) > jobstartSeconds + self.stopSigStartSeconds) and (
            wallClockSecondsLeft < self.stopSigFinishSeconds + self.wallClockCheckSeconds
        ):
            # Need to send the signal! Assume it works to avoid sending the signal more than once
            self.log.info("Sending signal to JobWrapper children", "(%s)" % self.stopSigNumber)
            self.stopSigSent = True

            kill_proc_tree(self.wrapperPID, includeParent=False)

        return S_OK()

    #############################################################################
    def _performChecks(self):
        """The Watchdog checks are performed at a different period to the checking of the
        application thread and correspond to the checkingTime.
        """
        self.log.verbose("------------------------------------")
        self.log.verbose("Checking loop starts for Watchdog")
        heartBeatDict = {}

        msg = ""

        loadAvg = float(os.getloadavg()[0])
        msg += "LoadAvg: %d " % loadAvg
        heartBeatDict["LoadAverage"] = loadAvg
        if "LoadAverage" not in self.parameters:
            self.parameters["LoadAverage"] = []
        self.parameters["LoadAverage"].append(loadAvg)

        memoryUsed = self.getMemoryUsed()
        msg += "MemUsed: %.1f kb " % (memoryUsed)
        heartBeatDict["MemoryUsed"] = memoryUsed
        if "MemoryUsed" not in self.parameters:
            self.parameters["MemoryUsed"] = []
        self.parameters["MemoryUsed"].append(memoryUsed)

        result = self.profiler.vSizeUsage(withChildren=True)
        if not result["OK"]:
            self.log.warn("Could not get vSize info from profiler", result["Message"])
        else:
            vsize = result["Value"] * 1024.0
            heartBeatDict["Vsize"] = vsize
            self.parameters.setdefault("Vsize", [])
            self.parameters["Vsize"].append(vsize)
            msg += "Job Vsize: %.1f kb " % vsize

        result = self.profiler.memoryUsage(withChildren=True)
        if not result["OK"]:
            self.log.warn("Could not get rss info from profiler", result["Message"])
        else:
            rss = result["Value"] * 1024.0
            heartBeatDict["RSS"] = rss
            self.parameters.setdefault("RSS", [])
            self.parameters["RSS"].append(rss)
            msg += "Job RSS: %.1f kb " % rss

        if "DiskSpace" not in self.parameters:
            self.parameters["DiskSpace"] = []

        # We exclude fuse so that mountpoints can be cleaned up by automount after a period unused
        # (specific request from CERN batch service).
        result = self.getDiskSpace(exclude="fuse")
        if not result["OK"]:
            self.log.warn("Could not establish DiskSpace", result["Message"])
        else:
            msg += "DiskSpace: %.1f MB " % (result["Value"])
            self.parameters["DiskSpace"].append(result["Value"])
            heartBeatDict["AvailableDiskSpace"] = result["Value"]

        cpu = self.__getCPU()
        if not cpu["OK"]:
            msg += "CPU: ERROR "
            hmsCPU = 0
        else:
            cpu = cpu["Value"]
            msg += "CPU: %s (h:m:s) " % (cpu)
            if "CPUConsumed" not in self.parameters:
                self.parameters["CPUConsumed"] = []
            self.parameters["CPUConsumed"].append(cpu)
            hmsCPU = cpu
            rawCPU = self.__convertCPUTime(hmsCPU)
            if rawCPU["OK"]:
                heartBeatDict["CPUConsumed"] = rawCPU["Value"]

        result = self.__getWallClockTime()
        if not result["OK"]:
            self.log.warn("Failed determining wall clock time", result["Message"])
        else:
            msg += "WallClock: %.2f s " % (result["Value"])
            self.parameters.setdefault("WallClockTime", list()).append(result["Value"])
            heartBeatDict["WallClockTime"] = result["Value"] * self.processors
        self.log.info(msg)

        result = self._checkProgress()
        if not result["OK"]:
            self.checkError = result["Message"]
            self.log.warn(self.checkError)

            if self.jobPeekFlag:
                result = self.__peek()
                if result["OK"]:
                    outputList = result["Value"]
                    self.log.info("Last lines of available application output:")
                    self.log.info("================START================")
                    for line in outputList:
                        self.log.info(line)

                    self.log.info("=================END=================")

            self.__killRunningThread()
            return S_OK()

        recentStdOut = "None"
        if self.jobPeekFlag:
            result = self.__peek()
            if result["OK"]:
                outputList = result["Value"]
                size = len(outputList)
                recentStdOut = "Last {} lines of application output from Watchdog on {} [UTC]:".format(
                    size,
                    datetime.datetime.utcnow(),
                )
                border = "=" * len(recentStdOut)
                cpuTotal = "Last reported CPU consumed for job is %s (h:m:s)" % (hmsCPU)
                if self.timeLeft:
                    cpuTotal += ", Batch Queue Time Left %s (s @ HS06)" % self.timeLeft
                recentStdOut = f"\n{border}\n{recentStdOut}\n{cpuTotal}\n{border}\n"
                self.log.info(recentStdOut)
                for line in outputList:
                    self.log.info(line)
                    recentStdOut += line + "\n"
            else:
                recentStdOut = (
                    "Watchdog is initializing and will attempt to obtain standard output from application thread"
                )
                self.log.info(recentStdOut)
                self.peekFailCount += 1
                if self.peekFailCount > self.peekRetry:
                    self.jobPeekFlag = 0
                    self.log.warn("Turning off job peeking for remainder of execution")

        if "JOBID" not in os.environ:
            self.log.info("Running without JOBID so parameters will not be reported")
            return S_OK()

        jobID = os.environ["JOBID"]
        staticParamDict = {"StandardOutput": recentStdOut}
        self.__sendSignOfLife(int(jobID), heartBeatDict, staticParamDict)
        return S_OK("Watchdog checking cycle complete")

    #############################################################################
    def __getCPU(self):
        """Uses the profiler to get CPU time for current process, its child, and the terminated child,
        and returns HH:MM:SS after conversion.
        """
        result = self.profiler.cpuUsageUser(withChildren=True, withTerminatedChildren=True)
        if not result["OK"]:
            self.log.warn("Issue while checking consumed CPU for user", result["Message"])
            if result["Errno"] == errno.ESRCH:
                self.log.warn("The main process does not exist (anymore). This might be correct.")
            return result
        cpuUsageUser = result["Value"]

        result = self.profiler.cpuUsageSystem(withChildren=True, withTerminatedChildren=True)
        if not result["OK"]:
            self.log.warn("Issue while checking consumed CPU for system", result["Message"])
            if result["Errno"] == errno.ESRCH:
                self.log.warn("The main process does not exist (anymore). This might be correct.")
            return result
        cpuUsageSystem = result["Value"]

        cpuTimeTotal = cpuUsageUser + cpuUsageSystem
        if cpuTimeTotal:
            self.log.verbose("Raw CPU time consumed (s) =", cpuTimeTotal)
            return self.__getCPUHMS(cpuTimeTotal)
        self.log.error("CPU time consumed found to be 0")
        return S_ERROR()

    #############################################################################
    def __getCPUHMS(self, cpuTime):
        mins, secs = divmod(cpuTime, 60)
        hours, mins = divmod(mins, 60)
        humanTime = "%02d:%02d:%02d" % (hours, mins, secs)
        self.log.verbose("Human readable CPU time is: %s" % humanTime)
        return S_OK(humanTime)

    #############################################################################
    def __interpretControlSignal(self, signalDict):
        """This method is called whenever a signal is sent via the result of
        sending a sign of life.
        """
        self.log.info("Received control signal")
        if isinstance(signalDict, dict):
            if "Kill" in signalDict:
                self.log.info("Received Kill signal, stopping job via control signal")
                self.checkError = JobMinorStatus.RECEIVED_KILL_SIGNAL
                self.__killRunningThread()
            else:
                self.log.info("The following control signal was sent but not understood by the watchdog:")
                self.log.info(signalDict)
        else:
            self.log.info("Expected dictionary for control signal", "received:\n%s" % (signalDict))

        return S_OK()

    #############################################################################
    def _checkProgress(self):
        """This method calls specific tests to determine whether the job execution
        is proceeding normally.  CS flags can easily be added to add or remove
        tests via central configuration.
        """
        report = ""

        if self.testWallClock:
            result = self.__checkWallClockTime()
            if not result["OK"]:
                self.log.warn(result["Message"])
                return result
            report += "WallClock: OK, "
        else:
            report += "WallClock: NA,"

        if self.testDiskSpace:
            result = self.__checkDiskSpace()
            if not result["OK"]:
                self.log.warn(result["Message"])
                return result
            report += "DiskSpace: OK, "
        else:
            report += "DiskSpace: NA,"

        if self.testLoadAvg:
            result = self.__checkLoadAverage()
            if not result["OK"]:
                self.log.warn(
                    "Check of load average failed, but won't fail because of that", ": %s" % result["Message"]
                )
                report += "LoadAverage: ERROR, "
                return S_OK()
            report += "LoadAverage: OK, "
        else:
            report += "LoadAverage: NA,"

        if self.testCPUConsumed:
            result = self.__checkCPUConsumed()
            if not result["OK"]:
                return result
            report += "CPUConsumed: OK, "
        else:
            report += "CPUConsumed: NA, "

        if self.testCPULimit:
            result = self.__checkCPULimit()
            if not result["OK"]:
                self.log.warn(result["Message"])
                return result
            report += "CPULimit OK, "
        else:
            report += "CPULimit: NA, "

        if self.testTimeLeft:
            self.__timeLeft()
            if self.timeLeft:
                report += "TimeLeft: OK"
        else:
            report += "TimeLeft: NA"

        if self.testMemoryLimit:
            result = self.__checkMemoryLimit()
            if not result["OK"]:
                self.log.warn(result["Message"])
                return result
            report += "MemoryLimit OK, "
        else:
            report += "MemoryLimit: NA, "

        self.log.info(report)
        return S_OK("All enabled checks passed")

    #############################################################################
    def __checkCPUConsumed(self):
        """Checks whether the CPU consumed by application process is reasonable. This
        method will report stalled jobs to be killed.
        """
        self.log.info("Checking CPU Consumed")

        if "WallClockTime" not in self.parameters:
            return S_ERROR("Missing WallClockTime info")
        if "CPUConsumed" not in self.parameters:
            return S_ERROR("Missing CPUConsumed info")

        wallClockTime = self.parameters["WallClockTime"][-1]
        if wallClockTime < self.sampleCPUTime:
            self.log.info(
                "Stopping check, wallclock time is still smaller than sample time",
                f"({wallClockTime}) < ({self.sampleCPUTime})",
            )
            return S_OK()

        intervals = max(1, int(self.sampleCPUTime / self.checkingTime))
        if len(self.parameters["CPUConsumed"]) < intervals + 1:
            self.log.info(
                "Not enough snapshots to calculate",
                "there are {} and we need {}".format(len(self.parameters["CPUConsumed"]), intervals + 1),
            )
            return S_OK()

        wallClockTime = self.parameters["WallClockTime"][-1] - self.parameters["WallClockTime"][-1 - intervals]
        try:
            cpuTime = self.__convertCPUTime(self.parameters["CPUConsumed"][-1])["Value"]
            # For some reason, some times the CPU consumed estimation returns 0
            # if cpuTime == 0:
            #   return S_OK()
            cpuTime -= self.__convertCPUTime(self.parameters["CPUConsumed"][-1 - intervals])["Value"]
            if cpuTime < 0:
                self.log.warn("Consumed CPU time negative, something wrong may have happened, ignore")
                return S_OK()
            if wallClockTime <= 0:
                self.log.warn("Wallclock time should not be negative or zero, Ignore")
                return S_OK()

            ratio = (cpuTime / wallClockTime) * 100

            self.log.info("CPU/Wallclock ratio is %.2f%%" % ratio)
            # in case of error cpuTime might be 0, exclude this
            if ratio < self.minCPUWallClockRatio:
                if (
                    os.path.exists("DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK")
                    or "DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK" in os.environ
                ):
                    self.log.warn(
                        "N.B. job would be declared as stalled but CPU / WallClock check is disabled by payload"
                    )
                    return S_OK()
                self.log.info("Job is stalled!")
                return S_ERROR(JobMinorStatus.WATCHDOG_STALLED)
        except Exception as e:
            self.log.error("Cannot convert CPU consumed from string to int", str(e))

        return S_OK()

    #############################################################################

    def __convertCPUTime(self, cputime):
        """Method to convert the CPU time as returned from the Watchdog
        instances to the equivalent DIRAC normalized CPU time to be compared
        to the Job CPU requirement.
        """
        cpuValue = 0
        cpuHMS = cputime.split(":")
        # for i in range( len( cpuHMS ) ):
        #   cpuHMS[i] = cpuHMS[i].replace( '00', '0' )

        try:
            hours = float(cpuHMS[0]) * 60 * 60
            mins = float(cpuHMS[1]) * 60
            secs = float(cpuHMS[2])
            cpuValue = float(hours + mins + secs)
        except Exception as x:
            self.log.warn(str(x))
            return S_ERROR("Could not calculate CPU time")

        # Normalization to be implemented
        normalizedCPUValue = cpuValue

        result = S_OK()
        result["Value"] = normalizedCPUValue
        self.log.debug(f"CPU value {cputime} converted to {normalizedCPUValue}")
        return result

    #############################################################################

    def __checkCPULimit(self):
        """Checks that the job has consumed more than the job CPU requirement
        (plus a configurable margin) and kills them as necessary.
        """
        consumedCPU = 0
        if "CPUConsumed" in self.parameters:
            consumedCPU = self.parameters["CPUConsumed"][-1]

        consumedCPUDict = self.__convertCPUTime(consumedCPU)
        if consumedCPUDict["OK"]:
            currentCPU = consumedCPUDict["Value"]
        else:
            return S_OK("Not possible to determine current CPU consumed")

        if consumedCPU:
            limit = int(self.jobCPUTime + self.jobCPUTime * (self.jobCPUMargin / 100))
            cpuConsumed = float(currentCPU)
            if cpuConsumed > limit:
                self.log.info(
                    "Job has consumed more than the specified CPU limit",
                    "with an additional %s%% margin" % (self.jobCPUMargin),
                )
                return S_ERROR("Job has exceeded maximum CPU time limit")
            return S_OK("Job within CPU limit")

        if not currentCPU:
            self.log.verbose("Both initial and current CPU consumed are null")
            return S_OK("CPU consumed is not measurable yet")

        return S_OK("Not possible to determine CPU consumed")

    def __checkMemoryLimit(self):
        """Checks that the job memory consumption is within a limit"""
        vsize = 0
        if "Vsize" in self.parameters:
            vsize = self.parameters["Vsize"][-1]

        if vsize and self.memoryLimit:
            if vsize > self.memoryLimit:
                # Just a warning for the moment
                self.log.warn(f"Job has consumed {vsize:f}.2 KB of memory with the limit of {self.memoryLimit:f}.2 KB")

        return S_OK()

    #############################################################################
    def __checkDiskSpace(self):
        """Checks whether the CS defined minimum disk space is available."""
        if "DiskSpace" in self.parameters:
            availSpace = self.parameters["DiskSpace"][-1]
            if availSpace >= 0 and availSpace < self.minDiskSpace:
                self.log.info(
                    "Not enough local disk space for job to continue, defined in CS as %s MB" % (self.minDiskSpace)
                )
                return S_ERROR(JobMinorStatus.JOB_INSUFFICIENT_DISK)
            else:
                return S_OK("Job has enough disk space available")
        else:
            return S_ERROR("Available disk space could not be established")

    #############################################################################
    def __checkWallClockTime(self):
        """Checks whether the job has been running for the CS defined maximum
        wall clock time.
        """
        if "StartTime" in self.initialValues:
            startTime = self.initialValues["StartTime"]
            if time.time() - startTime > self.maxWallClockTime:
                self.log.info("Job has exceeded maximum wall clock time of %s seconds" % (self.maxWallClockTime))
                return S_ERROR(JobMinorStatus.JOB_EXCEEDED_WALL_CLOCK)
            else:
                return S_OK("Job within maximum wall clock time")
        else:
            return S_ERROR("Job start time could not be established")

    #############################################################################
    def __checkLoadAverage(self):
        """Checks whether the CS defined maximum load average is exceeded."""
        if "LoadAverage" in self.parameters:
            loadAvg = self.parameters["LoadAverage"][-1]
            if loadAvg > float(self.loadAvgLimit):
                self.log.info("Maximum load average exceeded, defined in CS as %s " % (self.loadAvgLimit))
                return S_ERROR("Job exceeded maximum load average")
            return S_OK("Job running with normal load average")
        return S_ERROR("Job load average not established")

    #############################################################################
    def __peek(self):
        """Uses ExecutionThread.getOutput() method to obtain standard output
        from running thread via subprocess callback function.
        """
        result = self.exeThread.getOutput()
        if not result["OK"]:
            self.log.warn("Could not obtain output from running application thread")
            self.log.warn(result["Message"])

        return result

    #############################################################################
    def calibrate(self):
        """The calibrate method obtains the initial values for system memory and load
        and calculates the margin for error for the rest of the Watchdog cycle.
        """
        self.__getWallClockTime()
        self.parameters["WallClockTime"] = []

        cpuConsumed = self.__getCPU()
        if not cpuConsumed["OK"]:
            self.log.warn("Could not establish CPU consumed, setting to 0.0")
            cpuConsumed = 0.0
        else:
            cpuConsumed = cpuConsumed["Value"]

        self.initialValues["CPUConsumed"] = cpuConsumed
        self.parameters["CPUConsumed"] = []

        self.initialValues["LoadAverage"] = float(os.getloadavg()[0])
        self.parameters["LoadAverage"] = []

        memUsed = self.getMemoryUsed()

        self.initialValues["MemoryUsed"] = memUsed
        self.parameters["MemoryUsed"] = []

        result = self.profiler.vSizeUsage(withChildren=True)
        if not result["OK"]:
            self.log.warn("Could not get vSize info from profiler", result["Message"])
        else:
            vsize = result["Value"] * 1024.0
            self.initialValues["Vsize"] = vsize
            self.log.verbose("Vsize(kb)", "%.1f" % vsize)
        self.parameters["Vsize"] = []

        result = self.profiler.memoryUsage(withChildren=True)
        if not result["OK"]:
            self.log.warn("Could not get rss info from profiler", result["Message"])
        else:
            rss = result["Value"] * 1024.0
            self.initialValues["RSS"] = rss
            self.log.verbose("RSS(kb)", "%.1f" % rss)
        self.parameters["RSS"] = []

        # We exclude fuse so that mountpoints can be cleaned up by automount after a period unused
        # (specific request from CERN batch service).
        result = self.getDiskSpace(exclude="fuse")
        self.log.verbose("DiskSpace: %s" % (result))
        if not result["OK"]:
            self.log.warn("Could not establish DiskSpace")
        else:
            self.initialValues["DiskSpace"] = result["Value"]
        self.parameters["DiskSpace"] = []

        result = self.getNodeInformation()
        self.log.verbose("NodeInfo", result)

        if "LSB_JOBID" in os.environ:
            result["LocalJobID"] = os.environ["LSB_JOBID"]
        if "PBS_JOBID" in os.environ:
            result["LocalJobID"] = os.environ["PBS_JOBID"]
        if "QSUB_REQNAME" in os.environ:
            result["LocalJobID"] = os.environ["QSUB_REQNAME"]
        if "JOB_ID" in os.environ:
            result["LocalJobID"] = os.environ["JOB_ID"]

        self.__reportParameters(result, "NodeInformation", True)
        self.__reportParameters(self.initialValues, "InitialValues")
        return S_OK()

    def __timeLeft(self):
        """
        return Normalized CPU time left in the batch system
        0 if not available
        update self.timeLeft and self.littleTimeLeft
        """
        # Get CPU time left in the batch system
        result = self.timeLeftUtil.getTimeLeft(0.0)
        if not result["OK"]:
            # Could not get CPU time left, we might need to wait for the first loop
            # or the Utility is not working properly for this batch system
            # or we are in a batch system
            timeLeft = 0
        else:
            timeLeft = result["Value"]

        self.timeLeft = timeLeft
        if not self.littleTimeLeft:
            if timeLeft and timeLeft < self.grossTimeLeftLimit:
                self.log.info("Checking with higher frequency as TimeLeft below grossTimeLeftLimit", f"{timeLeft=}")
                self.littleTimeLeft = True
                # TODO: better configurable way of doing this to be coded
                self.littleTimeLeftCount = 15
        else:
            if self.timeLeft and self.timeLeft < self.fineTimeLeftLimit:
                timeLeft = -1

        return timeLeft

    #############################################################################
    def __getUsageSummary(self):
        """Returns average load, memory etc. over execution of job thread"""
        summary = {}
        # CPUConsumed
        if "CPUConsumed" in self.parameters:
            cpuList = self.parameters["CPUConsumed"]
            if cpuList:
                hmsCPU = cpuList[-1]
                rawCPU = self.__convertCPUTime(hmsCPU)
                if rawCPU["OK"]:
                    summary["LastUpdateCPU(s)"] = rawCPU["Value"]
            else:
                summary["LastUpdateCPU(s)"] = "Could not be estimated"
        # DiskSpace
        if "DiskSpace" in self.parameters:
            space = self.parameters["DiskSpace"]
            if space:
                value = abs(float(space[-1]) - float(self.initialValues["DiskSpace"]))
                if value < 0:
                    value = 0
                summary["DiskSpace(MB)"] = value
            else:
                summary["DiskSpace(MB)"] = "Could not be estimated"
        # MemoryUsed
        if "MemoryUsed" in self.parameters:
            memory = self.parameters["MemoryUsed"]
            if memory:
                summary["MemoryUsed(kb)"] = abs(float(memory[-1]) - float(self.initialValues["MemoryUsed"]))
            else:
                summary["MemoryUsed(kb)"] = "Could not be estimated"
        # LoadAverage
        if "LoadAverage" in self.parameters:
            laList = self.parameters["LoadAverage"]
            if laList:
                summary["LoadAverage"] = sum(laList) / len(laList)
            else:
                summary["LoadAverage"] = "Could not be estimated"

        result = self.__getWallClockTime()
        if not result["OK"]:
            self.log.warn("Failed determining wall clock time", result["Message"])
            summary["WallClockTime(s)"] = 0
            summary["ScaledCPUTime(s)"] = 0
        else:
            wallClock = result["Value"]
            summary["WallClockTime(s)"] = wallClock * self.processors
            summary["ScaledCPUTime(s)"] = wallClock * self.cpuPower * self.processors

        self.__reportParameters(summary, "UsageSummary", True)
        self.currentStats = summary

    #############################################################################
    def __reportParameters(self, params, title=None, report=False):
        """Will report parameters for job."""
        try:
            parameters = []
            self.log.info("", "==========================================================")
            if title:
                self.log.info("Watchdog will report", title)
            else:
                self.log.info("Watchdog will report parameters")
            self.log.info("", "==========================================================")
            vals = params
            if "Value" in params:
                if vals["Value"]:
                    vals = params["Value"]
            for k, v in vals.items():
                if v:
                    self.log.info(str(k) + " = " + str(v))
                    parameters.append([k, v])
            if report:
                self.__setJobParamList(parameters)

            self.log.info("", "==========================================================")
        except Exception as x:
            self.log.warn("Problem while reporting parameters")
            self.log.warn(repr(x))

    #############################################################################
    def __getWallClockTime(self):
        """Establishes the Wall Clock time spent since the Watchdog initialization"""
        result = S_OK()
        if "StartTime" in self.initialValues:
            currentTime = time.time()
            wallClock = currentTime - self.initialValues["StartTime"]
            result["Value"] = wallClock
        else:
            self.initialValues["StartTime"] = time.time()
            result["Value"] = 0.0

        return result

    #############################################################################
    def __killRunningThread(self):
        """Will kill the running thread process and any child processes."""
        self.log.info("Sending kill signal to application PID", self.spObject.getChildPID())
        self.spObject.killChild()
        return S_OK("Thread killed")

    #############################################################################
    def __sendSignOfLife(self, jobID, heartBeatDict, staticParamDict):
        """Sends sign of life 'heartbeat' signal and triggers control signal
        interpretation.
        """
        result = JobStateUpdateClient().sendHeartBeat(jobID, heartBeatDict, staticParamDict)
        if not result["OK"]:
            self.log.warn("Problem sending sign of life")
            self.log.warn(result)

        if result["OK"] and result["Value"]:
            self.__interpretControlSignal(result["Value"])

        return result

    #############################################################################
    def __setJobParamList(self, value):
        """Wraps around setJobParameters of state update client"""
        # job wrapper template sets the jobID variable
        if "JOBID" not in os.environ:
            self.log.info("Running without JOBID so parameters will not be reported")
            return S_OK()
        jobID = os.environ["JOBID"]
        jobParam = JobStateUpdateClient().setJobParameters(int(jobID), value)
        self.log.verbose(f"setJobParameters({jobID},{value})")
        if not jobParam["OK"]:
            self.log.warn(jobParam["Message"])

        return jobParam

    #############################################################################
    def getNodeInformation(self):
        """Retrieves all static system information"""
        result = {}
        result["HostName"] = socket.gethostname()
        result["Memory(kB)"] = int(psutil.virtual_memory()[1] / 1024)
        result["LocalAccount"] = getpass.getuser()

        path = Path("/proc/cpuinfo")
        if path.is_file():
            # Assume all of the CPUs are the same and only consider the first one
            raw_info = path.read_text().split("\n\n")[0]
            info = dict(map(str.strip, x.split(":", 1)) for x in raw_info.split("\n"))
            result["ModelName"] = info.get("model name", "Unknown")

        return result

    #############################################################################
    def getMemoryUsed(self):
        """Obtains the memory used."""
        mem = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        )
        return float(mem)

    #############################################################################
    def getDiskSpace(self, exclude=None):
        """Obtains the available disk space."""
        result = S_OK()
        diskSpace = getDiskSpace(exclude=exclude)

        if diskSpace == -1:
            result = S_ERROR("Could not obtain disk usage")
            self.log.warn(" Could not obtain disk usage")
            result["Value"] = float(-1)
            return result

        result["Value"] = float(diskSpace)
        return result


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
