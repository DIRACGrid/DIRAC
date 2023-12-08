""" The PBS TimeLeft utility interrogates the PBS batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""
import os
import re
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class PBSResourceUsage(ResourceUsage):
    """
    This is the PBS plugin of the TimeLeft Utility
    """

    def __init__(self, jobID, parameters):
        """Standard constructor"""
        super().__init__("PBS", jobID, parameters)

        if self.binary_path and self.binary_path != "Unknown":
            os.environ["PATH"] += ":" + self.binary_path

        self.log.verbose(f"PBS_JOBID={self.jobID}, PBS_O_QUEUE={self.queue}")
        self.startTime = time.time()

    #############################################################################
    def getResourceUsage(self):
        """Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
        """
        cmd = f"qstat -f {self.jobID}"
        result = runCommand(cmd)
        if not result["OK"]:
            return result

        cpu = None
        cpuLimit = None
        wallClock = None
        wallClockLimit = None

        lines = str(result["Value"]).split("\n")
        for line in lines:
            info = line.split()
            if re.search(".*resources_used.cput.*", line):
                if len(info) >= 3:
                    cpuList = info[2].split(":")
                    newcpu = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
                    if not cpu or newcpu > cpu:
                        cpu = newcpu
                else:
                    self.log.warn(f'Problem parsing "{line}" for CPU consumed')
            if re.search(".*resources_used.pcput.*", line):
                if len(info) >= 3:
                    cpuList = info[2].split(":")
                    newcpu = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
                    if not cpu or newcpu > cpu:
                        cpu = newcpu
                else:
                    self.log.warn(f'Problem parsing "{line}" for CPU consumed')
            if re.search(".*resources_used.walltime.*", line):
                if len(info) >= 3:
                    wcList = info[2].split(":")
                    wallClock = (float(wcList[0]) * 60 + float(wcList[1])) * 60 + float(wcList[2])
                else:
                    self.log.warn(f'Problem parsing "{line}" for elapsed wall clock time')
            if re.search(".*Resource_List.cput.*", line):
                if len(info) >= 3:
                    cpuList = info[2].split(":")
                    newcpuLimit = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
                    if not cpuLimit or newcpuLimit < cpuLimit:
                        cpuLimit = newcpuLimit
                else:
                    self.log.warn(f'Problem parsing "{line}" for CPU limit')
            if re.search(".*Resource_List.pcput.*", line):
                if len(info) >= 3:
                    cpuList = info[2].split(":")
                    newcpuLimit = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
                    if not cpuLimit or newcpuLimit < cpuLimit:
                        cpuLimit = newcpuLimit
                else:
                    self.log.warn(f'Problem parsing "{line}" for CPU limit')
            if re.search(".*Resource_List.walltime.*", line):
                if len(info) >= 3:
                    wcList = info[2].split(":")
                    wallClockLimit = (float(wcList[0]) * 60 + float(wcList[1])) * 60 + float(wcList[2])
                else:
                    self.log.warn(f'Problem parsing "{line}" for wall clock limit')

        consumed = {"CPU": cpu, "CPULimit": cpuLimit, "WallClock": wallClock, "WallClockLimit": wallClockLimit}
        self.log.debug(consumed)

        if None not in consumed.values():
            self.log.debug("TimeLeft counters complete:", str(consumed))
            return S_OK(consumed)
        missed = [key for key, val in consumed.items() if val is None]
        self.log.info("Could not determine parameter", ",".join(missed))
        self.log.info(f"This is the stdout from the batch system call\n{result['Value']}")

        if cpuLimit or wallClockLimit:
            # We have got a partial result from PBS, assume that we ran for too short time
            if not cpuLimit:
                consumed["CPULimit"] = wallClockLimit * 0.8
            if not wallClockLimit:
                consumed["WallClockLimit"] = cpuLimit / 0.8
            if not cpu:
                consumed["CPU"] = int(time.time() - self.startTime)
            if not wallClock:
                consumed["WallClock"] = int(time.time() - self.startTime)
            self.log.verbose("TimeLeft counters restored:", str(consumed))
            return S_OK(consumed)
        msg = "Could not determine some parameters"
        self.log.info(msg, f":\nThis is the stdout from the batch system call\n{result['Value']}")
        retVal = S_ERROR(msg)
        retVal["Value"] = consumed
        return retVal
