""" The TimeLeft utility allows to calculate the amount of CPU time
    left for a given batch system slot.  This is essential for the 'Filling
    Mode' where several VO jobs may be executed in the same allocated slot.

    The prerequisites for the utility to run are:
      - Plugin for extracting information from local batch system
      - Scale factor for the local site.

    With this information the utility can calculate in normalized units the
    CPU time remaining for a given slot.
"""
import shlex

import DIRAC

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.Subprocess import systemCall


class TimeLeft:
    """This generally does not run alone"""

    def __init__(self):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.cpuPower = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
        if not self.cpuPower:
            self.log.warn(f"/LocalSite/CPUNormalizationFactor not defined for site {DIRAC.siteName()}")

        result = self._getBatchSystemPlugin()
        if result["OK"]:
            self.batchPlugin = result["Value"]
        else:
            self.batchPlugin = None
            self.batchError = result["Message"]

    def getScaledCPU(self, processors=1):
        """Returns the current CPU Time spend (according to batch system) scaled according
        to /LocalSite/CPUNormalizationFactor
        """
        # Quit if no scale factor available
        if not self.cpuPower:
            return 0

        # Quit if Plugin is not available
        if not self.batchPlugin:
            return 0

        resourceDict = self.batchPlugin.getResourceUsage()

        if "Value" in resourceDict:
            if resourceDict["Value"].get("CPU"):
                return resourceDict["Value"]["CPU"] * self.cpuPower
            if resourceDict["Value"].get("WallClock"):
                # When CPU value missing, guess from WallClock and number of processors
                return resourceDict["Value"]["WallClock"] * self.cpuPower * processors

        return 0

    def getTimeLeft(self, cpuConsumed=0.0, processors=1):
        """Returns the CPU Time Left for supported batch systems.
        The CPUConsumed is the current raw total CPU.
        """
        # Quit if no norm factor available
        if not self.cpuPower:
            return S_ERROR(
                DErrno.ESECTION, f"/LocalSite/CPUNormalizationFactor not defined for site {DIRAC.siteName()}"
            )

        if not self.batchPlugin:
            return S_ERROR(self.batchError)

        resourceDict = self.batchPlugin.getResourceUsage()
        if not resourceDict["OK"]:
            self.log.warn(f"Could not determine timeleft for batch system at site {DIRAC.siteName()}")
            return resourceDict

        resources = resourceDict["Value"]
        self.log.debug(f"self.batchPlugin.getResourceUsage(): {str(resources)}")
        if not resources.get("CPULimit") and not resources.get("WallClockLimit"):
            # This should never happen
            return S_ERROR("No CPU or WallClock limit obtained")

        # if one of CPULimit or WallClockLimit is missing, compute a reasonable value
        if not resources.get("CPULimit"):
            resources["CPULimit"] = resources["WallClockLimit"] * processors
        elif not resources.get("WallClockLimit"):
            resources["WallClockLimit"] = resources["CPULimit"] / processors

        # if one of CPU or WallClock is missing, compute a reasonable value
        if not resources.get("CPU"):
            resources["CPU"] = resources["WallClock"] * processors
        elif not resources.get("WallClock"):
            resources["WallClock"] = resources["CPU"] / processors

        cpu = float(resources["CPU"])
        cpuLimit = float(resources["CPULimit"])
        wallClock = float(resources["WallClock"])
        wallClockLimit = float(resources["WallClockLimit"])
        batchSystemTimeUnit = resources.get("Unit", "Both")

        # Some batch systems rely on wall clock time and/or cpu time to make allocations
        if batchSystemTimeUnit == "WallClock":
            time = wallClock
            timeLimit = wallClockLimit
        else:
            time = cpu
            timeLimit = cpuLimit

        if time and cpuConsumed > 3600.0 and self.cpuPower:
            # If there has been more than 1 hour of consumed CPU and
            # there is a Normalization set for the current CPU
            # use that value to renormalize the values returned by the batch system
            # NOTE: cpuConsumed is non-zero for call by the JobAgent and 0 for call by the watchdog
            # cpuLimit and cpu may be in the units of the batch system, not real seconds...
            # (in this case the other case won't work)
            # therefore renormalise it using cpuConsumed (which is in real seconds)
            cpuWorkLeft = (timeLimit - time) * self.cpuPower * cpuConsumed / time
        else:
            # FIXME: this is always used by the watchdog... Also used by the JobAgent
            #        if consumed less than 1 hour of CPU
            # It was using self.scaleFactor but this is inconsistent: use the same as above
            # In case the returned cpu and cpuLimit are not in real seconds, this is however rubbish
            cpuWorkLeft = (timeLimit - time) * self.cpuPower

        self.log.verbose(f"Remaining CPU in normalized units is: {cpuWorkLeft:.2f}")
        return S_OK(cpuWorkLeft)

    def _getBatchSystemPlugin(self):
        """Using the name of the batch system plugin, will return an instance of the plugin class."""
        result = gConfig.getOptionsDictRecursively("/LocalSite/BatchSystemInfo")
        if not result["OK"]:
            self.log.warn(f"Batch system information not available")
            return result

        batchSystemInfo = result["Value"]
        type = batchSystemInfo.get("Type")
        jobID = batchSystemInfo.get("JobID")
        parameters = batchSystemInfo.get("Parameters")

        if not type or type == "Unknown":
            self.log.warn(f"Batch system type for site {DIRAC.siteName()} is not currently supported")
            return S_ERROR(DErrno.ERESUNK, "Current batch system is not supported")

        self.log.debug(f"Creating plugin for {type} batch system")
        result = ObjectLoader().loadObject(f"DIRAC.Resources.Computing.BatchSystems.TimeLeft.{type}ResourceUsage")
        if not result["OK"]:
            return result
        batchClass = result["Value"]
        batchInstance = batchClass(jobID, parameters)

        return S_OK(batchInstance)


#############################################################################


def runCommand(cmd, timeout=120):
    """Wrapper around systemCall to return S_OK(stdout) or S_ERROR(message)"""
    result = systemCall(timeout=timeout, cmdSeq=shlex.split(cmd))
    if not result["OK"]:
        return result
    status, stdout, stderr = result["Value"][0:3]

    if status:
        gLogger.warn(f"Status {status} while executing {cmd}")
        gLogger.warn(stderr)
        if stdout:
            return S_ERROR(stdout)
        if stderr:
            return S_ERROR(stderr)
        return S_ERROR(f"Status {status} while executing {cmd}")

    return S_OK(str(stdout))
