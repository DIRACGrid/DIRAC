""" The HTCondor TimeLeft utility interrogates the HTCondor batch system for the
    current CPU consumed, as well as its limit.
"""
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class HTCondorResourceUsage(ResourceUsage):
    """
    This is the HTCondor plugin of the TimeLeft Utility.
    HTCondor does not provide any way to get the wallclock/cpu limit, the batch system just provides fair-sharing to
    users and groups: the limit depends on many parameters.
    However, CERN has introduced a MaxRuntime variable that sets a wallclock time limit to the allocations and
    allow us to get an estimation of the resources usage.
    """

    def __init__(self, jobID, parameters):
        """Standard constructor"""
        super().__init__("HTCondor", jobID, parameters)

    def getResourceUsage(self):
        """Returns S_OK with a dictionary containing the entries WallClock, WallClockLimit, and Unit for current slot."""
        # $_CONDOR_JOB_AD corresponds to the path to the .job.ad file
        # It contains info about the job:
        # - MaxRuntime: wallclock time allocated to the job - not officially supported by HTCondor,
        #   only present at CERN
        # - CurrentTime: current time
        # - JobCurrentStartDate: start of the job execution
        cmd = f"condor_status -ads {self.info_path} -af MaxRuntime CurrentTime-JobCurrentStartDate"
        # SKIP_LOCAL_CONFIG_FILE prevents condor_status from loading the local config file, which
        # would otherwise trigger authentication errors at CERN. runCommand() executes without a
        # shell (systemCall with shell=False), so the variable has to be passed via the environment
        # rather than as a "VAR=value cmd" prefix. Merge with os.environ so PATH/CONDOR_CONFIG/etc.
        # are preserved (subprocess replaces, not augments, the environment when env is given).
        result = runCommand(cmd, env={**os.environ, "SKIP_LOCAL_CONFIG_FILE": "true"})
        if not result["OK"]:
            return S_ERROR("Current batch system is not supported")

        output = str(result["Value"]).split(" ")
        if len(output) != 2:
            self.log.warn("Cannot open $_CONDOR_JOB_AD: output probably empty")
            return S_ERROR("Current batch system is not supported")

        wallClockLimit = output[0]
        wallClock = output[1]
        if wallClockLimit == "undefined":
            self.log.warn("MaxRuntime attribute is not supported")
            return S_ERROR("Current batch system is not supported")

        wallClockLimit = float(wallClockLimit)
        wallClock = float(wallClock)

        consumed = {"WallClock": wallClock, "WallClockLimit": wallClockLimit, "Unit": "WallClock"}

        self.log.debug("TimeLeft counters complete:", str(consumed))
        return S_OK(consumed)
