""" The Machine/Job Features TimeLeft utility interrogates the MJF values
    for the current CPU and Wallclock consumed, as well as their limits.
"""
import os
import time
from urllib.request import urlopen

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class MJFResourceUsage(ResourceUsage):
    """
    This is the MJF plugin of the TimeLeft Utility
    """

    #############################################################################
    def __init__(self):
        """Standard constructor"""
        super().__init__("MJF", "JOB_ID")

        self.queue = os.environ.get("QUEUE")

        self.log.verbose(f"jobID={self.jobID}, queue={self.queue}")
        self.startTime = time.time()

    #############################################################################
    def getResourceUsage(self):
        """Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
        """

        cpuLimit = None
        wallClockLimit = None
        wallClock = None
        jobStartSecs = None

        jobFeaturesPath = None
        machineFeaturesPath = None

        # Getting info from JOBFEATURES
        try:
            # We are not called from TimeLeft.py if these are not set
            jobFeaturesPath = os.environ["JOBFEATURES"]
        except KeyError:
            self.log.warn("$JOBFEATURES is not set")

        if jobFeaturesPath:
            try:
                wallClockLimit = int(urlopen(jobFeaturesPath + "/wall_limit_secs").read())
                self.log.verbose("wallClockLimit from JF = %d" % wallClockLimit)
            except ValueError:
                self.log.warn("/wall_limit_secs is unreadable")
            except OSError as e:
                self.log.exception("Issue with $JOBFEATURES/wall_limit_secs", lException=e)
                self.log.warn("Could not determine cpu limit from $JOBFEATURES/wall_limit_secs")

            try:
                jobStartSecs = int(urlopen(jobFeaturesPath + "/jobstart_secs").read())
                self.log.verbose("jobStartSecs from JF = %d" % jobStartSecs)
            except ValueError:
                self.log.warn("/jobstart_secs is unreadable, setting a default")
                jobStartSecs = self.startTime
            except OSError as e:
                self.log.exception("Issue with $JOBFEATURES/jobstart_secs", lException=e)
                self.log.warn("Can't open jobstart_secs, setting a default")
                jobStartSecs = self.startTime

            try:
                cpuLimit = int(urlopen(jobFeaturesPath + "/cpu_limit_secs").read())
                self.log.verbose("cpuLimit from JF = %d" % cpuLimit)
            except ValueError:
                self.log.warn("/cpu_limit_secs is unreadable")
            except OSError as e:
                self.log.exception("Issue with $JOBFEATURES/cpu_limit_secs", lException=e)
                self.log.warn("Could not determine cpu limit from $JOBFEATURES/cpu_limit_secs")

            wallClock = int(time.time()) - jobStartSecs

        # Getting info from MACHINEFEATURES
        try:
            # We are not called from TimeLeft.py if these are not set
            machineFeaturesPath = os.environ["MACHINEFEATURES"]
        except KeyError:
            self.log.warn("$MACHINEFEATURES is not set")

        if machineFeaturesPath and jobStartSecs:
            try:
                shutdownTime = int(urlopen(machineFeaturesPath + "/shutdowntime").read())
                self.log.verbose("shutdownTime from MF = %d" % shutdownTime)
                if int(time.time()) + wallClockLimit > shutdownTime:
                    # reduce wallClockLimit if would overrun shutdownTime
                    wallClockLimit = shutdownTime - jobStartSecs
            except ValueError:
                self.log.warn("/shutdowntime is unreadable")
            except OSError as e:
                self.log.warn("Issue with $MACHINEFEATURES/shutdowntime", repr(e))
                self.log.warn("Could not determine a shutdowntime value from $MACHINEFEATURES/shutdowntime")

        # Reporting
        consumed = {"CPU": None, "CPULimit": cpuLimit, "WallClock": wallClock, "WallClockLimit": wallClockLimit}
        if cpuLimit and wallClock and wallClockLimit:
            self.log.verbose("MJF consumed: %s" % str(consumed))
            return S_OK(consumed)
        self.log.info("Could not determine some parameters")
        retVal = S_ERROR("Could not determine some parameters")
        retVal["Value"] = consumed
        return retVal
