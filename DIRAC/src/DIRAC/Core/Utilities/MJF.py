""" The MJF utility calculates the amount of wall clock time
    left for a given batch system slot or VM. This is essential for the
    'Filling Mode' where several jobs may be executed in the same slot.

    Machine Job/Features are used following HSF-TN-2016-02 if available.
    Otherwise values are filled in using the batch system and CS
    information.
"""
import os
import ssl
import time
from urllib.request import urlopen

import DIRAC

from DIRAC import gLogger, gConfig


class MJF:
    """Machine/Job Features methods"""

    mjfKeys = {
        "MACHINEFEATURES": ["total_cpu", "hs06", "shutdowntime", "grace_secs"],
        "JOBFEATURES": [
            "allocated_cpu",
            "hs06_job",
            "shutdowntime_job",
            "grace_secs_job",
            "jobstart_secs",
            "job_id",
            "wall_limit_secs",
            "cpu_limit_secs",
            "max_rss_bytes",
            "max_swap_bytes",
            "scratch_limit_bytes",
        ],
    }

    #############################################################################
    def __init__(self):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)

        capath = DIRAC.Core.Security.Locations.getCAsLocation()

        if not capath:
            raise Exception("Unable to find CA files location! Not in /etc/grid-security/certificates/ etc.")

        # Used by urllib when talking to HTTPS web servers
        self.context = ssl.create_default_context(capath=capath)

    def updateConfig(self, pilotStartTime=None):
        """Populate /LocalSite/MACHINEFEATURES and /LocalSite/JOBFEATURES with MJF values
        This is run early in the job to update the configuration file that subsequent DIRAC
        scripts read when they start.
        """

        if pilotStartTime:
            gConfig.setOptionValue("/LocalSite/JOBFEATURES/jobstart_secs", str(pilotStartTime))

        for mORj in ["MACHINEFEATURES", "JOBFEATURES"]:
            for key in self.mjfKeys[mORj]:
                value = self.__fetchMachineJobFeature(mORj, key)

                if value is not None:
                    gConfig.setOptionValue(f"/LocalSite/{mORj}/{key}", value)

    def getMachineFeature(self, key):
        """Returns MACHINEFEATURES/key value saved in /LocalSite configuration by
        updateConfigFile() unless MACHINEFEATURES/shutdowntime when we try to fetch
        from the source URL itself again in case it changes.
        """
        if key == "shutdowntime":
            value = self.__fetchMachineJobFeature("MACHINEFEATURES", "shutdowntime")
            # If unable to fetch shutdowntime, go back to any value in /LocalSite
            # in case HTTP(S) server is down
            if value is not None:
                return value

        return gConfig.getValue("/LocalSite/MACHINEFEATURES/" + key, None)

    def getIntMachineFeature(self, key):
        """Returns MACHINEFEATURES/key as an int or None if not an int or not present"""
        value = self.getMachineFeature(key)

        try:
            return int(value)
        except ValueError:
            return None

    def getJobFeature(self, key):
        """Returns JOBFEATURES/key value saved in /LocalSite configuration by
        updateConfigFile() unless JOBFEATURES/shutdowntime_job when we try to fetch
        from the source URL itself again in case it changes.
        """
        if key == "shutdowntime_job":
            value = self.__fetchMachineJobFeature("JOBFEATURES", "shutdowntime_job")
            # If unable to fetch shutdowntime_job, go back to any value in /LocalSite
            # in case HTTP(S) server is down
            if value is not None:
                return value

        return gConfig.getValue("/LocalSite/JOBFEATURES/" + key, None)

    def getIntJobFeature(self, key):
        """Returns JOBFEATURES/key as an int or None if not an int or not present"""
        value = self.getJobFeature(key)

        try:
            return int(value)
        except ValueError:
            return None

    def __fetchMachineJobFeature(self, mORj, key):
        """Returns raw MJF value for a given key, perhaps by HTTP(S), perhaps from a local file
        mORj must be MACHINEFEATURES or JOBFEATURES
        If the value cannot be found, then return None. There are many legitimate ways for
        a site not to provide some MJF values so we don't log errors, failures etc.
        """
        if mORj != "MACHINEFEATURES" and mORj != "JOBFEATURES":
            raise Exception("Must request MACHINEFEATURES or JOBFEATURES")

        if mORj not in os.environ:
            return None

        url = os.environ[mORj] + "/" + key

        # Simple if a file
        if url[0] == "/":
            try:
                with open(url) as fd:
                    return fd.read().strip()
            except Exception:
                return None

        # Otherwise make sure it's an HTTP(S) URL
        if not url.startswith("http://") and not url.startswith("https://"):
            return None

        # We could have used urlopen() for local files too, but we also
        # need to check HTTP return code in case we get an HTML error page
        # instead of a true key value.
        try:
            mjfUrl = urlopen(url=url, context=self.context)
            # HTTP return codes other than 2xx mean failure
            if int(mjfUrl.getcode() / 100) != 2:
                return None
            return mjfUrl.read().strip()
        except Exception:
            return None
        finally:
            try:
                mjfUrl.close()
            except UnboundLocalError:
                pass

    def getWallClockSecondsLeft(self):
        """Returns the number of seconds until either the wall clock limit
        or the shutdowntime(_job) is reached.
        """

        now = int(time.time())
        secondsLeft = None
        jobstartSecs = self.getIntJobFeature("jobstart_secs")
        wallLimitSecs = self.getIntJobFeature("wall_limit_secs")
        shutdowntimeJob = self.getIntJobFeature("shutdowntime_job")
        shutdowntime = self.getIntMachineFeature("shutdowntime")

        # look for local shutdown file
        try:
            with open("/var/run/shutdown_time") as fd:
                shutdowntimeLocal = int(fd.read().strip())
        except (OSError, ValueError):
            shutdowntimeLocal = None

        if jobstartSecs is not None and wallLimitSecs is not None:
            secondsLeft = jobstartSecs + wallLimitSecs - now

        if shutdowntimeJob is not None:
            if secondsLeft is None:
                secondsLeft = shutdowntimeJob - now
            elif shutdowntimeJob - now < secondsLeft:
                secondsLeft = shutdowntimeJob - now

        if shutdowntime is not None:
            if secondsLeft is None:
                secondsLeft = shutdowntime - now
            elif shutdowntime - now < secondsLeft:
                secondsLeft = shutdowntime - now

        if shutdowntimeLocal is not None:
            if secondsLeft is None:
                secondsLeft = shutdowntimeLocal - now
            elif shutdowntimeLocal - now < secondsLeft:
                secondsLeft = shutdowntimeLocal - now

        # Wall Clock time left or None if unknown
        return secondsLeft
