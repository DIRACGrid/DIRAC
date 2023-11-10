"""
Resource Usage
"""
import os

from DIRAC import gLogger


class ResourceUsage:
    """Resource Usage is an abstract class that has to be implemented for every batch system used by DIRAC
    to get the resource usage of a given job. This information can then be processed by other modules
    (e.g. getting the time left in a Pilot)
    """

    def __init__(self, batchSystemName, jobID, parameters):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(f"{batchSystemName}ResourceUsage")
        self.jobID = jobID

        # Parameters
        self.binary_path = parameters.get("BinaryPath")
        self.info_path = parameters.get("InfoPath")
        self.host = parameters.get("Host")
        self.queue = parameters.get("Queue")

    def getResourceUsage(self):
        """Returns S_OK with a dictionary that can contain entries:

        - CPU: the CPU time consumed since the beginning of the execution for current slot (seconds)
        - CPULimit: the CPU time limit for current slot (seconds)
        - WallClock: the wall clock time consumed since the beginning of the execution for current slot (seconds)
        - WallClockLimit: the wall clock time limit for current slot (seconds)
        - Unit: indicates whether the Batch System allocates resources with limited CPU time and/or wallclock time
          Unit can take the following values: 'CPU', 'WallClock' or 'Both'.

        :return: dict such as {CPU, CPULimit, WallClock, WallClockLimit, Unit}
        """
        raise NotImplementedError("getResourceUsage not implemented")
