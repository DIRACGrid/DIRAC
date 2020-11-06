"""
Resource Usage
"""
import os

from DIRAC import gLogger


class ResourceUsage(object):
  """ Resource Usage is an abstract class that has to be implemented for every batch system used by DIRAC
      to get the resource usage of a given job. This information can then be processed by other modules
      (e.g. getting the time left in a Pilot)
  """

  def __init__(self, batchSystemName, jobIdEnvVar):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('%sResourceUsage' % batchSystemName)
    self.jobID = os.environ.get(jobIdEnvVar)

  def getResourceUsage(self):
    """ Returns a dictionary that can contain CPUConsumed, CPULimit, WallClockConsumed
        and WallClockLimit for current slot.  All values returned in seconds.
        The dictionary can also contain Unit indicating whether the Batch System allocates
        resources with limited CPU time and or Wallclock time.

        :return: dict such as {cpuConsumed, cpuLimit, wallClockConsumed, wallClockLimit, unit}
    """
    raise NotImplementedError("getResourceUsage not implemented")
