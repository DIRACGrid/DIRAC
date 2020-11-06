""" The HTCondor TimeLeft utility interrogates the HTCondor batch system for the
    current CPU consumed, as well as its limit.
"""

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class HTCondorResourceUsage(ResourceUsage):
  """
   This is the HTCondor plugin of the TimeLeft Utility.
   HTCondor does not provide any way to get the wallclock/cpu limit, the batch system just provides fair-sharing to
   users and groups: the limit depends on many parameters.
   However, some Sites have introduced a MaxRuntime variable that sets a wallclock time limit to the allocations and
   allow us to get an estimation of the resources usage.
  """

  def __init__(self):
    """ Standard constructor
    """
    super(HTCondorResourceUsage, self).__init__('HTCondor', '_CONDOR_JOB_AD')

  def getResourceUsage(self):
    """ Returns a dictionary containing WallClockConsumed and WallClockLimit for current slot.
        All values returned in seconds.
    """
    # $_CONDOR_JOB_AD corresponds to the path to the .job.ad file
    # It contains info about the job:
    # - MaxRuntime: wallclock time allocated to the job - not officially supported by HTCondor,
    #   only present on some Sites
    # - CurrentTime: current time
    # - JobCurrentStartDate: start of the job execution
    cmd = 'condor_status -ads $_CONDOR_JOB_AD -af MaxRuntime CurrentTime-JobCurrentStartDate'
    result = runCommand(cmd)
    if not result['OK']:
      return S_ERROR('Current batch system is not supported')

    output = str(result['Value']).split(' ')
    if len(output) != 2:
      self.log.warn("Cannot open $_CONDOR_JOB_AD: output probably empty")
      return S_ERROR('Current batch system is not supported')

    wallClockLimit = output[0]
    wallClock = output[1]
    if wallClockLimit == 'undefined':
      self.log.warn("MaxRuntime attribute is not supported")
      return S_ERROR('Current batch system is not supported')

    wallClockLimit = float(wallClockLimit)
    wallClock = float(wallClock)

    consumed = {'WallClock': wallClock,
                'WallClockLimit': wallClockLimit,
                'Unit': 'WallClock'}

    if None not in consumed.values():
      self.log.debug("TimeLeft counters complete:", str(consumed))
      return S_OK(consumed)
    else:
      missed = [key for key, val in consumed.items() if val is None]
      msg = 'Could not determine parameter'
      self.log.warn('Could not determine parameter', ','.join(missed))
      self.log.debug('This is the stdout from the batch system call\n%s' % (result['Value']))
      return S_ERROR(msg)
