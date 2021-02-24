""" The SLURM TimeLeft utility interrogates the SLURM batch system for the
    current CPU consumed, as well as its limit.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class SLURMResourceUsage(ResourceUsage):
  """
   This is the SLURM plugin of the TimeLeft Utility
  """

  def __init__(self):
    """ Standard constructor
    """
    super(SLURMResourceUsage, self).__init__('SLURM', 'SLURM_JOB_ID')

    self.log.verbose('JOB_ID=%s' % self.jobID)

  def getResourceUsage(self):
    """ Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
    """
    # sacct displays accounting data for all jobs and job steps
    # -j is the given job, -o the information of interest, -X to get rid of intermediate steps
    # -n to remove the header, -P to make the output parseable (remove tabs, spaces, columns)
    # --delimiter to specify character that splits the fields
    cmd = 'sacct -j %s -o JobID,CPUTimeRAW,AllocCPUS,ElapsedRaw,Timelimit -X -n -P --delimiter=,' % (self.jobID)
    result = runCommand(cmd)
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    output = str(result['Value']).split(',')
    if len(output) == 5:
      _, cpu, allocCPUs, wallClock, wallClockLimitFormatted = output
      # Timelimit is in a specific format and have to be converted in seconds
      # TimelimitRaw is in seconds but only available from Slurm 18.08...
      wallClockLimit = self._getFormattedTimeInSeconds(wallClockLimitFormatted)
      wallClock = float(wallClock)
      if wallClockLimit:
        cpuLimit = wallClockLimit * int(allocCPUs)
      cpu = float(cpu)

    # Slurm allocations are based on wallclock time, not cpu time.
    # We precise it in the 'Unit' field
    consumed = {'CPU': cpu,
                'CPULimit': cpuLimit,
                'WallClock': wallClock,
                'WallClockLimit': wallClockLimit,
                'Unit': 'WallClock'}

    if None in consumed.values():
      missed = [key for key, val in consumed.items() if val is None]
      msg = 'Could not determine parameter'
      self.log.warn('Could not determine parameter', ','.join(missed))
      self.log.debug('This is the stdout from the batch system call\n%s' % (result['Value']))
      return S_ERROR(msg)

    self.log.debug("TimeLeft counters complete:", str(consumed))
    return S_OK(consumed)

  def _getFormattedTimeInSeconds(self, slurmTime):
    """ Convert SLURM time format into seconds

    According to the SLURM documentation, the format can be:
    - MM:SS
    - HH:MM:SS
    - DD-HH:MM:SS

    :param str slurmTime: time in SLURM format
    """
    slurmTimeList = slurmTime.split('-')
    try:
      # If slurmTime limit does not contain days
      if len(slurmTimeList) == 1:
        day = 0
        timeLeft = slurmTimeList[0]
      # Else
      elif len(slurmTimeList) == 2:
        day, timeLeft = slurmTimeList
      else:
        self.log.warn('Problem parsing "%s"' % slurmTime)
        return None

      timeLeftList = timeLeft.split(':')
      if len(timeLeftList) == 2:
        hours = 0
        minutes, seconds = timeLeftList
      elif len(timeLeftList) == 3:
        hours, minutes, seconds = timeLeftList
      else:
        self.log.warn('Problem parsing "%s"' % slurmTime)
        return None

      return ((int(day) * 24 + int(hours)) * 60 + int(minutes)) * 60 + float(seconds)
    except ValueError:
      self.log.warn('Problem parsing "%s"' % slurmTime)
      return None
