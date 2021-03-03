""" The SGE TimeLeft utility interrogates the SGE batch system for the
    current CPU consumed, as well as its limit.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import re
import time
import socket

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


class SGEResourceUsage(ResourceUsage):
  """
   This is the SGE plugin of the TimeLeft Utility
  """

  def __init__(self):
    """ Standard constructor
    """
    super(SGEResourceUsage, self).__init__('SGE', 'JOB_ID')

    self.queue = os.environ.get('QUEUE')
    sgePath = os.environ.get('SGE_BINARY_PATH')
    if sgePath:
      os.environ['PATH'] += ':' + sgePath

    self.log.verbose('JOB_ID=%s, QUEUE=%s' % (self.jobID, self.queue))
    self.startTime = time.time()

  def getResourceUsage(self):
    """ Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
    """
    cmd = 'qstat -f -j %s' % (self.jobID)
    result = runCommand(cmd)
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    lines = str(result['Value']).split('\n')
    for line in lines:
      if re.search('usage.*cpu.*', line):
        match = re.search(r'cpu=([\d,:]*),', line)
        if match:
          cpuList = match.groups()[0].split(':')
        try:
          newcpu = 0.
          if len(cpuList) == 3:
            newcpu = float(cpuList[0]) * 3600 + \
                float(cpuList[1]) * 60 + \
                float(cpuList[2])
          elif len(cpuList) == 4:
            newcpu = float(cpuList[0]) * 24 * 3600 + \
                float(cpuList[1]) * 3600 + \
                float(cpuList[2]) * 60 + \
                float(cpuList[3])
          if not cpu or newcpu > cpu:
            cpu = newcpu
        except ValueError:
          self.log.warn('Problem parsing "%s" for CPU consumed' % line)
      if re.search('hard resource_list.*cpu.*', line):
        match = re.search(r'_cpu=(\d*)', line)
        if match:
          cpuLimit = float(match.groups()[0])
        match = re.search(r'_rt=(\d*)', line)
        if match:
          wallClockLimit = float(match.groups()[0])
      else:
        self.log.warn("No hard limits found")

    # Some SGE batch systems apply CPU scaling factor to the CPU consumption figures
    if cpu:
      factor = _getCPUScalingFactor()
      if factor:
        cpu = cpu / factor

    consumed = {'CPU': cpu,
                'CPULimit': cpuLimit,
                'WallClock': wallClock,
                'WallClockLimit': wallClockLimit}

    if None in consumed.values():
      missed = [key for key, val in consumed.items() if val is None]
      msg = 'Could not determine parameter'
      self.log.warn('Could not determine parameter', ','.join(missed))
      self.log.debug('This is the stdout from the batch system call\n%s' % (result['Value']))
    else:
      self.log.debug("TimeLeft counters complete:", str(consumed))

    if cpuLimit or wallClockLimit:
      # We have got a partial result from SGE
      if not cpuLimit:
        # Take some margin
        consumed['CPULimit'] = wallClockLimit * 0.8
      if not wallClockLimit:
        consumed['WallClockLimit'] = cpuLimit / 0.8
      if not cpu:
        consumed['CPU'] = time.time() - self.startTime
      if not wallClock:
        consumed['WallClock'] = time.time() - self.startTime
      self.log.debug("TimeLeft counters restored:", str(consumed))
      return S_OK(consumed)
    else:
      msg = 'Could not determine necessary parameters'
      self.log.info(msg, ':\nThis is the stdout from the batch system call\n%s' % (result['Value']))
      retVal = S_ERROR(msg)
      retVal['Value'] = consumed
      return retVal


def _getCPUScalingFactor():

  host = socket.getfqdn()
  cmd = 'qconf -se %s' % host
  result = runCommand(cmd)
  if not result['OK']:
    return None

  lines = str(result['Value']).split('\n')
  for line in lines:
    if re.search('usage_scaling', line):
      match = re.search(r'cpu=([\d,\.]*),', line)
      if match:
        return float(match.groups()[0])
  return None
