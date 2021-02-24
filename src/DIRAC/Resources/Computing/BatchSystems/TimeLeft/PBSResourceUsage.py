""" The PBS TimeLeft utility interrogates the PBS batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

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

  def __init__(self):
    """ Standard constructor
    """
    super(PBSResourceUsage, self).__init__('PBS', 'PBS_JOBID')

    self.queue = os.environ.get('PBS_O_QUEUE')
    pbsPath = os.environ.get('PBS_O_PATH')
    if pbsPath:
      os.environ['PATH'] += ':' + pbsPath

    self.log.verbose('PBS_JOBID=%s, PBS_O_QUEUE=%s' % (self.jobID, self.queue))
    self.startTime = time.time()

  #############################################################################
  def getResourceUsage(self):
    """ Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
    """
    cmd = 'qstat -f %s' % (self.jobID)
    result = runCommand(cmd)
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    lines = str(result['Value']).split('\n')
    for line in lines:
      info = line.split()
      if re.search('.*resources_used.cput.*', line):
        if len(info) >= 3:
          cpuList = info[2].split(':')
          newcpu = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
          if not cpu or newcpu > cpu:
            cpu = newcpu
        else:
          self.log.warn('Problem parsing "%s" for CPU consumed' % line)
      if re.search('.*resources_used.pcput.*', line):
        if len(info) >= 3:
          cpuList = info[2].split(':')
          newcpu = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
          if not cpu or newcpu > cpu:
            cpu = newcpu
        else:
          self.log.warn('Problem parsing "%s" for CPU consumed' % line)
      if re.search('.*resources_used.walltime.*', line):
        if len(info) >= 3:
          wcList = info[2].split(':')
          wallClock = (float(wcList[0]) * 60 + float(wcList[1])) * 60 + float(wcList[2])
        else:
          self.log.warn('Problem parsing "%s" for elapsed wall clock time' % line)
      if re.search('.*Resource_List.cput.*', line):
        if len(info) >= 3:
          cpuList = info[2].split(':')
          newcpuLimit = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
          if not cpuLimit or newcpuLimit < cpuLimit:
            cpuLimit = newcpuLimit
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' % line)
      if re.search('.*Resource_List.pcput.*', line):
        if len(info) >= 3:
          cpuList = info[2].split(':')
          newcpuLimit = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
          if not cpuLimit or newcpuLimit < cpuLimit:
            cpuLimit = newcpuLimit
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' % line)
      if re.search('.*Resource_List.walltime.*', line):
        if len(info) >= 3:
          wcList = info[2].split(':')
          wallClockLimit = (float(wcList[0]) * 60 + float(wcList[1])) * 60 + float(wcList[2])
        else:
          self.log.warn('Problem parsing "%s" for wall clock limit' % line)

    consumed = {'CPU': cpu, 'CPULimit': cpuLimit, 'WallClock': wallClock, 'WallClockLimit': wallClockLimit}
    self.log.debug(consumed)

    if None not in consumed.values():
      self.log.debug("TimeLeft counters complete:", str(consumed))
      return S_OK(consumed)
    else:
      missed = [key for key, val in consumed.items() if val is None]
      self.log.info('Could not determine parameter', ','.join(missed))
      self.log.debug('This is the stdout from the batch system call\n%s' % (result['Value']))

    if cpuLimit or wallClockLimit:
      # We have got a partial result from PBS, assume that we ran for too short time
      if not cpuLimit:
        consumed['CPULimit'] = wallClockLimit * 0.8
      if not wallClockLimit:
        consumed['WallClockLimit'] = cpuLimit / 0.8
      if not cpu:
        consumed['CPU'] = int(time.time() - self.startTime)
      if not wallClock:
        consumed['WallClock'] = int(time.time() - self.startTime)
      self.log.debug("TimeLeft counters restored:", str(consumed))
      return S_OK(consumed)
    else:
      msg = 'Could not determine some parameters'
      self.log.info(msg, ':\nThis is the stdout from the batch system call\n%s' % (result['Value']))
      retVal = S_ERROR(msg)
      retVal['Value'] = consumed
      return retVal
