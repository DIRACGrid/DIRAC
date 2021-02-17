""" The SGE TimeLeft utility interrogates the SGE batch system for the
    current CPU consumed, as well as its limit.
"""

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

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    super(SGEResourceUsage, self).__init__('SGE', 'JOB_ID')

    self.queue = os.environ.get('QUEUE')
    pbsPath = os.environ.get('SGE_BINARY_PATH')
    if pbsPath:
      os.environ['PATH'] += ':' + pbsPath

    self.log.verbose('JOB_ID=%s, QUEUE=%s' % (self.jobID, self.queue))
    self.startTime = time.time()

  #############################################################################
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
            newcpu = (float(cpuList[0]) * 60 + float(cpuList[1])) * 60 + float(cpuList[2])
          elif len(cpuList) == 4:
            newcpu = ((float(cpuList[0]) * 24 + float(cpuList[1])) * 60 + float(cpuList[2])) * 60 + float(cpuList[3])
          if not cpu or newcpu > cpu:
            cpu = newcpu
        except ValueError:
          self.log.warn('Problem parsing "%s" for CPU consumed' % line)
      elif re.search('hard resource_list.*cpu.*', line):
        match = re.search(r'_cpu=(\d*)', line)
        if match:
          cpuLimit = float(match.groups()[0])
        match = re.search(r'_rt=(\d*)', line)
        if match:
          wallClockLimit = float(match.groups()[0])

    # Some SGE batch systems apply CPU scaling factor to the CPU consumption figures
    if cpu:
      factor = _getCPUScalingFactor()
      if factor:
        cpu = cpu / factor

    consumed = {'CPU': cpu, 'CPULimit': cpuLimit, 'WallClock': wallClock, 'WallClockLimit': wallClockLimit}

    if None not in consumed.values():
      # This cannot happen as we can't get wallClock from anywhere
      self.log.debug("TimeLeft counters complete:", str(consumed))
      return S_OK(consumed)
    else:
      missed = [key for key, val in consumed.items() if val is None]
      self.log.info('Could not determine parameter', ','.join(missed))
      self.log.debug('This is the stdout from the batch system call\n%s' % (result['Value']))

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
      msg = 'Could not determine some parameters'
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
  _example = """Example of output for qconf -se ccwsge0640
hostname              ccwsge0640.in2p3.fr
load_scaling          NONE
complex_values        m_mem_free=131022.000000M,m_mem_free_n0=65486.613281M, \
                      m_mem_free_n1=65536.000000M,os=sl6
load_values           arch=lx-amd64,cpu=89.400000,fsize_used_rate=0.089, \
                      load_avg=36.300000,load_long=36.020000, \
                      load_medium=36.300000,load_short=35.960000, \
                      m_cache_l1=32.000000K,m_cache_l2=256.000000K, \
                      m_cache_l3=25600.000000K,m_core=20, \
                      m_mem_free=72544.000000M,m_mem_free_n0=18696.761719M, \
                      m_mem_free_n1=22139.621094M,m_mem_total=131022.000000M, \
                      m_mem_total_n0=65486.613281M, \
                      m_mem_total_n1=65536.000000M,m_mem_used=58478.000000M, \
                      m_mem_used_n0=46789.851562M,m_mem_used_n1=43396.378906M, \
                      m_numa_nodes=2,m_socket=2,m_thread=40, \
                      m_topology=SCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTSCTTCTTCTTCTTCTTCTTCTTCTTCTTCTT, \
                      m_topology_inuse=SCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTSCTTCTTCTTCTTCTTCTTCTTCTTCTTCTT, \
                      m_topology_numa=[SCTTCTTCTTCTTCTTCTTCTTCTTCTTCTT][SCTTCTTCTTCTTCTTCTTCTTCTTCTTCTT], \
                      mem_free=70513.675781M,mem_total=129001.429688M, \
                      mem_used=58487.753906M,memory_used_rate=0.468, \
                      np_load_avg=0.907500,np_load_long=0.900500, \
                      np_load_medium=0.907500,np_load_short=0.899000, \
                      num_proc=40,swap_free=0.000000M,swap_total=266.699219M, \
                      swap_used=266.699219M,virtual_free=70513.675781M, \
                      virtual_total=129268.128906M,virtual_used=58754.453125M
processors            40
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         cpu=11.350000,acct_cpu=11.350000
report_variables      NONE

"""
  lines = str(result['Value']).split('\n')
  for line in lines:
    if re.search('usage_scaling', line):
      match = re.search(r'cpu=([\d,\.]*),', line)
      if match:
        return float(match.groups()[0])
  return None
