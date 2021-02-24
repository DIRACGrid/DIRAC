""" The LSF TimeLeft utility interrogates the LSF batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import runCommand
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.ResourceUsage import ResourceUsage


from DIRAC.Core.Utilities.Os import sourceEnv

__RCSID__ = "$Id$"


class LSFResourceUsage(ResourceUsage):
  """
   This is the LSF plugin of the TimeLeft Utility
  """

  def __init__(self):
    """ Standard constructor
    """
    super(LSFResourceUsage, self).__init__('LSF', 'LSB_JOBID')

    self.queue = os.environ.get('LSB_QUEUE')
    self.bin = os.environ.get('LSF_BINDIR')
    self.host = os.environ.get('LSB_HOSTS')
    self.year = time.strftime('%Y', time.gmtime())
    self.log.verbose('LSB_JOBID=%s, LSB_QUEUE=%s, LSF_BINDIR=%s, LSB_HOSTS=%s' % (self.jobID,
                                                                                  self.queue,
                                                                                  self.bin,
                                                                                  self.host))

    self.cpuLimit = None
    self.cpuRef = None
    self.normRef = None
    self.wallClockLimit = None
    self.hostNorm = None

    cmd = '%s/bqueues -l %s' % (self.bin, self.queue)
    result = runCommand(cmd)
    if not result['OK']:
      return

    lines = str(result['Value']).split('\n')
    self.log.debug('From %s' % cmd, '\n'.join([line if len(line) <= 128 else line[:128] + ' [...]' for line in lines]))
    for i in range(len(lines)):
      if re.search('.*CPULIMIT.*', lines[i]):
        info = lines[i + 1].split()
        if len(info) >= 4:
          self.cpuLimit = float(info[0]) * 60
          self.cpuRef = info[3]
        elif len(info) == 2 and info[1] == "min":
          self.cpuLimit = float(info[0]) * 60
          self.cpuRef = None
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' % lines[i + 1])
          self.cpuLimit = -1
      elif re.search('.*RUNLIMIT.*', lines[i]):
        info = lines[i + 1].split()
        if len(info) >= 1:
          self.wallClockLimit = float(info[0]) * 60
        else:
          self.log.warn('Problem parsing "%s" for wall clock limit' % lines[i + 1])
          self.wallClockLimit = -1

    modelMaxNorm = 0
    if self.cpuRef:
      # Now try to get the CPU_FACTOR for this reference CPU,
      # it must be either a Model, a Host or the largest Model

      cmd = '%s/lshosts -w %s' % (self.bin, self.cpuRef)
      result = runCommand(cmd)
      if result['OK']:
        # At CERN this command will return an error since there is no host defined
        # with the name of the reference Host.
        lines = str(result['Value']).split('\n')
        l1 = lines[0].split()
        l2 = lines[1].split()
        if len(l1) > len(l2):
          self.log.error("Failed lshost command", "%s:\n %s\n %s" % (cmd, lines[0], lines[0]))
        else:
          for i in range(len(l1)):
            if l1[i] == 'cpuf':
              try:
                self.normRef = float(l2[i])
                self.log.info('Reference Normalization taken from Host', '%s: %s' % (self.cpuRef, self.normRef))
              except ValueError as e:
                self.log.exception('Exception parsing lshosts output', '', e)

      if not self.normRef:
        # Try if there is a model define with the name of cpuRef
        cmd = '%s/lsinfo -m' % (self.bin)
        result = runCommand(cmd)
        if result['OK']:
          lines = str(result['Value']).split('\n')
          for line in lines[1:]:
            words = line.split()
            if len(words) > 1:
              try:
                norm = float(words[1])
                if norm > modelMaxNorm:
                  modelMaxNorm = norm
                if words[0].find(self.cpuRef) > -1:
                  self.normRef = norm
                  self.log.info('Reference Normalization taken from Host Model',
                                '%s: %s' % (self.cpuRef, self.normRef))
              except ValueError as e:
                self.log.exception('Exception parsing lsfinfo output', '', e)

      if not self.normRef:
        # Now parse LSF configuration files
        if not os.path.isfile('./lsf.sh'):
          os.symlink(os.path.join(os.environ['LSF_ENVDIR'], 'lsf.conf'), './lsf.sh')
        # As the variables are not exported, we must force it
        ret = sourceEnv(10, ['./lsf', '&& export LSF_CONFDIR'])
        if ret['OK']:
          lsfEnv = ret['outputEnv']
          shared = None
          try:
            egoShared = os.path.join(lsfEnv['LSF_CONFDIR'], 'ego.shared')
            lsfShared = os.path.join(lsfEnv['LSF_CONFDIR'], 'lsf.shared')
            if os.path.exists(egoShared):
              shared = egoShared
            elif os.path.exists(lsfShared):
              shared = lsfShared
          except KeyError as e:
            self.log.exception('Exception getting LSF configuration', '', e)
          if shared:
            with open(shared) as f:
              hostModelSection = False
              for line in f.readlines():
                if line.find('Begin HostModel') == 0:
                  hostModelSection = True
                  continue
                if not hostModelSection:
                  continue
                if line.find('End HostModel') == 0:
                  break
                line = line.strip()
                if line and line.split()[0] == self.cpuRef:
                  try:
                    self.normRef = float(line.split()[1])
                    self.log.info('Reference Normalization taken from Configuration File',
                                  '(%s) %s: %s' % (shared, self.cpuRef, self.normRef))
                  except ValueError as e:
                    self.log.exception('Exception reading LSF configuration', '', e)
          else:
            self.log.warn('Could not find LSF configuration')
        else:
          self.log.error('Cannot source the LSF environment', ret['Message'])
    if not self.normRef:
      # If nothing works take this as the unit
      self.normRef = 1.
      # If nothing worked, take the maximum defined for a Model
      # if modelMaxNorm:
      #  self.normRef = modelMaxNorm
      #  self.log.info('Reference Normalization taken from Max Model:', self.normRef)

    # Now get the Normalization for the current Host
    if self.host:
      cmd = '%s/lshosts -w %s' % (self.bin, self.host)
      result = runCommand(cmd)
      if result['OK']:
        lines = str(result['Value']).split('\n')
        l1 = lines[0].split()
        l2 = lines[1].split()
        if len(l1) > len(l2):
          self.log.error("Failed lshost command", "%s:\n %s\n %s" % (cmd, lines[0], lines[0]))
        else:
          for i in range(len(l1)):
            if l1[i] == 'cpuf':
              try:
                self.hostNorm = float(l2[i])
                self.log.info('Host Normalization', '%s: %s' % (self.host, self.hostNorm))
              except ValueError as e:
                self.log.exception('Exception parsing lshosts output', l1, e)
              finally:
                break

      if self.hostNorm and self.normRef:
        self.hostNorm /= self.normRef
        self.log.info('CPU power w.r.t. batch unit', self.hostNorm)

      if self.hostNorm:
        # Set the limits in real seconds
        self.cpuLimit /= self.hostNorm
        self.wallClockLimit /= self.hostNorm

  #############################################################################
  def getResourceUsage(self):
    """ Returns S_OK with a dictionary containing the entries CPU, CPULimit,
        WallClock, WallClockLimit, and Unit for current slot.
    """
    if not self.bin:
      return S_ERROR('Could not determine bin directory for LSF')
    if not self.hostNorm:
      return S_ERROR('Could not determine host Norm factor')

    cpu = None
    wallClock = None

    cmd = '%s/bjobs -W %s' % (self.bin, self.jobID)
    result = runCommand(cmd)
    if not result['OK']:
      return result
    lines = str(result['Value']).split('\n')
    l1 = lines[0].split()
    l2 = lines[1].split()
    if len(l1) > len(l2):
      self.log.error("Failed bjobs command", "%s:\n %s\n %s" % (cmd, lines[0], lines[0]))
      return S_ERROR('Can not parse LSF output')

    sCPU = None
    sStart = None
    for i in range(len(l1)):
      if l1[i] == 'CPU_USED':
        sCPU = l2[i]
        lCPU = sCPU.split(':')
        try:
          cpu = float(lCPU[0]) * 3600 + float(lCPU[1]) * 60 + float(lCPU[2])
        except (ValueError, IndexError) as _e:
          pass
      elif l1[i] == 'START_TIME':
        sStart = l2[i]
        sStart = '%s %s' % (sStart, self.year)
        try:
          timeTup = time.strptime(sStart, '%m/%d-%H:%M:%S %Y')
          wallClock = time.mktime(time.localtime()) - time.mktime(timeTup)
        except ValueError:
          pass

    if cpu is None or wallClock is None:
      return S_ERROR('Failed to parse LSF output')

    consumed = {'CPU': cpu, 'CPULimit': self.cpuLimit, 'WallClock': wallClock, 'WallClockLimit': self.wallClockLimit}
    self.log.debug(consumed)

    if None not in consumed.values():
      return S_OK(consumed)
    else:
      missed = [key for key, val in consumed.items() if val is None]
      msg = 'Could not determine some parameters'
      self.log.info(msg,
                    ': %s\nThis is the stdout from the batch system call\n%s' % (','.join(missed), result['Value']))
      return S_ERROR(msg)
