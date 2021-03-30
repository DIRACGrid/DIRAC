""" The TimeLeft utility allows to calculate the amount of CPU time
    left for a given batch system slot.  This is essential for the 'Filling
    Mode' where several VO jobs may be executed in the same allocated slot.

    The prerequisites for the utility to run are:
      - Plugin for extracting information from local batch system
      - Scale factor for the local site.

    With this information the utility can calculate in normalized units the
    CPU time remaining for a given slot.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import shlex

import DIRAC

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall


class TimeLeft(object):
  """ This generally does not run alone
  """

  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('TimeLeft')
    # This is the ratio SpecInt published by the site over 250 (the reference used for Matching)
    self.scaleFactor = gConfig.getValue('/LocalSite/CPUScalingFactor', 0.0)
    if not self.scaleFactor:
      self.log.warn('/LocalSite/CPUScalingFactor not defined for site %s' % DIRAC.siteName())

    self.normFactor = gConfig.getValue('/LocalSite/CPUNormalizationFactor', 0.0)
    if not self.normFactor:
      self.log.warn('/LocalSite/CPUNormalizationFactor not defined for site %s' % DIRAC.siteName())

    result = self.__getBatchSystemPlugin()
    if result['OK']:
      self.batchPlugin = result['Value']
    else:
      self.batchPlugin = None
      self.batchError = result['Message']

  def getScaledCPU(self, processors=1):
    """ Returns the current CPU Time spend (according to batch system) scaled according
        to /LocalSite/CPUScalingFactor
    """
    # Quit if no scale factor available
    if not self.scaleFactor:
      return 0

    # Quit if Plugin is not available
    if not self.batchPlugin:
      return 0

    resourceDict = self.batchPlugin.getResourceUsage()

    if 'Value' in resourceDict:
      if resourceDict['Value'].get('CPU'):
        return resourceDict['Value']['CPU'] * self.scaleFactor
      elif resourceDict['Value'].get('WallClock'):
        # When CPU value missing, guess from WallClock and number of processors
        return resourceDict['Value']['WallClock'] * self.scaleFactor * processors

    return 0

  def getTimeLeft(self, cpuConsumed=0.0, processors=1):
    """ Returns the CPU Time Left for supported batch systems.
        The CPUConsumed is the current raw total CPU.
    """
    # Quit if no scale factor available
    if not self.scaleFactor:
      return S_ERROR('/LocalSite/CPUScalingFactor not defined for site %s' % DIRAC.siteName())

    if not self.batchPlugin:
      return S_ERROR(self.batchError)

    resourceDict = self.batchPlugin.getResourceUsage()
    if not resourceDict['OK']:
      self.log.warn('Could not determine timeleft for batch system at site %s' % DIRAC.siteName())
      return resourceDict

    resources = resourceDict['Value']
    self.log.debug("self.batchPlugin.getResourceUsage(): %s" % str(resources))
    if not resources.get('CPULimit') and not resources.get('WallClockLimit'):
      # This should never happen
      return S_ERROR('No CPU or WallClock limit obtained')

    # if one of CPULimit or WallClockLimit is missing, compute a reasonable value
    if not resources.get('CPULimit'):
      resources['CPULimit'] = resources['WallClockLimit'] * processors
    elif not resources.get('WallClockLimit'):
      resources['WallClockLimit'] = resources['CPULimit'] / processors

    # if one of CPU or WallClock is missing, compute a reasonable value
    if not resources.get('CPU'):
      resources['CPU'] = resources['WallClock'] * processors
    elif not resources.get('WallClock'):
      resources['WallClock'] = resources['CPU'] / processors

    timeLeft = 0.
    cpu = float(resources['CPU'])
    cpuLimit = float(resources['CPULimit'])
    wallClock = float(resources['WallClock'])
    wallClockLimit = float(resources['WallClockLimit'])
    batchSystemTimeUnit = resources.get('Unit', 'Both')

    # Some batch systems rely on wall clock time and/or cpu time to make allocations
    if batchSystemTimeUnit == 'WallClock':
      time = wallClock
      timeLimit = wallClockLimit
    else:
      time = cpu
      timeLimit = cpuLimit

    if time and cpuConsumed > 3600. and self.normFactor:
      # If there has been more than 1 hour of consumed CPU and
      # there is a Normalization set for the current CPU
      # use that value to renormalize the values returned by the batch system
      # NOTE: cpuConsumed is non-zero for call by the JobAgent and 0 for call by the watchdog
      # cpuLimit and cpu may be in the units of the batch system, not real seconds...
      # (in this case the other case won't work)
      # therefore renormalise it using cpuConsumed (which is in real seconds)
      cpuWorkLeft = (timeLimit - time) * self.normFactor * cpuConsumed / time
    elif self.normFactor:
      # FIXME: this is always used by the watchdog... Also used by the JobAgent
      #        if consumed less than 1 hour of CPU
      # It was using self.scaleFactor but this is inconsistent: use the same as above
      # In case the returned cpu and cpuLimit are not in real seconds, this is however rubbish
      cpuWorkLeft = (timeLimit - time) * self.normFactor
    else:
      # Last resort recovery...
      cpuWorkLeft = (timeLimit - time) * self.scaleFactor

    self.log.verbose('Remaining CPU in normalized units is: %.02f' % timeLeft)
    return S_OK(cpuWorkLeft)

  def __getBatchSystemPlugin(self):
    """ Using the name of the batch system plugin, will return an instance of the plugin class.
    """
    batchSystems = {
        'LSF': 'LSB_JOBID',
        'PBS': 'PBS_JOBID',
        'BQS': 'QSUB_REQNAME',
        'SGE': 'SGE_TASK_ID',
        'SLURM': 'SLURM_JOB_ID',
        'HTCondor': '_CONDOR_JOB_AD'}  # more to be added later
    name = None
    for batchSystem, envVar in batchSystems.items():
      if envVar in os.environ:
        name = batchSystem
        break

    if name is None and 'MACHINEFEATURES' in os.environ and 'JOBFEATURES' in os.environ:
      # Only use MJF if legacy batch system information not available for now
      name = 'MJF'

    if name is None:
      self.log.warn('Batch system type for site %s is not currently supported' % DIRAC.siteName())
      return S_ERROR('Current batch system is not supported')

    self.log.debug('Creating plugin for %s batch system' % (name))
    try:
      batchSystemName = "%sResourceUsage" % (name)
      batchPlugin = __import__('DIRAC.Resources.Computing.BatchSystems.TimeLeft.%s' %  # pylint: disable=unused-variable
                               batchSystemName, globals(), locals(), [batchSystemName])
    except ImportError as x:
      msg = 'Could not import DIRAC.Resources.Computing.BatchSystems.TimeLeft.%s' % (batchSystemName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    try:
      batchStr = 'batchPlugin.%s()' % (batchSystemName)
      batchInstance = eval(batchStr)
    except Exception as x:  # pylint: disable=broad-except
      msg = 'Could not instantiate %s()' % (batchSystemName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(batchInstance)

#############################################################################


def runCommand(cmd, timeout=120):
  """ Wrapper around systemCall to return S_OK(stdout) or S_ERROR(message)
  """
  result = systemCall(timeout=timeout, cmdSeq=shlex.split(cmd))
  if not result['OK']:
    return result
  status, stdout, stderr = result['Value'][0:3]

  if status:
    gLogger.warn('Status %s while executing %s' % (status, cmd))
    gLogger.warn(stderr)
    if stdout:
      return S_ERROR(stdout)
    if stderr:
      return S_ERROR(stderr)
    return S_ERROR('Status %s while executing %s' % (status, cmd))
  else:
    return S_OK(str(stdout))
