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

    # CPU and wall clock margins, which don't seem to be set anywhere
    self.cpuMargin = gConfig.getValue('/LocalSite/CPUMargin', 2)  # percent
    self.wallClockMargin = gConfig.getValue('/LocalSite/wallClockMargin', 8)  # percent

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
      if resourceDict['Value']['CPU']:
        return resourceDict['Value']['CPU'] * self.scaleFactor
      elif resourceDict['Value']['WallClock']:
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
    if not resources['CPULimit'] and not resources['WallClockLimit']:
      # This should never happen
      return S_ERROR('No CPU or WallClock limit obtained')

    # if one of CPULimit or WallClockLimit is missing, compute a reasonable value
    if not resources['CPULimit']:
      resources['CPULimit'] = resources['WallClockLimit'] * processors
    elif not resources['WallClockLimit']:
      resources['WallClockLimit'] = resources['CPULimit']

    # if one of CPU or WallClock is missing, compute a reasonable value
    if not resources['CPU']:
      resources['CPU'] = resources['WallClock'] * processors
    elif not resources['WallClock']:
      resources['WallClock'] = resources['CPU']

    timeLeft = 0.
    cpu = float(resources['CPU'])
    cpuLimit = float(resources['CPULimit'])
    wallClock = float(resources['WallClock'])
    wallClockLimit = float(resources['WallClockLimit'])

    validTimeLeft = enoughTimeLeft(cpu, cpuLimit, wallClock, wallClockLimit, self.cpuMargin, self.wallClockMargin)

    if validTimeLeft:
      if cpu and cpuConsumed > 3600. and self.normFactor:
        # If there has been more than 1 hour of consumed CPU and
        # there is a Normalization set for the current CPU
        # use that value to renormalize the values returned by the batch system
        # NOTE: cpuConsumed is non-zero for call by the JobAgent and 0 for call by the watchdog
        # cpuLimit and cpu may be in the units of the batch system, not real seconds...
        # (in this case the other case won't work)
        # therefore renormalise it using cpuConsumed (which is in real seconds)
        timeLeft = (cpuLimit - cpu) * self.normFactor * cpuConsumed / cpu
      elif self.normFactor:
        # FIXME: this is always used by the watchdog... Also used by the JobAgent
        #        if consumed less than 1 hour of CPU
        # It was using self.scaleFactor but this is inconsistent: use the same as above
        # In case the returned cpu and cpuLimit are not in real seconds, this is however rubbish
        timeLeft = (cpuLimit - cpu) * self.normFactor
      else:
        # Last resort recovery...
        timeLeft = (cpuLimit - cpu) * self.scaleFactor

      self.log.verbose('Remaining CPU in normalized units is: %.02f' % timeLeft)
      return S_OK(timeLeft)
    else:
      return S_ERROR('No time left for slot')

  def __getBatchSystemPlugin(self):
    """ Using the name of the batch system plugin, will return an instance of the plugin class.
    """
    batchSystems = {
        'LSF': 'LSB_JOBID',
        'PBS': 'PBS_JOBID',
        'BQS': 'QSUB_REQNAME',
        'SGE': 'SGE_TASK_ID',
        'SLURM': 'SLURM_JOB_ID'}  # more to be added later
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


def enoughTimeLeft(cpu, cpuLimit, wallClock, wallClockLimit, cpuMargin, wallClockMargin):
  """ Is there enough time?

      :returns: True/False
  """

  cpuRemainingFraction = 100 * (1. - cpu / cpuLimit)
  wallClockRemainingFraction = 100 * (1. - wallClock / wallClockLimit)
  fractionTuple = (cpuRemainingFraction, wallClockRemainingFraction, cpuMargin, wallClockMargin)
  gLogger.verbose('Used CPU is %.1f s out of %.1f, Used WallClock is %.1f s out of %.1f.' % (cpu,
                                                                                             cpuLimit,
                                                                                             wallClock,
                                                                                             wallClockLimit))
  gLogger.verbose('Remaining CPU %.02f%%, Remaining WallClock %.02f%%, margin CPU %s%%, margin WC %s%%' % fractionTuple)

  if cpuRemainingFraction > cpuMargin \
          and wallClockRemainingFraction > wallClockMargin:
    gLogger.verbose(
        'Remaining CPU %.02f%% < Remaining WallClock %.02f%% and margins respected (%s%% and %s%%)' %
        fractionTuple)
    return True
  else:
    gLogger.verbose(
        'Remaining CPU %.02f%% or WallClock %.02f%% fractions < margin (%s%% and %s%%) so no time left' %
        fractionTuple)
    return False
