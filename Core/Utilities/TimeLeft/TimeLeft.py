########################################################################
# $Id: TimeLeft.py,v 1.3 2008/03/10 12:48:06 paterson Exp $
########################################################################

""" The TimeLeft utility allows to calculate the amount of CPU time
    left for a given batch system slot.  This is essential for the 'Filling
    Mode' where several VO jobs may be executed in the same allocated slot.

    The prerequisites for the utility to run are:
      - Plugin for extracting information from local batch system
      - Scale factor for the local site.

    With this information the utility can calculate in normalized units the
    CPU time remaining for a given slot.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR

__RCSID__ = "$Id: TimeLeft.py,v 1.3 2008/03/10 12:48:06 paterson Exp $"

import os,re

class TimeLeft:

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.__loadLocalCFGFiles()
    self.log = gLogger.getSubLogger('TimeLeft')
    self.site = gConfig.getValue('/LocalSite/Site','Unknown')
    self.scaleFactor = gConfig.getValue('/LocalSite/CPUScalingFactor',0.0)
    self.cpuMargin = 10 #percent

  #############################################################################
  def getTimeLeft(self,cpuConsumed):
    """Returns the CPU Time Left for supported batch systems.  The CPUConsumed
       is the current raw total CPU.
    """
    #Quit if no scale factor available
    if not self.scaleFactor:
      self.log.warn('/LocalSite/CPUScalingFactor not defined for site %s' %self.site)
      return S_ERROR('/LocalSite/CPUScalingFactor not defined for site %s' %self.site)

    #Work out which type of batch system to query and attempt to instantiate plugin
    result = self.__checkCurrentBatchSystem()
    if not result['OK']:
      return result
    name = result['Value']

    batchInstance = self.__getBatchSystemPlugin(name)
    if not batchInstance['OK']:
      return batchInstance

    batchSystem = batchInstance['Value']
    resourceDict = batchSystem.getResourceUsage()
    if not resourceDict['OK']:
      self.log.warn('Could not determine timeleft for batch system %s at site %s' %(name,self.site))
      return resourceDict

    resources = resourceDict['Value']
    self.log.verbose(resources)
    cpuFactor = float(resources['CPU'])/float(resources['CPULimit'])
    cpuRemaining = 1.0-cpuFactor
    wcFactor = float(resources['WallClock'])/float(resources['WallClockLimit'])
    wcRemaining = 1.0-wcFactor
    self.log.verbose('Used CPU factor is %.02f, Used WallClock factor is %.02f.' %(cpuFactor,wcFactor))

    if wcRemaining > cpuRemaining+self.cpuMargin:
      remainingCPU = float(cpuConsumed*self.scaleFactor*cpuRemaining/cpuFactor)
      self.log.verbose('Remaining WallClock %s > Remaining CPU %s + margin %s' %(wcFactor,cpuFactor,cpuMargin))
    else:
      self.log.verbose('Remaining WallClock %s < Remaining CPU %s + margin %s' %(wcFactor,cpuFactor,cpuMargin))
      remainingCPU = float(cpuConsumed*self.scaleFactor*(wcRemaining-(wcRemaining-cpuRemaining)-self.cpuMargin)/wcFactor)

    self.log.verbose('Remaining CPU in normalized units is: %.02f' %remainingCPU)
    return S_OK(remainingCPU)

  #############################################################################
  def __loadLocalCFGFiles(self):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    localRoot=gConfig.getValue('/LocalSite/Root',os.getcwd())
    files = os.listdir(localRoot)
    for i in files:
      if re.search('.cfg$',i):
        gConfig.loadFile(i)

  #############################################################################
  def __getBatchSystemPlugin(self,name):
    """Using the name of the batch system plugin, will return an instance
       of the plugin class.
    """
    self.log.debug('Creating plugin for %s batch system' %(name))
    try:
      batchSystemName = "%sTimeLeft" % (name)
      batchPlugin = __import__('DIRAC.Core.Utilities.TimeLeft.%s' % batchSystemName,globals(),locals(),[batchSystemName])
    except Exception, x:
      msg = 'Could not import DIRAC.Core.Utilities.TimeLeft.%s' %(batchSystemName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    try:
      batchStr = 'batchPlugin.%s()' %(batchSystemName)
      batchInstance = eval(batchStr)
    except Exception, x:
      msg = 'Could not instantiate %s()' %(batchSystemName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(batchInstance)

  #############################################################################
  def __checkCurrentBatchSystem(self):
    """Based on the current environment, this utility will return the
       current batch system name.
    """
    batchSystems = {'LSF':'LSB_JOBID','PBS':'PBS_JOBID'} #more to be added later
    current = None
    for batchSystem,envVar in batchSystems.items():
      if os.environ.has_key(envVar):
        current = batchSystem

    if current:
      return S_OK(current)
    else:
      self.log.warn('Batch system type for site %s is not currently supported' %self.site)
      return S_ERROR('Currrent batch system is not supported')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#