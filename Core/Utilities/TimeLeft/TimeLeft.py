########################################################################
# $Id$
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
__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

import DIRAC

import os

class TimeLeft:

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'TimeLeft' )
    # This is the ratio SpecInt published by the site over 250 (the reference used for Matching)
    self.scaleFactor = gConfig.getValue( '/LocalSite/CPUScalingFactor', 0.0 )
    if not self.scaleFactor:
      self.log.warn( '/LocalSite/CPUScalingFactor not defined for site %s' % DIRAC.siteName() )

    self.normFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    if not self.normFactor:
      self.log.warn( '/LocalSite/CPUNormalizationFactor not defined for site %s' % DIRAC.siteName() )

    self.cpuMargin = gConfig.getValue( '/LocalSite/CPUMargin', 10 ) #percent
    result = self.__getBatchSystemPlugin()
    if result['OK']:
      self.batchPlugin = result['Value']
    else:
      self.batchPlugin = None
      self.batchError = result['Message']

  def getScaledCPU( self ):
    """Returns the current CPU Time spend (according to batch system) scaled according 
       to /LocalSite/CPUScalingFactor
    """
    #Quit if no scale factor available
    if not self.scaleFactor:
      return S_OK( 0.0 )

    #Quit if Plugin is not available
    if not self.batchPlugin:
      return S_OK( 0.0 )

    resourceDict = self.batchPlugin.getResourceUsage()

    if 'Value' in resourceDict and resourceDict['Value']['CPU']:
      return S_OK( resourceDict['Value']['CPU'] * self.scaleFactor )

    return S_OK( 0.0 )

  #############################################################################
  def getTimeLeft( self, cpuConsumed = 0.0 ):
    """Returns the CPU Time Left for supported batch systems.  The CPUConsumed
       is the current raw total CPU.
    """
    #Quit if no scale factor available
    if not self.scaleFactor:
      return S_ERROR( '/LocalSite/CPUScalingFactor not defined for site %s' % DIRAC.siteName() )

    if not self.batchPlugin:
      return S_ERROR( self.batchError )

    resourceDict = self.batchPlugin.getResourceUsage()
    if not resourceDict['OK']:
      self.log.warn( 'Could not determine timeleft for batch system at site %s' % DIRAC.siteName() )
      return resourceDict

    resources = resourceDict['Value']
    self.log.verbose( resources )
    if not resources['CPULimit'] or not resources['WallClockLimit']:
      return S_ERROR( 'No CPU / WallClock limits obtained' )

    cpu = float( resources['CPU'] )
    cpuFactor = 100 * float( resources['CPU'] ) / float( resources['CPULimit'] )
    cpuRemaining = 100 - cpuFactor
    cpuLimit = float( resources['CPULimit'] )
    wcFactor = 100 * float( resources['WallClock'] ) / float( resources['WallClockLimit'] )
    wcRemaining = 100 - wcFactor
    wcLimit = float( resources['WallClockLimit'] )
    self.log.verbose( 'Used CPU is %.02f, Used WallClock is %.02f.' % ( cpuFactor, wcFactor ) )
    self.log.verbose( 'Remaining WallClock %.02f, Remaining CPU %.02f, margin %s' %
                      ( wcRemaining, cpuRemaining, self.cpuMargin ) )

    timeLeft = None
    if wcRemaining > cpuRemaining and ( wcRemaining - cpuRemaining ) > self.cpuMargin:
      self.log.verbose( 'Remaining WallClock %.02f > Remaining CPU %.02f and difference > margin %s' %
                        ( wcRemaining, cpuRemaining, self.cpuMargin ) )
      timeLeft = True
    else:
      if cpuRemaining > self.cpuMargin and wcRemaining > self.cpuMargin:
        self.log.verbose( 'Remaining WallClock %.02f and Remaining CPU %.02f both > margin %s' %
                          ( wcRemaining, cpuRemaining, self.cpuMargin ) )
        timeLeft = True
      else:
        self.log.verbose( 'Remaining CPU %.02f < margin %s and WallClock %.02f < margin %s so no time left' %
                          ( cpuRemaining, self.cpuMargin, wcRemaining, self.cpuMargin ) )
    if timeLeft:
      if cpu and cpuConsumed > 3600. and self.normFactor:
        # If there has been more than 1 hour of consumed CPU and 
        # there is a Normalization set for the current CPU
        # use that value to renormalize the values returned by the batch system
        cpuWork = cpuConsumed * self.normFactor
        timeLeft = ( cpuLimit - cpu ) * cpuWork / cpu
      else:
        # In some cases cpuFactor might be 0
        # timeLeft = float(cpuConsumed*self.scaleFactor*cpuRemaining/cpuFactor)
        # We need time left in the same units used by the Matching
        timeLeft = float( cpuRemaining * cpuLimit / 100 * self.scaleFactor )

      self.log.verbose( 'Remaining CPU in normalized units is: %.02f' % timeLeft )
      return S_OK( timeLeft )
    else:
      return S_ERROR( 'No time left for slot' )

  #############################################################################
  def __getBatchSystemPlugin( self ):
    """Using the name of the batch system plugin, will return an instance
       of the plugin class.
    """
    batchSystems = {'LSF':'LSB_JOBID', 'PBS':'PBS_JOBID', 'BQS':'QSUB_REQNAME', 'SGE':'SGE_TASK_ID'} #more to be added later
    name = None
    for batchSystem, envVar in batchSystems.items():
      if os.environ.has_key( envVar ):
        name = batchSystem
        break

    if name == None:
      self.log.warn( 'Batch system type for site %s is not currently supported' % DIRAC.siteName() )
      return S_ERROR( 'Current batch system is not supported' )

    self.log.debug( 'Creating plugin for %s batch system' % ( name ) )
    try:
      batchSystemName = "%sTimeLeft" % ( name )
      batchPlugin = __import__( 'DIRAC.Core.Utilities.TimeLeft.%s' %
                                batchSystemName, globals(), locals(), [batchSystemName] )
    except Exception, x:
      msg = 'Could not import DIRAC.Core.Utilities.TimeLeft.%s' % ( batchSystemName )
      self.log.warn( x )
      self.log.warn( msg )
      return S_ERROR( msg )

    try:
      batchStr = 'batchPlugin.%s()' % ( batchSystemName )
      batchInstance = eval( batchStr )
    except Exception, x:
      msg = 'Could not instantiate %s()' % ( batchSystemName )
      self.log.warn( x )
      self.log.warn( msg )
      return S_ERROR( msg )

    return S_OK( batchInstance )

#############################################################################
def runCommand( cmd, timeout = 120 ):
  """Wrapper around shellCall to return S_OK(stdout) or S_ERROR(message)
  """
  result = shellCall( timeout, cmd )
  if not result['OK']:
    return result
  status = result['Value'][0]
  stdout = result['Value'][1]
  stderr = result['Value'][2]

  if status:
    gLogger.warn( 'Status %s while executing %s' % ( status, cmd ) )
    gLogger.warn( stderr )
    if stdout:
      return S_ERROR( stdout )
    if stderr:
      return S_ERROR( stderr )
    return S_ERROR( 'Status %s while executing %s' % ( status, cmd ) )
  else:
    return S_OK( stdout )


#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
