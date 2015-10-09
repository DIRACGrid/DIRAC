########################################################################
# $Id$
########################################################################

""" The BQS TimeLeft utility interrogates the BQS batch system for the
    current CPU consumed and CPU limit.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR, siteName
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import runCommand

__RCSID__ = "$Id$"

import os

class BQSTimeLeft:

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'BQSTimeLeft' )
    self.jobID = os.environ.get( 'QSUB_REQNAME' )

    self.log.verbose( 'QSUB_REQNAME=%s' % ( self.jobID ) )
    self.normFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    if not self.normFactor:
      self.log.warn( '/LocalSite/CPUNormalizationFactor not defined for site %s' % siteName() )

  #############################################################################
  def getResourceUsage( self ):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    if not self.jobID:
      return S_ERROR( 'Could not determine batch jobID from QSUB_REQNAME env var.' )

    if not self.normFactor:
      return S_ERROR( 'CPU normalization factor is not defined' )

    cmd = 'qjob -a -nh -wide %s' % ( self.jobID )
    result = runCommand( cmd )
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    try:
      cpuItems = str( result['Value'] ).split()
      if cpuItems[5][-1] == '/':
        cpu = float( cpuItems[5][:-1] )
        cpuLimit = float( cpuItems[6] )
      else:
        cpuList = cpuItems[5].split( '/' )
        cpu = float( cpuList[0] )
        cpuLimit = float( cpuList[1] )
    except Exception:
      self.log.warn( 'Problem parsing "%s" for CPU usage' % ( result['Value'] ) )

    # BQS has no wallclock limit so will simply return the same as for CPU to the TimeLeft utility
    # Divide the numbers by 5 to bring it to HS06 units from the CC UI units
    # and remove HS06 normalization factor
    if None not in ( cpu, cpuLimit ):
      cpu /= 5. * self.normFactor
      cpuLimit /= 5. * self.normFactor
      consumed = {'CPU':cpu , 'CPULimit':cpuLimit, 'WallClock':cpu , 'WallClockLimit':cpuLimit }
      self.log.debug( "TimeLeft counters complete:", str( consumed ) )
      return S_OK( consumed )

    msg = 'Could not determine some parameters'
    self.log.info( msg, ':\nThis is the stdout from the batch system call\n%s' % ( result['Value'] ) )
    return S_ERROR( msg )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
