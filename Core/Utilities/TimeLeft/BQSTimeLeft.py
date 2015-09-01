########################################################################
# $Id$
########################################################################

""" The BQS TimeLeft utility interrogates the BQS batch system for the
    current CPU consumed and CPU limit.
"""

import os

from DIRAC import gLogger, gConfig, S_OK, DError
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import runCommand

__RCSID__ = "$Id$"

class BQSTimeLeft( object ):

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'BQSTimeLeft' )
    self.jobID = None
    if os.environ.has_key( 'QSUB_REQNAME' ):
      self.jobID = os.environ['QSUB_REQNAME']

    self.log.verbose( 'QSUB_REQNAME=%s' % ( self.jobID ) )
    self.scaleFactor = gConfig.getValue( '/LocalSite/CPUScalingFactor', 0.0 )

  #############################################################################
  def getResourceUsage( self ):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    if not self.jobID:
      return DError( 'Could not determine batch jobID from QSUB_REQNAME env var.' )

    if not self.scaleFactor:
      return DError( 'CPU scale factor is not defined' )

    cmd = 'qjob -a -nh -wide %s' % ( self.jobID )
    result = runCommand( cmd )
    if not result['OK']:
      return result

    self.log.verbose( result['Value'] )

    cpu = None
    cpuLimit = None
    try:
      cpuItems = result['Value'].split()
      if cpuItems[5][-1] == '/':
        cpu = float( cpuItems[5][:-1] )
        cpuLimit = float( cpuItems[6] )
      else:
        cpuList = cpuItems[5].split( '/' )
        cpu = float( cpuList[0] )
        cpuLimit = float( cpuList[1] )
    except Exception:
      self.log.warn( 'Problem parsing "%s" for CPU usage' % ( result['Value'] ) )

    #BQS has no wallclock limit so will simply return the same as for CPU to the TimeLeft utility
    wallClock = cpu
    wallClockLimit = cpuLimit
    # Divide the numbers by 5 to bring it to HS06 units from the CC UI units
    # and remove HS06 normalization factor
    consumed = {'CPU':cpu / 5. / self.scaleFactor,
                'CPULimit':cpuLimit / 5. / self.scaleFactor,
                'WallClock':wallClock / 5. / self.scaleFactor,
                'WallClockLimit':wallClockLimit / 5. / self.scaleFactor}
    self.log.debug( consumed )
    failed = False
    for key, val in consumed.items():
      if val == None:
        failed = True
        self.log.warn( 'Could not determine %s' % key )

    if not failed:
      return S_OK( consumed )
    else:
      msg = 'Could not determine some parameters,' \
            ' this is the stdout from the batch system call\n%s' % ( result['Value'] )
      self.log.info( msg )
      return S_ERROR( 'Could not determine some parameters' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
