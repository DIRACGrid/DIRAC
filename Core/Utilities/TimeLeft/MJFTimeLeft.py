""" The Machine/Job Features TimeLeft utility interrogates the MJF values
    for the current CPU and Wallclock consumed, as well as their limits.
"""


__RCSID__ = "$Id$"

import os
import time
import urllib

from DIRAC import gLogger, S_OK, S_ERROR

class MJFTimeLeft( object ):

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'MJFTimeLeft' )
    self.jobID = None
    if os.environ.has_key( 'JOB_ID' ):
      self.jobID = os.environ['JOB_ID']
    self.queue = None
    if os.environ.has_key( 'QUEUE' ):
      self.queue = os.environ['QUEUE']

    self.cpuLimit = None
    self.wallClockLimit = None
    self.log.verbose( 'jobID=%s, queue=%s' % ( self.jobID, self.queue ) )
    self.startTime = time.time()

  #############################################################################
  def getResourceUsage( self ):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """

    cpuLimit = None
    wallClockLimit = None

    try:
      # We are not called from TimeLeft.py if these are not set
      jobFeaturesPath = os.environ['JOBFEATURES']
      machineFeaturesPath = os.environ['MACHINEFEATURES']
    except:
      self.log.warn( '$JOBFEATURES and $MACHINEFEATURES not set' )

    try:
      wallClockLimit = int( urllib.urlopen(jobFeaturesPath + '/wall_limit_secs').read() )
    except:
      self.log.warn( 'Could not determine wallclock limit from $JOBFEATURES/wall_limit_secs' )

    try:
      jobStartSecs = int( urllib.urlopen(jobFeaturesPath + '/jobstart_secs').read() )
    except:
      self.log.warn( 'Could not determine job start time from $JOBFEATURES/jobstart_secs' )
      jobStartSecs = self.startTime

    try:
      shutdownTime = int( urllib.urlopen(machineFeaturesPath + '/shutdowntime').read() )
    except:
      self.log.info( 'Could not determine a shutdowntime value from $MACHINEFEATURES/shutdowntime' )
    else:
      if int(time.time()) + wallClockLimit > shutdownTime:
        # reduce wallClockLimit if would overrun shutdownTime
        wallClockLimit = shutdownTime - jobStartSecs

    try:
      cpuLimit = int( urllib.urlopen(jobFeaturesPath + '/cpu_limit_secs').read() )
    except:
      self.log.warn( 'Could not determine cpu limit from $JOBFEATURES/cpu_limit_secs' )
      cpuLimit = wallClockLimit

    wallClock = int(time.time()) - jobStartSecs
    # We cannot get CPU usage from MJF, so for now use wallClock figure
    cpu = wallClock
      
    consumed = {'CPU':cpu, 'CPULimit':cpuLimit, 'WallClock':wallClock, 'WallClockLimit':wallClockLimit}
    self.log.debug( consumed )

    if cpu and cpuLimit and wallClock and wallClockLimit:
      return S_OK( consumed )
    else:
      self.log.info( 'Could not determine some parameters' )
      retVal = S_ERROR( 'Could not determine some parameters' )
      retVal['Value'] = consumed
      return retVal

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
