""" The Machine/Job Features TimeLeft utility interrogates the MJF values
    for the current CPU and Wallclock consumed, as well as their limits.
"""

import os
import time
import urllib

from DIRAC import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class MJFTimeLeft( object ):
  """ Class for creating objects that deal with MJF
  """

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'MJFTimeLeft' )
    self.jobID = None
    if 'JOB_ID' in os.environ:
      self.jobID = os.environ['JOB_ID']
    self.queue = None
    if 'QUEUE' in os.environ:
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
    except KeyError:
      self.log.warn( '$JOBFEATURES and/or $MACHINEFEATURES is not set' )

    try:
      wallClockLimit = int( urllib.urlopen( jobFeaturesPath + '/wall_limit_secs' ).read() )
    except ValueError:
      self.log.warn( "/wall_limit_secs is unreadable" )
    except IOError:
      self.log.warn( "Can't open wall_limit_secs" )

    try:
      jobStartSecs = int( urllib.urlopen( jobFeaturesPath + '/jobstart_secs' ).read() )
    except ValueError:
      self.log.warn( "/jobstart_secs is unreadable, setting a default" )
      jobStartSecs = self.startTime
    except IOError:
      self.log.warn( "Can't open jobstart_secs, setting a default" )
      jobStartSecs = self.startTime

    try:
      shutdownTime = int( urllib.urlopen( machineFeaturesPath + '/shutdowntime' ).read() )
      if int( time.time() ) + wallClockLimit > shutdownTime:
        # reduce wallClockLimit if would overrun shutdownTime
        wallClockLimit = shutdownTime - jobStartSecs
    except ValueError:
      self.log.warn( "/shutdowntime is unreadable" )
    except IOError:
      self.log.info( 'Could not determine a shutdowntime value from $MACHINEFEATURES/shutdowntime' )

    try:
      cpuLimit = int( urllib.urlopen( jobFeaturesPath + '/cpu_limit_secs' ).read() )
    except ValueError:
      self.log.warn( "/cpu_limit_secs is unreadable" )
    except IOError:
      self.log.warn( 'Could not determine cpu limit from $JOBFEATURES/cpu_limit_secs' )

    wallClock = int( time.time() ) - jobStartSecs

    consumed = {'CPU':None, 'CPULimit':cpuLimit, 'WallClock':wallClock, 'WallClockLimit':wallClockLimit}
    self.log.debug( "MJF consumed: %s" % str( consumed ) )

    if cpuLimit and wallClock and wallClockLimit:
      return S_OK( consumed )
    else:
      self.log.info( 'Could not determine some parameters' )
      retVal = S_ERROR( 'Could not determine some parameters' )
      retVal['Value'] = consumed
      return retVal

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
