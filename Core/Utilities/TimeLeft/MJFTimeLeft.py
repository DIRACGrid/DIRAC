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

    self.jobID = os.environ.get( 'JOB_ID' )
    self.queue = os.environ.get( 'QUEUE' )
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
    wallClock = None
    jobStartSecs = None

    jobFeaturesPath = None
    machineFeaturesPath = None

    #Getting info from JOBFEATURES
    try:
      # We are not called from TimeLeft.py if these are not set
      jobFeaturesPath = os.environ['JOBFEATURES']
    except KeyError:
      self.log.warn( '$JOBFEATURES is not set' )

    if jobFeaturesPath:
      try:
        wallClockLimit = int( urllib.urlopen( jobFeaturesPath + '/wall_limit_secs' ).read() )
        self.log.verbose( "wallClockLimit from JF = %d" %wallClockLimit )
      except ValueError:
        self.log.warn( "/wall_limit_secs is unreadable" )
      except IOError as e:
        self.log.exception( "Issue with $JOBFEATURES/wall_limit_secs", lException = e )
        self.log.warn( "Could not determine cpu limit from $JOBFEATURES/wall_limit_secs" )

      try:
        jobStartSecs = int( urllib.urlopen( jobFeaturesPath + '/jobstart_secs' ).read() )
        self.log.verbose( "jobStartSecs from JF = %d" %jobStartSecs )
      except ValueError:
        self.log.warn( "/jobstart_secs is unreadable, setting a default" )
        jobStartSecs = self.startTime
      except IOError as e:
        self.log.exception( "Issue with $JOBFEATURES/jobstart_secs", lException = e )
        self.log.warn( "Can't open jobstart_secs, setting a default" )
        jobStartSecs = self.startTime

      try:
        cpuLimit = int( urllib.urlopen( jobFeaturesPath + '/cpu_limit_secs' ).read() )
        self.log.verbose( "cpuLimit from JF = %d" %cpuLimit )
      except ValueError:
        self.log.warn( "/cpu_limit_secs is unreadable" )
      except IOError as e:
        self.log.exception( "Issue with $JOBFEATURES/cpu_limit_secs", lException = e )
        self.log.warn( 'Could not determine cpu limit from $JOBFEATURES/cpu_limit_secs' )

      wallClock = int( time.time() ) - jobStartSecs



    #Getting info from MACHINEFEATURES
    try:
      # We are not called from TimeLeft.py if these are not set
      machineFeaturesPath = os.environ['MACHINEFEATURES']
    except KeyError:
      self.log.warn( '$MACHINEFEATURES is not set' )

    if machineFeaturesPath and jobStartSecs:
      try:
        shutdownTime = int( urllib.urlopen( machineFeaturesPath + '/shutdowntime' ).read() )
        self.log.verbose( "shutdownTime from MF = %d" %shutdownTime )
        if int( time.time() ) + wallClockLimit > shutdownTime:
          # reduce wallClockLimit if would overrun shutdownTime
          wallClockLimit = shutdownTime - jobStartSecs
      except ValueError:
        self.log.warn( "/shutdowntime is unreadable" )
      except IOError as e:
        self.log.exception( "Issue with $MACHINEFEATURES/shutdowntime", lException = e )
        self.log.warn( 'Could not determine a shutdowntime value from $MACHINEFEATURES/shutdowntime' )


    #Reporting
    consumed = {'CPU':None, 'CPULimit':cpuLimit, 'WallClock':wallClock, 'WallClockLimit':wallClockLimit}
    if cpuLimit and wallClock and wallClockLimit:
      self.log.verbose( "MJF consumed: %s" % str( consumed ) )
      return S_OK( consumed )
    else:
      self.log.info( 'Could not determine some parameters' )
      retVal = S_ERROR( 'Could not determine some parameters' )
      retVal['Value'] = consumed
      return retVal

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
