"""  The Watchdog Factory instantiates a given Watchdog based on a quick
     determination of the local operating system.
"""
__RCSID__ = "$Id$"

import re, platform

from DIRAC import S_OK, S_ERROR, gLogger


class WatchdogFactory( object ):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.version = platform.uname()
    self.log = gLogger.getSubLogger( 'WatchdogFactory' )
    self.watchDogsLocation = 'DIRAC.WorkloadManagementSystem.JobWrapper'

  #############################################################################
  def getWatchdog( self, pid, thread, spObject, jobcputime, memoryLimit ):
    """ This method returns the CE instance corresponding to the local OS. The Linux watchdog is returned by default.
    """
    if re.search( 'Darwin', self.version[0] ):
      localOS = 'Mac'
      self.log.info( 'WatchdogFactory will create Watchdog%s instance' % ( localOS ) )
#     elif re.search( 'Windows', self.version[0] ):
#       localOS = 'Windows'
#       self.log.info( 'WatchdogFactory will create Watchdog%s instance' % ( localOS ) )
    else:
      localOS = 'Linux'
      self.log.info( 'WatchdogFactory will create Watchdog%s instance' % ( localOS ) )

    subClassName = "Watchdog%s" % ( localOS )

    try:
      wdModule = __import__( self.watchDogsLocation + '.%s' % subClassName, globals(), locals(), [subClassName] )
    except ImportError, e:
      self.log.exception( "Failed to import module" + self.watchDogsLocation + '.%s' % subClassName + '.%s' % subClassName + ': ' + e )
      return S_ERROR( "Failed to import module" )
    try:
      wd_o = getattr( wdModule, subClassName )( pid, thread, spObject, jobcputime, memoryLimit )
      return S_OK( wd_o )
    except AttributeError, e:
      self.log.exception( "Failed to create %s(): %s." % ( subClassName, e ) )
      return S_ERROR( "Failed to create object" )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
