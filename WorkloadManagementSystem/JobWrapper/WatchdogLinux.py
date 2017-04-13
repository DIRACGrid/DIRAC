########################################################################
# Author: Stuart Paterson
# eMail : Stuart.Paterson@cern.ch
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system CPU and memory consumed.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.

     This is the Unix / Linux compatible Watchdog subclass.
"""

__RCSID__ = "$Id$"

import socket

from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog  import Watchdog
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Core.Utilities.Os import getDiskSpace


class WatchdogLinux( Watchdog ):

  def __init__( self, pid, thread, spObject, jobCPUtime, memoryLimit = 0, processors = 1, systemFlag = 'linux' ):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__( self, pid, thread, spObject, jobCPUtime, memoryLimit, processors, systemFlag )

  ############################################################################
  def getNodeInformation( self ):
    """Try to obtain system HostName, CPU, Model, cache and memory.  This information
       is not essential to the running of the jobs but will be reported if
       available.
    """
    result = S_OK()
    try:
      result["HostName"] = socket.gethostname()
      with open( "/proc/cpuinfo", "r" ) as cpuInfo:
        info = cpuInfo.readlines()
        result["CPU(MHz)"] = info[7].split(':')[1].replace(' ', '').replace('\n', '')
        result["ModelName"] = info[4].split(':')[1].replace(' ', '').replace('\n', '')
        result["CacheSize(kB)"] = info[8].split(':')[1].replace(' ', '').replace('\n', '')
      with open( "/proc/meminfo", "r" ) as memInfo:
        info = memInfo.readlines()
        result["Memory(kB)"] = info[3].split(':')[1].replace(' ', '').replace('\n', '')
      account = 'Unknown'
      localID = shellCall(10,'whoami')
      if localID['OK']:
        account = localID['Value'][1].strip()
      result["LocalAccount"] = account
    except Exception as x:
      self.log.fatal('Watchdog failed to obtain node information with Exception:')
      self.log.fatal(str(x))
      result = S_ERROR()
      result['Message']='Failed to obtain system information for '+self.systemFlag
      return result

    return result

  ############################################################################
  def getLoadAverage(self):
    """Obtains the load average.
    """
    comm = '/bin/cat /proc/loadavg'
    loadAvgDict = shellCall( 5, comm )
    if loadAvgDict['OK']:
      return S_OK( float( loadAvgDict['Value'][1].split( )[0] ) )
    else:
      self.log.warn( 'Could not obtain load average' )
      return S_ERROR( 'Could not obtain load average' )

  #############################################################################
  def getMemoryUsed(self):
    """Obtains the memory used.
    """
    comm = '/usr/bin/free'
    memDict = shellCall( 5, comm )
    if memDict['OK']:
      mem = memDict['Value'][1].split()[8]
      return S_OK( float( mem ) )
    else:
      self.log.warn( 'Could not obtain memory used' )
      return S_ERROR( 'Could not obtain memory used' )

  #############################################################################
  def getDiskSpace(self):
    """Obtains the disk space used.
    """
    result = S_OK()
    diskSpace = getDiskSpace()

    if diskSpace == -1:
      result = S_ERROR( 'Could not obtain disk usage' )
      self.log.warn( ' Could not obtain disk usage' )
      result['Value'] = -1

    result['Value'] = float( diskSpace )
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
