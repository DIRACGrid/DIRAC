########################################################################
# $HeadURL$
# Author: Stuart Paterson
# eMail : Stuart.Paterson@cern.ch
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system CPU and memory consumed.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.

     This is the Unix / Linux compatible Watchdog subclass.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog  import Watchdog
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Core.Utilities.Os import getDiskSpace

import string
import socket

class WatchdogLinux(Watchdog):

  def __init__(self, pid, thread, spObject, jobCPUtime, memoryLimit = 0, systemFlag='linux'):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__( self, pid, thread, spObject, jobCPUtime, memoryLimit, systemFlag )
    self.systemFlag = systemFlag
    self.pid = pid

  ############################################################################
  def getNodeInformation(self):
    """Try to obtain system HostName, CPU, Model, cache and memory.  This information
       is not essential to the running of the jobs but will be reported if
       available.
    """
    result = S_OK()
    try:
      _file = open ("/proc/cpuinfo","r")
      info =  file.readlines()
      _file.close()
      result["HostName"] = socket.gethostname()
      result["CPU(MHz)"]   = string.replace(string.replace(string.split(info[6],":")[1]," ",""),"\n","")
      result["ModelName"] = string.replace(string.replace(string.split(info[4],":")[1]," ",""),"\n","")
      result["CacheSize(kB)"] = string.replace(string.replace(string.split(info[7],":")[1]," ",""),"\n","")
      _file = open ("/proc/meminfo","r")
      info =  file.readlines()
      _file.close()
      result["Memory(kB)"] =  string.replace(string.replace(string.split(info[3],":")[1]," ",""),"\n","")
      account = 'Unknown'
      localID = shellCall(10,'whoami')
      if localID['OK']:
        account = localID['Value'][1].strip()
      result["LocalAccount"] = account
    except Exception, x:
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
    result = S_OK()
    comm = '/bin/cat /proc/loadavg'
    loadAvgDict = shellCall(5,comm)
    if loadAvgDict['OK']:
      la = float(string.split(loadAvgDict['Value'][1])[0])
      result['Value'] = la
    else:
      result = S_ERROR('Could not obtain load average')
      self.log.warn('Could not obtain load average')
      result['Value'] = 0

    return result

  #############################################################################
  def getMemoryUsed(self):
    """Obtains the memory used.
    """
    result = S_OK()
    comm = '/usr/bin/free'
    memDict = shellCall(5,comm)
    if memDict['OK']:
      mem = string.split(memDict['Value'][1]) [8]
      result['Value'] = float(mem)
    else:
      result = S_ERROR('Could not obtain memory used')
      self.log.warn('Could not obtain memory used')
      result['Value'] = 0
    return result

  #############################################################################
  def getDiskSpace(self):
    """Obtains the disk space used.
    """
    result = S_OK()
    diskSpace = getDiskSpace()

    if diskSpace == -1:
      result = S_ERROR('Could not obtain disk usage')
      self.log.warn('Could not obtain disk usage')
      result['Value'] = -1

    result['Value'] = float(diskSpace)
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
