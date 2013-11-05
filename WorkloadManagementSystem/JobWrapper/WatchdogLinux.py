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

import socket

class WatchdogLinux(Watchdog):

  def __init__(self, pid, thread, spObject, jobCPUtime, systemFlag='linux'):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__(self, pid, thread, spObject, jobCPUtime, systemFlag)
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
      infofile = open ("/proc/cpuinfo","r")
      info =  infofile.readlines()
      infofile.close()
      result["HostName"] = socket.gethostname()
      result["CPU(MHz)"]   = info[6].split(":")[1].replace(" ","").replace("\n","")
      result["ModelName"] = info[4].split(":")[1].replace(" ","").replace("\n","")
      result["CacheSize(kB)"] = info[7].split(":")[1].replace(" ","").replace("\n","")
      infofile = open ("/proc/meminfo", "r")
      info =  infofile.readlines()
      infofile.close()
      result["Memory(kB)"] =  info[3].split(":")[1].replace(" ","").replace("\n","")
      account = 'Unknown'
      localID = shellCall(10,'whoami')
      if localID['OK']:
        account = localID['Value'][1].strip()
      result["LocalAccount"] = account
    except Exception, x:
      self.log.fatal('Watchdog failed to obtain node information with Exception:')
      self.log.fatal(str(x))
      return S_ERROR('Failed to obtain system information for '+self.systemFlag)

    return result

  ############################################################################
  def getLoadAverage(self):
    """Obtains the load average.
    """
    result = S_OK()
    comm = '/bin/cat /proc/loadavg'
    loadAvgDict = shellCall(5, comm)
    if loadAvgDict['OK']:
      la = float(loadAvgDict['Value'][1].split()[0])
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
    memDict = shellCall(5, comm)
    if memDict['OK']:
      mem = memDict['Value'][1].split() [8]
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
