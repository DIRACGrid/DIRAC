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

import os
import socket
import resource
import getpass

from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Os import getDiskSpace


class WatchdogLinux(Watchdog):

  def __init__(self, pid, exeThread, spObject, jobCPUTime,
               memoryLimit=0, processors=1, systemFlag='linux', jobArgs={}):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__(self,
                      pid=pid,
                      exeThread=exeThread,
                      spObject=spObject,
                      jobCPUTime=jobCPUTime,
                      memoryLimit=memoryLimit,
                      processors=processors,
                      systemFlag=systemFlag,
                      jobArgs=jobArgs)

  ############################################################################
  def getNodeInformation(self):
    """Try to obtain system HostName, CPU, Model, cache and memory.  This information
       is not essential to the running of the jobs but will be reported if
       available.
    """
    result = S_OK()
    try:
      result["HostName"] = socket.gethostname()
      with open("/proc/cpuinfo", "r") as cpuInfo:
        info = cpuInfo.readlines()
        result["CPU(MHz)"] = info[7].split(':')[1].replace(' ', '').replace('\n', '')
        result["ModelName"] = info[4].split(':')[1].replace(' ', '').replace('\n', '')
        result["CacheSize(kB)"] = info[8].split(':')[1].replace(' ', '').replace('\n', '')
      with open("/proc/meminfo", "r") as memInfo:
        info = memInfo.readlines()
        result["Memory(kB)"] = info[3].split(':')[1].replace(' ', '').replace('\n', '')
      result["LocalAccount"] = getpass.getuser()
    except Exception as x:
      self.log.fatal('Watchdog failed to obtain node information with Exception:')
      self.log.fatal(str(x))
      result = S_ERROR()
      result['Message'] = 'Failed to obtain system information for ' + self.systemFlag
      return result

    return result

  ############################################################################
  def getLoadAverage(self):
    """Obtains the load average.
    """
    return S_OK(float(os.getloadavg()[0]))

  #############################################################################
  def getMemoryUsed(self):
    """Obtains the memory used.
    """
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss + \
        resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return S_OK(float(mem))

  #############################################################################
  def getDiskSpace(self, exclude=None):
    """Obtains the disk space used.
    """
    result = S_OK()
    diskSpace = getDiskSpace(exclude=exclude)

    if diskSpace == -1:
      result = S_ERROR('Could not obtain disk usage')
      self.log.warn(' Could not obtain disk usage')
      result['Value'] = -1

    result['Value'] = float(diskSpace)
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
