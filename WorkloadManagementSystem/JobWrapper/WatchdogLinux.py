########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/JobWrapper/WatchdogLinux.py,v 1.3 2007/12/06 21:36:39 paterson Exp $
# Author: Stuart Paterson
# eMail : Stuart.Paterson@cern.ch
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system CPU and memory consumed.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.

     This is the Unix / Linux compatible Watchdog subclass.
"""

__RCSID__ = "$Id: WatchdogLinux.py,v 1.3 2007/12/06 21:36:39 paterson Exp $"

from DIRAC.Core.Base.Agent                              import Agent
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.Core.Utilities.Subprocess                    import shellCall
from DIRAC                                              import S_OK, S_ERROR

import string,re

class WatchdogLinux(Watchdog):

  def __init__(self, pid, thread, spObject, jobCPUtime, systemFlag='mac'):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__(self,pid,thread,spObject,jobCPUtime,systemFlag)
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
      file = open ("/proc/cpuinfo","r")
      info =  file.readlines()
      file.close()
      #Value  = {"CPU":6, "MODEL":4,"CACHE":7}
      result["HostName"] = socket.gethostname()
      result["CPU"]   = string.replace(string.replace(string.split(info[6],":")[1]," ",""),"\n","")
      result["Model"] = string.replace(string.replace(string.split(info[4],":")[1]," ",""),"\n","")
      result["Cache"] = string.replace(string.replace(string.split(info[7],":")[1]," ",""),"\n","")
      file = open ("/proc/meminfo","r")
      info =  file.readlines()
      file.close()
      result["Memory"] =  string.replace(string.replace(string.split(info[3],":")[1]," ",""),"\n","")
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

    return result

 #############################################################################
  def getDiskSpace(self):
    """Obtains the disk space used.
    """
    result = S_OK()
    comm = 'df -P -m .'
    spaceDict = shellCall(5,comm)
    print spaceDict
    if spaceDict['OK']:
      space = string.split(spcaeDict['Value'][1]) [10]
      result['Value'] = float(mem)
    else:
      result = S_ERROR('Could not obtain disk usage')

    return result

 #############################################################################
  def getCPUConsumed(self,pid):
    """Obtains the CPU consumed via PID.
    """
    result = S_OK()
    comm = ' ps -p '+str(pid)+' -o time | grep -v TIME'
    cpuDict = shellCall(5,comm)
    if cpuDict['OK']:
      cpu = string.split(cpuDict['Value'][1]) [0]
      result['Value'] = cpu
    else:
      result = S_ERROR('Could not obtain CPU consumed')

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
