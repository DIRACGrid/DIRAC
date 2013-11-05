########################################################################
# $HeadURL$
# Author: Stuart Paterson
# eMail : Stuart.Paterson@cern.ch
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system CPU and memory consumed.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.

     This is the Mac compatible Watchdog subclass.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.Core.Utilities.Subprocess                    import shellCall
from DIRAC                                              import S_OK, S_ERROR

import re

class WatchdogMac(Watchdog):

  def __init__(self, pid, thread, spObject, jobCPUtime, systemFlag='mac'):
    """ Constructor, takes system flag as argument.
    """
    Watchdog.__init__(self, pid, thread, spObject, jobCPUtime, systemFlag)
    self.systemFlag = systemFlag
    self.pid = pid

  #############################################################################
  def getNodeInformation(self):
    """Try to obtain system HostName, CPU, Model, cache and memory.  This information
       is not essential to the running of the jobs but will be reported if
       available.
    """
    result = S_OK()
    result['Value'] = {}
    comm = 'sysctl -a'
    parameterDict = shellCall(5, comm)
    if parameterDict['OK']:
      info = parameterDict['Value'][1].split('\n')
      for val in info:
        if re.search('^kern.hostname', val):
          hostname = 'NA'
          if re.search('=', val):
            hostname = val.split('=')[1].strip()
          else:
            hostname = val.split(':')[1].strip()  
          result['Value']['HostName'] = hostname
        if re.search('^hw.model', val):
          model = val.split('=')[1].strip()
          result['Value']['Model'] = model
        if re.search('^hw.machine', val):
          cpu = val.split('=')[1].strip()
          result['Value']['CPU'] = cpu
        if re.search('^hw.cachelinesize =', val):
          cache = val.split('=')[1].strip()+'KB'
          result['Value']['Cache'] = cache
        if re.search('^hw.memsize =', val):
          memory = str(int(val.split('=')[1].strip())/2**20)+'MB'
          result['Value']['Memory'] = memory
      account = 'Unknown'
      localID = shellCall(10,'whoami')
      if localID['OK']:
        account = localID['Value'][1].strip()
      result['LocalAccount'] = account
    else:
      result = S_ERROR('Could not obtain system information')

    return result

  #############################################################################
  def getLoadAverage(self):
    """Obtains the load average.
    """
    result = S_OK()
    comm = 'sysctl vm.loadavg'
    loadAvgDict = shellCall(5, comm)
    if loadAvgDict['OK']:
      la = float(loadAvgDict['Value'][1].split()[3])
      result['Value'] = la
    else:
      result = S_ERROR('Could not obtain load average')

    return result

  #############################################################################
  def getMemoryUsed(self):
    """Obtains the memory used.
    """
    result = S_OK()
    comm = 'sysctl vm.swapusage'
    memDict = shellCall(5, comm)
    if memDict['OK']:
      mem = memDict['Value'][1].split() [6]
      if re.search('M$', mem):
        mem = float(mem.replace('M', ''))
        mem = 2**20*mem

      result['Value'] = float(mem)
    else:
      result = S_ERROR('Could not obtain memory used')

    return result

  #############################################################################
  def getDiskSpace(self):
    """Obtains the available disk space.
    """
    result = S_OK()
    comm = 'df -P -m .'
    spaceDict = shellCall(5, comm)
    if spaceDict['OK']:
      space = spaceDict['Value'][1].split() [10]
      result['Value'] = float(space)  # MB
    else:
      result = S_ERROR('Could not obtain disk usage')

    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

