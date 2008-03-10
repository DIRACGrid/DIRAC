########################################################################
# $Id: PBSTimeLeft.py,v 1.1 2008/03/10 09:32:25 paterson Exp $
########################################################################

""" The PBS TimeLeft utility interrogates the PBS batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id: PBSTimeLeft.py,v 1.1 2008/03/10 09:32:25 paterson Exp $"

import os, string, re, time

class PBSTimeLeft:

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('PBSTimeLeft')
    self.jobID = None
    if os.environ.has_key('PBS_JOBID'):
      self.jobID = os.environ['PBS_JOBID']
    self.queue = None
    if os.environ.has_key('PBS_O_QUEUE'):
      self.queue = os.environ['PBS_O_QUEUE']
    if os.environ.has_key('PBS_O_PATH'):
      pbsPath = os.environ['PBS_O_PATH']
      os.environ['PATH'] = os.environ['PATH']+':'+pbsPath

    self.cpuLimit = None
    self.wallClockLimit = None
    self.log.verbose('PBS_JOBID=%s, PBS_O_QUEUE=%s' %(self.jobID,self.queue))

  #############################################################################
  def getResourceUsage(self):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    cmd = 'qstat -f %s' %(self.jobid)
    result = self.__runCommand(cmd)
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    lines = result['Value'].split('\n')
    for line in lines:
      info = line.split()
      if re.search('.*resources_used.cput.*',line):
        if len(info)>=3:
          cpuList = info[2].split(':')
          cpu = (float(cpuList[0])*60+float(cpuList[1]))*60+float(cpuList[2])
        else:
          self.log.warn('Poblem parsing "%s" for CPU consumed' %line)
      if re.search('.*resources_used.walltime.*',line):
        if len(info)>=3:
          wcList = info[2].split(':')
          wallClock = float(wcList[0])*60+float(wcList[1])*60+float(wcList[2])*60
        else:
          self.log.warn('Poblem parsing "%s" for elapsed wall clock time' %line)
      if re.search('.*Resource_List.cput.*',line):
        if len(info)>=3:
          cpuList = info[2].split(':')
          cpuLimit = (float(cpuList[0])*60+float(cpuList[1]))*60+float(cpuList[2])
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' %line)
      if re.search('.*Resource_List.walltime.*',line):
        if len(info)>=3:
          wcList = info[2].split(':')
          wallClockLimit = float(wcList[0])*60+float(wcList[1])*60+float(wcList[2])*60
        else:
          self.log.warn('Problem parsing "%s" for wall clock limit' %line)

    consumed = {'CPU':cpu,'CPULimit':cpuLimit,'WallClock':wallClock,'WallClockLimit':wallClockLimit}
    self.log.debug(consumed)
    failed = False
    for k,v in consumed.items():
      if not v:
        failed = True
        self.log.warn('Could not determine %s' %k)

    if not failed:
      return S_OK(consumed)
    else:
      self.log.info('Could not determine some parameters, this is the stdout from the batch system call\n%s' %(result['Value']))
      return S_ERROR('Could not determine some parameters')

  #############################################################################
  def __runCommand(self,cmd):
    """Wrapper around shellCall to return S_OK(stdout) or S_ERROR(message)
    """
    result = shellCall(0,cmd)
    if not result['OK']:
      return result
    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]

    if status:
      self.log.warn('Status %s while executing %s' %(status,cmd))
      self.log.warn(stderr)
      return S_ERROR(stdout)
    else:
      return S_OK(stdout)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#