########################################################################
# $Id: LSFTimeLeft.py,v 1.5 2008/03/10 17:43:23 paterson Exp $
########################################################################

""" The LSF TimeLeft utility interrogates the LSF batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id: LSFTimeLeft.py,v 1.5 2008/03/10 17:43:23 paterson Exp $"

import os, string, re, time

class LSFTimeLeft:

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('LSFTimeLeft')
    self.jobID = None
    if os.environ.has_key('LSB_JOBID'):
      self.jobID = os.environ['LSB_JOBID']
    self.queue = None
    if os.environ.has_key('LSB_QUEUE'):
      self.queue = os.environ['LSB_QUEUE']
    self.bin = None
    if os.environ.has_key('LSF_BINDIR'):
      self.bin = os.environ['LSF_BINDIR']
    self.year = time.strftime('%Y',time.gmtime())
    self.log.verbose('LSB_JOBID=%s, LSB_QUEUE=%s, LSF_BINDIR=%s' %(self.jobID,self.queue,self.bin))

  #############################################################################
  def getResourceUsage(self):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    if not self.bin:
      return S_ERROR('Could not determine bin directory for LSF')

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    cmd = '%s/bjobs -l %s' %(self.bin,self.jobID)
    result = self.__runCommand(cmd)
    if not result['OK']:
      return result

    self.log.debug(result['Value'])
    lines = result['Value'].split('\n')
    for line in lines:
      if re.search('.*Started on.*',line):
        info = line.split(': ')
        if len(info)>=1:
          timeStr = '%s %s' %(info[0],self.year)
          timeTup=time.strptime(timeStr, '%a %b %d %H:%M:%S %Y')
          wallClock=time.mktime(timeTup)
          if time.daylight==1:
            wallClock -= 3600
          wallClock = time.time()-wallClock
        else:
          self.log.warn('Problem parsing "%s" for elapsed wall clock time' %line)
      if re.search('.*The CPU time used is.*',line):
        info = line.split()
        if len(info)>=5:
          cpu = float(info[5])
        else:
          self.log.warn('Problem parsing "%s" for CPU consumed' %line)

    cmd = '%s/bqueues -l %s' %(self.bin,self.queue)
    result = self.__runCommand(cmd)
    if not result['OK']:
      return result

    self.log.debug(result['Value'])
    lines = result['Value'].split('\n')
    for i in xrange(len(lines)):
      if re.search('.*CPULIMIT.*',lines[i]):
        info = lines[i+1].split()
        if len(info)>=1:
          cpuLimit = float(info[0])*60
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' %line)
      if re.search('.*RUNLIMIT.*',lines[i]):
        info = lines[i+1].split()
        if len(info)>=1:
          wallClockLimit = float(info[0])*60
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