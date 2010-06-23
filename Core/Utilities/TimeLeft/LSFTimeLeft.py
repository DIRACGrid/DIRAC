########################################################################
# $Id$
########################################################################

""" The LSF TimeLeft utility interrogates the LSF batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"

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

    self.cpuLimit = None
    self.cpuRef = None
    self.wallClockLimit = None
    self.hostNorm = None

    cmd = '%s/bqueues -l %s' %(self.bin,self.queue)
    result = self.__runCommand(cmd)
    if not result['OK']:
      return result

    self.log.debug(result['Value'])
    lines = result['Value'].split('\n')
    for i in xrange(len(lines)):
      if re.search('.*CPULIMIT.*',lines[i]):
        info = lines[i+1].split()
        if len(info)>=4:
          self.cpuLimit = float(info[0])*60
          self.cpuRef = info[3]
        else:
          self.log.warn('Problem parsing "%s" for CPU limit' % lines[i+1])
          self.cpuLimit = -1
      if re.search('.*RUNLIMIT.*',lines[i]):
        info = lines[i+1].split()
        if len(info)>=1:
          self.wallClockLimit = float(info[0])*60
        else:
          self.log.warn('Problem parsing "%s" for wall clock limit' % lines[i+1])


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

    cmd = '%s/bjobs -W %s' %( self.bin, self.jobID )
    result = self.__runCommand( cmd )
    if not result['OK']:
      return result
    lines = result['Value'].split( '\n' )
    l1 = lines[0].split()
    l2 = lines[1].split()
    if len(l1) > len(l2):
      self.log.error( cmd )
      self.log.error( lines[0] )
      self.log.error( lines[1] )
      return S_ERROR( 'Can not parse LSF output' )

    sCPU = None
    sStart = None
    host = None
    for i in range( len(l1) ):
      if l1[i] == 'CPU_USED':
        sCPU = l2[i]
        lCPU = sCPU.split( ':' )
        try:
          cpu = float(lCPU[0]) * 3600 + float(lCPU[1]) * 60 + float(lCPU[2]) 
        except:
          pass
      elif l1[i] == 'START_TIME':
        sStart = l2[i]
        sStart = '%s %s' %( sStart, self.year)
        try:
          timeTup=time.strptime(sStart, '%m/%d-%H:%M:%S %Y')
          wallClock=time.mktime(timeTup)
          wallClock = time.mktime(time.localtime())-wallClock
        except:
          pass
      elif l1[i] == 'EXEC_HOST':
        host = l2[i]
    if not self.hostNorm and host:
      cmd = '%s/lshosts %s' % ( self.bin, host )
      result = self.__runCommand(cmd)
      if not result['OK']:
        return result
      lines = result['Value'].split('\n') 
      l1 = lines[0].split()
      l2 = lines[1].split()
      if len(l1) > len(l2):
        self.log.error( cmd )
        self.log.error( lines[0] )
        self.log.error( lines[1] )
        return S_ERROR( 'Can not parse LSF output' )
      for i in range(len(l1)):
        if l1[i] == 'cpuf':
          try:
            self.hostNorm = float(l2[i])
          except:
            pass
      if self.hostNorm and self.cpuRef:
        cmd = '%s/lshosts %s' % ( self.bin, self.cpuRef )
        result = self.__runCommand(cmd)
        if result['OK']:
          # At CERN this command will return an error since there is no host defined 
          # with the name of the reference Host. The Reference Host has Norm factor = 1
          lines = result['Value'].split('\n') 
          l1 = lines[0].split()
          l2 = lines[1].split()
          if len(l1) > len(l2):
            self.log.error( cmd )
            self.log.error( lines[0] )
            self.log.error( lines[1] )
            return S_ERROR( 'Can not parse LSF output' )
          for i in range(len(l1)):
            if l1[i] == 'cpuf':
              try:
                self.hostNorm = self.hostNorm / float(l2[i])
              except:
                pass

    if not cpu or not wallClock:
      return S_ERROR( 'Failed to parse LSF output' )

    if not self.hostNorm:
      return S_ERROR( 'Can not determine host Norm factor' )

    cpu = cpu * self.hostNorm

    consumed = {'CPU':cpu,'CPULimit':self.cpuLimit,'WallClock':wallClock,'WallClockLimit':self.wallClockLimit}
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
      self.log.debug( stdout )
      return S_OK(stdout)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
