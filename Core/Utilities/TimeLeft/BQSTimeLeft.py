########################################################################
# $Id: BQSTimeLeft.py,v 1.1 2008/03/13 16:25:58 paterson Exp $
########################################################################

""" The BQS TimeLeft utility interrogates the BQS batch system for the
    current CPU consumed and CPU limit.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id: BQSTimeLeft.py,v 1.1 2008/03/13 16:25:58 paterson Exp $"

import os, string, re, time

class BQSTimeLeft:

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('BQSTimeLeft')
    self.jobID = None
    if os.environ.has_key('QSUB_REQNAME'):
      self.jobID = os.environ['QSUB_REQNAME']

    self.log.verbose('QSUB_REQNAME=%s' %(self.jobID))

  #############################################################################
  def getResourceUsage(self):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    if not self.jobID:
      return S_ERROR('Could not determine batch jobID from QSUB_REQNAME env var.')

    cmd = 'qjob -a -nh -wide %s' %(self.jobID)
    result = self.__runCommand(cmd)
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    try:
      cpuList = result['Value'].split()[5].split('/')
      cpu = cpuList[0]
      cpuLimit = cpuList[1]
    except Exception, x:
      self.log.warn('Problem parsing "%s" for CPU usage' %(result['Value']))

    #BQS has no wallclock limit so will simply return the same as for CPU to the TimeLeft utility
    wallClock = cpu
    wallClockLimit = cpuLimit
    consumed = {'CPU':cpu,'CPULimit':cpuLimit,'WallClock':wallClock,'WallClockLimit':wallClockLimit}
    self.log.debug(consumed)
    failed = False
    for k,v in consumed.items():
      if v==None:
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