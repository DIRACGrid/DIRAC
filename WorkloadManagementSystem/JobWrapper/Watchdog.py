########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/JobWrapper/Watchdog.py,v 1.1 2007/10/29 17:38:49 paterson Exp $
# File  : Watchdog.py
# Author: Stuart Paterson
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system CPU and memory consumed.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.

     This is a prototype and is currently untidy... other caveats include:
     - log levels need to be set correctly
     - still to implement:
          - checkProgress()  - this makes the important decisions
          - killRunningThread()
          - sendSignOfLife()
     - still to think about other messages etc.
     - need to add call to report job parameters.
"""

from DIRAC.Core.Base.Agent                          import Agent
#from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR
import os,thread,time

AGENT_NAME = 'WorkloadManagement/Watchdog'

class Watchdog(Agent):

  def __init__(self, pid, thread, systemFlag='linux2.4'):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self,AGENT_NAME)
    self.systemFlag = systemFlag
    self.thread = thread
    self.pid = pid
    self.initialValues = {}
    self.parameters = {}

  #############################################################################
  def initialize(self,loops=0):
    """ Watchdog initialization.
    """
    self.maxcount = loops
    result = Agent.initialize(self)
    if os.path.exists(self.controlDir+'/stop_agent'):
      os.remove(self.controlDir+'/stop_agent')
    self.log.debug('Watchdog initialization')
    self.log.info('Attempting to Initialize Watchdog for: %s' % (self.systemFlag))
    self.maxWallClockTime = gConfig.getValue(self.section+'/MaxWallClockTime',24*60*60) # 1 day
    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method of the Watchdog.
    """
    self.log.info('------------------------------------')
    self.log.info('Execution loop starts for Watchdog')
    if not self.thread.isAlive():
      print self.parameters
      self.getUsageSummary()
      self.log.info('Process to monitor has completed, Watchdog will exit.')
      self.finish()
      result = S_OK()
      return result

    result = self.getLoadAverage()
    self.log.info('LoadAverage: %s' % (result['Value']) )
    self.parameters['LoadAverage'].append(result['Value'])
    result = self.getMemoryUsed()
    self.log.info('MemoryUsed: %.1f bytes' % (result['Value']) )
    self.parameters['MemoryUsed'].append(result['Value'])
    result = self.getDiskSpace()
    self.log.info('DiskSpace: %.1f MB' % (result['Value']) )
    self.parameters['DiskSpace'].append(result['Value'])
    result = self.getCPUConsumed(self.pid)
    self.log.info('CPUConsumed: %s (hours:mins:secs)' % (result['Value']) )
    self.parameters['CPUConsumed'].append(result['Value'])
    result = self.getWallClockTime()
    self.log.info('WallClockTime: %.2f s' % (result['Value']) )
    self.parameters['WallClockTime'].append(result['Value'])

    result = self.checkProgress()
    if not result['OK']:
      self.killRunningThread(self.thread)

    result = S_OK()
    return result

  #############################################################################
  def checkProgress(self):
    """This method calls specific tests to determine whether the job execution
       is proceeding normally.
    """
    #to Implement
    return S_OK()

  #############################################################################
  def calibrate(self):
    """ The calibrate method obtains the initial values for system memory and load
        and calculates the margin for error for the rest of the Watchdog cycle.
    """
    self.getWallClockTime()
    self.parameters['WallClockTime'] = []

    result = self.getCPUConsumed(self.pid)
    if not result['OK']:
      msg = 'Could not establish CPU consumed'
      self.log.error(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['CPUConsumed']=result['Value']
    self.parameters['CPUConsumed'] = []

    result = self.getLoadAverage()
    if not result['OK']:
      msg = 'Could not establish LoadAverage'
      self.log.error(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['LoadAverage']=result['Value']
    self.parameters['LoadAverage'] = []

    result = self.getMemoryUsed()
    if not result['OK']:
      msg = 'Could not establish MemoryUsed'
      self.log.error(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['MemoryUsed']=result['Value']
    self.parameters['MemoryUsed'] = []

    result = self. getDiskSpace()
    if not result['OK']:
      msg = 'Could not establish DiskSpace'
      self.log.error(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['DiskSpace']=result['Value']
    self.parameters['DiskSpace'] = []

    result = self.getNodeInformation()
    if not result['OK']:
      msg = 'Could not establish static system information'
      self.log.error(msg)
      result = S_ERROR(msg)
      return result

    if os.environ.has_key('LSB_JOBID'):
      result['Value']['LocalJobID'] = os.environ['LSB_JOBID']
    if os.environ.has_key('PBS_JOBID'):
      result['Value']['LocalJobID'] = os.environ['PBS_JOBID']
    if os.environ.has_key('QSUB_REQNAME'):
      result['Value']['LocalJobID'] = os.environ['QSUB_REQNAME']

    self.reportParameters(result,'NodeInformation')
    self.reportParameters(self.initialValues,'InitialValues')

    result = S_OK()
    return result

  #############################################################################
  def finish(self):
    """Force the Watchdog to complete gracefully.
    """
    self.log.info('Watchdog has completed monitoring of the task')
    shellCall(5,'touch '+self.controlDir+'/stop_agent')

  #############################################################################
  def getUsageSummary(self):
    """ Returns average load, memory etc. over execution of job thread
    """
    summary = {}
    #CPUConsumed
    cpuList = self.parameters['CPUConsumed']
    summary['CPUConsumed(secs)'] = cpuList[-1] # to do, the initial value should be subtracted
    #DiskSpace
    space = self.parameters['DiskSpace']
    summary['DiskSpace(MB)'] = space[-1] - self.initialValues['DiskSpace']
    #MemoryUsed
    memory = self.parameters['MemoryUsed']
    summary['MemoryUsed(bytes)'] = memory[-1] - self.initialValues['MemoryUsed']
    #LoadAverage
    laList = self.parameters['LoadAverage']
    la = 0.0
    for load in laList: la += load
    summary['LoadAverage'] = float(la) / float(len(laList))
    self.reportParameters(summary,'UsageSummary')

  #############################################################################
  def reportParameters(self,params,title=None):
    """Will report parameters for job.
    """
    #To implement
    self.log.info('==========================================================')
    if title:
      self.log.info('Watchdog will report %s' % (title))
    else:
      self.log.info('Watchdog will report parameters')
    self.log.info('==========================================================')
    vals = params
    if params.has_key('Value'):
      vals = params['Value']
    for k,v in vals.items():
      self.log.info(str(k)+' = '+str(v))
    self.log.info('==========================================================')

  #############################################################################
  def getWallClockTime(self):
    """ Establishes the Wall Clock time spent since the Watchdog initialization"""
    result = S_OK()
    if self.initialValues.has_key('StartTime'):
      currentTime = time.time()
      wallClock = currentTime - self.initialValues['StartTime']
      result['Value'] = wallClock
    else:
      self.initialValues['StartTime'] = time.time()

    return result

  #############################################################################
  def killRunningThread(self,thread):
    """ Will kill the running thread process"""
    #To implement
    #thread.stop()
    return S_OK()

  #############################################################################
  def sendSignOfLife(self):
    """ Will send sign of life 'heartbeat' signal"""
    #To implement
    return S_OK()

  #############################################################################
  def getNodeInformation(self):
    """ Attempts to retrieve all static system information, should be overridden in a subclass"""
    methodName = 'getNodeInformation'
    self.log.error('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getLoadAverage(self):
    """ Attempts to get the load average, should be overridden in a subclass"""
    methodName = 'getLoadAverage'
    self.log.error('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getMemoryUsed(self):
    """ Attempts to get the memory used, should be overridden in a subclass"""
    methodName = 'getMemoryUsed'
    self.log.error('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getDiskSpace(self):
    """ Attempts to get the available disk space, should be overridden in a subclass"""
    methodName = 'getDiskSpace'
    self.log.error('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getCPUConsumed(self):
    """ Attempts to get the CPU consumed, should be overridden in a subclass"""
    methodName = 'getCPUConsumed'
    self.log.error('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#