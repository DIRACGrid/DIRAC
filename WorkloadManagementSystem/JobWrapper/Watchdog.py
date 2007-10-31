########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/JobWrapper/Watchdog.py,v 1.2 2007/10/31 22:14:22 paterson Exp $
# File  : Watchdog.py
# Author: Stuart Paterson
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system resource consumption.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.
     Furthermore, the Watchdog will identify when the Job CPU limit has been
     exceeded and fail jobs meaningfully.

     - Still to implement:
          - Heartbeat, composition of all necessary information
          - CPU normalization for comparison with job limit
     - Waiting for:
          - Job parameter reporting call
          - Means to send heartbeat signal.
"""

from DIRAC.Core.Base.Agent                          import Agent
#from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

import os,thread,time,shutil

AGENT_NAME = 'WorkloadManagement/Watchdog'

class Watchdog(Agent):

  def __init__(self, pid, thread, spObject, systemFlag='linux2.4'):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self,AGENT_NAME)
    self.systemFlag = systemFlag
    self.thread = thread
    self.pid = pid
    self.spObject = spObject
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
    self.log.debug('Attempting to Initialize Watchdog for: %s' % (self.systemFlag))
    #Test control flags
    self.testWallClock   = gConfig.getValue(self.section+'/CheckWallClockFlag',1)
    self.testDiskSpace   = gConfig.getValue(self.section+'/CheckDiskSpaceFlag',1)
    self.testLoadAvg     = gConfig.getValue(self.section+'/CheckLoadAvgFlag',1)
    self.testCPUConsumed = gConfig.getValue(self.section+'/CheckCPUConsumedFlag',1)
    self.testCPULimit    = gConfig.getValue(self.section+'/CheckCPULimitFlag',1)
    #Other parameters
    self.maxWallClockTime = gConfig.getValue(self.section+'/MaxWallClockTime',36*60*60) # e.g. 1.5 days
    self.jobPeekFlag      = gConfig.getValue(self.section+'/JobPeekFlag',1) # on / off
    self.minDiskSpace     = gConfig.getValue(self.section+'/MinDiskSpace',10) #MB
    self.loadAvgLimit     = gConfig.getValue(self.section+'/LoadAverageLimit',10) # > 10 and jobs killed
    self.sampleCPUTime    = gConfig.getValue(self.section+'/CPUSampleTime',15*60) # e.g. up to 15mins sample
    self.calibFactor      = gConfig.getValue(self.section+'/CPUFactor',2) #multiple of the cpu consumed during calibration
    self.heartbeatPeriod  = gConfig.getValue(self.section+'/HeartbeatPeriod',5) #mins
    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method of the Watchdog.
    """
    self.log.debug('------------------------------------')
    self.log.debug('Execution loop starts for Watchdog')
    if not self.thread.isAlive():
      print self.parameters
      self.getUsageSummary()
      self.log.info('Process to monitor has completed, Watchdog will exit.')
      self.finish()
      result = S_OK()
      return result

    msg = ''
    result = self.getLoadAverage()
    msg += 'LoadAvg: %s ' % (result['Value'])
    self.parameters['LoadAverage'].append(result['Value'])
    result = self.getMemoryUsed()
    msg += 'MemUsed: %.1f bytes ' % (result['Value'])
    self.parameters['MemoryUsed'].append(result['Value'])
    result = self.getDiskSpace()
    msg += 'DiskSpace: %.1f MB ' % (result['Value'])
    self.parameters['DiskSpace'].append(result['Value'])
    result = self.getCPUConsumed(self.pid)
    msg += 'CPU: %s (h:m:s) ' % (result['Value'])
    self.parameters['CPUConsumed'].append(result['Value'])
    result = self.getWallClockTime()
    msg += 'WallClock: %.2f s ' % (result['Value'])
    self.parameters['WallClockTime'].append(result['Value'])
    self.log.info(msg)

    result = self.checkProgress()
    if not result['OK']:
      self.log.error('Watchdog identified problem with running job')
      if self.jobPeekFlag:
        result = self.peek()
        if result['OK']:
          outputList = result['Value']
          size = len(outputList)
          self.log.info('Last %s lines of application output:' % (size) )
          for line in outputList:
            self.log.info(line)

      self.killRunningThread(self.spObject)
      self.finish()
      self.log.error(result['Message'])

    if self.jobPeekFlag:
      result = self.peek()
      if result['OK']:
        outputList = result['Value']
        size = len(outputList)
        self.log.info('Last %s lines of application output:' % (size) )
        for line in outputList:
          self.log.info(line)
      else:
        self.log.error('Watchdog could not obtain standard output from application thread')
        self.log.error('Turning off job peeking for remainder of execution')
        self.jobPeekFlag = 0

    result = S_OK()
    return result

  #############################################################################
  def checkProgress(self):
    """This method calls specific tests to determine whether the job execution
       is proceeding normally.  CS flags can easily be added to add or remove
       tests via central configuration.
    """
    report = ''

    if self.testWallClock:
      result = self.checkWallClockTime()
      report += 'WallClock: OK, '
      if not result['OK']:
        return result
    else:
      report += 'WallClock: NA'

    if self.testDiskSpace:
      result = self.checkDiskSpace()
      report += 'DiskSpace: OK, '
      if not result['OK']:
        return result
    else:
      report += 'DiskSpace: NA'

    if self.testLoadAvg:
      result = self.checkLoadAverage()
      report += 'LoadAverage: OK, '
      if not result['OK']:
        return result
    else:
      report += 'LoadAverage: NA'

    if self.testCPUConsumed:
      result = self.checkCPUConsumed()
      report += 'CPUConsumed: OK, '
      if not result['OK']:
        return result
    else:
      report += 'CPUConsumed: NA'

    self.log.info(report)
    return S_OK('All enabled checks passed')

  #############################################################################

  def checkCPUConsumed(self):
    """ Checks whether the CPU consumed is reasonable, taking Watchdog
        process into account via calibration. This method will report stalled
        jobs to be killed.
    """
    # to be implemented
    return S_OK()

  #############################################################################

  def convertCPUTime(self,cpu):
    """ Method to convert the CPU time as returned from the Watchdog
        instances to the equivalent DIRAC normalized CPU time to be compared
        to the Job CPU requirements etc.
    """
    #Normalization to be implemented
    return S_OK()

  #############################################################################
  def checkDiskSpace(self):
    """Checks whether the CS defined minimum disk space is available.
    """
    if self.parameters.has_key('DiskSpace'):
      availSpace = self.parameters['DiskSpace'][-1]
      if availSpace < self.minDiskSpace:
        self.info('Not enough local disk space for job to continue, defined in CS as %s MB' % (self.minDiskSpace))
      else:
        result = S_OK('Job has enough disk space available')
    else:
      return S_ERROR('Job has insufficient disk space to continue')

  #############################################################################
  def checkWallClockTime(self):
    """Checks whether the job has been running for the CS defined maximum
       wall clock time.
    """
    if self.initialValues.has_key('StartTime'):
      startTime = self.initialValues['StartTime']
      if time.time() - startTime > self.maxWallClockTime:
        self.info('Job has exceeded maximum wall clock time of %s seconds' % (self.maxWallClockTime))
      else:
        result = S_OK('Job within maximum wall clock time')
    else:
      return S_ERROR('Job start time could not be established')

  #############################################################################
  def checkLoadAverage(self):
    """Checks whether the CS defined maximum load average is exceeded.
    """
    if self.parameters.has_key('LoadAverage'):
      loadAvg = self.parameters['LoadAverage'][-1]
      if loadAvg > float(self.loadAvgLimit):
        self.info('Maximum load average exceeded, defined in CS as %s ' % (self.loadAvgLimit))
      else:
        result = S_OK('Job running with normal load average')
    else:
      return S_ERROR('Job exceeded maximum load average')

  #############################################################################
  def peek(self):
    """ Uses ExecutionThread.getOutput() method to obtain standard output
        from running thread via subprocess callback function.
    """
    result = self.thread.getOutput()
    if not result['OK']:
      self.log.error('Could not obtain output from running application thread')
      self.log.error(result['Message'])

    return result

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
    fd = open(self.controlDir+'/stop_agent','w')
    fd.write('Watchdog Agent Stopped at %s' % (time.asctime()))
    fd.close()

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
    value = space[-1] - self.initialValues['DiskSpace']
    if value < 0:
      value = 0
    summary['DiskSpace(MB)'] = value
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
  def killRunningThread(self,spObject):
    """ Will kill the running thread process"""
    #To implement
    #thread.stop()
    os.kill( spObject.child.pid, 0 )
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