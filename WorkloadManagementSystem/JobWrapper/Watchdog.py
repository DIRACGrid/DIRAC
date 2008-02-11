########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/JobWrapper/Watchdog.py,v 1.20 2008/02/11 21:19:31 paterson Exp $
# File  : Watchdog.py
# Author: Stuart Paterson
########################################################################

"""  The Watchdog class is used by the Job Wrapper to resolve and monitor
     the system resource consumption.  The Watchdog can determine if
     a running job is stalled and indicate this to the Job Wrapper.
     Furthermore, the Watchdog will identify when the Job CPU limit has been
     exceeded and fail jobs meaningfully.

     Information is returned to the WMS via the heart-beat mechanism.  This
     also interprets control signals from the WMS e.g. to kill a running
     job.

     - Still to implement:
          - CPU normalization for correct comparison with job limit
"""

__RCSID__ = "$Id: Watchdog.py,v 1.20 2008/02/11 21:19:31 paterson Exp $"

from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

import os,thread,time,shutil

AGENT_NAME = 'WorkloadManagement/Watchdog'

class Watchdog(Agent):

  def __init__(self, pid, exeThread, spObject, jobCPUtime, systemFlag='linux2.4'):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self,AGENT_NAME)
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.systemFlag = systemFlag
    self.exeThread = exeThread
    self.wrapperPID = pid
    self.appPID = self.exeThread.getCurrentPID()
    self.spObject = spObject
    self.jobCPUtime = jobCPUtime
    self.watchdogCPU = 0
    self.calibration = 0
    self.initialValues = {}
    self.parameters = {}
    self.peekFailCount = 0
    self.peekRetry = 5

  #############################################################################
  def initialize(self,loops=0):
    """ Watchdog initialization.
    """
    self.maxcount = loops
    result = Agent.initialize(self)
    if os.path.exists(self.controlDir+'/stop_agent'):
      os.remove(self.controlDir+'/stop_agent')
    self.log.verbose('Watchdog initialization')
    self.log.info('Attempting to Initialize Watchdog for: %s' % (self.systemFlag))
    #Test control flags
    self.testWallClock   = gConfig.getValue(self.section+'/CheckWallClockFlag',1)
    self.testDiskSpace   = gConfig.getValue(self.section+'/CheckDiskSpaceFlag',1)
    self.testLoadAvg     = gConfig.getValue(self.section+'/CheckLoadAvgFlag',1)
    self.testCPUConsumed = gConfig.getValue(self.section+'/CheckCPUConsumedFlag',1)
    self.testCPULimit    = gConfig.getValue(self.section+'/CheckCPULimitFlag',1)
    #Other parameters
    self.pollingTime      = gConfig.getValue(self.section+'/PollingTime',5*60)
    self.minPollingTime   = gConfig.getValue(self.section+'/MinPollingTime',60)
    self.maxWallClockTime = gConfig.getValue(self.section+'/MaxWallClockTime',48*60*60) # e.g.2 days
    self.jobPeekFlag      = gConfig.getValue(self.section+'/JobPeekFlag',1) # on / off
    self.minDiskSpace     = gConfig.getValue(self.section+'/MinDiskSpace',10) #MB
    self.loadAvgLimit     = gConfig.getValue(self.section+'/LoadAverageLimit',100) # > 100 and jobs killed
    self.sampleCPUTime    = gConfig.getValue(self.section+'/CPUSampleTime',20*60) # e.g. up to 20mins sample
    self.calibFactor      = gConfig.getValue(self.section+'/CPUFactor',0.5) # multiple of the cpu consumed during calibration
    self.heartbeatPeriod  = gConfig.getValue(self.section+'/HeartbeatPeriod',5) #mins
    self.jobCPUMargin     = gConfig.getValue(self.section+'/JobCPULimitMargin',10) # %age buffer before killing job
    self.peekOutputLines  = gConfig.getValue(self.section+'/PeekOutputLines',5) # regularly printed # lines (up to)
    self.finalOutputLines = gConfig.getValue(self.section+'/FinalOutputLines',50) # lines to print after failure (up to)
    self.minCPUWallClockRatio  = gConfig.getValue(self.section+'/MinCPUWallClockRatio',5) #ratio %age
    self.__getAppPID()
    if self.pollingTime < self.minPollingTime:
      self.log.info('Requested PollingTime of %s setting to %s seconds (minimum)' %(self.pollingTime,self.minPollingTime))
      self.pollingTime=self.minPollingTime
    return result

  #############################################################################
  def __getAppPID(self):
    """ This function attempts to obtain the application PID.
    """
    self.log.verbose('PID of child is %s' %self.appPID)
    if self.appPID==self.wrapperPID or not int(self.appPID):
      self.log.verbose('Attempting to get PID of child')
      self.appPID = self.exeThread.getCurrentPID()

    if not int(self.appPID):
      self.appPID = self.wrapperPID
      self.log.info('Could not establish distinct child PID so monitoring job wrapper process...')

  #############################################################################
  def execute(self):
    """ The main agent execution method of the Watchdog.
    """
    if self.appPID == self.wrapperPID or not int(self.appPID):
      self.__getAppPID()

    self.log.info('------------------------------------')
    self.log.info('Execution loop starts for Watchdog')
    if not self.exeThread.isAlive():
      #print self.parameters
      self.getUsageSummary()
      self.log.info('Process to monitor has completed, Watchdog will exit.')
      self.finish()
      result = S_OK()
      return result

    heartBeatDict = {}
    msg = ''
    result = self.getLoadAverage()
    msg += 'LoadAvg: %s ' % (result['Value'])
    heartBeatDict['LoadAverage'] = result['Value']
    self.parameters['LoadAverage'].append(result['Value'])
    result = self.getMemoryUsed()
    msg += 'MemUsed: %.1f bytes ' % (result['Value'])
    heartBeatDict['MemoryUsed'] = result['Value']
    self.parameters['MemoryUsed'].append(result['Value'])
    result = self.getDiskSpace()
    msg += 'DiskSpace: %.1f MB ' % (result['Value'])
    self.parameters['DiskSpace'].append(result['Value'])
    heartBeatDict['AvailableDiskSpace'] = result['Value']
    result = self.getCPUConsumed(self.appPID)
    msg += 'CPU: %s (h:m:s) ' % (result['Value'])
    self.parameters['CPUConsumed'].append(result['Value'])
    heartBeatDict['CPUConsumed'] = result['Value']
    if self.appPID != self.wrapperPID:
      result = self.getCPUConsumed(self.wrapperPID)
      msg += 'WrapperCPU: %s (h:m:s) ' % (result['Value'])
      self.parameters['WrapperCPUConsumed'].append(result['Value'])
      heartBeatDict['WrapperCPUConsumed'] = result['Value']

    #Estimate Watchdog overhead
    if not self.watchdogCPU:
      self.watchdogCPU=result['Value']
    result = self.getWallClockTime()
    msg += 'WallClock: %.2f s ' % (result['Value'])
    self.parameters['WallClockTime'].append(result['Value'])

    self.log.info(msg)

    result = self.checkProgress()
    if not result['OK']:
      self.log.warn(result['Message'])
      if self.jobPeekFlag:
        result = self.peek(self.finalOutputLines)
        if result['OK']:
          outputList = result['Value']
          size = len(outputList)
          self.log.info('Last %s lines of available application output:' % (size) )
          self.log.info('================START================')
          for line in outputList:
            self.log.info(line)

          self.log.info('=================END=================')

      self.killRunningThread(self.spObject)
      self.finish()

    recentStdOut = 'None'
    if self.jobPeekFlag:
      result = self.peek(self.peekOutputLines)
      if result['OK']:
        outputList = result['Value']
        size = len(outputList)
        recentStdOut = 'Last %s lines of application output from Watchdog:' % (size)
        border = ''
        for i in xrange(len(recentStdOut)):
          border+='='
        recentStdOut = '\n%s\n%s\n%s\n' % (border,recentStdOut,border)
        self.log.info(recentStdOut)
        for line in outputList:
          self.log.info(line)
          recentStdOut += line+'\n'
      else:
        recentStdOut = 'Watchdog is initializing and will attempt to obtain standard output from application thread'
        self.log.warn(recentStdOut)
        self.peekFailCount += 1
        if self.peekFailCount > self.peekRetry:
          self.jobPeekFlag = 0
          self.log.warn('Turning off job peeking for remainder of execution')

    if not os.environ.has_key('JOBID'):
      self.log.info('Running without JOBID so parameters will not be reported')
      return S_OK()

    jobID = os.environ['JOBID']
    staticParamDict = {'StandardOutput':recentStdOut}
    self.sendSignOfLife(int(jobID),heartBeatDict,staticParamDict)
    return S_OK('Watchdog cycle complete')

  #############################################################################
  def __interpretControlSignal(self,signalDict):
    """This method is called whenever a signal is sent via the result of
       sending a sign of life.
    """
    self.log.info('Received control signal %s')
    if type(signal) == type({}):
      if signal.has_key('Command'):
        if signal['Command'].lower() == 'kill':
          self.log.info('Killing job via control signal')
          self.killRunningThread(self.spObject)
          self.finish()
        else:
          if signal['Command']:
            self.log.info('Control signal sent but not understood by watchdog:')
            self.log.info(signal)

    return S_OK()

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
        self.log.warn(result['Message'])
        return result
    else:
      report += 'WallClock: NA,'

    if self.testDiskSpace:
      result = self.checkDiskSpace()
      report += 'DiskSpace: OK, '
      if not result['OK']:
        self.log.warn(result['Message'])
        return result
    else:
      report += 'DiskSpace: NA,'

    if self.testLoadAvg:
      result = self.checkLoadAverage()
      report += 'LoadAverage: OK, '
      if not result['OK']:
        self.log.warn(result['Message'])
        return result
    else:
      report += 'LoadAverage: NA,'

    if self.testCPUConsumed:
      result = self.checkCPUConsumed()
      report += 'CPUConsumed: OK, '
      if not result['OK']:
        return result
    else:
      report += 'CPUConsumed: NA,'

    if self.testCPULimit:
      result = self.checkCPULimit()
      report += 'CPULimit OK. '
      if not result['OK']:
        self.log.warn(result['Message'])
        return result
    else:
      report += 'CPUConsumed: NA.'


    self.log.info(report)
    return S_OK('All enabled checks passed')

  #############################################################################

  def checkCPUConsumed(self):
    """ Checks whether the CPU consumed by application process is reasonable. This
        method will report stalled jobs to be killed.
    """
    cpuList=[]
    if self.parameters.has_key('CPUConsumed'):
      cpuList = self.parameters['CPUConsumed']
    else:
      return S_ERROR('No CPU consumed in collected parameters')

    #First restrict cpuList to specified sample time interval
    #Return OK if time not reached yet
    interval = self.pollingTime
    sampleTime = self.sampleCPUTime
    iterations = int(sampleTime/interval)
    if len(cpuList) < iterations:
      return S_OK('Job running for less than CPU sample time')
    else:
      cut = len(cpuList) - iterations
      cpuList = cpuList[cut:]

    cpuSample = []
    for value in cpuList:
      valueTime = self.convertCPUTime(value)
      if not valueTime['OK']:
        return valueTime
      cpuSample.append(valueTime['Value'])

    #If total CPU consumed / WallClock for iterations less than X
    #can fail job.

    totalCPUConsumed = cpuSample[-1]
    self.log.info('Cumulative CPU consumed by application process is: %s' % (totalCPUConsumed))
    if len(cpuSample)>1:
      cpuConsumedInterval = cpuSample[-1]-cpuSample[0]  #during last interval

      ratio = float(totalCPUConsumed)/float(sampleTime)
      limit = float( self.minCPUWallClockRatio * 0.01 )
      self.log.info('CPU consumed / Wallclock time ratio during last %s seconds is %f' % (sampleTime,ratio))

      if ratio < limit:
        self.log.info(cpuSample)
        noCPURecordedFlag = True
        for i in cpuSample:
          if i:
            noCPURecordedFlag = False
        if noCPURecordedFlag:
          self.log.info('Watchdog would have identified job as stalled but CPU is consistently zero')
          return S_OK('Watchdog cannot obtain CPU')

        return S_ERROR('Watchdog identified this job as stalled')

      if self.appPID != self.wrapperPID:
        if self.parameters.has_key('WrapperCPUConsumed') and self.initialValues.has_key('WrapperCPUConsumed'):
          wrapperCPUList = self.parameters['WrapperCPUConsumed']
          result = self.convertCPUTime(wrapperCPUList[-1])
          if result['OK']:
            currentWrapperCPU = float(result['Value']) - float(self.initialValues['WrapperCPUConsumed'])
            self.log.info('Cumulative Wrapper CPU consumed is: %s' %(currentWrapperCPU))
          else:
            self.log.info('Not possible to convert current wrapper CPU consumed from %s' %(wrapperCPUList[-1]))
        else:
          self.log.info('Not enough information to calculate CPU consumed by wrapper')

    else:
      self.log.info('Insufficient CPU consumed information')
      self.log.verbose(cpuList)


    return S_OK('Job consuming CPU')

  #############################################################################

  def convertCPUTime(self,cputime):
    """ Method to convert the CPU time as returned from the Watchdog
        instances to the equivalent DIRAC normalized CPU time to be compared
        to the Job CPU requirement.
    """
    cpuValue = 0
    cpuHMS = cputime.split(':')
    for i in xrange(len(cpuHMS)):
      cpuHMS[i] = cpuHMS[i].replace('00','0')

    try:
      hours = float(cpuHMS[0])*60*60
      mins  = float(cpuHMS[1])*60
      secs  = float(cpuHMS[2])
      cpuValue = float(hours+mins+secs)
    except Exception,x:
      self.log.warn(str(x))
      return S_ERROR('Could not calculate CPU time')

    #Normalization to be implemented
    normalizedCPUValue = cpuValue

    result = S_OK()
    result['Value'] = normalizedCPUValue
    self.log.debug('CPU value %s converted to %s' %(cputime,normalizedCPUValue))
    return result

  #############################################################################

  def checkCPULimit(self):
    """ Checks that the job has consumed more than the job CPU requirement
        (plus a configurable margin) and kills them as necessary.
    """
    #Here we 'charge' for the CPU time including Watchdog.
    initialCPU = 0
    currentCPU = 0
    if self.initialValues.has_key('CPUConsumed'):
      initialCPU = self.initialValues['CPUConsumed']
    if self.parameters.has_key('CPUConsumed'):
      currentCPUps = self.parameters['CPUConsumed'][-1]

    initialCPUDict = self.convertCPUTime(initialCPU)
    if initialCPUDict['OK']:
      initialCPU = initialCPUDict['Value']
    else:
      return S_ERROR('Not possible to determine initial CPU consumed')

    currCPUDict = self.convertCPUTime(currentCPUps)
    if currCPUDict['OK']:
      currentCPU = currCPUDict['Value']
    else:
      return S_ERROR('Not possible to determine current CPU consumed')

    if initialCPU and currentCPU:
      limit = self.jobCPUtime + self.jobCPUtime * (self.jobCPUMargin / 100 )
      cpuConsumed = float(currentCPU)-float(initialCPU)
      if cpuConsumed > limit:
        self.log.info('Job has consumed more than the specified CPU limit with an additional %s% margin' % (self.jobCPUMargin))
        return S_ERROR('Job has exceeded maximum CPU time limit')
      else:
        return S_OK('Job within CPU limit')
    elif not initialCPU and not currentCPU:
      self.log.verbose('Both initial and current CPU consumed are null')
      return S_OK('CPU consumed is not measurable yet')
    else:
      return S_ERROR('Not possible to determine CPU consumed')

  #############################################################################
  def checkDiskSpace(self):
    """Checks whether the CS defined minimum disk space is available.
    """
    if self.parameters.has_key('DiskSpace'):
      availSpace = self.parameters['DiskSpace'][-1]
      if availSpace < self.minDiskSpace:
        self.log.info('Not enough local disk space for job to continue, defined in CS as %s MB' % (self.minDiskSpace))
        return S_ERROR('Job has insufficient disk space to continue')
      else:
        return S_OK('Job has enough disk space available')
    else:
      return S_ERROR('Available disk space could not be established')

  #############################################################################
  def checkWallClockTime(self):
    """Checks whether the job has been running for the CS defined maximum
       wall clock time.
    """
    if self.initialValues.has_key('StartTime'):
      startTime = self.initialValues['StartTime']
      if time.time() - startTime > self.maxWallClockTime:
        self.log.info('Job has exceeded maximum wall clock time of %s seconds' % (self.maxWallClockTime))
        return S_ERROR('Job has exceeded maximum wall clock time')
      else:
        return S_OK('Job within maximum wall clock time')
    else:
      return S_ERROR('Job start time could not be established')

  #############################################################################
  def checkLoadAverage(self):
    """Checks whether the CS defined maximum load average is exceeded.
    """
    if self.parameters.has_key('LoadAverage'):
      loadAvg = self.parameters['LoadAverage'][-1]
      if loadAvg > float(self.loadAvgLimit):
        self.log.info('Maximum load average exceeded, defined in CS as %s ' % (self.loadAvgLimit))
        return S_ERROR('Job exceeded maximum load average')
      else:
        return S_OK('Job running with normal load average')
    else:
      return S_ERROR('Job load average not established')

  #############################################################################
  def peek(self,lines=0):
    """ Uses ExecutionThread.getOutput() method to obtain standard output
        from running thread via subprocess callback function.
    """

    result = self.exeThread.getOutput()
    if not result['OK']:
      self.log.warn('Could not obtain output from running application thread')
      self.log.warn(result['Message'])

    return result

  #############################################################################
  def calibrate(self):
    """ The calibrate method obtains the initial values for system memory and load
        and calculates the margin for error for the rest of the Watchdog cycle.
    """
    self.__getAppPID()
    self.log.verbose('Child PID %s, Wrapper PID %s' %(self.appPID,self.wrapperPID))
    self.getWallClockTime()
    self.parameters['WallClockTime'] = []

    initialCPU=0.0
    if self.appPID:
      result = self.getCPUConsumed(self.appPID)
      self.log.verbose('CPU consumed %s' %(result))
      if not result['OK']:
        msg = 'Could not establish CPU consumed'
        self.log.warn(msg)
        result = S_ERROR(msg)
        return result

      initialCPU = result['Value']

    self.initialValues['CPUConsumed']=initialCPU
    self.parameters['CPUConsumed'] = []

    if self.appPID != self.wrapperPID:
      result = self.getCPUConsumed(self.wrapperPID)
      self.log.verbose('Wrapper CPU consumed %s' %(result))
      if not result['OK']:
        msg = 'Could not establish Wrapper CPU consumed'
        self.log.warn(msg)
        result = S_ERROR(msg)
        return result

      wrapCPU = result['Value']
      self.initialValues['WrapperCPUConsumed']=wrapCPU
      self.parameters['WrapperCPUConsumed'] = []
    else:
      self.initialValues['WrapperCPUConsumed']=0.0
      self.parameters['WrapperCPUConsumed'] = []

    result = self.getLoadAverage()
    self.log.verbose('LoadAverage: %s' %(result))
    if not result['OK']:
      msg = 'Could not establish LoadAverage'
      self.log.warn(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['LoadAverage']=result['Value']
    self.parameters['LoadAverage'] = []

    result = self.getMemoryUsed()
    self.log.verbose('MemUsed: %s' %(result))
    if not result['OK']:
      msg = 'Could not establish MemoryUsed'
      self.log.warn(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['MemoryUsed']=result['Value']
    self.parameters['MemoryUsed'] = []

    result = self. getDiskSpace()
    self.log.verbose('DiskSpace: %s' %(result))
    if not result['OK']:
      msg = 'Could not establish DiskSpace'
      self.log.warn(msg)
      result = S_ERROR(msg)
      return result

    self.initialValues['DiskSpace']=result['Value']
    self.parameters['DiskSpace'] = []

    result = self.getNodeInformation()
    self.log.verbose('NodeInfo: %s' %(result))
    if not result['OK']:
      msg = 'Could not establish static system information'
      self.log.warn(msg)
      result = S_ERROR(msg)
      return result

    if os.environ.has_key('LSB_JOBID'):
      result['LocalJobID'] = os.environ['LSB_JOBID']
    if os.environ.has_key('PBS_JOBID'):
      result['LocalJobID'] = os.environ['PBS_JOBID']
    if os.environ.has_key('QSUB_REQNAME'):
      result['LocalJobID'] = os.environ['QSUB_REQNAME']

    self.reportParameters(result,'NodeInformation',True)
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
    if self.parameters.has_key('CPUConsumed'):
      cpuList = self.parameters['CPUConsumed']
      summary['CPUConsumed(h:m:s)'] = cpuList[-1] # to do, the initial value should be subtracted
    #DiskSpace
    if self.parameters.has_key('DiskSpace'):
      space = self.parameters['DiskSpace']
      value = float(space[-1]) - float(self.initialValues['DiskSpace'])
      if value < 0:
        value = 0
      summary['DiskSpace(MB)'] = value
    #MemoryUsed
    if self.parameters.has_key('MemoryUsed'):
      memory = self.parameters['MemoryUsed']
      summary['MemoryUsed(bytes)'] = float(memory[-1]) - float(self.initialValues['MemoryUsed'])
    #LoadAverage
    if self.parameters.has_key('LoadAverage'):
      laList = self.parameters['LoadAverage']
      la = 0.0
      for load in laList: la += load
      summary['LoadAverage'] = float(la) / float(len(laList))
    #wrapper CPU
    if self.parameters.has_key('WrapperCPUConsumed'):
      wrapCPU = self.parameters['WrapperCPUConsumed']
      value = wrapCPU[-1]
      summary['WrapperCPUConsumed(h:m:s)']=value

    self.reportParameters(summary,'UsageSummary')

  #############################################################################
  def reportParameters(self,params,title=None,report=False):
    """Will report parameters for job.
    """
    parameters = []
    self.log.info('==========================================================')
    if title:
      self.log.info('Watchdog will report %s' % (title))
    else:
      self.log.info('Watchdog will report parameters')
    self.log.info('==========================================================')
    vals = params
    if params.has_key('Value'):
      if vals['Value']:
        vals = params['Value']
    for k,v in vals.items():
      if v:
        self.log.info(str(k)+' = '+str(v))
        parameters.append((k,v))
    if report:
      self.__setJobParamList(parameters)

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
    os.kill( spObject.child.pid, 9 )
    self.log.info('Sent kill signal to application PID %s' %(spObject.child.pid))
    return S_OK('Thread killed')

  #############################################################################
  def sendSignOfLife(self,jobID,heartBeatDict,staticParamDict):
    """ Sends sign of life 'heartbeat' signal and triggers control signal
        interpretation.
    """
    result = self.jobReport.sendHeartBeat(jobID,heartBeatDict,staticParamDict)
    if not result['OK']:
      self.log.warn('Problem sending sign of life')
      self.log.warn(result)

    if result['OK'] and result['Value']:
      self.__interpretControlSignal(result['Value'])

    return result

  #############################################################################
  def __setJobParamList(self,value):
    """Wraps around setJobParameters of state update client
    """
    #job wrapper template sets the jobID variable
    if not os.environ.has_key('JOBID'):
      self.log.info('Running without JOBID so parameters will not be reported')
      return S_OK()
    jobID = os.environ['JOBID']
    jobParam = self.jobReport.setJobParameters(int(jobID),value)
    self.log.verbose('setJobParameters(%s,%s)' %(jobID,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def getNodeInformation(self):
    """ Attempts to retrieve all static system information, should be overridden in a subclass"""
    methodName = 'getNodeInformation'
    self.log.warn('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getLoadAverage(self):
    """ Attempts to get the load average, should be overridden in a subclass"""
    methodName = 'getLoadAverage'
    self.log.warn('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getMemoryUsed(self):
    """ Attempts to get the memory used, should be overridden in a subclass"""
    methodName = 'getMemoryUsed'
    self.log.warn('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getDiskSpace(self):
    """ Attempts to get the available disk space, should be overridden in a subclass"""
    methodName = 'getDiskSpace'
    self.log.warn('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #############################################################################
  def getCPUConsumed(self):
    """ Attempts to get the CPU consumed, should be overridden in a subclass"""
    methodName = 'getCPUConsumed'
    self.log.warn('Watchdog: '+methodName+' method should be implemented in a subclass')
    return S_ERROR('Watchdog: '+methodName+' method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
