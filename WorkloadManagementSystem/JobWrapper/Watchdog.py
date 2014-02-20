########################################################################
# $HeadURL$
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

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities                               import Time
from DIRAC.Core.DISET.RPCClient                         import RPCClient
from DIRAC.ConfigurationSystem.Client.Config            import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder        import getSystemInstance
from DIRAC.Core.Utilities.ProcessMonitor                import ProcessMonitor
from DIRAC                                              import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.TimeLeft.TimeLeft             import TimeLeft

import os, time

class Watchdog:

  #############################################################################
  def __init__( self, pid, exeThread, spObject, jobCPUtime, systemFlag = 'linux2.4' ):
    """ Constructor, takes system flag as argument.
    """
    self.log = gLogger.getSubLogger( "Watchdog" )
    self.systemFlag = systemFlag
    self.exeThread = exeThread
    self.wrapperPID = pid
    self.appPID = self.exeThread.getCurrentPID()
    self.spObject = spObject
    self.jobCPUtime = jobCPUtime
    self.calibration = 0
    self.initialValues = {}
    self.parameters = {}
    self.peekFailCount = 0
    self.peekRetry = 5
    self.processMonitor = ProcessMonitor()
    self.checkError = ''
    self.currentStats = {}
    self.initialized = False
    self.count = 0


  #############################################################################
  def initialize( self, loops = 0 ):
    """ Watchdog initialization.
    """
    if self.initialized:
      self.log.info( 'Watchdog already initialized' )
      return S_OK()
    else:
      self.initialized = True

    setup = gConfig.getValue( '/DIRAC/Setup', '' )
    if not setup:
      return S_ERROR( 'Can not get the DIRAC Setup value' )
    wms_instance = getSystemInstance( "WorkloadManagement" )
    if not wms_instance:
      return S_ERROR( 'Can not get the WorkloadManagement system instance' )
    self.section = '/Systems/WorkloadManagement/%s/JobWrapper' % wms_instance

    self.maxcount = loops
    self.log.verbose( 'Watchdog initialization' )
    self.log.info( 'Attempting to Initialize Watchdog for: %s' % ( self.systemFlag ) )
    #Test control flags
    self.testWallClock = gConfig.getValue( self.section + '/CheckWallClockFlag', 1 )
    self.testDiskSpace = gConfig.getValue( self.section + '/CheckDiskSpaceFlag', 1 )
    self.testLoadAvg = gConfig.getValue( self.section + '/CheckLoadAvgFlag', 1 )
    self.testCPUConsumed = gConfig.getValue( self.section + '/CheckCPUConsumedFlag', 1 )
    self.testCPULimit = gConfig.getValue( self.section + '/CheckCPULimitFlag', 0 )
    self.testTimeLeft = gConfig.getValue( self.section + '/CheckTimeLeftFlag', 1 )
    #Other parameters
    self.pollingTime = gConfig.getValue( self.section + '/PollingTime', 10 ) # 10 seconds
    self.checkingTime = gConfig.getValue( self.section + '/CheckingTime', 30 * 60 ) #30 minute period
    self.minCheckingTime = gConfig.getValue( self.section + '/MinCheckingTime', 20 * 60 ) # 20 mins
    self.maxWallClockTime = gConfig.getValue( self.section + '/MaxWallClockTime', 3 * 24 * 60 * 60 ) # e.g. 4 days
    self.jobPeekFlag = gConfig.getValue( self.section + '/JobPeekFlag', 1 ) # on / off
    self.minDiskSpace = gConfig.getValue( self.section + '/MinDiskSpace', 10 ) #MB
    self.loadAvgLimit = gConfig.getValue( self.section + '/LoadAverageLimit', 1000 ) # > 1000 and jobs killed
    self.sampleCPUTime = gConfig.getValue( self.section + '/CPUSampleTime', 30 * 60 ) # e.g. up to 20mins sample
    self.jobCPUMargin = gConfig.getValue( self.section + '/JobCPULimitMargin', 20 ) # %age buffer before killing job
    self.minCPUWallClockRatio = gConfig.getValue( self.section + '/MinCPUWallClockRatio', 5 ) #ratio %age
    self.nullCPULimit = gConfig.getValue( self.section + '/NullCPUCountLimit', 5 ) #After 5 sample times return null CPU consumption kill job
    self.checkCount = 0
    self.nullCPUCount = 0
    if self.checkingTime < self.minCheckingTime:
      self.log.info( 'Requested CheckingTime of %s setting to %s seconds (minimum)' % ( self.checkingTime, self.minCheckingTime ) )
      self.checkingTime = self.minCheckingTime

    # The time left is returned in seconds @ 250 SI00 = 1 HS06,
    # the self.checkingTime and self.pollingTime are in seconds,
    # thus they need to be multiplied by a large enough factor
    self.grossTimeLeftLimit = 10 * self.checkingTime
    self.fineTimeLeftLimit = gConfig.getValue( self.section + '/TimeLeftLimit', 150 * self.pollingTime )

    self.timeLeftUtil = TimeLeft()
    self.timeLeft = 0
    self.littleTimeLeft = False
    return S_OK()

  def run( self ):
    """ The main watchdog execution method
    """

    result = self.initialize()
    if not result['OK']:
      gLogger.always( 'Can not start watchdog for the following reason' )
      gLogger.always( result['Message'] )
      return result

    try:
      while True:
        gLogger.debug( 'Starting watchdog loop # %d' % self.count )
        start_cycle_time = time.time()
        result = self.execute()
        exec_cycle_time = time.time() - start_cycle_time
        if not result[ 'OK' ]:
          gLogger.error( "Watchdog error during execution", result[ 'Message' ] )
          break
        elif result['Value'] == "Ended":
          break
        self.count += 1
        if exec_cycle_time < self.pollingTime:
          time.sleep( self.pollingTime - exec_cycle_time )
      return S_OK()
    except Exception:
      gLogger.exception()
      return S_ERROR( 'Exception' )

  #############################################################################
  def execute( self ):
    """ The main agent execution method of the Watchdog.
    """
    if not self.exeThread.isAlive():
      #print self.parameters
      self.__getUsageSummary()
      self.log.info( 'Process to monitor has completed, Watchdog will exit.' )
      return S_OK( "Ended" )

    if self.littleTimeLeft:
      # if we have gone over enough iterations query again
      if self.littleTimeLeftCount == 0 and self.__timeLeft() == -1:
        self.checkError = 'Job has reached the CPU limit of the queue'
        self.log.error( self.checkError, self.timeLeft )
        self.__killRunningThread()
        return S_OK()
      else:
        self.littleTimeLeftCount -= 1


    #Note: need to poll regularly to see if the thread is alive
    #      but only perform checks with a certain frequency
    if ( time.time() - self.initialValues['StartTime'] ) > self.checkingTime * self.checkCount:
      self.checkCount += 1
      result = self.__performChecks()
      if not result['OK']:
        self.log.warn( 'Problem during recent checks' )
        self.log.warn( result['Message'] )
      return S_OK()
    else:
      #self.log.debug('Application thread is alive: checking count is %s' %(self.checkCount))
      return S_OK()


  #############################################################################
  def __performChecks( self ):
    """The Watchdog checks are performed at a different period to the checking of the
       application thread and correspond to the checkingTime.
    """
    self.log.verbose( '------------------------------------' )
    self.log.verbose( 'Checking loop starts for Watchdog' )
    heartBeatDict = {}
    msg = ''
    result = self.getLoadAverage()
    msg += 'LoadAvg: %s ' % ( result['Value'] )
    heartBeatDict['LoadAverage'] = result['Value']
    if not self.parameters.has_key( 'LoadAverage' ):
      self.parameters['LoadAverage'] = []
    self.parameters['LoadAverage'].append( result['Value'] )
    result = self.getMemoryUsed()
    msg += 'MemUsed: %.1f kb ' % ( result['Value'] )
    heartBeatDict['MemoryUsed'] = result['Value']
    if not self.parameters.has_key( 'MemoryUsed' ):
      self.parameters['MemoryUsed'] = []
    self.parameters['MemoryUsed'].append( result['Value'] )
    result = self.getDiskSpace()
    msg += 'DiskSpace: %.1f MB ' % ( result['Value'] )
    if not self.parameters.has_key( 'DiskSpace' ):
      self.parameters['DiskSpace'] = []
    self.parameters['DiskSpace'].append( result['Value'] )
    heartBeatDict['AvailableDiskSpace'] = result['Value']
    result = self.__getCPU()
    msg += 'CPU: %s (h:m:s) ' % ( result['Value'] )
    if not self.parameters.has_key( 'CPUConsumed' ):
      self.parameters['CPUConsumed'] = []
    self.parameters['CPUConsumed'].append( result['Value'] )
    hmsCPU = result['Value']
    rawCPU = self.__convertCPUTime( hmsCPU )
    if rawCPU['OK']:
      heartBeatDict['CPUConsumed'] = rawCPU['Value']
    result = self.__getWallClockTime()
    msg += 'WallClock: %.2f s ' % ( result['Value'] )
    self.parameters['WallClockTime'].append( result['Value'] )
    heartBeatDict['WallClockTime'] = result['Value']
    self.log.info( msg )

    result = self.__checkProgress()
    if not result['OK']:
      self.checkError = result['Message']
      self.log.warn( self.checkError )

      if self.jobPeekFlag:
        result = self.__peek()
        if result['OK']:
          outputList = result['Value']
          size = len( outputList )
          self.log.info( 'Last %s lines of available application output:' % ( size ) )
          self.log.info( '================START================' )
          for line in outputList:
            self.log.info( line )

          self.log.info( '=================END=================' )

      self.__killRunningThread()
      return S_OK()

    recentStdOut = 'None'
    if self.jobPeekFlag:
      result = self.__peek()
      if result['OK']:
        outputList = result['Value']
        size = len( outputList )
        recentStdOut = 'Last %s lines of application output from Watchdog on %s [UTC]:' % ( size, Time.dateTime() )
        border = '=' * len( recentStdOut )
        cpuTotal = 'Last reported CPU consumed for job is %s (h:m:s)' % ( hmsCPU )
        if self.timeLeft:
          cpuTotal += ', Batch Queue Time Left %s (s @ HS06)' % self.timeLeft
        recentStdOut = '\n%s\n%s\n%s\n%s\n' % ( border, recentStdOut, cpuTotal, border )
        self.log.info( recentStdOut )
        for line in outputList:
          self.log.info( line )
          recentStdOut += line + '\n'
      else:
        recentStdOut = 'Watchdog is initializing and will attempt to obtain standard output from application thread'
        self.log.info( recentStdOut )
        self.peekFailCount += 1
        if self.peekFailCount > self.peekRetry:
          self.jobPeekFlag = 0
          self.log.warn( 'Turning off job peeking for remainder of execution' )

    if not os.environ.has_key( 'JOBID' ):
      self.log.info( 'Running without JOBID so parameters will not be reported' )
      return S_OK()

    jobID = os.environ['JOBID']
    staticParamDict = {'StandardOutput':recentStdOut}
    self.__sendSignOfLife( int( jobID ), heartBeatDict, staticParamDict )
    return S_OK( 'Watchdog checking cycle complete' )

  #############################################################################
  def __getCPU( self ):
    """Uses os.times() to get CPU time and returns HH:MM:SS after conversion.
    """
    cpuTime = '00:00:00'
    try:
      cpuTime = self.processMonitor.getCPUConsumed( self.wrapperPID )
    except Exception:
      self.log.warn( 'Could not determine CPU time consumed with exception' )
      self.log.exception()
      return S_OK( cpuTime ) #just return null CPU

    if not cpuTime['OK']:
      self.log.warn( 'Problem while checking consumed CPU' )
      self.log.warn( cpuTime )
      return S_OK( '00:00:00' ) #again return null CPU in this case

    cpuTime = cpuTime['Value']
    self.log.verbose( "Raw CPU time consumed (s) = %s" % ( cpuTime ) )
    result = self.__getCPUHMS( cpuTime )
    return result

  #############################################################################
  def __getCPUHMS( self, cpuTime ):
    mins, secs = divmod( cpuTime, 60 )
    hours, mins = divmod( mins, 60 )
    humanTime = '%02d:%02d:%02d' % ( hours, mins, secs )
    self.log.verbose( 'Human readable CPU time is: %s' % humanTime )
    return S_OK( humanTime )

  #############################################################################
  def __interpretControlSignal( self, signalDict ):
    """This method is called whenever a signal is sent via the result of
       sending a sign of life.
    """
    self.log.info( 'Received control signal' )
    if type( signalDict ) == type( {} ):
      if signalDict.has_key( 'Kill' ):
        self.log.info( 'Received Kill signal, stopping job via control signal' )
        self.checkError = 'Received Kill signal'
        self.__killRunningThread()
      else:
        self.log.info( 'The following control signal was sent but not understood by the watchdog:' )
        self.log.info( signalDict )
    else:
      self.log.info( 'Expected dictionary for control signal, received:\n%s' % ( signalDict ) )

    return S_OK()

  #############################################################################
  def __checkProgress( self ):
    """This method calls specific tests to determine whether the job execution
       is proceeding normally.  CS flags can easily be added to add or remove
       tests via central configuration.
    """
    report = ''

    if self.testWallClock:
      result = self.__checkWallClockTime()
      report += 'WallClock: OK, '
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result
    else:
      report += 'WallClock: NA,'

    if self.testDiskSpace:
      result = self.__checkDiskSpace()
      report += 'DiskSpace: OK, '
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result
    else:
      report += 'DiskSpace: NA,'

    if self.testLoadAvg:
      result = self.__checkLoadAverage()
      report += 'LoadAverage: OK, '
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result
    else:
      report += 'LoadAverage: NA,'

    if self.testCPUConsumed:
      result = self.__checkCPUConsumed()
      report += 'CPUConsumed: OK, '
      if not result['OK']:
        return result
    else:
      report += 'CPUConsumed: NA, '

    if self.testCPULimit:
      result = self.__checkCPULimit()
      report += 'CPULimit OK, '
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result
    else:
      report += 'CPULimit: NA, '

    if self.testTimeLeft:
      self.__timeLeft()
      if self.timeLeft:
        report += 'TimeLeft: OK'
    else:
      report += 'TimeLeft: NA'


    self.log.info( report )
    return S_OK( 'All enabled checks passed' )

  #############################################################################
  def __checkCPUConsumed( self ):
    """ Checks whether the CPU consumed by application process is reasonable. This
        method will report stalled jobs to be killed.
    """
    self.log.info( "Checking CPU Consumed" )

    if 'WallClockTime' not in self.parameters:
      return S_ERROR( 'Missing WallClockTime info' )
    if 'CPUConsumed' not in self.parameters:
      return S_ERROR( 'Missing CPUConsumed info' )

    wallClockTime = self.parameters['WallClockTime'][-1]
    if wallClockTime < self.sampleCPUTime:
      self.log.info( "Stopping check, wallclock time (%s) is still smalled than sample time (%s)" % ( wallClockTime,
                                                                                            self.sampleCPUTime ) )
      return S_OK()

    intervals = max( 1, int( self.sampleCPUTime / self.checkingTime ) )
    if len( self.parameters['CPUConsumed'] ) < intervals + 1:
      self.log.info( "Not enough snapshots to calculate, there are %s and we need %s" % ( len( self.parameters['CPUConsumed'] ),
                                                                                          intervals + 1 ) )
      return S_OK()

    wallClockTime = self.parameters['WallClockTime'][-1] - self.parameters['WallClockTime'][-1 - intervals ]
    try:
      cpuTime = self.__convertCPUTime( self.parameters['CPUConsumed'][-1] )['Value']
      # For some reason, some times the CPU consumed estimation returns 0
      # if cpuTime == 0:
      #   return S_OK()
      cpuTime -= self.__convertCPUTime( self.parameters['CPUConsumed'][-1 - intervals ] )['Value']

      ratio = ( cpuTime / wallClockTime ) * 100.

      self.log.info( "CPU/Wallclock ratio is %.2f%%" % ratio )
      # in case of error cpuTime might be 0, exclude this
      if wallClockTime and ratio < self.minCPUWallClockRatio:
        if os.path.exists( 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK' ):
          self.log.info( 'N.B. job would be declared as stalled but CPU / WallClock check is disabled by payload' )
          return S_OK()
        self.log.info( "Job is stalled!" )
        return S_ERROR( 'Watchdog identified this job as stalled' )
    except Exception, e:
      self.log.error( "Cannot convert CPU consumed from string to int: %s" % str( e ) )

    return S_OK()

  #############################################################################

  def __convertCPUTime( self, cputime ):
    """ Method to convert the CPU time as returned from the Watchdog
        instances to the equivalent DIRAC normalized CPU time to be compared
        to the Job CPU requirement.
    """
    cpuValue = 0
    cpuHMS = cputime.split( ':' )
    # for i in xrange( len( cpuHMS ) ):
    #   cpuHMS[i] = cpuHMS[i].replace( '00', '0' )

    try:
      hours = float( cpuHMS[0] ) * 60 * 60
      mins = float( cpuHMS[1] ) * 60
      secs = float( cpuHMS[2] )
      cpuValue = float( hours + mins + secs )
    except Exception, x:
      self.log.warn( str( x ) )
      return S_ERROR( 'Could not calculate CPU time' )

    #Normalization to be implemented
    normalizedCPUValue = cpuValue

    result = S_OK()
    result['Value'] = normalizedCPUValue
    self.log.debug( 'CPU value %s converted to %s' % ( cputime, normalizedCPUValue ) )
    return result

  #############################################################################

  def __checkCPULimit( self ):
    """ Checks that the job has consumed more than the job CPU requirement
        (plus a configurable margin) and kills them as necessary.
    """
    consumedCPU = 0
    if self.parameters.has_key( 'CPUConsumed' ):
      consumedCPU = self.parameters['CPUConsumed'][-1]

    consumedCPUDict = self.__convertCPUTime( consumedCPU )
    if consumedCPUDict['OK']:
      currentCPU = consumedCPUDict['Value']
    else:
      return S_OK( 'Not possible to determine current CPU consumed' )

    if consumedCPU:
      limit = self.jobCPUtime + self.jobCPUtime * ( self.jobCPUMargin / 100 )
      cpuConsumed = float( currentCPU )
      if cpuConsumed > limit:
        self.log.info( 'Job has consumed more than the specified CPU limit with an additional %s%% margin' % ( self.jobCPUMargin ) )
        return S_ERROR( 'Job has exceeded maximum CPU time limit' )
      else:
        return S_OK( 'Job within CPU limit' )
    elif not currentCPU:
      self.log.verbose( 'Both initial and current CPU consumed are null' )
      return S_OK( 'CPU consumed is not measurable yet' )
    else:
      return S_OK( 'Not possible to determine CPU consumed' )

  #############################################################################
  def __checkDiskSpace( self ):
    """Checks whether the CS defined minimum disk space is available.
    """
    if self.parameters.has_key( 'DiskSpace' ):
      availSpace = self.parameters['DiskSpace'][-1]
      if availSpace >= 0 and availSpace < self.minDiskSpace:
        self.log.info( 'Not enough local disk space for job to continue, defined in CS as %s MB' % ( self.minDiskSpace ) )
        return S_ERROR( 'Job has insufficient disk space to continue' )
      else:
        return S_OK( 'Job has enough disk space available' )
    else:
      return S_ERROR( 'Available disk space could not be established' )

  #############################################################################
  def __checkWallClockTime( self ):
    """Checks whether the job has been running for the CS defined maximum
       wall clock time.
    """
    if self.initialValues.has_key( 'StartTime' ):
      startTime = self.initialValues['StartTime']
      if time.time() - startTime > self.maxWallClockTime:
        self.log.info( 'Job has exceeded maximum wall clock time of %s seconds' % ( self.maxWallClockTime ) )
        return S_ERROR( 'Job has exceeded maximum wall clock time' )
      else:
        return S_OK( 'Job within maximum wall clock time' )
    else:
      return S_ERROR( 'Job start time could not be established' )

  #############################################################################
  def __checkLoadAverage( self ):
    """Checks whether the CS defined maximum load average is exceeded.
    """
    if self.parameters.has_key( 'LoadAverage' ):
      loadAvg = self.parameters['LoadAverage'][-1]
      if loadAvg > float( self.loadAvgLimit ):
        self.log.info( 'Maximum load average exceeded, defined in CS as %s ' % ( self.loadAvgLimit ) )
        return S_ERROR( 'Job exceeded maximum load average' )
      else:
        return S_OK( 'Job running with normal load average' )
    else:
      return S_ERROR( 'Job load average not established' )

  #############################################################################
  def __peek( self ):
    """ Uses ExecutionThread.getOutput() method to obtain standard output
        from running thread via subprocess callback function.
    """
    result = self.exeThread.getOutput()
    if not result['OK']:
      self.log.warn( 'Could not obtain output from running application thread' )
      self.log.warn( result['Message'] )

    return result

  #############################################################################
  def calibrate( self ):
    """ The calibrate method obtains the initial values for system memory and load
        and calculates the margin for error for the rest of the Watchdog cycle.
    """
    self.__getWallClockTime()
    self.parameters['WallClockTime'] = []

    initialCPU = 0.0
    result = self.__getCPU()
    self.log.verbose( 'CPU consumed %s' % ( result ) )
    if not result['OK']:
      msg = 'Could not establish CPU consumed'
      self.log.warn( msg )
#      result = S_ERROR(msg)
#      return result

    initialCPU = result['Value']

    self.initialValues['CPUConsumed'] = initialCPU
    self.parameters['CPUConsumed'] = []

    result = self.getLoadAverage()
    self.log.verbose( 'LoadAverage: %s' % ( result ) )
    if not result['OK']:
      msg = 'Could not establish LoadAverage'
      self.log.warn( msg )
#      result = S_ERROR(msg)
#      return result

    self.initialValues['LoadAverage'] = result['Value']
    self.parameters['LoadAverage'] = []

    result = self.getMemoryUsed()
    self.log.verbose( 'MemUsed: %s' % ( result ) )
    if not result['OK']:
      msg = 'Could not establish MemoryUsed'
      self.log.warn( msg )
#      result = S_ERROR(msg)
#      return result

    self.initialValues['MemoryUsed'] = result['Value']
    self.parameters['MemoryUsed'] = []

    result = self. getDiskSpace()
    self.log.verbose( 'DiskSpace: %s' % ( result ) )
    if not result['OK']:
      msg = 'Could not establish DiskSpace'
      self.log.warn( msg )
#      result = S_ERROR(msg)
#      return result

    self.initialValues['DiskSpace'] = result['Value']
    self.parameters['DiskSpace'] = []

    result = self.getNodeInformation()
    self.log.verbose( 'NodeInfo: %s' % ( result ) )
    if not result['OK']:
      msg = 'Could not establish static system information'
      self.log.warn( msg )
#      result = S_ERROR(msg)
#      return result

    if os.environ.has_key( 'LSB_JOBID' ):
      result['LocalJobID'] = os.environ['LSB_JOBID']
    if os.environ.has_key( 'PBS_JOBID' ):
      result['LocalJobID'] = os.environ['PBS_JOBID']
    if os.environ.has_key( 'QSUB_REQNAME' ):
      result['LocalJobID'] = os.environ['QSUB_REQNAME']
    if os.environ.has_key( 'JOB_ID' ):
      result['LocalJobID'] = os.environ['JOB_ID']

    self.__reportParameters( result, 'NodeInformation', True )
    self.__reportParameters( self.initialValues, 'InitialValues' )
    return S_OK()

  def __timeLeft( self ):
    """
      return Normalized CPU time left in the batch system
      0 if not available
      update self.timeLeft and self.littleTimeLeft
    """
    # Get CPU time left in the batch system
    result = self.timeLeftUtil.getTimeLeft( 0.0 )
    if not result['OK']:
      # Could not get CPU time left, we might need to wait for the first loop
      # or the Utility is not working properly for this batch system
      # or we are in a batch system
      timeLeft = 0
    else:
      timeLeft = result['Value']

    self.timeLeft = timeLeft
    if not self.littleTimeLeft:
      if timeLeft and timeLeft < self.grossTimeLeftLimit:
        self.log.info( 'TimeLeft bellow %s, now checking with higher frequency' % timeLeft )
        self.littleTimeLeft = True
        # TODO: better configurable way of doing this to be coded
        self.littleTimeLeftCount = 15
    else:
      if self.timeLeft and self.timeLeft < self.fineTimeLeftLimit:
        timeLeft = -1

    return timeLeft

  #############################################################################
  def __getUsageSummary( self ):
    """ Returns average load, memory etc. over execution of job thread
    """
    summary = {}
    #CPUConsumed
    if self.parameters.has_key( 'CPUConsumed' ):
      cpuList = self.parameters['CPUConsumed']
      if cpuList:
        hmsCPU = cpuList[-1]
        rawCPU = self.__convertCPUTime( hmsCPU )
        if rawCPU['OK']:
          summary['LastUpdateCPU(s)'] = rawCPU['Value']
      else:
        summary['LastUpdateCPU(s)'] = 'Could not be estimated'
    #DiskSpace
    if self.parameters.has_key( 'DiskSpace' ):
      space = self.parameters['DiskSpace']
      if space:
        value = abs( float( space[-1] ) - float( self.initialValues['DiskSpace'] ) )
        if value < 0:
          value = 0
        summary['DiskSpace(MB)'] = value
      else:
        summary['DiskSpace(MB)'] = 'Could not be estimated'
    #MemoryUsed
    if self.parameters.has_key( 'MemoryUsed' ):
      memory = self.parameters['MemoryUsed']
      if memory:
        summary['MemoryUsed(kb)'] = abs( float( memory[-1] ) - float( self.initialValues['MemoryUsed'] ) )
      else:
        summary['MemoryUsed(kb)'] = 'Could not be estimated'
    #LoadAverage
    if self.parameters.has_key( 'LoadAverage' ):
      laList = self.parameters['LoadAverage']
      if laList:
        summary['LoadAverage'] = float( sum( laList ) ) / float( len( laList ) )
      else:
        summary['LoadAverage'] = 'Could not be estimated'

    result = self.__getWallClockTime()
    wallClock = result['Value']
    summary['WallClockTime(s)'] = wallClock

    self.__reportParameters( summary, 'UsageSummary', True )
    self.currentStats = summary


  #############################################################################
  def __reportParameters( self, params, title = None, report = False ):
    """Will report parameters for job.
    """
    try:
      parameters = []
      self.log.info( '==========================================================' )
      if title:
        self.log.info( 'Watchdog will report %s' % ( title ) )
      else:
        self.log.info( 'Watchdog will report parameters' )
      self.log.info( '==========================================================' )
      vals = params
      if params.has_key( 'Value' ):
        if vals['Value']:
          vals = params['Value']
      for k, v in vals.items():
        if v:
          self.log.info( str( k ) + ' = ' + str( v ) )
          parameters.append( ( k, v ) )
      if report:
        self.__setJobParamList( parameters )

      self.log.info( '==========================================================' )
    except Exception, x:
      self.log.warn( 'Problem while reporting parameters' )
      self.log.warn( str( x ) )

  #############################################################################
  def __getWallClockTime( self ):
    """ Establishes the Wall Clock time spent since the Watchdog initialization"""
    result = S_OK()
    if self.initialValues.has_key( 'StartTime' ):
      currentTime = time.time()
      wallClock = currentTime - self.initialValues['StartTime']
      result['Value'] = wallClock
    else:
      self.initialValues['StartTime'] = time.time()
      result['Value'] = 0.0

    return result

  #############################################################################
  def __killRunningThread( self ):
    """ Will kill the running thread process and any child processes."""
    self.log.info( 'Sending kill signal to application PID %s' % ( self.spObject.getChildPID() ) )
    result = self.spObject.killChild()
    self.applicationKilled = True
    self.log.info( 'Subprocess.killChild() returned:%s ' % ( result ) )
    return S_OK( 'Thread killed' )

  #############################################################################
  def __sendSignOfLife( self, jobID, heartBeatDict, staticParamDict ):
    """ Sends sign of life 'heartbeat' signal and triggers control signal
        interpretation.
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 )
    result = jobReport.sendHeartBeat( jobID, heartBeatDict, staticParamDict )
    if not result['OK']:
      self.log.warn( 'Problem sending sign of life' )
      self.log.warn( result )

    if result['OK'] and result['Value']:
      self.__interpretControlSignal( result['Value'] )

    return result

  #############################################################################
  def __setJobParamList( self, value ):
    """Wraps around setJobParameters of state update client
    """
    #job wrapper template sets the jobID variable
    if not os.environ.has_key( 'JOBID' ):
      self.log.info( 'Running without JOBID so parameters will not be reported' )
      return S_OK()
    jobID = os.environ['JOBID']
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 )
    jobParam = jobReport.setJobParameters( int( jobID ), value )
    self.log.verbose( 'setJobParameters(%s,%s)' % ( jobID, value ) )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

    return jobParam

  #############################################################################
  def getNodeInformation( self ):
    """ Attempts to retrieve all static system information, should be overridden in a subclass"""
    methodName = 'getNodeInformation'
    self.log.warn( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )
    return S_ERROR( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )

  #############################################################################
  def getLoadAverage( self ):
    """ Attempts to get the load average, should be overridden in a subclass"""
    methodName = 'getLoadAverage'
    self.log.warn( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )
    return S_ERROR( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )

  #############################################################################
  def getMemoryUsed( self ):
    """ Attempts to get the memory used, should be overridden in a subclass"""
    methodName = 'getMemoryUsed'
    self.log.warn( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )
    return S_ERROR( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )

  #############################################################################
  def getDiskSpace( self ):
    """ Attempts to get the available disk space, should be overridden in a subclass"""
    methodName = 'getDiskSpace'
    self.log.warn( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )
    return S_ERROR( 'Watchdog: ' + methodName + ' method should be implemented in a subclass' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
