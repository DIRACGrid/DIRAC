########################################################################
# $HeadURL$
# File :   StalledJobAgent.py
########################################################################
"""  The StalledJobAgent hunts for stalled jobs in the Job database. Jobs in "running"
     state not receiving a heart beat signal for more than stalledTime
     seconds will be assigned the "Stalled" state.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Base.AgentModule                    import AgentModule
from DIRAC.Core.Utilities.Time                      import fromString, toEpoch, dateTime
from DIRAC                                          import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.AccountingSystem.Client.Types.Job        import Job
import time, types

class StalledJobAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  #############################################################################
  def initialize( self ):
    """Sets default parameters
    """
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.am_setOption( 'PollingTime', 60 * 60 )
    self.enable = self.am_getOption( 'Enable', True )
    if not self.enable:
      self.log.info( 'Stalled Job Agent running in disabled mode' )
    return S_OK()

  #############################################################################
  def execute( self ):
    """ The main agent execution method
    """
    self.log.verbose( 'Waking up Stalled Job Agent' )
    stalledTime = self.am_getOption( 'StalledTimeHours', 2 )
    failedTime = self.am_getOption( 'FailedTimeHours', 6 )
    self.log.verbose( 'StalledTime = %s hours' % ( stalledTime ) )
    self.log.verbose( 'FailedTime = %s hours' % ( failedTime ) )
    try:
      stalledTime = int( stalledTime ) * 60 * 60
      failedTime = int( failedTime ) * 60 * 60
    except Exception, x:
      self.log.warn( 'Problem while converting stalled time (hours) to seconds' )
      self.log.warn( str( x ) )
      return S_OK( 'Problem while converting configuration times' )

    result = self.__markStalledJobs( stalledTime )
    if not result['OK']:
      self.log.info( result['Message'] )

    #Note, jobs will be revived automatically during the heartbeat signal phase and
    #subsequent status changes will result in jobs not being selected by the
    #stalled job agent.

    result = self.__failStalledJobs( failedTime )
    if not result['OK']:
      self.log.info( result['Message'] )

    return S_OK( 'Stalled Job Agent cycle complete' )

  #############################################################################
  def __markStalledJobs( self, stalledTime ):
    """ Identifies stalled jobs running without update longer than stalledTime.
    """
    stalledCounter = 0
    runningCounter = 0
    result = self.jobDB.selectJobs( {'Status':'Running'} )
    if not result['OK'] or not result['Value']:
      self.log.warn( result )
      return result
    else:
      jobs = result['Value']
      self.log.info( '%s Running jobs will be checked for being stalled' % ( len( jobs ) ) )
      jobs.sort()
#      jobs = jobs[:10] #for debugging
      for job in jobs:
        result = self.__getStalledJob( job, stalledTime )
        if result['OK']:
          self.log.verbose( 'Updating status to Stalled for job %s' % ( job ) )
          self.__updateJobStatus( job, 'Stalled' )
          stalledCounter += 1
        else:
          self.log.verbose( result['Message'] )
          runningCounter += 1

    self.log.info( 'Total jobs: %s, Stalled job count: %s, Running job count: %s' % ( len( jobs ), stalledCounter, runningCounter ) )
    return S_OK()

  #############################################################################
  def __failStalledJobs( self, failedTime ):
    """ Changes the Stalled status to Failed for jobs long in the Stalled status
    """

    result = self.jobDB.selectJobs( {'Status':'Stalled'} )
    if not result['OK']:
      self.log.error( result['Message'] )
      return result

    failedCounter = 0

    if not result['OK'] or not result['Value']:
      self.log.warn( result )
    else:
      jobs = result['Value']
      self.log.info( '%s Stalled jobs will be checked for failure' % ( len( jobs ) ) )

      for job in jobs:

        # Check if the job pilot is lost
        result = self.__getJobPilotStatus( job )
        if result['OK']:
          pilotStatus = result['Value']
          if pilotStatus != "Running":
            result = self.__updateJobStatus( job, 'Failed',
                                             "Job stalled: pilot not running" )
            failedCounter += 1
            result = self.sendAccounting( job )
            continue

        result = self.__getLatestUpdateTime( job )
        if not result['OK']:
          return result
        currentTime = time.mktime( time.gmtime() )
        lastUpdate = result['Value']
        elapsedTime = currentTime - lastUpdate
        if elapsedTime > failedTime:
          self.__updateJobStatus( job, 'Failed', 'Stalling for more than %d sec' % failedTime )
          failedCounter += 1
          result = self.sendAccounting( job )

    recoverCounter = 0

    for minor in ["Job stalled: pilot not running", 'Stalling for more than %d sec' % failedTime]:
      result = self.jobDB.selectJobs( {'Status':'Failed', 'MinorStatus':  minor, 'AccountedFlag': 'False' } )
      if not result['OK']:
        self.log.error( result['Message'] )
        return result
      if result['Value']:
        jobs = result['Value']
        self.log.info( '%s Stalled jobs will be Accounted' % ( len( jobs ) ) )
        for job in jobs:
          result = self.sendAccounting( job )
          if not result['OK']:
            break
          recoverCounter += 1
      if not result['OK']:
        break

    self.log.info( '%d jobs set to Failed' % failedCounter )
    if recoverCounter:
      self.log.info( '%d jobs properly Accounted' % recoverCounter )
    return S_OK( failedCounter )

  #############################################################################
  def __getJobPilotStatus( self, jobID ):
    """ Get the job pilot status
    """
    result = self.jobDB.getJobParameter( jobID, 'Pilot_Reference' )
    if result['OK'] and result['Value']:
      pilotReference = result['Value']
      wmsAdminClient = RPCClient( 'WorkloadManagement/WMSAdministrator' )
      result = wmsAdminClient.getPilotInfo( pilotReference )
      if result['OK']:
        pilotStatus = result['Value'][pilotReference]['Status']
        return S_OK( pilotStatus )
      else:
        return S_ERROR( 'Failed to get the pilot status' )
    else:
      return S_ERROR( 'Failed to get the pilot reference' )


  #############################################################################
  def __getStalledJob( self, job, stalledTime ):
    """ Compares the most recent of LastUpdateTime and HeartBeatTime against
        the stalledTime limit.
    """
    result = self.__getLatestUpdateTime( job )
    if not result['OK']:
      return result

    currentTime = time.mktime( time.gmtime() )
    lastUpdate = result['Value']

    elapsedTime = currentTime - lastUpdate
    self.log.verbose( '(CurrentTime-LastUpdate) = %s secs' % ( elapsedTime ) )
    if elapsedTime > stalledTime:
      self.log.info( 'Job %s is identified as stalled with last update > %s secs ago' % ( job, elapsedTime ) )
      return S_OK( 'Stalled' )

    return S_ERROR( 'Job %s is running and will be ignored' % job )

  #############################################################################
  def __getLatestUpdateTime( self, job ):
    """Returns the most recent of HeartBeatTime and LastUpdateTime
    """
    result = self.jobDB.getJobAttributes( job, ['HeartBeatTime', 'LastUpdateTime'] )
    if not result['OK'] or not result['Value']:
      self.log.warn( result )
      return S_ERROR( 'Could not get attributes for job %s' % job )

    self.log.verbose( result )
    latestUpdate = 0
    if not result['Value']['HeartBeatTime'] or result['Value']['HeartBeatTime'] == 'None':
      self.log.verbose( 'HeartBeatTime is null for job %s' % job )
    else:
      latestUpdate = toEpoch( fromString( result['Value']['HeartBeatTime'] ) )

    if not result['Value']['LastUpdateTime'] or result['Value']['LastUpdateTime'] == 'None':
      self.log.verbose( 'LastUpdateTime is null for job %s' % job )
    else:
      lastUpdate = toEpoch( fromString( result['Value']['LastUpdateTime'] ) )
      if latestUpdate < lastUpdate:
        latestUpdate = lastUpdate

    if not latestUpdate:
      return S_ERROR( 'LastUpdate and HeartBeat times are null for job %s' % job )
    else:
      self.log.verbose( 'Latest update time from epoch for job %s is %s' % ( job, latestUpdate ) )
      return S_OK( latestUpdate )

  #############################################################################
  def __updateJobStatus( self, job, status, minorstatus = None ):
    """This method updates the job status in the JobDB, this should only be
       used to fail jobs due to the optimizer chain.
    """
    self.log.verbose( "self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" % ( job, status ) )
    if self.enable:
      result = self.jobDB.setJobAttribute( job, 'Status', status, update = True )
    else:
      result = S_OK( 'DisabledMode' )

    if result['OK']:
      if minorstatus:
        self.log.verbose( "self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" % ( job, minorstatus ) )
        if self.enable:
          result = self.jobDB.setJobAttribute( job, 'MinorStatus', minorstatus, update = True )
        else:
          result = S_OK( 'DisabledMode' )

    if not minorstatus: #Retain last minor status for stalled jobs
      result = self.jobDB.getJobAttributes( job, ['MinorStatus'] )
      if result['OK']:
        minorstatus = result['Value']['MinorStatus']

    if self.enable:
      logStatus = status
      result = self.logDB.addLoggingRecord( job, status = logStatus, minor = minorstatus, source = 'StalledJobAgent' )
      if not result['OK']:
        self.log.warn( result )

    return result

  #############################################################################
  def sendAccounting( self, jobID ):
    """Send WMS accounting data for the given job
    """

    accountingReport = Job()

    result = self.jobDB.getJobAttributes( jobID )
    if not result['OK']:
      return result
    jobDict = result['Value']

    result = self.logDB.getJobLoggingInfo( jobID )
    if not result['OK']:
      logList = []
    else:
      logList = result['Value']

    startTime = jobDict['StartExecTime']
    endTime = ''

    if not startTime or startTime == 'None':
      for status, minor, app, stime, source in logList:
        if status == 'Running':
          startTime = stime
          break
      for status, minor, app, stime, source in logList:
        if status == 'Stalled':
          endTime = stime
      if not startTime or startTime == 'None':
        startTime = jobDict['SubmissionTime']

    if type( startTime ) in types.StringTypes:
      startTime = fromString( startTime )


    result = self.logDB.getJobLoggingInfo( jobID )
    if not result['OK']:
      endTime = dateTime()
    else:
      for status, minor, app, stime, source in result['Value']:
        if status == 'Stalled':
          endTime = stime
          break
    if not endTime:
      endTime = dateTime()

    if type( endTime ) in types.StringTypes:
      endTime = fromString( endTime )

    result = self.jobDB.getHeartBeatData( jobID )

    lastCPUTime = 0
    lastWallTime = 0
    lastHeartBeatTime = jobDict['StartExecTime']
    if result['OK']:
      for name, value, heartBeatTime in result['Value']:
        if 'CPUConsumed' == name:
          try:
            value = int( float( value ) )
            if value > lastCPUTime:
              lastCPUTime = value
          except:
            pass
        if 'WallClockTime' == name:
          try:
            value = int( float( value ) )
            if value > lastWallTime:
              lastWallTime = value
          except:
            pass
        if heartBeatTime > lastHeartBeatTime:
          lastHeartBeatTime = heartBeatTime

    accountingReport.setStartTime( startTime )
    accountingReport.setEndTime()
    # execTime = toEpoch( endTime ) - toEpoch( startTime )
    #Fill the accounting data
    acData = { 'Site' : jobDict['Site'],
               'User' : jobDict['Owner'],
               'UserGroup' : jobDict['OwnerGroup'],
               'JobGroup' : jobDict['JobGroup'],
               'JobType' : jobDict['JobType'],
               'JobClass' : jobDict['JobSplitType'],
               'ProcessingType' : 'unknown',
               'FinalMajorStatus' : 'Failed',
               'FinalMinorStatus' : 'Stalled',
               'CPUTime' : lastCPUTime,
               'NormCPUTime' : 0.0,
               'ExecTime' : lastWallTime,
               'InputDataSize' : 0.0,
               'OutputDataSize' : 0.0,
               'InputDataFiles' : 0,
               'OutputDataFiles' : 0,
               'DiskSpace' : 0.0,
               'InputSandBoxSize' : 0.0,
               'OutputSandBoxSize' : 0.0,
               'ProcessedEvents' : 0
             }
    self.log.verbose( 'Accounting Report is:' )
    self.log.verbose( acData )
    accountingReport.setValuesFromDict( acData )

    result = accountingReport.commit()
    if result['OK']:
      self.jobDB.setJobAttribute( jobID, 'AccountedFlag', 'True' )
    else:
      self.log.warn( 'Failed to send accounting report for job %d' % int( jobID ) )
      self.log.error( result['Message'] )
    return result
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
