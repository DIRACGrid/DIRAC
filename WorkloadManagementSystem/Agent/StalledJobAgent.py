########################################################################
# $HeadURL$
# File :   StalledJobAgent.py
########################################################################

"""  The StalledJobAgent hunts for stalled jobs in the Job database. Jobs in "running"
     state not receiving a heartbeat signal for more than stalledTime
     seconds will be assigned the "Stalled" state.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Base.AgentModule                    import AgentModule
from DIRAC.Core.Utilities.Time                      import fromString, toEpoch
from DIRAC                                          import gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                     import RPCClient
import time

class StalledJobAgent( AgentModule ):

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
    
    failedCounter = 0
    
    if not result['OK'] or not result['Value']:
      self.log.warn( result )
      return result
    else:
      jobs = result['Value']
      self.log.info( '%s Stalled jobs will be checked for failure' % ( len( jobs ) ) )
            
      for job in jobs:
        
        # Check if the job pilot is lost
        result = self.__getJobPilotStatus(job)
        if result['OK']:
          pilotStatus = result['Status']
          if pilotStatus != "Running":
            result = self.__updateJobStatus( job, 'Failed', "Pilot stopped after job stalling" )  
            failedCounter += 1
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
          
    self.log.info('%d jobs set to Failed' % failedCounter)       
    return S_OK(failedCounter)      
        
 #############################################################################
  def __getJobPilotStatus( self, jobID ):
    """ Get the job pilot status
    """       
    result = self.jobDB.getJobParameter(jobID,'Pilot_Reference')
    if result['OK']:
      pilotReference = result['Value']
      wmsAdminClient = RPCClient('WorkloadManagement/WMSAdministrator')
      result = wmsAdminClient.getPilotInfo(pilotReference)
      if result['OK']:
        pilotStatus = result['Value']['Status']
        return S_OK(pilotStatus)
      else:
        return S_ERROR('Failed to get the pilot status')
    else:
      return S_ERROR('Failed to get the pilot reference')
    

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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
