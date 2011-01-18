########################################################################
# $HeadURL$
# File :    JobCleaningAgent.py
# Author :  A.T.
########################################################################
"""
The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                      import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB          import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB    import TaskQueueDB
from DIRAC                                            import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient  import SandboxStoreClient
import DIRAC.Core.Utilities.Time as Time

REMOVE_STATUS_DELAY = {'Deleted':0,
                       'Done':14,
                       'Killed':7,
                       'Failed':14 }
PRODUCTION_TYPES = ['DataReconstruction', 'DataStripping', 'MCSimulation', 'Merge', 'production']

class JobCleaningAgent( AgentModule ):
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
    """Sets defaults
    """

    self.am_setOption( "PollingTime", 120.0 )
    self.jobDB = JobDB()
    self.taskQueueDB = TaskQueueDB()
    # self.sandboxDB = SandboxDB( 'SandboxDB' )

    return S_OK()

  #############################################################################
  def execute( self ):
    """The PilotAgent execution method.
    """

    # Remove jobs with final status
    for status, delay in REMOVE_STATUS_DELAY.items():
      if delay > 0:
        delTime = str( Time.dateTime() - delay * Time.day )
      else:
        delTime = ''
      result = self.removeJobsByStatus( status, delTime )
      if not result['OK']:
        gLogger.warn( 'Failed to remove jobs in status %s' % status )
    return S_OK()

  def removeJobsByStatus( self, status, delay ):
    """ Remove deleted jobs
    """
    if delay:
      gLogger.verbose( "Removing jobs with %s status and older than %s" % ( status, delay ) )
    else:
      gLogger.verbose( "Removing jobs with %s status" % status )

    result = self.jobDB.selectJobs( {'Status':status}, older = delay )
    if not result['OK']:
      return result

    jobList = result['Value']

    if status != "Deleted":
      # get job types to skip production jobs
      result = self.jobDB.getAttributesForJobList( jobList, ['JobType'] )
      if not result['OK']:
        return S_ERROR( 'Failed to get job types' )
      attDict = result['Value']
      newJobList = []
      for j in jobList:
        if not attDict[int( j )]['JobType'] in PRODUCTION_TYPES:
          newJobList.append( j )
      jobList = newJobList

    count = 0
    error_count = 0
    result = SandboxStoreClient( useCertificates = True ).unassignJobs( jobList )
    if not result[ 'OK' ]:
      gLogger.warn( "Cannot unassign jobs to sandboxes", result[ 'Message' ] )
    for jobID in jobList:
      resultJobDB = self.jobDB.removeJobFromDB( jobID )
      resultTQ = self.taskQueueDB.deleteJob( jobID )
      # resultISB = self.sandboxDB.removeJob( jobID, 'InputSandbox' )
      # resultOSB = self.sandboxDB.removeJob( jobID, 'OutputSandbox' )
      if not resultJobDB['OK']:
        gLogger.warn( 'Failed to remove job %d from JobDB' % jobID, result['Message'] )
        error_count += 1
      elif not resultTQ['OK']:
        gLogger.warn( 'Failed to remove job %d from TaskQueueDB' % jobID, result['Message'] )
        error_count += 1
      # elif not resultISB['OK']:
      #   gLogger.warn( 'Failed to remove job %d from InputSandboxDB' % jobID, result['Message'] )
      #   error_count += 1
      # elif not resultOSB['OK']:
      #   gLogger.warn( 'Failed to remove job %d from OutputSandboxDB' % jobID, result['Message'] )
      #   error_count += 1
      else:
        count += 1

    if count > 0 or error_count > 0 :
      gLogger.info( 'Deleted %d jobs from JobDB, %d errors' % ( count, error_count ) )
    return S_OK()
