########################################################################
# $HeadURL$
# File :    TaskQueueAgent.py
# Author :  Stuart Paterson
########################################################################
"""  The Task Queue Agent acts after Job Scheduling to place the ready jobs
     into a Task Queue.
"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB         import TaskQueueDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import S_OK, S_ERROR

class TaskQueueAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for TaskQueueAgent.
    """
    self.waitingStatus = self.am_getOption( 'WaitingStatus', 'Waiting' )
    self.waitingMinorStatus = self.am_getOption( 'WaitingMinorStatus', 'Pilot Agent Submission' )
    try:
      self.taskQueueDB = TaskQueueDB()
      result = self.taskQueueDB.enableAllTaskQueues()
      if not result[ 'OK' ]:
        raise Exception( "Can't enable TaskQueues: %s" % result[ 'Message' ] )

    except Exception, e:
      self.log.exception()
      return S_ERROR( "Cannot initialize taskqueueDB: %s" % str( e ) )
    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """
    result = self.insertJobInQueue( job, classAdJob )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return S_ERROR( result[ 'Message' ] )

    result = self.updateJobStatus( job, self.waitingStatus, self.waitingMinorStatus, 'Unknown' )
    if not result['OK']:
      self.log.warn( result['Message'] )

    return S_OK()

  #############################################################################
  def insertJobInQueue( self, job, classAdJob ):
    """ Check individual job and add to the Task Queue eventually.
    """

    jobReq = classAdJob.get_expression( "JobRequirements" )
    classAdJobReq = ClassAd( jobReq )
    jobReqDict = {}
    for name in self.taskQueueDB.getSingleValueTQDefFields():
      if classAdJobReq.lookupAttribute( name ):
        if name == 'CPUTime':
          jobReqDict[name] = classAdJobReq.getAttributeInt( name )
        else:
          jobReqDict[name] = classAdJobReq.getAttributeString( name )

    for name in self.taskQueueDB.getMultiValueTQDefFields():
      if classAdJobReq.lookupAttribute( name ):
        jobReqDict[name] = classAdJobReq.getListFromExpression( name )

    jobPriority = classAdJobReq.getAttributeInt( 'UserPriority' )

    result = self.taskQueueDB.insertJob( job, jobReqDict, jobPriority )
    if not result[ 'OK' ]:
      self.log.error( "Cannot insert job %s in task queue: %s" % ( job, result[ 'Message' ] ) )
      # Force removing the job from the TQ if it was actually inserted
      result = self.taskQueueDB.deleteJob( job )
      if result['OK']:
        if result['Value']:
          self.log.info( "Job %s removed from the TQ" % job )
      return S_ERROR( "Cannot insert in task queue" )
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
