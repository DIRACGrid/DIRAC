########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/TaskQueueAgent.py,v 1.23 2008/12/20 18:02:23 rgracian Exp $
# File :   TaskQueueAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Task Queue Agent acts after Job Scheduling to place the ready jobs
     into a Task Queue.
"""

__RCSID__ = "$Id: TaskQueueAgent.py,v 1.23 2008/12/20 18:02:23 rgracian Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB         import TaskQueueDB
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Security.CS                                import getPropertiesForGroup
from DIRAC                                                 import S_OK, S_ERROR, Time
import string,re

class TaskQueueAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """Initialize specific parameters for TaskQueueAgent.
    """
    self.waitingStatus      = self.am_getOption( 'WaitingStatus', 'Waiting' )
    self.waitingMinorStatus = self.am_getOption( 'WaitingMinorStatus', 'Pilot Agent Submission' )
    try:
      self.taskQueueDB        = TaskQueueDB()
    except Exception, e:
      return S_ERROR( "Cannot initialize taskqueueDB: %s" % str(e) )
    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """
    result = self.insertJobInQueue( job, classAdJob )
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR( result[ 'Message' ] )

    result = self.updateJobStatus( job, self.waitingStatus, self.waitingMinorStatus )
    if not result['OK']:
      self.log.warn(result['Message'])

    return S_OK()

  #############################################################################
  def insertJobInQueue( self, job, classAdJob ):
    """ Check individual job and add to the Task Queue eventually.
    """
    retVal = self.jobDB.getJobAttributes( job, ['UserPriority'] )
    if retVal['OK']:
      if retVal['Value']:
        jobPriority = retVal['Value']['UserPriority']
      else:
        self.log.warn('No priority specified for job %d' % int(job))
        jobPriority = 0
    else:
      jobPriority=0

    requirements = classAdJob.get_expression("Requirements")
    jobType = classAdJob.get_expression("JobType").replace('"','')
    pilotType = classAdJob.get_expression( "PilotType" ).replace('"','')

    if pilotType == 'private':
      ownerDN = classAdJob.get_expression( "OwnerDN" ).replace('"','')
      ownerGroup = classAdJob.get_expression( "OwnerGroup" ).replace('"','')
      ownerGroupProperties = getPropertiesForGroup( ownerGroup )
      if not 'JobSharing' in ownerGroupProperties:
        requirements += ' && other.OwnerDN == "%s"' % ownerDN
      requirements += ' && other.OwnerGroup == "%s"' % ownerGroup
    requirements += ' && other.PilotType == "%s"' % pilotType

    jobReq = classAdJob.get_expression("JobRequirements")
    classAdJobReq = ClassAd(jobReq)
    jobReqDict = {}
    for name in self.taskQueueDB.getSingleValueTQDefFields():
      if classAdJobReq.lookupAttribute(name):
        if name == 'CPUTime':
          jobReqDict[name] = classAdJobReq.getAttributeInt(name)
        else:
          jobReqDict[name] = classAdJobReq.getAttributeString(name)

    for name in self.taskQueueDB.getMultiValueTQDefFields():
      if classAdJobReq.lookupAttribute(name):
        jobReqDict[name] = classAdJobReq.getListFromExpression(name)

    result = self.taskQueueDB.insertJob( job, jobReqDict, jobPriority )
    if not result[ 'OK' ]:
      self.log.error( "Cannot insert job %s in task queue: %s" % ( job, result[ 'Message' ] ) )
      return S_ERROR( "Cannot insert in task queue" )
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
