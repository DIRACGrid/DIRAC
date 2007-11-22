########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/TaskQueueAgent.py,v 1.2 2007/11/22 11:19:49 paterson Exp $
# File :   TaskQueueAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Task Queue Agent acts after Job Scheduling to place the ready jobs
     into a Task Queue.
"""

__RCSID__ = "$Id: TaskQueueAgent.py,v 1.2 2007/11/22 11:19:49 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import S_OK, S_ERROR
import string,re

OPTIMIZER_NAME = 'TaskQueue'

class TaskQueueAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for TaskQueueAgent.
    """
    result = Optimizer.initialize(self)
    self.stagingRequest     = gConfig.getValue(self.section+'/StagingRequest','StagingRequest')
    self.stagingStatus      = gConfig.getValue(self.section+'/StagingStatus','Staging')
    self.stagingMinorStatus = gConfig.getValue(self.section+'/StagingMinorStatus','Request Sent')
    self.waitingStatus      = gConfig.getValue(self.section+'/WaitingStatus','Waiting')
    self.waitingMinorStatus = gConfig.getValue(self.section+'/WaitingMinorStatus','Pilot Agent Submission')
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    primaryStatus = None
    minorStatus   = None
    result = self.getOptimizerJobInfo(job,'StagingRequest')
    if result['OK']:
      primaryStatus  = self.stagingStatus
      minorStatus    = self.stagingMinorStatus
      stagingRequest = result['Value']
      self.log.debug('StagingRequest is: %s' %s)
    else:
      primaryStatus = self.waitingStatus
      minorStatus   = self.waitingMinorStatus

    result = self.insertJobInQueue(job)
    if not result['OK']:
      self.log.warn(result['Message'])

    result = self.updateJobStatus(job,primaryStatus,minorStatus)
    if not result['OK']:
      self.log.warn(result['Message'])

    return result

  #############################################################################
  def insertJobInQueue(self,job):
    """ Check individual job and add to the Task Queue eventually.
    """
    retVal = self.jobDB.getJobParameters(job,['Priority'])
    if retVal['OK']:
      priority = retVal['Value']['Priority']
    else:
      self.log.warn('No priority specified for job %d' % int(job))
      priority = 0

    result = self.jobDB.getJobJDL(job)
    if result['OK']:
      jdl = result['Value']
    else:
      return S_ERROR('Could not obtain JDL for job')

    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.error("Illegal JDL for job %d " % int(job))
      self.log.error("The job will be marked problematic")
      return S_ERROR("Warning: illegal JDL for job %d " % int(job))

    requirements = classadJob.get_expression("Requirements")
    jobType = classadJob.get_expression("JobType")

    result = self.jobDB.selectQueue(requirements)
    if result['OK']:
      queueID = result['Value']
    else:
      self.log.error("Failed to obtain a task queue with the following requirements")
      self.log.error(requirements)
      return S_ERROR("Failed to obtain a task queue")

    rank = priority
    result = self.jobDB.addJobToQueue(job,queueID,rank)
    if not result['OK']:
      return result

    return S_OK('Job Added to Task Queue')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#