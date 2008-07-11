########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/TaskQueueAgent.py,v 1.7 2008/07/11 16:09:39 acasajus Exp $
# File :   TaskQueueAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Task Queue Agent acts after Job Scheduling to place the ready jobs
     into a Task Queue.
"""

__RCSID__ = "$Id: TaskQueueAgent.py,v 1.7 2008/07/11 16:09:39 acasajus Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.StagerSystem.Client.StagerClient                import StagerClient
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
    self.stagerClient = StagerClient(True)
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    primaryStatus = None
    minorStatus   = None
    result = self.getOptimizerJobInfo(job,'StagingRequest')
    if result['OK']:
      self.log.info('Attempting to send staging request for job %s' %job)
      primaryStatus  = self.stagingStatus
      minorStatus    = self.stagingMinorStatus
      site = result['Value']['Sites']
      self.log.verbose('Site: %s' %site)
      files = result['Value']['Files']
      self.log.verbose('Files: %s' %files)
      if self.enable:
        request = self.stagerClient.stageFiles(str(job),str(site),files,'WorkloadManagement')
        if not request['OK']:
          self.log.warn('Problem sending Staging request:')
          self.log.warn(request)
          return S_ERROR('Sending Staging Request')
        else:
          self.log.info('Staging request successfully sent')
      else:
        self.log.info('TaskQueue agent disabled via enable flag')
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
      if retVal['Value']:
        priority = retVal['Value']['Priority']
      else:
        self.log.warn('No priority specified for job %d' % int(job))
        priority = 0
    else:
      priority=0

    result = self.jobDB.getJobJDL(job)
    if result['OK']:
      jdl = result['Value']
    else:
      return S_ERROR('Could not obtain JDL for job')

    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.warn("Illegal JDL for job %d " % int(job))
      self.log.warn("The job will be marked problematic")
      return S_ERROR('Illegal JDL')

    requirements = classadJob.get_expression("Requirements")
    jobType = classadJob.get_expression("JobType")
    pilotType = classadJob.get_expression( "PilotType" )

    if pilotType == "private":
      ownerDN = classadJob.get_expression( "OwnerDN" )
      ownerGroup = classadJob.get_expression( "OwnerGroup" )
      requirements += ' && other.OwnerDN = "%s" && other.OwnerGroup = "%s"' % ( ownerDN , ownerGroup )
    requirements += ' && other.PilotType = "%s"' % pilotType

    result = self.jobDB.selectQueue(requirements)
    if result['OK']:
      queueID = result['Value']
    else:
      self.log.warn("Failed to obtain a task queue with the following requirements")
      self.log.warn(requirements)
      return S_ERROR("Failed to obtain a task queue")

    rank = priority
    if self.enable:
      result = self.jobDB.addJobToQueue(job,queueID,rank)
      if not result['OK']:
        return result
    else:
      self.log.info('TaskQueue agent disabled via enable flag')

    return S_OK('Job Added to Task Queue')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
