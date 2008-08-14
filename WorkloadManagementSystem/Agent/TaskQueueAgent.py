########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/TaskQueueAgent.py,v 1.14 2008/08/14 10:13:28 rgracian Exp $
# File :   TaskQueueAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Task Queue Agent acts after Job Scheduling to place the ready jobs
     into a Task Queue.
"""

__RCSID__ = "$Id: TaskQueueAgent.py,v 1.14 2008/08/14 10:13:28 rgracian Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Security.CS                                import getPropertiesForGroup
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
    self.waitingStatus      = gConfig.getValue(self.section+'/WaitingStatus','Waiting')
    self.waitingMinorStatus = gConfig.getValue(self.section+'/WaitingMinorStatus','Pilot Agent Submission')
    self.stagerClient = StagerClient(True)
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    result = self.insertJobInQueue(job)
    if not result['OK']:
      self.log.warn(result['Message'])

    result = self.updateJobStatus(job,self.waitingStatus,self.waitingMinorStatus)
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
    jobType = classadJob.get_expression("JobType").replace('"','')
    pilotType = classadJob.get_expression( "PilotType" ).replace('"','')

    if pilotType == 'private':
      ownerDN = classadJob.get_expression( "OwnerDN" ).replace('"','')
      ownerGroup = classadJob.get_expression( "OwnerGroup" ).replace('"','')
      ownerGroupProperties = getPropertiesForGroup( ownerGroup )
      if not 'JobSharing' in ownerGroupProperties:
        requirements += ' && other.OwnerDN == "%s"' % ownerDN
      requirements += ' && other.OwnerGroup == "%s"' % ownerGroup
    requirements += ' && other.PilotType == "%s"' % pilotType

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
