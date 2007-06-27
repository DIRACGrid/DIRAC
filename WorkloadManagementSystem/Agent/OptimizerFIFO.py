########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/OptimizerFIFO.py,v 1.4 2007/06/27 15:22:41 atsareg Exp $
########################################################################

"""  Optimizer FIFO is the simplest job validation optimizer

"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd

AGENT_NAME = 'WorkloadManagement/OptimizerFIFO'

class OptimizerFIFO(Agent):

  def __init__(self):
    """ Standard constructor
    """

    Agent.__init__(self,AGENT_NAME)

  def initialize(self):

    result = Agent.initialize(self)
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    return result

  def execute(self):
    """ The main agent execution method
    """

    result = self.jobDB.selectJobWithStatus('received')
    if not result['OK']:
      self.log.error('Failed to get a job list from the JobDB')
      return S_ERROR('Failed to get a job list from the JobDB')

    if not len(result['Value']):
      return S_OK()

    jobList = result['Value']
    for job in jobList:
      result = self.insertJobInQueue(job)

    return result

  def insertJobInQueue(self,jobID):
    """ Check individual job and add to the Task Queue eventually
    """

    # Check if the job is suitable for FIFO
    result = self.jobDB.getInputData(jobID)
    if result['OK']:
      if  result['Value']:
        return

    retVal = self.jobDB.getJobParameters(jobID,['Priority'])
    if retVal['OK']:
      priority = retVal['Value']['Priority']
    else:
      self.log.error('Failed to get parameters for job %d' % int(jobID))
      return S_ERROR('Failed to get parameters for job %d' % int(jobID))
      
    result = self.jobDB.getJobJDL(jobID) 
    if result['OK']:
      jdl = result['Value']
    else:
      jdl = None   

    if not jdl:
      self.log.error("JDL not found for job %d" % int(jobID))
      self.log.error("The job will be marked problematic")
      self.jobDB.setJobStatus(jobID,status='problem',
                              minor='JDL not found')
      self.logDB.addLoggingRecord(jobID,status='problem',
                                  minor='JDL not found',
                                  source="OptimizerFIFO")
      return S_ERROR('Failed to get jdl for job %d' % int(jobID))

    print "$$$$$$$$$$$$$$",jdl
    
    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.error("Illegal JDL for job %d " % int(jobID))
      self.log.error("The job will be marked problematic")
      self.jobDB.setJobStatus(jobID,status='problem',
                              minor='JDL illegal')
      self.logDB.addLoggingRecord(jobID,status='problem',
                                  minor='JDL illegal',
                                  source="OptimizerFIFO")
      return S_ERROR("Warning: illegal JDL for job %d " % int(jobID))

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
    self.jobDB.addJobToQueue(jobID,queueID,rank)
    # Update status
    self.jobDB.setJobStatus(jobID,status='waiting',
                            minor='PilotAgent Submission')
    self.logDB.addLoggingRecord(jobID,status="waiting",
                                minor='PilotAgent Submission',
                                source="OptimizerFIFO")
    return S_OK()
