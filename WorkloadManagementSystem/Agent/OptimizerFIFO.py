########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/OptimizerFIFO.py,v 1.1 2007/05/16 11:12:59 atsareg Exp $
########################################################################

"""  Optimizer FIFO is the simplest job validation optimizer

"""

from DIRAC.Core.Agent.AgentBase import AgentBase
from DIRAC  import S_OK, S_ERROR, gConfig
import time
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd

AGENT_NAME = 'WorkloadManagement/OptimizerFIFO'

class OptimizerFIFO(AgentBase):

  def __init__(self):
    """ Standard constructor
    """
    
    AgentBase.__init__(self,AGENT_NAME)
    
  def initialize(self):
  
    result = AgentBase.initialize(self) 
    instance = gConfig.getValue('/DIRAC') 
    jobdb_section = getDatabaseSection('WorkloadManagement/JobDB')
    self.jobDB = JobDB(jobdb_section,20)
    logdb_section = getDatabaseSection('WorkloadManagement/JobLoggingDB')
    self.logDB = JobLoggingDB(jobdb_section,20)
    return result
    
  def execute(self):
    """ The main agent execution method
    """
    
    result = self.jobDB.getJobWithStatus('received')['Value'] 
    if not result['OK']:
      self.log.error('Failed to get a job list from the JobDB')
      return S_ERROR('Failed to get a job list from the JobDB')
      
    jobList = result['Value']
    for job in jobList:
      result = self.insertJobInQueue(job)
    
    
  def insertJobInQueue(self,jobID):
    """ Check individual job and add to the Task Queue eventually
    """
      
    # Check if the job is suitable for FIFO
    result = self.jobDB.getInputData(jobID)
    if result['OK']:
      if  result['Value']:
        return

    retVal = self.jobDB.getJobParameters(jobID,['JDL','Priority'])
    if retVal['OK']:
      jdl = retVal['Value']['JDL']
      priority = retVal['Value']['Priority']
    else:
      self.log.error('Failed to get parameters for job %d' % int(jobID))  
      return S_ERROR('Failed to get parameters for job %d' % int(jobID))
      
    if not jdl:
      self.log.error("JDL not found for job %d" % int(jobID))
      self.log.error("The job will be marked problematic")
      self.jobDB.setJobStatus(jobID,status='problem',
                              minor='JDL not found')
      self.logDB.addLoggingRecord(jobID,status='problem',
                                  minor='JDL not found',
                                  source="OptimizerFIFO")                          
      return S_ERROR('Failed to get jdl for job %d' % int(jobID))
      
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
