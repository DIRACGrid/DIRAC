########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobPathAgent.py,v 1.1 2007/11/11 11:24:17 paterson Exp $
# File :   JobPathAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Path Agent determines the chain of Agents that must work on the job
      prior to the scheduling decision.

      Initially this takes jobs in the received state and starts the jobs on the
      optimizer chain.  The next development will be to explicitly specify the 
      path through the optimizers. 

"""
__RCSID__ = "$Id: JobPathAgent.py,v 1.1 2007/11/11 11:24:17 paterson Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC                                          import S_OK, S_ERROR

AGENT_NAME = 'WorkloadManagement/JobPathAgent'

class JobPathAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """ Initialization of the Agent.
    """

    result = Agent.initialize(self)
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.FileCatalog        = None
    self.optimizerName      = 'JobPath'
    self.nextOptimizerName  = 'JobSanity'

    self.pollingTime          = gConfig.getValue(self.section+'/PollingTime',60)
    self.jobStatus            = gConfig.getValue(self.section+'/JobStatus','Received')
    self.optStatus            = gConfig.getValue(self.section+'/OptimizerJobStatus','Checking')
    self.nextOptMinorStatus   = gConfig.getValue(self.section+'/OptimizerMinorStatus',self.nextOptimizerName)
    self.failedStatus         = gConfig.getValue(self.section+'/FailedJobStatus','Failed')
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','Input Data Not Available')

    self.log.debug( '==========================================='           )
    self.log.debug( 'DIRAC '+self.optimizerName+' Agent is started with'    )
    self.log.debug( 'the following parameters:'                             )
    self.log.debug( '==========================================='           )
    self.log.debug( 'Polling Time        ==> %s' % self.pollingTime         )
    self.log.debug( 'Job Status          ==> %s' % self.jobStatus           )
    self.log.debug( 'Opt Status          ==> %s' % self.minorStatus         )
    self.log.debug( 'Opt Minor Status     ==> %s' % self.nextOptMinorStatus )
    self.log.debug( '==========================================='           )

    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method
    """
    condition = {'Status':self.jobStatus}
    result = self.jobDB.selectJobs(condition)
    if not result['OK']:
      self.log.error('Failed to get a job list from the JobDB')
      return S_ERROR('Failed to get a job list from the JobDB')

    if not len(result['Value']):
      self.log.debug('No pending jobs to process')
      return S_OK('No work to do')

    jobList = result['Value']
    for job in jobList:
      result = self.checkJob(job)
      if not result['OK']:
        return result

    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    self.log.info('Job %s will be processed' % (job))
    result = self.updateJobStatus(job,self.optStatus,self.nextOptimizerMinorStatus)
    if not result['OK']:
      self.log.error(result['Message'])

    return result

  #############################################################################
  def updateJobStatus(self,job,status,minorstatus=None):
    """This method updates the job status in the JobDB.
    """
    self.log.debug("self.jobDB.setJobAttribute("+str(job)+",Status,"+status+" update=True)")
    result = self.jobDB.setJobAttribute(job,'Status',status, update=True)
    if result['OK']:
      if minorstatus:
        self.log.debug("self.jobDB.setJobAttribute("+str(job)+","+minorstatus+",update=True)")
        result = self.jobDB.setJobAttribute(job,'MinorStatus',minorstatus,update=True)

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#