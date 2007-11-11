########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/InputDataAgent.py,v 1.3 2007/11/11 11:25:43 paterson Exp $
# File :   InputDataAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Input Data Agent queries the file catalogue for specified job input data and adds the
      relevant information to the job optimizer parameters to be used during the
      scheduling decision.

"""

__RCSID__ = "$Id: InputDataAgent.py,v 1.3 2007/11/11 11:25:43 paterson Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

AGENT_NAME = 'WorkloadManagement/InputDataAgent'

class InputDataAgent(Agent):

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
    self.optimizerName      = 'InputData'
    self.nextOptimizerName  = 'AncestorFiles'
    self.finalOptimizerName = 'JobScheduling'

    self.FileCatalogName      = 'LFC'
    self.pollingTime          = gConfig.getValue(self.section+'/PollingTime',60)
    self.jobStatus            = gConfig.getValue(self.section+'/JobStatus','Checking')
    self.minorStatus          = gConfig.getValue(self.section+'/InitialJobMinorStatus',self.optimizerName)
    self.nextOptMinorStatus   = gConfig.getValue(self.section+'/FinalJobMinorStatus',self.nextOptimizerName)
    self.schedulingStatus     = gConfig.getValue(self.section+'/SchedulingJobMinorStatus',self.finalOptimizerName)
    self.failedStatus         = gConfig.getValue(self.section+'/FailedJobStatus','Failed')
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','Input Data Not Available')

    infosys = gConfig.getValue(self.section,'LCG_GFAL_INFOSYS','lcg-bdii.cern.ch:2170')
    host    = gConfig.getValue(self.section,'LFC_HOST','lhcb-lfc.cern.ch')

    try:
      from DIRAC.DataManagement.Client.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.FileCatalog = LcgFileCatalogCombinedClient()
      self.log.debug("Instantiating LFC File Catalog in mode %s %s %s" % (mode,host,infosys) )
    except Exception,x:
      msg = "Failed to create LcgFileCatalogClient"
      self.log.fatal(msg)
      self.log.fatal(str(x))
      result = S_ERROR(msg)

    self.log.debug( '==========================================='           )
    self.log.debug( 'DIRAC '+self.optimizerName+' Agent is started with'    )
    self.log.debug( 'the following parameters:'                             )
    self.log.debug( '==========================================='           )
    self.log.debug( 'Polling Time        ==> %s' % self.pollingTime         )
    self.log.debug( 'Job Status          ==> %s' % self.jobStatus           )
    self.log.debug( 'Job Minor Status    ==> %s' % self.minorStatus         )
    self.log.debug( 'Next Opt Status     ==> %s' % self.nextOptMinorStatus  )
    self.log.debug( 'Scheduling Status   ==> %s' % self.schedulingStatus    )
    self.log.debug( 'Failed Job Status   ==> %s' % self.failedStatus        )
    self.log.debug( 'Failed Minor Status ==> %s' % self.failedMinorStatus   )
    self.log.debug( '==========================================='           )

    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method
    """
    condition = {'Status':self.jobStatus,'MinorStatus':self.minorStatus}
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

    result = self.jobDB.getInputData(job)
    if result['OK']:
      if result['Value']:
        self.log.debug('Job %s has an input data requirement and will be processed' % (job))
        inputData = result['Value']
        result = self.resolveInputData(job,inputData)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
        resolvedData = result['Value']
        result = self.setOptimizerJobInfo(job,self.optimizerName,resolvedData)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
        result = self.updateJobStatus(job,self.jobStatus,self.nextOptimizerName)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
      else:
        self.log.debug('Job %s has no input data requirement' % (job) )
        result = self.updateJobStatus(job,self.jobStatus,self.schedulingStatus)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
    else:
      self.log.error('Failed to get input data from JobdB for %s' % (job) )
      self.log.error(result['Message'])

  #############################################################################
  def resolveInputData(self,job,inputData):
    """This method checks the file catalogue for replica information.
    """

    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    result = self.FileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info(self.FileCatalogName+' Lookup Time: %s seconds ' % (timing) )
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    badLFNCount = 0
    catalogResult = result['Value']
    for lfn,replicas in catalogResult.items():
      if not replicas:
        badLFNCount+=1

    if badLFNCount:
      self.log.info('Found %s LFNs not existing for job %s' % (job,badLFNCount) )
      result = self.updateJobStatus(job,self.failedStatus,self.failedMinorStatus)
      if not result['OK']:
        self.log.error(result['Message'])
        return result

    return result

  #############################################################################
  def setOptimizerJobInfo(self,job,reportName,value):
    """This method sets the job optimizer information that will subsequently
       be used for job scheduling and TURL queries on the WN.
    """
    self.log.debug("self.jobDB.setJobOptParameter(+str(job)+,"+self.OptimizerName+","+value+")")
    result = self.jobDB.setJobOptParameter(jobID,name,value)
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