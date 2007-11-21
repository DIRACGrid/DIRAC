########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobSchedulingAgent.py,v 1.1 2007/11/21 10:24:03 paterson Exp $
# File :   JobSchedulingAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Scheduling Agent takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.  Subsequent to this
      jobs are added into a Task Queue by the next optimizer and pilot agents can
      be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.  This Agent will fail affected jobs
      meaningfully.

"""
__RCSID__ = "$Id: JobSchedulingAgent.py,v 1.1 2007/11/21 10:24:03 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC                                                 import S_OK, S_ERROR

OPTIMIZER_NAME = 'JobScheduling'

class JobSchedulingAgent(Optimizer):
  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """ Initialization of the Agent.
    """
    result = Optimizer.initialize(self)
    #Flags to switch off the ProcessingDB and Ancestor files checks
    self.disableAncestorCheck = gConfig.getValue(self.section+'/DisableAncestorsCheck',True)
    self.disableProcDBCheck   = gConfig.getValue(self.section+'/DisableProcDBCheck',True)
    self.dataAgentName        = gConfig.getValue(self.section+'/InputDataAgent','InputData')
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    self.log.info('Job %s will be processed' % (job))
    #First check whether the job has an input data requirement
    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.warn('Failed to get input data from JobdB for %s' % (job))
      self.log.error(result['Message'])
    if not result['Value']:
      #With no input data requirement, job can proceed directly to task queue
      self.log.info('Job %s has no input data requirement' % (job))
      result = sendJobToTaskQueue(job)
      return result

    self.log.debug('Job %s has an input data requirement ' % (job))

    #Check all optimizer information
    optInfo = self.checkOptimizerInfo(job)
    if not optInfo.has_key('SiteCandidates'):
      msg = optInfo['Value']
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)

    #Compare site candidates with current mask
    siteCandidates = optInfo['SiteCandidates'].keys()
    self.log.info('Site Candidates: %s' % (siteCandidates))
    maskSiteCandidates = self.checkSitesInMask(job,siteCandidates)
    siteCandidates = maskSiteCandidates['Value']
    if not siteCandidates:
      msg = 'No site candidates in mask'
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    #Compare site candidates with site requirement / banned sites in JDL
    jobReqtCandidates = self.checkJobSiteRequirement(job,siteCandidates)
    siteCandidates = jobReqtCandidates['Value']
    if not siteCandidates:
      msg = 'Conflict with job site requirement'
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    #Set stager request as necessary, optimize for smallest #files on tape if
    #more than one site candidate left at this point
    checkStaging = resolveSitesForStaging(job,optInfo['SiteCandidates'])
    if not checkStaging['OK']:
      self.log.warn(result['Message'])
      return result

    stagerDict = checkStaging['Value']
    if stagerDict:
      #Single site candidate chosen and staging required
      self.log.info('Job %s requires staging of input data' %(job))
      self.log.debug('%s : %s ' %(self.stageRequest,stagerDict))
      #Save stager request as job opt parameter and continue
    else:
      #No staging required, can proceed to task queue agent and then waiting status
      self.log.info('Job %s does not require staging of input data' %(job))

    destinationSites = stagerDict['SiteCandidates']
    #Finally send job to TaskQueueAgent
    result = sendJobToTaskQueue(job,destinationSites)


  #############################################################################
  def checkOptimizerInfo(self,job):
    """This method aggregates information from optimizers to return a list of
       site candidates and all information regarding input data.
    """
    siteCandidates = {}
    #Check input data agent result and limit site candidates accordingly
    dataResult = self.getOptimizerJobInfo(job,self.dataAgentName)
    if dataResult['OK'] and len(dataResult['Value']):
      dataDict = dataResult['Value']
      siteCandidates = dataDict['SiteCandidates']
    else:
      self.log.info('No information available for optimizer %s' %(self.dataAgentName))

    if not len(siteCandidates.keys()):
      msg = 'No possible sites for input data'
      self.log.info(msg)
      return S_OK(msg)

  #############################################################################
  def resolveSitesForStaging(self, job, inputDataDict):
    """Site candidates are resolved from potential candidates and any job site
       requirement is compared at this point.  A Staging request is formulated
       if necessary.
    """
    return S_OK()
    # returns S_OK({sitecandidates:<>,value:''}) for nothing and S_OK({value:stageRequest,sitecandidates:''}) for value


  #############################################################################
  def checkJobSiteRequirement(self):
    """Get Grid site list from the DIRAC CS, choose only those which are allowed
       in the Matcher mask for the scheduling decision.
    """
    # must check job site requirement as well as any banned sites
    return S_OK()

  #############################################################################
  def __getJobSiteRequirement(self,job):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """
    return S_OK()

  #############################################################################
  def __setJobSiteRequirement(self,job,siteCandidates):
    """Will set the job site requirement for the final candidate sites.
    """
    return S_OK()

  #############################################################################
  def sendJobToTaskQueue(self,job,siteCandidates=None):
    """This method sends jobs to the task queue agent and if candidate sites
       are defined, updates job JDL accordingly.
    """
    if siteCandidates:
      # must update JDL to include only selected sites
      self.log.info('Restricting possible sites to %s for job %s' % (job,siteCandidates))

    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.error(result['Message'])

    return result

  #############################################################################
  def getGridSitesInMask(self):
    """Get Grid site list from the DIRAC CS, choose only those which are allowed
       in the Matcher mask for the scheduling decision.
    """
    result = self.jobDB.getMask()
    self.log.debug(result)
    if result['OK'] and result['Value']:
      tmp_list = result['Value'].split('"')
      mask = []
      for i in range(1,len(tmp_list),2):
        mask.append(tmp_list[i])
      return S_OK(mask)
    else:
      self.log.warn('Failed to get mask from JobdB')
      self.log.warn(result['Message'])
      return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#