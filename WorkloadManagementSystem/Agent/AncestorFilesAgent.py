########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/AncestorFilesAgent.py,v 1.4 2007/11/17 15:58:43 paterson Exp $
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently checked in the LFC and the result is stored for the
      scheduling decision.

"""

__RCSID__ = "$Id: AncestorFilesAgent.py,v 1.4 2007/11/17 15:58:43 paterson Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
#from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
#this won't function until the getAncestors call is available...
#from DIRAC.Core.Utilities.genCatalog                import getAncestors
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

import os, re, time, string

AGENT_NAME = 'WorkloadManagement/AncestorFilesAgent'

class AncestorFilesAgent(Agent):

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
    #self.logDB = JobLoggingDB()
    self.optimizerName      = 'AncestorFiles'
    self.nextOptimizerName  = 'ProcessingDB'
    self.finalOptimizerName = 'JobScheduling'

    #until the BK interface is available, can disable the optimizer and pass jobs through
    self.disableAncestorCheck = True

    #In disabled mode, no statuses will be updated to allow debugging.
    self.enable               = gConfig.getValue(self.section+'/EnableFlag',True)
    #other parameters
    self.pollingTime          = gConfig.getValue(self.section+'/PollingTime',60)
    self.jobStatus            = gConfig.getValue(self.section+'/JobStatus','Checking')
    self.minorStatus          = gConfig.getValue(self.section+'/InitialJobMinorStatus',self.optimizerName)
    self.nextOptMinorStatus   = gConfig.getValue(self.section+'/FinalJobMinorStatus',self.nextOptimizerName)
    self.schedulingStatus     = gConfig.getValue(self.section+'/SchedulingJobMinorStatus',self.finalOptimizerName)
    self.failedStatus         = gConfig.getValue(self.section+'/FailedJobStatus','Failed')
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','genCatalog Error')

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
      result = self.getAncestorDepth(job)
      if not result['OK']:
        self.log.error(result['Message'])
        return result

      ancestorDepth=result['Value']
      if ancestorDepth:
        self.log.info('Job %s has ancestor depth of %s' % (job,ancestorDepth))
        if int(ancestorDepth) > 0:
          result = self.checkJob(job,ancestorDepth)
          if not result['OK']:
            return result
      else:
        self.log.info('Job %s has no AncestorDepth (>0) specified in JDL' %(job))
        self.updateJobStatus(job,self.jobStatus,self.nextOptMinorStatus)

    return result

  #############################################################################
  def getAncestorDepth(self,job):
    """This method checks for any JDL parameters that require the treatment
       of further optimizers.  The JDL would already have been checked by the
       JobSanityAgent at this point.
    """
    jobID = str(job)
    self.log.debug("Checking JDL for job: "+jobID)
    retVal = self.jobDB.getJobJDL(jobID)

    if not retVal['OK']:
      result = S_ERROR()
      result['Value'] = "Job JDL not found in JobDB"
      return result

    jdl = retVal['Value']
    classadJob = ClassAd('['+jdl+']')
    ancestorDepth = classadJob.get_expression('AncestorDepth').replace('"','').replace('Unknown','')
    if not ancestorDepth:
      ancestorDepth=0
    result = S_OK()
    result['Value'] = ancestorDepth
    return result

  #############################################################################
  def checkJob(self,job,ancestorDepth):
    """This method controls the checking of input data with ancestors and updates
       the optimizer information for the scheduling.
    """

    for job in jobList:
      # Check if the job is suitable for the AncestorFilesAgent
      result = self.jobDB.getInputData(job)
      if result['OK']:
        if result['Value']:
          self.log.info('Job %s has an input data requirement and Ancestor Files will be checked' % (job))
          inputData = result['Value']
          if not self.disableAncestorCheck:
            result = self.getInputDataWithAncestors(job,inputData,ancestorDepth)
            if not result['OK']:
              self.log.error(result['Message'])
              self.updateJobStatus(job,self.failedStatus,self.failedMinorStatus)
              return result
            ancestorFiles = result['Value']
            result = self.setOptimizerJobInfo(job,self.optimizerName,ancestorFiles)
            if not result['OK']:
              self.log.error(result['Message'])
              return result
          else:
            self.log.info('Ancestor files check is disabled, passing job to %s' %(self.nextOptMinorStatus))
          result = self.updateJobStatus(job,self.jobStatus,self.nextOptMinorStatus)
          if not result['OK']:
            self.log.error(result['Message'])
          return result
        else:
          self.log.debug('Job %s has no input data requirement' % (job))
          result = self.updateJobStatus(job,self.jobStatus,self.schedulingStatus)
          if not result['OK']:
            self.log.error(result['Message'])
          return result
      else:
        self.log.error('Failed to get input data from JobdB for %s' %(job) )
        self.log.error(result['Message'])
        return result

  ############################################################################
  def getInputDataWithAncestors(self,inputData,ancestorDepth):
    """Extend the list of LFNs with the LFNs for their ancestor files
       for the generation depth specified in the job.
    """

    inputData = [ i.replace('LFN:','') for i in inputData]

    start = time.time()
    self.log.info('Need to remove when getAncestors call available')
    result = S_ERROR()
    #result = getAncestors(inputData,ancestorDepth)
    self.log.info('getAncestors lookup time %.2f' %(time.time()-start))

    if not result['OK']:
      self.log.error('Failed to get ancestor LFNs')
      if result.has_key('Message'):
        self.log.info('----------BK-Result------------')
        self.log.info(result['Message'])
        self.log.info('--------End-BK-Result----------')
        return result
      else:
        print result
        return S_ERROR('Failed to get ancestor LFNs')

    ancestorFiles = result['PFNs']
    result = S_OK()
    result['Value'] = ancestorFiles
    return result

  #############################################################################
  def setOptimizerJobInfo(self,job,reportName,value):
    """This method sets the job optimizer information that will subsequently
       be used for job scheduling and TURL queries on the WN.
    """
    self.log.debug("self.jobDB.setJobOptParameter(%s,'%s','%s')" %(job,reportName,value))
    if self.enable:
      result = self.jobDB.setJobOptParameter(job,reportName,str(value))
    else:
      result = S_OK('DisabledMode')

    return result

  #############################################################################
  def updateJobStatus(self,job,status,minorstatus=None):
    """This method updates the job status in the JobDB.
    """
    self.log.debug("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" %(job,status))
    if self.enable:
      result = self.jobDB.setJobAttribute(job,'Status',status, update=True)
    else:
      result = S_OK('DisabledMode')

    if result['OK']:
      if minorstatus:
        self.log.debug("self.jobDB.setJobAttribute(%s,'%s',update=True)" %(job,minorstatus))
        if self.enable:
          result = self.jobDB.setJobAttribute(job,'MinorStatus',minorstatus,update=True)
        else:
          result = S_OK('DisabledMode')

    return result

  #############################################################################
  def setJobParam(self,job,reportName,value):
    """This method updates a job parameter in the JobDB.
    """
    self.log.debug("self.jobDB.setJobParameter(%s,'%s','%s')" %(job,reportName,value))
    if self.enable:
      result = self.jobDB.setJobParameter(job,reportName,value)
    else:
      result = S_OK('DisabledMode')

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
