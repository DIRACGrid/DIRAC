########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobSanityAgent.py,v 1.4 2007/11/16 12:50:25 paterson Exp $
# File :   JobSanityAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Sanity Agent accepts all jobs from the Job
      receiver and screens them for the following problems:
       - Output data already exists
       - Problematic JDL
       - Jobs with too much input data e.g. > 100 files
       - Jobs with input data incorrectly specified e.g. castor:/
       - Input sandbox not correctly uploaded.
"""

__RCSID__ = "$Id: JobSanityAgent.py,v 1.4 2007/11/16 12:50:25 paterson Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

import os, re, time, string

AGENT_NAME = 'WorkloadManagement/JobSanityAgent'

class JobSanityAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets default parameters
    """
    result = Agent.initialize(self)
    self.jobDB = JobDB()
    self.optimizerName     = 'JobSanity'
    self.nextOptimizerName = 'InputData'

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','INFO') == 'DEBUG':
      self.dbg = True
      self.log.setLevel(logging.DEBUG)

    #Test control flags N.B. JDL check is mandatory
    self.inputDataCheck    = gConfig.getValue(self.section+'/InputDataCheck',1)
    self.outputDataCheck   = gConfig.getValue(self.section+'/OutputDataCheck',0)
    self.inputSandboxCheck = gConfig.getValue(self.section+'/InputSandboxCheck',0)
    self.platformCheck     = gConfig.getValue(self.section+'/PlatformCheck',0)
    #Other parameters
    self.pollingTime          = gConfig.getValue(self.section+'/PollingTime',10)
    self.jobStatus            = gConfig.getValue(self.section+'/JobStatus','Checking')
    self.minorStatus          = gConfig.getValue(self.section+'/InitialJobMinorStatus',self.optimizerName)
    self.nextOptMinorStatus   = gConfig.getValue(self.section+'/FinalJobMinorStatus',self.nextOptimizerName)
    self.successStatus        = gConfig.getValue(self.section+'/SuccessfulJobStatus','OutputReady')
    self.maxDataPerJob        = gConfig.getValue(self.section+'/MaxInputDataPerJob',200)
    self.jobCheckDelay        = gConfig.getValue(self.section+'/JobCheckingDelay',5)
    self.failedJobStatus      = gConfig.getValue(self.section+'/FailedJobStatus','failed')

    infosys = gConfig.getValue(self.section,'LCG_GFAL_INFOSYS','lcg-bdii.cern.ch:2170')
    host    = gConfig.getValue(self.section,'LFC_HOST','lhcb-lfc.cern.ch')

#needed for the output data check
#    try:
#      from DIRAC.DataManagement.Client.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
#      self.FileCatalog = LcgFileCatalogCombinedClient()
#      self.log.debug("Instantiating LFC File Catalog in mode %s %s %s" % (mode,host,infosys) )
#    except Exception,x:
#      msg = "Failed to create LcgFileCatalogClient"
#      self.log.fatal(msg)
#      self.log.fatal(str(x))
#      result = S_ERROR(msg)

    self.log.debug( '==========================================='          )
    self.log.debug( 'DIRAC '+self.optimizerName+' Agent is started with '  )
    self.log.debug( 'the following parameters and checks:'                 )
    self.log.debug( '==========================================='          )
    self.log.debug( 'JDL Check          ==>  Enabled'                      )
    if self.inputDataCheck:
      self.log.debug( 'Input Data Check   ==>  Enabled'                    )
    else:
      self.log.debug( 'Input Data Check   ==>  Disabled'                   )
    if self.outputDataCheck:
      self.log.debug( 'Output Data Check  ==>  Enabled'                    )
    else:
      self.log.debug( 'Output Data Check ==>  Disabled'                    )
    if self.inputSandboxCheck:
      self.log.debug( 'Input Sbox Check  ==>  Enabled'                     )
    else:
      self.log.debug( 'Input Sbox Check  ==>  Disabled'                    )
    if self.platformCheck:
      self.log.debug( 'Platform Check  ==>  Enabled'                       )
    else:
      self.log.debug( 'Platform Check  ==>  Disabled'                      )
    self.log.debug( '==========================================='          )
    self.log.debug( 'Polling Time       ==> %s' % self.pollingTime         )
    self.log.debug( 'Job Status         ==> %s' % self.jobStatus           )
    self.log.debug( 'Job Minor Status   ==> %s' % self.minorStatus         )
    self.log.debug( 'Max Data Per Job   ==> %s' % self.maxDataPerJob       )
    self.log.debug( 'Successful Status  ==> %s' % self.successStatus       )
    self.log.debug( 'Job Check Delay    ==> %s' % self.jobCheckDelay       )
    self.log.debug( 'Failed Job Status  ==> %s' % self.failedJobStatus     )
    self.log.debug( '==========================================='          )

    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method
    """
    self.log.debug( 'Waking up Job Sanity Agent.' )
    result = self.selectJobsAndCheck()
    return result

  #############################################################################
  def selectJobsAndCheck(self):
    """Selects jobs from JobDB. Also allows general
        conditions such as a delay before checking to
        be inserted for all tests.
    """
    delay = self.jobCheckDelay

    condition = {'Status':self.jobStatus,'MinorStatus':self.minorStatus}
    result = self.jobDB.selectJobs(condition)
    if not result['OK']:
      self.log.error('Failed to get a job list from the JobDB')
      return S_ERROR('Failed to get a job list from the JobDB')

    if not len(result['Value']):
      self.log.debug('No pending jobs to process')
      return S_OK('No work to do')

    jobs = result['Value']
    for job in jobs:
      jobAttributes = self.jobDB.getJobAttributes(job)
      if jobAttributes['OK']:
        timeDict = jobAttributes['Value']
        print timeDict
        if timeDict:
          subTime = timeDict['SubmissionTime'].replace(' ','T')
          timeTuple = time.strptime(subTime,"%Y-%m-%dT%H:%M:%S")
          submissionTime = time.mktime(timeTuple)
          currentTime = time.time()
          if currentTime - submissionTime > delay:
            self.checkJob(job)
          else:
            msg = 'Job %s, submitted less than %s seconds ago' % (job,delay)
            self.log.info(msg)
        else:
          self.log.error('No job attributes found for job %s' %(job))
      else:
        self.log.warn('JobDB returned error for job %s' %(job))
        print jobAttributes

    return S_OK('Successfully checked jobs')

  #############################################################################
  def checkJob(self,job):
    """ This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
    """

    #Job JDL check
    message = 'Job: '+str(job)+' '
    self.log.debug('Checking Loop Starts for job: '+str(job))
    checkJDL = self.checkJDL(job)
    if checkJDL['OK']:
      message+='JDL: OK, '
    else:
      res = 'Job: '+str(job)+' Failed JDL check.'
      minorStatus = checkJDL['Value']
      self.updateJobStatus(job,self.failedJobStatus,minorStatus)
      self.log.info(res)
      return

    jdl = checkJDL['JDL']
    classadJob = ClassAd('['+jdl+']')
    jobType = classadJob.get_expression('JobType').replace('"','')

    #Input data check
    if self.inputDataCheck:
      inputData = self.checkInputData(job)
      if inputData['OK']:
        number = inputData['Value']
        message += 'InputData: '+number+', '
      else:
        res = 'Job: '+str(job)+' Failed input data check.'
        minorStatus = inputData['Value']
        self.updateJobStatus(job,self.failedJobStatus,minorStatus)
        self.log.info(message)
        self.log.info(res)
        return

    #Platform check # disabled
    if self.platformCheck:
      platform = self.checkPlatformSupported(job,jdl)
      if platform['OK']:
        arch = platform['Value']
        message += 'Platform: '+arch+' OK, '
      else:
        res = 'No supported platform for job '+str(job)+'.'
        minorStatus = platform['Value']
        self.updateJobStatus(job,self.failedJobStatus,minorStatus)
        self.log.info(message)
        self.log.info(res)
        return

    #Output data exists check
    if self.outputDataCheck: # disabled
      if jobType != 'user':
        outputData = self.checkOutputDataExists(job,jdl)
        if outputData['OK']:
          if outputData.has_key('SUCCESS'):
            success = self.successStatus
            minorStatus = outputData['SUCCESS']
            report = outputData['Value']
            message += report
            self.log.info(message)
            self.setJobParam(job,'JobSanityCheck',message)
            self.updateJobStatus(job,success,minorStatus)
            return
          else:
            flag = outputData['Value']
            message += 'Output Data: '+flag+', '
        else:
          res = 'Job: '+str(job)+' Failed since output data exists.'
          minorStatus=outputData['Value']
          self.updateJobStatus(job,self.failedJobStatus, minorStatus)
          self.log.info(message)
          self.log.info(res)
          return

    #Input Sandbox uploaded check
    if self.inputSandboxCheck: # disabled
      inputSandbox = self.checkInputSandbox(job,jdl)
      if inputSandbox['OK']:
        filesUploaded = inputSandbox['Value']
        message+= ' Input Sandbox Files: '+filesUploaded+', OK.'
      else:
        res = 'Job: '+str(job)+' Failed since input sandbox not uploaded.'
        minorStatus=inputSandbox['Value']
        self.updateJobStatus(job,self.failedJobStatus, minorStatus)
        self.log.info(message)
        self.log.info(res)
        return

    self.log.info(message)
    self.setJobParam(job,'JobSanityCheck',message)
    result = self.updateJobStatus(job,self.jobStatus,self.nextOptMinorStatus)
    if not result['OK']:
      self.log.error(result['Message'])

  #############################################################################
  def checkJDL(self,job):
    """Checks JDL is OK for Job.
    """
    self.log.debug("Checking JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job)
    if not retVal['OK']:
      self.log.warn(retVal['Message'])
      retVal = self.jobDB.getJobJDL(job,original=True)
      self.log.warn('Could not get current JDL from JobDB, trying original')
      if not retVal['OK']:
        self.log.warn(retVal['Message'])

    if not retVal['OK']:
      result = S_ERROR()
      result['Value'] = "Job JDL not found in JobDB"
      return result

    jdl = retVal['Value']

    if not jdl:
      self.log.debug("Warning: JDL not found for job %s, will be marked problematic" % (job))
      result = S_ERROR()
      result['Value'] = "Job JDL Not Found"
      return result

    classadJob = ClassAd('['+jdl+']')
    if not classadJob.isOK():
      self.log.debug("Warning: illegal JDL for job %s, will be marked problematic" % (job))
      result = S_ERROR()
      result['Value'] = "Illegal Job JDL"
      return result
    else:
      param = classadJob

      result = S_OK('JDL OK')
      result['JDL'] = jdl
      return result

  #############################################################################
  def checkInputData(self,job):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """
    setup = self.setup
    maxData = int(self.maxDataPerJob)
    totalData = 0
    slashFlag = 0
    incorrectDataFlag = 0

    result = self.jobDB.getInputData(job)

    if not result['OK']:
      self.log.error('Failed to get input data from JobdB for %s' %(job) )
      self.log.error(result['Message'])

    if not result['Value']:
      self.log.debug('Job %s has no input data requirement' % (job))
      if not result['OK']:
        self.log.error(result['Message'])
        return result

    self.log.debug('Job %s has an input data requirement and will be checked' % (job))
    inputData = result['Value']
    self.log.debug('Data is: ')
    if self.dbg:
      for i in data: print i

    totalData = len(inputData)
    if totalData > maxData:
      message = '%s datasets selected. Max limit is %s.'  % (totalData,maxData)
      self.setJobParam(job,'DatasetCheck',message)
      result = S_ERROR()
      result['Value'] = "Exceeded maximum dataset limit"
      return result

    if totalData:
      for i in data:
        j = i.replace('LFN:','')
        if not re.search('^/'+setup+'/',j):
          incorrectDataFlag += 1
        if re.search('//',j):
          slashFlag +=1

    if incorrectDataFlag:
      result = S_ERROR()
      result['Value'] = "Input data not corrrectly specified"
      return result

    if slashFlag:
      result = S_ERROR()
      result['Value'] = "Input data contains //"
      return result

    number = str(totalData)
    result = S_OK()
    result['Value'] = number+' LFNs OK'
    return result

  #############################################################################
  def  checkOutputDataExists(self, job, jdl):
    """If the job output data is already in the LFC, this
       method will fail the job for the attention of the
       data manager. To be tidied for DIRAC3...
    """
    #To implement
    return S_OK()

  #############################################################################
  def checkPlatformSupported(self,job,jdl):
    """This method queries the CS for available platforms
       supported by DIRAC and will check these against what
       the job requests.
    """
    #To implement
    return S_OK()

  #############################################################################
  def checkInputSandbox(self,job,jdl):
    """The number of input sandbox files, as specified in the job
       JDL are checked in the JobDB.
    """
    #To implement
    return S_OK()

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

  #############################################################################
  def setJobParam(self,job,reportName,value):
    """This method updates a job parameter in the JobDB.
    """
    self.log.debug("self.jobDB.setJobParameter("+str(job)+","+reportName+","+value+")")
    result = self.jobDB.setJobParameter(job,reportName,value)
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
