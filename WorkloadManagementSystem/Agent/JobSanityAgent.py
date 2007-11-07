########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobSanityAgent.py,v 1.1 2007/11/07 14:26:28 paterson Exp $
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

__RCSID__ = "$Id: JobSanityAgent.py,v 1.1 2007/11/07 14:26:28 paterson Exp $"

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

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','INFO') == 'DEBUG':
      self.dbg = True
      self.log.setLevel(logging.DEBUG)

    #Test control flags N.B. JDL check is mandatory
    self.inputDataCheck    = gConfig.getValue(self.section+'/InputDataCheck',0)
    self.outputDataCheck   = gConfig.getValue(self.section+'/OutputDataCheck',0)
    self.inputSandboxCheck = gConfig.getValue(self.section+'/InputSandboxCheck',0)
    self.platformCheck     = gConfig.getValue(self.section+'/PlatformCheck',1)
    #Other parameters
    self.pollingTime     = gConfig.getValue(self.section+'/PollingTime',60)
    self.initialJobState = gConfig.getValue(self.section,'/InitialJobStatus','received')
    self.finalJobState   = gConfig.getValue(self.section,'/FinalJobStatus','checked')
    self.successStatus   = gConfig.getValue(self.section,'/SuccessfulJobStatus','outputready')
    self.maxDataPerJob   = gConfig.getValue(self.section,'/MaxInputDataPerJob',200)
    self.jobCheckDelay   = gConfig.getValue(self.section,'/JobCheckingDelay',10)

    self.log.debug( '==========================================='          )
    self.log.debug( 'DIRAC Job Sanity Agent is started with     '          )
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
    self.log.debug( 'Initial Job Status ==> %s' % self.initialJobState     )
    self.log.debug( 'Final Job Status   ==> %s' % self.finalJobState       )
    self.log.debug( 'Max Data Per Job   ==> %s' % self.initialJobState     )
    self.log.debug( 'Successful Status  ==> %s' % self.successStatus       )
    self.log.debug( 'Job Check Delay    ==> %s' % self.jobCheckDelay       )
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
    status = self.initialJobState
    delay = self.jobCheckDelay
    result = self.jobDB.getJobWithStatus(status)['Value']
    if self.dbg:
      print jobs

    if not result['OK']:
      self.log.error('Error retrieving job information from JobDB')
      print result
      return result

    jobs = result['Value']
    for job in jobs:
      jobAttributes = self.jobDB.getAttributes(job)
      if jobAttributes['OK']:
        timeDict = jobAttributes['Value']
        subTime = timeDict['SubmissionDate']+'T'+timeDict['SubmissionTime']
        timeTuple = time.strptime(subTime,"%Y-%m-%dT%H:%M:%S")
        submissionTime = time.mktime(timeTuple)
        currentTime = time.time()
        if currentTime - submissionTime > delay:
          self.checkJob(job)
        else:
          msg = 'Job %s, submitted less than %s seconds ago' % (job,delay)
          self.log.info(msg)
      else:
        self.log.warn('Could not retrieve attributes for job '+str(job))
        print timeDict

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
    self.debug('Checking Loop Starts for job: '+str(job))
    checkJDL = self.checkJDL(job)
    if checkJDL['OK']:
      message+='JDL: OK, '
    else:
      res = 'Job: '+str(job)+' Failed JDL check.'
      minorStatus = checkJDL['Value']
      self.updateJobStatus(job,'failed',minorStatus)
      self.info(res)
      return

    jdl = checkJDL['JDL']
    classadJob = Classad.Classad(jdl)
   # jobType = classadJob.get_expression('JobType').replace('"','')

    #Input data check
    if self.inputDataCheck: # disabled
      inputData = self.checkInputData(job)
      if inputData['OK']:
        number = inputData['Value']
        message += 'InputData: '+number+', '
      else:
        res = 'Job: '+str(job)+' Failed input data check.'
        minorStatus = inputData['Value']
        self.updateJobStatus(job,'failed',minorStatus)
        self.info(message)
        self.info(res)
        return

    #Platform check
    if self.platformCheck:
      platform = self.checkPlatformSupported(job,jdl)
      if platform['OK']:
        arch = platform['Value']
        message += 'Platform: '+arch+' OK, '
      else:
        res = 'No supported platform for job '+str(job)+'.'
        minorStatus = platform['Value']
        self.updateJobStatus(job,'failed',minorStatus)
        self.info(message)
        self.info(res)
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
            self.info(message)
            self.setJobParam(job,'JobSanityCheck',message)
            self.updateJobStatus(job,success,minorStatus)
            return
          else:
            flag = outputData['Value']
            message += 'Output Data: '+flag+', '
        else:
          res = 'Job: '+str(job)+' Failed since output data exists.'
          minorStatus=outputData['Value']
          self.updateJobStatus(job,'failed', minorStatus)
          self.info(message)
          self.info(res)
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
        self.updateJobStatus(job,'failed', minorStatus)
        self.info(message)
        self.info(res)
        return

    success = self.finalJobState
    self.info(message)
    self.setJobParam(job,'JobSanityCheck',message)
    self.updateJobStatus(job,success)


  #############################################################################
  def updateJobStatus(self,job,status,minorstatus=None):
    """This method updates the job status in the JobDB.
    """
    self.log.debug("self.jobDB.setJobAttribute("+str(job)+",Status,"+status+" update=True)")
    self.jobDB.setJobAttribute(job,'Status',status, update=True)
    if minorstatus:
      self.log.debug("self.jobDB.setJobAttribute("+str(job)+","+minorstatus+",update=True)")
      self.jobDB.setJobAttribute(job,'ApplicationStatus',minorstatus,update=True)

  #############################################################################
  def setJobParam(self,job,reportName,value):
    """This method updates a job parameter in the JobDB.
    """
    self.log.debug("self.jobDB.setJobParameter("+str(job)+","+reportName+","+value+")")
    self.jobDB.setJobParameter(job,reportName,value)

  #############################################################################
  def checkJDL(self,job):
    """Checks JDL is OK for Job.
    """
    jobID = str(job)
    self.log.debug("Checking JDL for job: "+jobID)
    retVal = self.jobDB.getJobParameters(jobID,['JDL'])
    if not retVal:
      result = S_ERROR()
      result['Value'] = "Job not found in JobDB"
      return result

    jdl = retVal['Value']['JDL']
    if not jdl:
      self.log.debug("Warning: JDL not found for job "+jobID+", job will be marked problematic")
      result = S_ERROR()
      result['Value'] = "Job JDL Not Found"
      return result

    classadJob = Classad.Classad(jdl)
    if not classadJob.isOK():
      self.log.debug("Warning: illegal JDL for job"+jobID+", job will be marked problematic")
      result = S_ERROR()
      result['Value'] = "Illegal Job JDL"
      return result
    else:
      result = S_OK()
      result['JDL'] = jdl
      return result

  #############################################################################
  def checkPlatformSupported(self,job,jdl):
    """This method queries the CS for available platforms
       supported by DIRAC and will check these against what
       the job requests.
    """
    classadJob = Classad.Classad(jdl)
    architecture = classadJob.get_expression("CompatiblePlatforms").replace('"','')
    if not architecture:
      result = S_OK()
      msg = 'No architecture requirement'
      result['Value'] = msg
      return result

    if architecture == 'ANY':
      result = S_OK()
      result['Value'] = architecture
      return result
    else:
      okPlatforms = cfgSvc.get('OS_Compatibility',architecture,'NA')
      if okPlatforms != 'NA':
        result = S_OK()
        result['Value'] = architecture
        return result
      else:
        result = S_ERROR()
        result['Value'] = architecture+" not supported"
        return result

  #############################################################################
  def checkInputSandbox(self,job,jdl):
    """The number of input sandbox files, as specified in the job
       JDL are checked in the JobDB.
    """
    result =  self.jobDB.getInputSandboxFileCount(job)
    if self.dbg:
      print result
    if result['OK']:
      dbCount = result['Value'][0][0]
      self.log.debug('Found '+str(dbCount)+' files in DB for job '+str(job))

      classadJob = Classad.Classad(jdl)
      sandbox = ClassAddStringListToPythonList(classadJob.get_expression("InputSandbox"))
      if self.dbg:
        print sandbox
      jdlCount = len(sandbox)
      self.log.debug('Found '+str(jdlCount)+' files in JDL for job '+str(job))

      if jdlCount > dbCount:
        result = S_ERROR()
        message = 'Input Sandbox not uploaded '+str(dbCount)+' / '+str(jdlCount)
        result['Value'] = message
        return result
      else:
        result = S_OK()
        number = str(dbCount)+' / '+str(jdlCount)
        result['Value'] = number
        return result
    else:
      return result

  #############################################################################
  def checkInputData(self,job):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """
    #To implement
    return S_OK()

  #############################################################################
  def  checkOutputDataExists(self, job, jdl):
    """If the job output data is already in the LFC, this
       method will fail the job for the attention of the
       data manager. To be tidied for DIRAC3...
    """
    #To implement
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#