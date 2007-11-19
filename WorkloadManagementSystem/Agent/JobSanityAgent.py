########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobSanityAgent.py,v 1.7 2007/11/19 10:59:29 paterson Exp $
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

__RCSID__ = "$Id: JobSanityAgent.py,v 1.7 2007/11/19 10:59:29 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from DIRAC                                                 import S_OK, S_ERROR
import os, re, time, string

OPTIMIZER_NAME = 'JobSanity'

class JobSanityAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for JobSanityAgent.
    """
    result = Optimizer.initialize(self)

    #Test control flags N.B. JDL check is mandatory
    self.inputDataCheck    = gConfig.getValue(self.section+'/InputDataCheck',1)
    self.outputDataCheck   = gConfig.getValue(self.section+'/OutputDataCheck',0)
    self.inputSandboxCheck = gConfig.getValue(self.section+'/InputSandboxCheck',0)
    self.platformCheck     = gConfig.getValue(self.section+'/PlatformCheck',0)
    #Other parameters
    self.setup                = gConfig.getValue(self.section+'/VO','lhcb')
    self.successStatus        = gConfig.getValue(self.section+'/SuccessfulJobStatus','OutputReady')
    self.maxDataPerJob        = gConfig.getValue(self.section+'/MaxInputDataPerJob',200)

    self.log.debug(   'JDL Check          ==>  Enabled'                    )
    if self.inputDataCheck:
      self.log.debug( 'Input Data Check   ==>  Enabled'                    )
    else:
      self.log.debug( 'Input Data Check   ==>  Disabled'                   )
    if self.outputDataCheck:
      self.log.debug( 'Output Data Check  ==>  Enabled'                    )
    else:
      self.log.debug( 'Output Data Check  ==>  Disabled'                   )
    if self.inputSandboxCheck:
      self.log.debug( 'Input Sbox Check   ==>  Enabled'                    )
    else:
      self.log.debug( 'Input Sbox Check   ==>  Disabled'                   )
    if self.platformCheck:
      self.log.debug( 'Platform Check     ==>  Enabled'                    )
    else:
      self.log.debug( 'Platform Check     ==>  Disabled'                   )

#needed eventually for the output data check
#    host    = gConfig.getValue(self.section+'/LFC_HOST','lhcb-lfc.cern.ch')
#    infosys = gConfig.getValue(self.section+'/LCG_GFAL_INFOSYS','lcg-bdii.cern.ch:2170')
#    try:
#      from DIRAC.DataManagement.Client.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
#      self.FileCatalog = LcgFileCatalogCombinedClient()
#      self.log.debug("Instantiating LFC File Catalog in mode %s %s %s" % (mode,host,infosys) )
#    except Exception,x:
#      msg = "Failed to create LcgFileCatalogClient"
#      self.log.fatal(msg)
#      self.log.fatal(str(x))
#      result = S_ERROR(msg)

    return result

  #############################################################################
  def checkJob(self,job):
    """ This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
    """
    self.log.info('Job %s will be processed by %sAgent' % (job,self.optimizerName))
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
      return S_ERROR(message)

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
        self.log.info(message)
        self.log.info(res)
        return S_ERROR(message)

    #Platform check # disabled
    if self.platformCheck:
      platform = self.checkPlatformSupported(job,jdl)
      if platform['OK']:
        arch = platform['Value']
        message += 'Platform: '+arch+' OK, '
      else:
        res = 'No supported platform for job '+str(job)+'.'
        minorStatus = platform['Value']
        self.log.info(message)
        self.log.info(res)
        return S_ERROR(message)

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
            return S_OK('Found successful job')
          else:
            flag = outputData['Value']
            message += 'Output Data: '+flag+', '
        else:
          res = 'Job: '+str(job)+' Failed since output data exists.'
          minorStatus=outputData['Value']
          self.log.info(message)
          self.log.info(res)
          return S_ERROR(message)

    #Input Sandbox uploaded check
    if self.inputSandboxCheck: # disabled
      inputSandbox = self.checkInputSandbox(job,jdl)
      if inputSandbox['OK']:
        filesUploaded = inputSandbox['Value']
        message+= ' Input Sandbox Files: '+filesUploaded+', OK.'
      else:
        res = 'Job: '+str(job)+' Failed since input sandbox not uploaded.'
        minorStatus=inputSandbox['Value']
        self.log.info(message)
        self.log.info(res)
        return S_ERROR(message)

    self.log.info(message)
    self.setJobParam(job,'JobSanityCheck',message)
    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.error(result['Message'])

    return S_OK('Job checking finished')

  #############################################################################
  def checkJDL(self,job):
    """Checks JDL is OK for Job.
    """
    self.log.debug("Checking JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job,original=True)
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
    data = result['Value']
    repData = '\n'
    for i in data: repData+=i+'\n'
    self.log.debug('Data is: %s' %(repData))


    totalData = len(data)
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

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
