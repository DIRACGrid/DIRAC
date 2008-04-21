########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Dirac.py,v 1.16 2008/04/21 17:20:27 paterson Exp $
# File :   DIRAC.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC API Class

All DIRAC functionality is exposed through the DIRAC API and this
serves as a source of documentation for the project via EpyDoc.

The DIRAC API provides the following functionality:
 - A transparent and secure way for users
   to submit jobs to the Grid, monitor them and
   retrieve outputs
 - Interaction with Grid storage and file catalogues
   via the DataManagement public interfaces
 ...

The initial instance just exposes job submission via the WMS client.

"""

__RCSID__ = "$Id: Dirac.py,v 1.16 2008/04/21 17:20:27 paterson Exp $"

import re, os, sys, string, time, shutil, types
import pprint
import DIRAC

from DIRAC.Interfaces.API.Job                            import Job
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
from DIRAC.WorkloadManagementSystem.Client.WMSClient     import WMSClient
from DIRAC.WorkloadManagementSystem.Client.SandboxClient import SandboxClient
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Core.Utilities.GridCert                       import getGridProxy
from DIRAC                                               import gConfig, gLogger, S_OK, S_ERROR

COMPONENT_NAME='DiracAPI'

class Dirac:

  #############################################################################
  def __init__(self):
    """Internal initialization of the DIRAC API.
    """
    #self.log = gLogger
    self.log = gLogger.getSubLogger(COMPONENT_NAME)
    self.site       = gConfig.getValue('/LocalSite/Site','Unknown')
    self.setup      = gConfig.getValue('/DIRAC/Setup','Unknown')
    self.section    = 'Interfaces/API/Dirac'
    self.cvsVersion = 'CVS version '+__RCSID__
    self.diracInfo  = 'DIRAC version v%dr%d build %d' \
                       %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)

    self.scratchDir = gConfig.getValue(self.section+'/LocalSite/ScratchDir','/tmp')
    self.outputSandboxClient = SandboxClient('Output')
    self.inputSandboxClient = SandboxClient('Input')
    self.client = WMSClient()
    self.monitoring = RPCClient('WorkloadManagement/JobMonitoring')
    self.pPrint = pprint.PrettyPrinter()
    try:
#      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
#      self.fileCatalog = LcgFileCatalogCombinedClient()
      from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
      self.fileCatalog=FileCatalog()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.verbose(msg)
      self.log.debug(str(x))
      self.fileCatalog=False
    self.rm = ReplicaManager()

  #############################################################################
  def submit(self,job,mode=None):
    """Submit jobs to DIRAC WMS.
       These can be either:

        - Instances of the Job Class
           - VO Application Jobs
           - Inline scripts
           - Scripts as executables
           - Scripts inside an application environment

        - JDL File
        - JDL String

       Example usage:

       >>> print dirac.submit(job)
       {'OK': True, 'Value': '12345'}

       @param job: Instance of Job class or JDL string
       @type job: Job() or string
       @return: S_OK,S_ERROR

       @param mode: Submit job locally
       @type mode: string

    """
    self.__printInfo()

    if type(job) == type(" "):
      if os.path.exists(job):
        self.log.verbose('Found job JDL file %s' % (job))
        subResult = self._sendJob(job)
        return subResult
      else:
        self.log.verbose('Job is a JDL string')
        guid = makeGuid()
        tmpdir = self.scratchDir+'/'+guid
        os.mkdir(tmpdir)
        jdlfile = open(tmpdir+'/job.jdl','w')
        print >> jdlfile, job
        jdlfile.close()
        jobID = self._sendJob(tmpdir+'/job.jdl')
        shutil.rmtree(tmpdir)
        return jobID

    #creating a /tmp/guid/ directory for job submission files
    guid = makeGuid()
    tmpdir = self.scratchDir+'/'+guid
    self.log.verbose('Created temporary directory for submission %s' % (tmpdir))
    os.mkdir(tmpdir)

    jfilename = tmpdir+'/jobDescription.xml'
    jfile=open(jfilename,'w')
    print >> jfile , job._toXML()
    jfile.close()

    jdlfilename = tmpdir+'/jobDescription.jdl'
    jdlfile=open(jdlfilename,'w')

    print >> jdlfile , job._toJDL(xmlFile=jfilename)
    jdlfile.close()

    jdl=jdlfilename

    if mode:
      if mode.lower()=='local':
        self.log.verbose('Executing job locally')
        result = self.runLocal(jdl,jfilename)
        self.log.verbose('Cleaning up %s...' %tmpdir)
        shutil.rmtree(tmpdir)
        return result

    jobID = self._sendJob(jdl)
    shutil.rmtree(tmpdir)
    if not jobID['OK']:
      self.log.warn(jobID['Message'])

    return jobID

  #############################################################################
  def runLocal(self,jobJDL,jobXML):
    """Under development.  This method is equivalent to submit(job,mode='Local').
       All output files are written to the local directory.
    """
    if not self.site or self.site == 'Unknown':
      return self.__errorReport('LocalSite/Site configuration section is unknown, please set this correctly')

    siteRoot = gConfig.getValue('/LocalSite/Root','')
    if not siteRoot:
      self.log.warn('LocalSite/Root configuration section is not defined, trying local directory')
      siteRoot = os.getcwd()
      if not os.path.exists('%s/DIRAC' %(siteRoot)):
        return self.__errorReport('LocalSite/Root should be set to DIRACROOT')

    self.log.info('Preparing environment for site %s to execute job' %self.site)

    os.environ['DIRACROOT'] = siteRoot
    self.log.verbose('DIRACROOT = %s' %(siteRoot))
    os.environ['DIRACPYTHON'] = sys.executable
    self.log.verbose('DIRACPYTHON = %s' %(sys.executable))
    self.log.verbose('JDL file is: %s' %jobJDL)
    self.log.verbose('Job XML file description is: %s' %jobXML)

    parameters = self.__getJDLParameters(jobJDL)
    if not parameters['OK']:
      self.log.warn('Could not extract job parameters from JDL file %s' %(jobJDL))
      return parameters

    self.log.verbose(parameters)
    inputData = None
    if parameters['Value'].has_key('InputData'):
      if parameters['Value']['InputData']:
        inputData = parameters['Value']['InputData']
        if type(inputData) == type(" "):
          inputData = [inputData]

    if inputData:
      localSEList = gConfig.getValue('/LocalSite/LocalSE','')
      if not localSEList:
        return self.__errorReport('LocalSite/LocalSE should be defined in your config file')
      if re.search(',',localSEList):
        localSEList = localSEList.replace(' ','').split(',')
      else:
        localSEList = [localSEList.replace(' ','')]
      self.log.verbose(localSEList)
      inputDataPolicy = gConfig.getValue('DIRAC/VOPolicy/InputDataModule','')
      if not inputDataPolicy:
        return self.__errorReport('Could not retrieve DIRAC/VOPolicy/InputDataModule for VO')

      self.log.info('Job has input data requirement, will attempt to resolve data for %s' %self.site)
      self.log.verbose('%s' %(string.join(inputData,'\n')))
      replicaDict = self.getReplicas(inputData)
      if not replicaDict['OK']:
        return replicaDict
      guidDict = self.getMetadata(inputData)
      if not guidDict['OK']:
        return guidDict
      for lfn,reps in replicaDict['Value']['Successful'].items():
        guidDict['Value']['Successful'][lfn].update(reps)
      resolvedData = guidDict
      diskSE = gConfig.getValue(self.section+'/DiskSE','-disk,-DST,-USER')
      if re.search(',',diskSE):
        diskSE = diskSE.split(',')
      tapeSE = gConfig.getValue(self.section+'/TapeSE','-tape,-RDST,-RAW')
      if re.search(',',tapeSE):
        tapeSE = tapeSE.split(',')
      configDict = {'JobID':None,'LocalSEList':localSEList,'DiskSEList':diskSE,'TapeSEList':tapeSE}
      self.log.verbose(configDict)
      argumentsDict = {'FileCatalog':resolvedData,'Configuration':configDict,'InputData':inputData}
      self.log.verbose(argumentsDict)
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(inputDataPolicy,argumentsDict)
      if not moduleInstance['OK']:
        self.log.warn('Could not create InputDataModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn('Input data resolution failed')
        return result

      softwarePolicy = gConfig.getValue('DIRAC/VOPolicy/SoftwareDistModule','')
      if not softwarePolicy:
        return self.__errorReport('Could not retrieve DIRAC/VOPolicy/SoftwareDistModule for VO')
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(softwarePolicy,argumentsDict)
      if not moduleInstance['OK']:
        self.log.warn('Could not create SoftwareDistModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn('Software installation failed')
        return result

    self.log.info('Attempting to submit job to local site: %s' %self.site)
    if parameters['Value'].has_key('Executable') and parameters['Value'].has_key('Arguments'):
      executable = os.path.expandvars(parameters['Value']['Executable'])
      arguments = parameters['Value']['Arguments']
      command = '%s %s' % (executable,arguments)
      self.log.verbose(command)
      result = shellCall(0,command,callbackFunction=self.__printOutput)
      if not result['OK']:
        return result

      status = result['Value'][0]
      self.log.verbose('Status after execution is %s' %(status))

      outputFileName = None
      errorFileName = None
      if parameters['Value'].has_key('StdOutput'):
        outputFileName = parameters['Value']['StdOutput']
      if parameters['Value'].has_key('StdError'):
        errorFileName = parameters['Value']['StdError']

      if outputFileName:
        stdout = result['Value'][1]
        if os.path.exists(outputFileName):
          os.remove(outputFileName)
        self.log.info('Standard output written to %s' %(outputFileName))
        outputFile =  open(outputFileName,'w')
        print >> outputFile, stdout
        outputFile.close()
      else:
        self.log.warn('Job JDL has no StdOutput file parameter defined')

      if errorFileName:
        stderr = result['Value'][2]
        if os.path.exists(errorFileName):
          os.remove(errorFileName)
        self.log.verbose('Standard error written to %s' %(errorFileName))
        errorFile = open(errorFileName,'w')
        print >> errorFile, stderr
        errorFile.close()
      else:
        self.log.warn('Job JDL has no StdError file parameter defined')
    else:
      return self.__errorReport('Missing job arguments or executable')

    return S_OK()

  #############################################################################
  def __printOutput(self,fd,message):
    """Internal callback function to return standard output when running locally.
    """
    print message

  #############################################################################
  def getReplicas(self,lfns):
    """ Under development. Obtain replica information from file catalogue client.
    """
    if not self.fileCatalog:
      return self.__errorReport('File catalog client was not successfully imported')

    bulkQuery = False
    if type(lfns)==type(" "):
      lfns = lfns.replace('LFN:','')
    elif type(lfns)==type([]):
      bulkQuery = True
      try:
        lfns = [str(lfn.replace('LFN:','')) for lfn in lfns]
      except Exception,x:
        return self.__errorReport(str(x),'Expected strings for LFNs')
    else:
      return self.__errorReport('Expected single string or list of strings for LFN(s)')

    start = time.time()
    repsResult = self.fileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing) )
    self.log.verbose(repsResult)
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])

    if not bulkQuery:
      if repsResult['OK']:
        if repsResult['Value'].has_key('Successful'):
          if repsResult['Value']['Successful'].has_key(lfns):
            print self.pPrint.pformat(repsResult['Value']['Successful'][lfns])

    return repsResult

  #############################################################################
  def getMetadata(self,lfns):
    """ Under development. Obtain replica information from file catalogue client.
    """
    if not self.fileCatalog:
      return self.__errorReport('File catalog client was not successfully imported')

    bulkQuery = False
    if type(lfns)==type(" "):
      lfns = lfns.replace('LFN:','')
    elif type(lfns)==type([]):
      bulkQuery=True
      try:
        lfns = [str(lfn.replace('LFN:','')) for lfn in lfns]
      except Exception,x:
        return self.__errorReport(str(x),'Expected strings for LFNs')
    else:
      return self.__errorReport('Expected single string or list of strings for LFN(s)')

    start = time.time()
    repsResult = self.fileCatalog.getFileMetadata(lfns)
    timing = time.time() - start
    self.log.info('Metadata Lookup Time: %.2f seconds ' % (timing) )
    self.log.verbose(repsResult)
    if not repsResult['OK']:
      self.log.warn('Failed to retrieve file metadata from the catalogue')
      self.log.warn(repsResult['Message'])

    if not bulkQuery:
      if repsResult['OK']:
        if repsResult['Value'].has_key('Successful'):
          if repsResult['Value']['Successful'].has_key(lfns):
            print self.pPrint.pformat(repsResult['Value']['Successful'][lfns])

    return repsResult

  #############################################################################
  def replicate(self,lfn,destinationSE,sourceSE):
    """Under development.
    """
    result = self.rm.replicateAndRegister(lfn,destinationSE,sourceSE)
    self.log.verbose(result)
    return result

  #############################################################################
  def _sendJob(self,jdl):
    """Internal function.
       Still to check proxy timeleft and VO eligibility etc.

       This is an internal wrapper for submit() in order to
       catch whether a user is authorized to submit to DIRAC or
       does not have a valid proxy. This is not intended for
       direct use.

    """
    jobID = None

    try:
      jobID = self.client.submitJob(jdl)
      #raise 'problem'
    except Exception,x:
      checkProxy = getGridProxy()
      if not checkProxy:
        return self.__errorReport(str(x),'No valid proxy found')

    return jobID

  #############################################################################
  def getInputSandbox(self,jobID,outputDir=None):
    """Retrieve input sandbox for existing JobID.

       This method allows the retrieval of an existing job input sandbox for
       debugging purposes.  By default the sandbox is downloaded to the current
       directory but this can be overidden via the outputDir parameter. All files
       are extracted into a InputSandbox<JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getInputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR

       @param outputDir: Optional directory for files
       @type outputDir: string
    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    dirPath = ''
    if outputDir:
      dirPath = '%s/InputSandbox%s' %(outputDir,jobID)
      if os.path.exists(dirPath):
        return self.__errorReport('Job input sandbox directory %s already exists' %(dirPath))
    else:
      dirPath = '%s/InputSandbox%s' %(os.getcwd(),jobID)
      if os.path.exists(dirPath):
        return self.__errorReport('Job input sandbox directory %s already exists' %(dirPath))

    try:
      os.mkdir(dirPath)
    except Exception,x:
      return self.__errorReport(str(x),'Could not create directory in %s' %(dirPath))

    result = self.inputSandboxClient.getSandbox(int(sys.argv[1]),dirPath)
    if not result['OK']:
      self.log.warn(result['Message'])
    else:
      self.log.info('Files retrieved and extracted in %s' %(dirPath))
    return result

  #############################################################################
  def getOutputSandbox(self,jobID,outputDir=None):
    """Retrieve output sandbox for existing JobID.

       This method allows the retrieval of an existing job output sandbox.
       By default the sandbox is downloaded to the current directory but
       this can be overidden via the outputDir parameter. All files are
       extracted into a <JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getOutputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR

       @param outputDir: Optional directory path
       @type outputDir: string
    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    dirPath = ''
    if outputDir:
      dirPath = '%s/%s' %(outputDir,jobID)
      if os.path.exists(dirPath):
        return self.__errorReport('Job output directory %s already exists' %(dirPath))
    else:
      dirPath = '%s/%s' %(os.getcwd(),jobID)
      if os.path.exists(dirPath):
        return self.__errorReport('Job output directory %s already exists' %(dirPath))

    try:
      os.mkdir(dirPath)
    except Exception,x:
      return self.__errorReport(str(x),'Could not create directory in %s' %(dirPath))

    result = self.outputSandboxClient.getSandbox(int(sys.argv[1]),dirPath)
    if not result['OK']:
      self.log.warn(result['Message'])
    else:
      self.log.info('Files retrieved and extracted in %s' %(dirPath))
    return result

  #############################################################################
  def delete(self,jobID):
    """Delete job or list of jobs from the WMS, if running these jobs will
       also be killed.

       Example Usage:

       >>> print dirac.delete(12345)
       {'OK': True, 'Value': [12345]}

       @param job: JobID
       @type job: int, string or list
       @return: S_OK,S_ERROR

    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    result = self.client.deleteJob(jobID)
    return result

  #############################################################################
  def reschedule(self,jobID):
    """Reschedule a job or list of jobs in the WMS.  This operation is the same
       as resubmitting the same job as new.  The rescheduling operation may be
       performed to a configurable maximum number of times but the owner of a job
       can also reset this counter and reschedule jobs again by hand.

       Example Usage:

       >>> print dirac.reschedule(12345)
       {'OK': True, 'Value': [12345]}

       @param job: JobID
       @type job: int, string or list
       @return: S_OK,S_ERROR

    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    result = self.client.rescheduleJob(jobID)
    return result

  #############################################################################
  def kill(self,jobID):
    """Issue a kill signal to a running job.  If a job has already completed this
       action is harmless but otherwise the process will be killed on the compute
       resource by the Watchdog.

       Example Usage:

        >>> print dirac.kill(12345)
       {'OK': True, 'Value': [12345]}

       @param job: JobID
       @type job: int, string or list
       @return: S_OK,S_ERROR

    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    result = self.client.killJob(jobID)
    return result

  #############################################################################
  def status(self,jobID):
    """Monitor the status of DIRAC Jobs.

       Example Usage:

       >>> print dirac.status(79241)
       {79241: {'status': 'Done', 'site': 'LCG.CERN.ch'}}

       @param jobID: JobID
       @type jobID: int, string or list
       @return: S_OK,S_ERROR
    """
    if type(jobID)==type(" "):
      try:
        jobID = [int(jobID)]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type(1):
      jobID = [jobID]

    statusDict = self.monitoring.getJobsStatus(jobID)
    minorStatusDict = self.monitoring.getJobsMinorStatus(jobID)
    siteDict = self.monitoring.getJobsSites(jobID)

    if not statusDict['OK']:
      self.log.warn('Could not obtain job status information')
      return statusDict
    if not siteDict['OK']:
      self.log.warn('Could not obtain job site information')
      return siteDict
    if not minorStatusDict['OK']:
      self.log.warn('Could not obtain job minor status information')
      return minorStatusDict

    result = {}
    for job,vals in statusDict['Value'].items():
      result[job]=vals
    for job,vals in siteDict['Value'].items():
      result[job].update(vals)
    for job,vals in minorStatusDict['Value'].items():
      result[job].update(vals)
    for job,vals in result.items():
      if result[job].has_key('JobID'):
        del result[job]['JobID']

    return S_OK(result)

  #############################################################################
  def selectJobs(self,Status=None,MinorStatus=None,ApplicationStatus=None,Site=None,Owner=None,JobGroup=None,Date=None):
    """Under development: all options correspond to the web-page table columns.
       Date must be specified as yyyy-mm-dd.  By default, the date is today.
       JobGroup corresponds to the name associated to a group of jobs, e.g. productionID.
       Site is the DIRAC site name.
       Owner is the immutable nickname.
    """
    options = {'Status':Status,'MinorStatus':MinorStatus,'ApplicationStatus':ApplicationStatus,
               'Site':Site,'JobGroup':JobGroup}
    conditions = {}
    for n,v in options.items():
      if v:
        try:
          conditions[n] = str(v)
        except Exception,x:
          return self.__errorReport(str(x),'Expected string for %s field' %n)

    if not type(Date)==type(" "):
      try:
        if Date:
          Date = str(Date)
      except Exception,x:
        return self.__errorReport(str(x),'Expected yyyy-mm-dd string for Date')

    if not Date:
      now = time.gmtime()
      Date = '%s-%s-%s' %(now[0],str(now[1]).zfill(2),str(now[2]).zfill(2))
      print Date

    self.log.verbose('Will select jobs with last update %s and following conditions' %Date)
    self.log.verbose(self.pPrint.pformat(conditions))
    result = self.monitoring.getJobs(conditions,Date)
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    jobIDs = result['Value']
    self.log.verbose('%s job(s) selected' %(len(jobIDs)))
    if not jobIDs:
      return S_ERROR('No jobs selected for conditions: %s' %conditions)
    else:
      return result

  #############################################################################
  def getJobSummary(self,jobID,outputFile=None,printOutput=False):
    """Under Development.  Output similar to the web page can be printed to the screen
       or stored as a file or just returned as a dictionary for further usage.

       Jobs can be specified individually or as a list.
    """
    if type(jobID)==type(" "):
      try:
        jobID = [int(jobID)]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    headers = ['Status','MinorStatus','ApplicationStatus','Site','JobGroup','LastUpdateTime',
               'HeartBeatTime','SubmissionTime','Owner']

    if type(jobID)==type(1):
      jobID = [jobID]

    result = self.monitoring.getJobsSummary(jobID)
    if not result['OK']:
      self.log.warn(result['Message'])
      return result
    try:
      jobSummary = eval(result['Value'])
      #self.log.info(self.pPrint.pformat(jobSummary))
    except Exception,x:
      self.log.warn('Problem interpreting result from job monitoring service')
      return S_ERROR('Problem while converting result from job monitoring')

    summary = {}
    for job in jobID:
      summary[job] = {}
      for key in headers:
        if not jobSummary.has_key(job):
          self.log.warn('No records for JobID %s' %job)
          value = 'None'
        elif jobSummary[job].has_key(key):
          value = jobSummary[job][key]
        else:
          value = 'None'
        summary[job][key] = value

    if outputFile:
      if os.path.exists(outputFile):
        return self.__errorReport('Output file %s already exists' %(outputFile))
      dirPath = os.path.basename(outputFile)
      if re.search('/',dirPath) and not os.path.exists(dirPath):
        try:
          os.mkdir(dirPath)
        except Exception,x:
          return self.__errorReport(str(x),'Could not create directory %s' %(dirPath))

      fopen = open(outputFile,'w')
      line = 'JobID'.ljust(12)
      for i in headers:
        line += i.ljust(35)
      fopen.write(line+'\n')
      for jobID,params in summary.items():
        line = str(jobID).ljust(12)
        for header in headers:
          for key,value in params.items():
            if header==key:
              line += value.ljust(35)
        fopen.write(line+'\n')
      fopen.close()
      self.log.verbose('Output written to %s' %outputFile)

    if printOutput:
      self.log.info(self.pPrint.pformat(summary))

    return S_OK(summary)

  #############################################################################
  def getJobCPUTime(self,jobID,printOutput=False):
    """Under development.  Retrieve job CPU consumed heartbeat data from job monitoring
       service.  Jobs can be specified individually or as a list.

       The time stamps and raw CPU consumed (s) are returned.
    """
    if type(jobID)==type(" "):
      try:
        jobID = [int(jobID)]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    if type(jobID)==type(1):
      jobID = [jobID]

    summary = {}
    for job in jobID:
      result = self.monitoring.getJobHeartBeatData(job)
      summary[job]={}
      if not result['OK']:
        return self.__errorReport(result['Message'],'Could not get heartbeat data for job %s' %job)
      if result['Value']:
        tupleList = result['Value']
        for tup in tupleList:
          if tup[0]=='CPUConsumed':
            summary[job][tup[2]]=tup[1]
      else:
        self.log.warn('No heartbeat data for job %s' %job)

    if printOutput:
      self.log.info(self.pPrint.pformat(summary))

    return S_OK(summary)

  #############################################################################
  def parameters(self,jobID):
    """Return DIRAC parameters associated with the given job.

       DIRAC keeps track of several job parameters which are kept in the job monitoring
       service, see example below. Selected parameters also printed to screen.

       Example Usage:

       >>> print dirac.parameters(79241)

       @param jobID: JobID
       @type jobID: int, string or list
       @return: S_OK,S_ERROR
    """
    if type(jobID)==type(" "):
      try:
        jobID = [int(jobID)]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    result = self.monitoring.getJobParameters(jobID)
    if not result['OK']:
      return result

    if result['Value'].has_key('StandardOutput'):
      del result['Value']['StandardOutput']

    self.log.info(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def loggingInfo(self,jobID):
    """DIRAC keeps track of job transitions which are kept in the job monitoring
       service, see example below.  Logging summary also printed to screen at the
       INFO level.

       Example Usage:

       >>> print dirac.loggingInfo(79241)
       {'OK': True, 'Value': [('Received', 'JobPath', 'Unknown', '2008-01-29 15:37:09', 'JobPathAgent'),
       ('Checking', 'JobSanity', 'Unknown', '2008-01-29 15:37:14', 'JobSanityAgent')]}

       @param jobID: JobID
       @type jobID: int or string
       @return: S_OK,S_ERROR
     """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      return self.__errorReport('Expected int or string, not list')

    result = self.monitoring.getJobLoggingInfo(jobID)
    if not result['OK']:
      self.log.warn('Could not retrieve logging information for job %s' %jobID)
      self.log.warn(result)
      return result

    loggingTupleList = result['Value']
    #source is removed for printing to control width
    headers = ('Status','MinorStatus','ApplicationStatus','DateTime')
    line = ''
    for i in headers:
      line += i.ljust(25)
    self.log.info(line)

    for i in loggingTupleList:
      line = ''
      for j in xrange(len(i)-1):
        line += i[j].ljust(25)
      self.log.info(line)

    return result

  #############################################################################
  def peek(self,jobID):
    """The peek function will attempt to return standard output from the WMS for
       a given job if this is available.  The standard output is periodically
       updated from the compute resource via the application Watchdog. Available
       standard output is  printed to screen at the INFO level.

       Example Usage:

       >>> print dirac.peek(1484)
       {'OK': True, 'Value': 'Job peek result printed at DIRAC INFO level'}

       @param jobID: JobID
       @type jobID: int or string
       @return: S_OK,S_ERROR
    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')
    elif type(jobID)==type([]):
      return self.__errorReport('Expected int or string, not list')

    result = self.monitoring.getJobParameter(jobID,'StandardOutput')
    if not result['OK']:
      return self.__errorReport(result,'Could not retrieve job attributes')

    if result['Value'].has_key('StandardOutput'):
      self.log.info(result['Value']['StandardOutput'])
    else:
      self.log.info('No standard output to print')

    #deliberately don't return result as this is strictly for visual inspection only
    return S_OK('Job peek result printed at DIRAC INFO level')

  #############################################################################
  def __getJDLParameters(self,jdl):
    """Internal function. Returns a dictionary of JDL parameters.
    """
    if os.path.exists(jdl):
      jdlFile = open(jdl,'r')
      jdl = jdlFile.read()
      jdlFile.close()

    try:
      parameters = {}
      if not re.search('\[',jdl):
        jdl = '['+jdl+']'
      classAdJob = ClassAd(jdl)
      paramsDict = classAdJob.contents
      for param,value in paramsDict.items():
        if re.search('{',value):
          self.log.debug('Found list type parameter %s' %(param))
          rawValues = value.replace('{','').replace('}','').replace('"','').replace('LFN:','').split()
          valueList = []
          for val in rawValues:
            if re.search(',$',val):
              valueList.append(val[:-1])
            else:
              valueList.append(val)
          parameters[param] = valueList
        else:
          self.log.debug('Found standard parameter %s' %(param))
          parameters[param]= value.replace('"','')
      return S_OK(parameters)
    except Exception, x:
      self.log.exception(x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #############################################################################
  def __printInfo(self):
    """Internal function to print the DIRAC API version and related information.
    """
    self.log.info('<=====%s=====>' % (self.diracInfo))
    self.log.verbose(self.cvsVersion)
    self.log.verbose('DIRAC is running at %s in setup %s' % (self.site,self.setup))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#