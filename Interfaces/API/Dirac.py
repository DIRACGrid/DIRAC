########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Dirac.py,v 1.6 2008/02/05 18:48:22 paterson Exp $
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

__RCSID__ = "$Id: Dirac.py,v 1.6 2008/02/05 18:48:22 paterson Exp $"

import re, os, sys, string, time, shutil, types

import DIRAC

from DIRAC.Interfaces.API.Job                            import Job
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.WorkloadManagementSystem.Client.WMSClient     import WMSClient
from DIRAC.WorkloadManagementSystem.Client.SandboxClient import SandboxClient
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
    self.section    = COMPONENT_NAME
    self.cvsVersion = 'CVS version '+__RCSID__
    self.diracInfo  = 'DIRAC version v%dr%d build %d' \
                       %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section+'/LocalSite/ScratchDir','/tmp')
    self.outputSandboxClient = SandboxClient('Output')
    self.inputSandboxClient = SandboxClient('Input')
    self.client = WMSClient()
    self.monitoring = RPCClient('WorkloadManagement/JobMonitoring')

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

    if mode=='local':
      self.log.info('Executing job locally')
      job.execute()

    if type(job) == type(" "):
      if os.path.exists(job):
        self.log.verbose('Found job JDL file %s' % (job))
        subResult = self._sendJob(job)
        return jobResult
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
    jobID = self._sendJob(jdl)
    shutil.rmtree(tmpdir)
    if not jobID['OK']:
      self.log.warn(jobID['Message'])

    return jobID

  #############################################################################
  def runLocal(self,job):
    """This method is equivalent to submit(job,mode='Local').
    """
    return S_OK()

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
       {79241: {'status': 'outputready', 'site': 'LCG.CERN.ch'}}

       @param jobID: JobID
       @type jobID: int, string or list
       @return: S_OK,S_ERROR
    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID).split()
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
    siteDict = self.monitoring.getJobsSites(jobID)
    if not statusDict['OK'] or not siteDict['OK']:
      self.log.warn('Could not obtain job status information')
      return statusDict

    result = {}
    for job,vals in statusDict['Value'].items():
      result[job]=vals
    for job,vals in siteDict['Value'].items():
      result[job].update(vals)
    for job,vals in result.items():
      if result[job].has_key('JobID'):
        del result[job]['JobID']

    return S_OK(result)

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
        jobID = int(jobID).split()
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

    #deliberately don't return result as this is strictly for visual inspection only
    return S_OK('Job peek result printed at DIRAC INFO level')

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
    if self.dbg:
      self.log.verbose(self.cvsVersion)
      self.log.verbose('DIRAC is running at %s in setup %s' % (self.site,self.setup))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#