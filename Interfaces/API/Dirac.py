########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Dirac.py,v 1.4 2008/01/09 09:11:32 paterson Exp $
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

__RCSID__ = "$Id: Dirac.py,v 1.4 2008/01/09 09:11:32 paterson Exp $"

import re, os, sys, string, time, shutil, types

import DIRAC

from DIRAC.Interfaces.API.Job                            import Job
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.WorkloadManagementSystem.Client.WMSClient     import WMSClient
from DIRAC.WorkloadManagementSystem.Client.SandboxClient import SandboxClient
from DIRAC.Core.Utilities.GridCert                       import getGridProxy
from DIRAC                                               import gConfig, gLogger, S_OK, S_ERROR

COMPONENT_NAME='/Interfaces/API/Dirac'

class Dirac:

  #############################################################################

  def __init__(self):
    """Internal initialization of the DIRAC API.
    """
    self.log = gLogger

    self.site       = gConfig.getValue('/LocalSite/Site','Unknown')
    self.setup      = gConfig.getValue('/DIRAC/Setup','Unknown')
    self.section    = COMPONENT_NAME
    self.cvsVersion = 'CVS version '+__RCSID__
    self.diracInfo  = 'DIRAC version v%dr%d build %d' \
                       %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section+'/ScratchDir','/tmp')

    self.client = WMSClient()

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
        jobid = self._sendJob(tmpdir+'/job.jdl')
        shutil.rmtree(tmpdir)
        return jobid

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
    jobid = self._sendJob(jdl)
    shutil.rmtree(tmpdir)
    return jobid

  #############################################################################
  def _sendJob(self,jdl):
    """Internal function.
       Still to check proxy timeleft and VO eligibility etc.

       This is an internal wrapper for submit() in order to
       catch whether a user is authorized to submit to DIRAC or
       does not have a valid proxy. This is not intended for
       direct use.

    """
    jobid = None

    try:
      jobid = self.client.submitJob(jdl)
      #raise 'problem'
    except Exception,x:
      checkProxy = getGridProxy()
      if not checkProxy:
        self.log.warn(str(x))
        self.log.warn('No valid proxy found')
        return S_ERROR('No valid proxy found')

    return jobid

  #############################################################################
  def getJobInputSandbox(self,jobID,outputDir=None):
    """Retrieve input sandbox for existing JobID.

       This method allows the retrieval of an existing job input sandbox for
       debugging purposes.  By default the sandbox is downloaded to the current
       directory but this can be overidden via the outputDir parameter.

       >>> print dirac.getInputSandbox(12345)
       {'OK': True, 'Value': '12345'}

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
        self.log.warn(str(x))
        return S_ERROR('Expected integer or convertible integer for existing jobID')
    inputSandboxClient = SandboxClient()
    if outputDir:
      self.log.verbose('Attempting to store InputSandbox files for job %s in %s' %(jobID,outputDir))

    result =  inputSandboxClient.getSandbox(int(sys.argv[1]),outputDir)
    if not result['OK']:
      self.log.warn(result['Message'])
    return result

  #############################################################################
  def __printInfo(self):
    """Internal function to print the DIRAC API version and related information.
    """
    self.log.info('<=====%s=====>' % (self.diracInfo))
    if self.dbg:
      self.log.verbose(self.cvsVersion)
      self.log.verbose('DIRAC is running at %s in setup %s' % (self.site,self.setup))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#