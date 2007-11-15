########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Dirac.py,v 1.1 2007/11/15 21:47:25 paterson Exp $
# File :   DIRAC.py
# Author : Stuart Paterson
########################################################################

"""
DIRAC API Class

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

__RCSID__ = "$Id: Dirac.py,v 1.1 2007/11/15 21:47:25 paterson Exp $"

import re, os, sys, string, time, shutil, types

import DIRAC

from DIRAC.Interfaces.API.Job                        import Job
from DIRAC.ConfigurationSystem.Client.Config         import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight       import ClassAd
from DIRAC.Core.Utilities.File                       import makeGuid
from DIRAC.Core.Utilities.Subprocess                 import shellCall
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.Core.Utilities.GridCert                   import getGridProxy
from DIRAC                                           import gLogger, S_OK, S_ERROR

COMPONENT_NAME='/Interfaces/API/Dirac'

class Dirac:

  #############################################################################

  def __init__(self):
    self.log = gLogger

    self.site       = gConfig.getValue('/DIRAC/Site','Unknown')
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
       @return: S_OK{}

       @param mode: Submit job locally
       @type mode: string

    """
    self.__printInfo()

    if mode=='local':
      self.log.debug('Executing job locally...')
      job.bootstrap()

    if type(job) == type(" "):
      if os.path.exists(job):
        self.log.debug('Found job JDL file %s' % (job))
        subResult = self.client.submitJob(job)
        return jobResult
      else:
        self.log.debug('Job is a JDL string')
        guid = makeGuid()
        tmpdir = self.scratchDir+'/'+guid
        os.mkdir(tmpdir)
        jdlfile = open(tmpdir+'/job.jdl','w')
        print >> jdlfile, job
        jobid = self._sendJob(jdl)
        subResult = self.client.submitJob(tmpdir+'/job.jdl')
        shutil.rmtree(tmpdir)
        return subResult

   # if self.dbg:
    #  job.bootstrap()
   #   job.dumpParameters()

    #creating a /tmp/guid/ directory for job submission files
    guid = makeGuid()
    tmpdir = self.scratchDir+'/'+guid
    self.log.debug('Created temporary directory for submission %s' % (tmpdir))
    os.mkdir(tmpdir)

    jfilename = tmpdir+'/jobDescription.xml'
    jfile=open(jfilename,'w')
    print >> jfile , job._toXML()
    jfile.close()

    jdlfilename = tmpdir+'/jobDescription.jdl'
    jdlfile=open(jdlfilename,'w')

    print >> jdlfile , job._toJDL(xmlFile = jfilename)
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
      jobid = self.client.SubmitJob(jdl)
      raise 'problem'
    except Exception,x:
      checkProxy = getGridProxy()
      if not checkProxy:
        self.log.error(str(x))
        self.log.error('ERROR: No valid proxy found')
        return S_ERROR('ERROR: No valid proxy found')

    return jobid

  #############################################################################
  def __printInfo(self):
    """Internal function to print the DIRAC API version.
    """
    self.log.info('<=====%s=====>' % (self.diracInfo))
    if self.dbg:
      self.log.debug(self.cvsVersion)
      self.log.debug('DIRAC is running at %s in setup %s' % (self.site,self.setup))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#